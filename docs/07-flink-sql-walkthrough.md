# 07. The Flink SQL jobs, line by line

Three jobs, each a file in [`jobs/`](../jobs), each prepended with [`jobs/00_session.sql`](../jobs/00_session.sql) by [`jobs/submit.sh`](../jobs/submit.sh). Separate jobs
are separate failure and upgrade domains: the alerting job can be redeployed without touching the lakehouse feed.

## The big idea: dynamic tables and changelogs

In Flink SQL a stream *is* a table that keeps changing, and a query over it produces another changing table. What
flows between operators is a **changelog**:

| Row kind | Meaning |
|---|---|
| `+I` insert | A new row |
| `-U` update-before | Retract the old version of a row |
| `+U` update-after | The new version |
| `-D` delete | Remove a row |

- A plain `SELECT ... WHERE` over Kafka is **append-only** (only `+I`).
- `GROUP BY` without windows, "latest row per key", and regular joins produce **updates**: the result for a key
  changes as new data arrives.
- Sinks must accept what the query produces. Kafka with a plain format and Iceberg append tables want append-only;
  JDBC with a primary key accepts upserts; `upsert-kafka` writes a compacted changelog.

When Flink refuses a query with "doesn't support consuming update changes", this table is why.

## Session setup (`00_session.sql`)

- `table.local-time-zone = UTC`: `TIMESTAMP_LTZ` values print and convert in UTC.
- `table.exec.source.idle-timeout = 10 s`: idle partitions stop holding back watermarks (lesson 06).
- `table.exec.state.ttl = 36 h`: bounded state.
- **Mini-batch is off**, on purpose. Mini-batching (buffer ~500 ms of input before updating state) makes
  aggregations much faster, but it turns the "keep first row" deduplication below into an *update* stream, and
  windows, OVER aggregates and `MATCH_RECOGNIZE` refuse update streams. We found this the hard way; see
  "Debugging with EXPLAIN" at the end of this lesson.
  per record. Far less state access, at the cost of half a second.
- `CREATE CATALOG lake`: the Iceberg REST catalog. Swap the options for Glue or S3 Tables and the SQL doesn't change.
- `ec2_state_changes`: the Kafka source. Notable lines:
  - `'value.format' = 'avro-confluent'` with the registry URL: decodes using the writer schema id in each message.
  - `event_time TIMESTAMP(3)` plus `event_ts AS CAST(event_time AS TIMESTAMP_LTZ(3))`: Flink's Avro format maps
    `timestamp-millis` to a zone-less `TIMESTAMP`. The computed column turns it into an instant (`TIMESTAMP_LTZ`),
    exact because the session time zone is UTC. The watermark is declared on `event_ts`, and the dedup view exposes it
    as `event_time` so the jobs read naturally. Get this wrong and every time shifts by your zone offset.
  - `kafka_partition`, `kafka_offset` as `METADATA ... VIRTUAL`: Kafka coordinates as columns, kept in the raw table.
  - `proc_time AS PROCTIME()`: a processing-time attribute, used for dedup.
  - `WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND`: makes `event_ts` the event-time attribute.
- `cmdb_ci`: the Debezium changelog as a table. `PRIMARY KEY` + `WATERMARK` make it a **versioned table**: Flink
  keeps each instance's row versions keyed by `updated_at`.
- `ec2_state_changes_dedup`: `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY proc_time ASC) ... WHERE rn = 1`.
  Flink recognizes this exact shape as **deduplication, keep first row**. Ordered by processing time ascending, the
  first copy wins and later copies are dropped, so the output stays append-only. (Ordering by event time, or keeping
  the last row, would produce updates instead.)

## Job 1: `10_lakehouse.sql`

```sql
INSERT INTO lake.ec2_stream.state_changes_raw SELECT ... FROM ec2_state_changes;
```

Everything Kafka delivered, duplicates and all, with partition and offset. This is your audit trail and your
replay source (lesson 11). Think of it as bronze.

```sql
INSERT INTO lake.ec2_stream.state_changes
SELECT ... FROM ec2_state_changes_dedup AS e
LEFT JOIN cmdb_ci FOR SYSTEM_TIME AS OF e.event_time AS c ON e.instance_id = c.instance_id;
```

An **event-time temporal join**: for an event at 10:00:03 it uses the CMDB version whose `updated_at` was the latest
at or before 10:00:03. If the owner changes at 10:05, the 10:00 event keeps the old owner, which is what an audit
needs. The join waits until the CMDB side's watermark passes the event time, so it knows no older version can still
arrive. The CMDB topic is quiet most of the time, so without the idle timeout its watermark would never move and the
join would emit nothing.

Both inserts are in one `EXECUTE STATEMENT SET`, so they become one job that reads Kafka once. Iceberg commits every
checkpoint.

## Job 2: `20_serving.sql`

Builds the tables an API or agent reads.

```sql
LAG(state) OVER (PARTITION BY instance_id ORDER BY event_time) AS previous_state          -- view with_previous
COUNT(*)   OVER (PARTITION BY instance_id ORDER BY event_time
                 RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW) AS transitions_1h  -- view with_hourly_count
```

**OVER aggregations** compute a value for every row from that instance's own history, in event-time order. Flink
buffers rows until the watermark guarantees their order is final, then emits. Late rows are dropped.

Two Flink SQL rules shaped this: every OVER aggregate in one `SELECT` must use the same window, and `LAG` accepts no
frame. So the previous state and the one-hour count are computed in two views and put back together with an
**interval join**:

```sql
FROM with_previous AS p JOIN with_hourly_count AS c
  ON p.event_id = c.event_id AND p.event_time BETWEEN c.event_time AND c.event_time
```

The time bound lets Flink drop join state as soon as the watermark passes, instead of holding it for the TTL.

```sql
ROW_NUMBER() OVER (PARTITION BY instance_id ORDER BY event_time DESC) ... WHERE rn = 1
```

**Keep last row** by event time: the latest state per instance. Its output is an update stream: a new event for
i-0abc retracts the previous row and emits the new one.

```sql
INSERT INTO pg_instance_current_state SELECT ... FROM latest AS l LEFT JOIN cmdb_ci AS c ON l.instance_id = c.instance_id;
```

A **regular join** (no `FOR SYSTEM_TIME`): both sides are kept in state and any change on either side updates the
result. Change an owner in the CMDB and the serving row updates even if the instance does nothing. That's the right
semantics for "who owns it *now*", and the opposite of job 1's "who owned it *then*". The JDBC sink has a primary
key, so it upserts (`INSERT ... ON CONFLICT DO UPDATE`); replays after a failure rewrite the same values, so the
result is exactly-once even though delivery is at-least-once.

```sql
FROM TABLE(TUMBLE(TABLE ec2_state_changes_dedup, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, region, state
```

A **window table-valued function**. Each (minute, region, state) is emitted once, when the watermark passes the end
of the minute, so roughly 30 s after the minute ends. `HOP` (sliding) and `CUMULATE` windows use the same syntax;
`SESSION` windows group bursts separated by gaps.

## Job 3: `30_alerts.sql`

```sql
MATCH_RECOGNIZE (
  PARTITION BY instance_id ORDER BY event_time
  MEASURES S1.event_time AS first_stop, S3.event_time AS last_stop, ...
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (S1 R1 S2 R2 S3) WITHIN INTERVAL '10' MINUTE
  DEFINE S1 AS S1.state = 'stopped', R1 AS R1.state = 'running', ...
)
```

**Complex event processing** in SQL: a regular expression over each instance's rows in event-time order. Input is
filtered first to `running` and `stopped` so the transitional states don't break the sequence. `WITHIN` bounds how
long a partial match is kept in state. `SKIP PAST LAST ROW` starts looking for the next match after this one, so an
instance flapping for an hour yields one alert per three cycles, not one per row.

The `alert_id` is an MD5 of instance, type and first-stop time: **deterministic**, so if anything upstream replays,
the same alert gets the same id and consumers can de-duplicate.

The second half of the `UNION ALL` joins terminations with the CMDB as of the event time and keeps tier1 prod ones.
Both alert types go through **one** `INSERT ... UNION ALL`, because two INSERTs would create two exactly-once Kafka
sinks with the same transactional-id prefix, which Flink rejects. Every timestamp written is wrapped in `CAST(... AS
TIMESTAMP_LTZ(3))`: that turns a rowtime attribute into a plain value, and a sink may receive at most one rowtime column.

The sink is Kafka with `sink.delivery-guarantee = exactly-once`: each checkpoint's alerts are written in a Kafka
transaction, committed when the checkpoint completes. `transaction.timeout.ms` must exceed the checkpoint interval
plus recovery time, or the broker aborts transactions that Flink still intends to commit.

## Explore interactively

Open a SQL client against the running cluster with the session setup (catalogs, sources, views) already loaded:

```bash
docker compose run --rm --no-deps --entrypoint bash submit-jobs /opt/flink/jobs/sql-client.sh
```

```sql
SET 'sql-client.execution.result-mode' = 'tableau';
SELECT state, COUNT(*) FROM ec2_state_changes_dedup GROUP BY state;
```

That last query never finishes: it's a continuously updating result. Press `Ctrl+C` to stop it. `EXPLAIN` before any
query shows the plan, including the changelog mode of each operator (`changelogMode=[I,UB,UA]`).

## Debugging with EXPLAIN (a real bug from building this)

The first version enabled mini-batch, and two jobs failed with:

```
StreamPhysicalOverAggregate doesn't support consuming update and delete changes which is produced by node Rank(...)
```

`EXPLAIN CHANGELOG_MODE` on the dedup query, with mini-batch on and then off, showed why:

```
mini-batch on : Deduplicate(keep=[FirstRow], key=[event_id], order=[PROCTIME], outputInsertOnly=[false])  changelogMode=[I,UA,D]
mini-batch off: Deduplicate(keep=[FirstRow], key=[event_id], order=[PROCTIME], outputInsertOnly=[true])   changelogMode=[I]
```

Whenever Flink rejects a query over "update changes", run `EXPLAIN CHANGELOG_MODE` and find the first operator
whose mode is more than `[I]`. That operator, or a setting that affects it, is your answer.

Next: [08. The streaming lakehouse](08-streaming-lakehouse.md)

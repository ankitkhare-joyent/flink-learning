# Lab 04 · CDC with Debezium: the database becomes a stream

One sitting, about 30 minutes. Background: [docs/05](../docs/05-cdc-with-debezium.md). Code:
[stack/connect/cmdb-source.json](../stack/connect/cmdb-source.json) (the connector),
[stack/postgres/init.sql](../stack/postgres/init.sql) (publication, replica identity, the `debezium` role),
[services/ingest/simulator.py](../services/ingest/simulator.py) (`cmdb_reassign_owner`, `launch_replacement`).

The CMDB is a Postgres table. Debezium reads its write-ahead log and publishes every change to
`cmdb.public.ec2_ci`. After this lab you can explain:

- what a replication slot and a publication are, and why the slot is a disk risk
- what a Debezium change event contains, and why `REPLICA IDENTITY FULL` matters
- how an owner change reaches the serving table without anyone polling
- what happens to changes made while Debezium is down

## Before you start

```bash
source lab/env.sh
curl -s $CONNECT/connectors/cmdb-source/status | jq -c '[.connector.state, .tasks[0].state]'
```

## 1. The plumbing in Postgres

```bash
psql_su cmdb "select slot_name, plugin, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as lag from pg_replication_slots"
psql_su cmdb "select pubname, (select string_agg(tablename, ',') from pg_publication_tables t where t.pubname = p.pubname) as tables from pg_publication p"
psql_su cmdb "select relreplident from pg_class where relname = 'ec2_ci'"
offsets cmdb.public.ec2_ci | tail -1
psql_su cmdb "select count(*) from ec2_ci"
```

**Predict (P1):** the table has about 2,100 rows. Does the topic have the same number of records, more, or fewer?

## 2. Change an owner and follow it

Pick a prod instance that has a row in the serving table, change its owner in the CMDB, then look at the change event
and the serving row. (You're writing as the Postgres superuser for convenience; never do that to a real source system.)

```bash
I=$(docker compose exec -T postgres psql -U postgres -d serving -At -c "select instance_id from instance_current_state where environment='prod' order by state_since desc limit 1"); echo $I
psql_su serving "select instance_id, owner_email, updated_at from instance_current_state where instance_id='$I'"
psql_su cmdb "update ec2_ci set owner_email='you@example.com', updated_at=now() where instance_id='$I' returning owner_email, updated_at"
sleep 8
kafka kafka-console-consumer.sh --topic cmdb.public.ec2_ci --from-beginning --max-messages 100000 --timeout-ms 8000 --property print.key=true \
  | grep you@example.com | tail -1 | cut -f2 | jq -c '{op, before: .before.owner_email, after: .after.owner_email, source: {lsn: .source.lsn, snapshot: .source.snapshot}}'
psql_su serving "select instance_id, owner_email, updated_at from instance_current_state where instance_id='$I'"
```

**Predict (P2):** what is `op`, and does the event carry the old owner?

**Predict (P3):** does the serving row change, even though the instance had no new state change? (Look at which kind
of join builds it: [jobs/20_serving.sql](../jobs/20_serving.sql).)

## Break it, then fix it

**Break: Debezium is down while the CMDB keeps changing.**

**Predict (P4):** you update 50 rows while Connect is stopped. Where do those changes wait? What happens to the slot?

```bash
docker compose stop connect
psql_su cmdb "update ec2_ci set owner_email='outage@example.com', updated_at=now() where instance_id in (select instance_id from ec2_ci order by instance_id limit 50)"
sleep 60
psql_su cmdb "select slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as retained_wal from pg_replication_slots"
before=$(offsets cmdb.public.ec2_ci | tail -1)
```

**Fix:** start it. Debezium resumes from the slot's position.

```bash
docker compose start connect
until curl -s $CONNECT/connectors/cmdb-source/status | jq -e '.tasks[0].state == "RUNNING"' >/dev/null 2>&1; do sleep 3; done; sleep 15
echo "before: $before  after: $(offsets cmdb.public.ec2_ci | tail -1)"
psql_su serving "select count(*) as serving_rows_with_outage_owner from instance_current_state where owner_email = 'outage@example.com'"
psql_su cmdb "select count(*) as cmdb_rows from ec2_ci where owner_email = 'outage@example.com'"
psql_su cmdb "select active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as retained_wal from pg_replication_slots"
```

## Check your predictions

**P1.** More: 2,166 records for 2,109 rows. The topic has one record per *change* (the initial snapshot of every row,
then every insert and owner change since). Compaction would eventually keep only the latest per key, but it runs
only on closed segments, so a young topic still has every version.

**P2.** `{"op":"u","before":"asha@example.com","after":"you@example.com","source":{"lsn":54423760,"snapshot":"false"}}`.
`u` is an update; snapshot reads are `r`, inserts `c`, deletes `d`. The event has the **old** owner because the
table is `REPLICA IDENTITY FULL` (`relreplident = f`). With the default identity, `before` would be null, and Flink's
`debezium-json` format would fail the job, because it can't retract the previous version of the row. (Don't try that
one: the bad record would stay in the topic and crash-loop every job that reads it.)

**P3.** Yes. The serving row went from `asha@example.com` to `you@example.com` a few seconds later, with no new EC2
event. `20_serving.sql` uses a regular join, so a change on either side updates the result. The lakehouse job uses a
temporal join instead and won't touch past events: an event from before your update keeps `asha` forever (Lab 06).

**P4.** In Postgres's WAL, held by the slot. With Connect stopped, the slot showed `active = f` and `retained_wal =
502 kB`. Postgres can't recycle WAL that the slot hasn't confirmed, so a long outage on a busy database fills the disk.
That's why `max_slot_wal_keep_size` and slot-lag alerts exist. After the restart, the topic grew by 70 records (your 50
plus the simulator's own changes), nothing was lost, and 47 serving rows showed `outage@example.com`. Not 50, because a
serving row exists only for instances that have had an EC2 event since the stream started, and the simulator had
already reassigned one of the 50 (the CMDB showed 49). Right after the restart the retained WAL even looked *larger*
(978 kB): Connect confirms its position to Postgres only when it flushes offsets (every 60 s by default), so the slot
catches up a minute later.

## What to remember

- CDC reads the log, so it sees deletes and every intermediate change, and never polls.
- A replication slot is a promise to keep WAL. An abandoned slot is a full disk waiting to happen.
- `REPLICA IDENTITY FULL` gives you `before`, which a changelog consumer needs.
- A regular join follows the latest CMDB value; a temporal join remembers the value at event time.

Next: [Lab 05 · The Flink runtime](05-flink-runtime.md).

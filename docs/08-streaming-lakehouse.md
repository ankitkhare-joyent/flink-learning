# 08. The streaming lakehouse

You know Iceberg from the batch side. Streaming into it works, with a few new concerns.

## How Flink writes Iceberg

1. Each parallel writer appends rows to open Parquet data files.
2. At a checkpoint barrier, writers close their files and report them to a single **committer**.
3. When the checkpoint completes, the committer makes **one Iceberg commit** (a new snapshot) with all those files.

So data becomes visible every checkpoint (30 s here), atomically: a reader sees all of a checkpoint's data or none.
After a crash, files from the incomplete checkpoint are never committed and replay writes them again, so the table is
exactly-once.

The same mechanism is the trade-off: **commit interval = freshness = file count**. A 30 s checkpoint with parallelism 2
creates up to 2 files per partition per table every 30 s, which is about 5,800 small files per table per day. The
metadata grows with them.

## Keeping tables healthy

What Spark batch jobs rarely worry about, streaming tables need on a schedule (an Airflow DAG is a good home):

| Maintenance | Why | How |
|---|---|---|
| **Compaction** (`rewrite_data_files`) | Merge small files into ~128-512 MB files | Spark `CALL lake.system.rewrite_data_files('ec2_stream.state_changes')`, or Flink's table maintenance, or a managed service (S3 Tables, Glue optimizer) |
| **Expire snapshots** | Every commit is a snapshot; old ones pin old files | `CALL ... expire_snapshots(older_than => ...)` |
| **Remove orphan files** | Files from failed commits | `CALL ... remove_orphan_files()` |
| **Rewrite manifests** | Many tiny manifests slow planning | `CALL ... rewrite_manifests()` |

[`scripts/init_streaming.py`](../scripts/init_streaming.py) also sets `write.metadata.delete-after-commit.enabled` and
`write.metadata.previous-versions-max = 50`, so old `metadata.json` files are cleaned up automatically.

Run compaction on **older** partitions (yesterday and before) so it doesn't conflict with the streaming writer's
commits to today's partition. Iceberg's optimistic concurrency retries conflicting commits, but avoiding them is
cheaper.

## Hidden partitioning, created outside Flink

Both tables are partitioned by `day(event_time)`. Flink SQL's `CREATE TABLE` can only declare identity partitions,
not transforms, so the tables are created with PyIceberg in [`scripts/init_streaming.py`](../scripts/init_streaming.py), and Flink only writes to
them. That split is common anyway: table DDL belongs to migrations, not to the job.

Queries filter on `event_time` and Iceberg prunes days automatically, like your batch tables.

## Raw and clean tables

| Table | Contents | Use |
|---|---|---|
| `ec2_stream.state_changes_raw` | Every Kafka record, duplicates included, with partition and offset | Audit, debugging ("was this event ever received?"), backfills |
| `ec2_stream.state_changes` | De-duplicated, CMDB as of event time | Analytics, history tool for the agent |

Try the dedup rate (raw rows per clean row) with DuckDB or Trino; it should sit around the simulator's 2% duplicate
rate. Late events (more than 30 s behind the watermark) are *not* dropped here: neither job-1 path has a window, so
the lakehouse is complete even when the real-time views skipped something.

## Upserts into Iceberg

Our tables are append-only, which is the easy case. Iceberg v2 also accepts upserts from Flink
(`'write.upsert.enabled' = 'true'` with a primary key) using equality deletes. They make reads slower until compaction
rewrites them, so for a current-state view a serving database (Postgres here) is usually the better target.
Apache Paimon is designed for exactly that streaming-upsert workload, and Apache Fluss adds a sub-second hot tier
in front of the lake; both are worth watching (lesson 01).

## Query it

From your laptop with DuckDB (the S3 keys are the local SeaweedFS ones):

```sql
INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
CREATE SECRET (TYPE s3, KEY_ID 's3admin', SECRET 's3admin-local-secret', ENDPOINT 'localhost:19000', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1');
ATTACH 'lakehouse' AS lake (TYPE iceberg, ENDPOINT 'http://localhost:18181/catalog', AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none');
SELECT state, count(*) FROM lake.ec2_stream.state_changes WHERE event_time > now() - INTERVAL 1 HOUR GROUP BY state;
SELECT * FROM iceberg_snapshots(lake.ec2_stream.state_changes) ORDER BY timestamp_ms DESC LIMIT 5;
```

(Lakekeeper stores `http://seaweedfs:8333` as the S3 endpoint for containers; from the laptop the `CREATE SECRET`
endpoint `localhost:19000` is what DuckDB uses.)

The second query shows one snapshot per checkpoint, about every 30 seconds.

Next: [09. Secrets with OpenBao](09-secrets-with-openbao.md)

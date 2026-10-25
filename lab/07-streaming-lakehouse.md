# Lab 07 · The streaming lakehouse: commits, files, replays and upgrades

One sitting, about 40 minutes. Background: [docs/08](../docs/08-streaming-lakehouse.md), [docs/11](../docs/11-operating-in-production.md)
(upgrades, reprocessing). Code: [jobs/10_lakehouse.sql](../jobs/10_lakehouse.sql),
[scripts/init_streaming.py](../scripts/init_streaming.py) (`iceberg_tables`), [jobs/submit.sh](../jobs/submit.sh) (`RESTORE_FROM`),
[lab/duck.py](duck.py).

You know Iceberg from batch. This lab is about what streaming does to it, and about the one operation every streaming
engineer must get right: changing a running job without losing or duplicating data. After it you can explain:

- why Iceberg gets one snapshot per checkpoint, and what that does to file counts
- why the clean table is always a little behind the raw one
- why replaying a job is harmless for one sink and dangerous for another
- how a savepoint carries a job across a redeploy

## Before you start

```bash
source lab/env.sh
mise run -q lab:duck
```

## 1. Commits follow checkpoints

```bash
mise run -q lab:duck -- "select count(*) as snapshots from iceberg_snapshots(lake.ec2_stream.state_changes)" \
  "select count(*) as data_files, round(avg(record_count)) as avg_rows_per_file from iceberg_metadata(lake.ec2_stream.state_changes) where manifest_content = 'DATA' and status != 'DELETED'"
curl -s $FLINK/jobs/$(fjid ec2-lakehouse)/checkpoints | jq .counts.completed
```

**Predict (P1):** how do the snapshot count and the lakehouse job's completed checkpoints compare? How many data files
per snapshot, and roughly how many rows in each?

**Predict (P2):** the first `lab:duck` output shows `raw_rows` and `clean_rows`. The difference is bigger than the 2%
duplicates. Why?

## 2. Replay into an upsert sink

Cancel the serving job and submit it again **without** state. It starts from the earliest Kafka offset and rebuilds
the serving table from 7 days of history. (Lab 05 restarted from a checkpoint; this is from nothing.)

**Predict (P3):** after the replay catches up, how many settled serving rows have a different state than before?

```bash
psql_su serving "create table lab_snapshot as select instance_id, state, state_since, owner_email from instance_current_state"
T0=$(date -u +"%Y-%m-%d %H:%M:%S")
curl -s -X PATCH "$FLINK/jobs/$(fjid ec2-serving)?mode=cancel" && sleep 5
mise run -q submit && sleep 45
psql_su serving "select count(*) as settled,
  count(*) filter (where (c.state, c.state_since) is distinct from (s.state, s.state_since)) as state_differs,
  count(*) filter (where c.owner_email is distinct from s.owner_email) as owner_differs
  from instance_current_state c join lab_snapshot s using (instance_id) where c.state_since < timestamp '$T0' - interval '60 seconds'"
psql_su serving "drop table lab_snapshot"
```

("Settled" means the row's last transition was at least a minute before the cancel, so the watermark had already
applied it.)

## Break it, then fix it

**The break you should not do:** cancel `ec2-lakehouse` and submit it again from scratch, as you just did with
`ec2-serving`.

**Predict (P4):** what would happen to the Iceberg tables? (Don't run it; reason from
[jobs/00_session.sql](../jobs/00_session.sql)'s `scan.startup.mode` and what kind of sink `10_lakehouse.sql` writes.)

**The fix, which is the production upgrade path:** stop the job *with a savepoint*, redeploy, and restore from it.

```bash
J=$(fjid ec2-lakehouse)
R=$(curl -s -X POST $FLINK/jobs/$J/stop -H 'Content-Type: application/json' -d '{"targetDirectory":"s3://flink/savepoints","drain":false}' | jq -r '."request-id"')
until S=$(curl -s $FLINK/jobs/$J/savepoints/$R) && [ "$(jq -r .status.id <<<"$S")" = COMPLETED ]; do sleep 2; done
SP=$(jq -r .operation.location <<<"$S"); echo $SP
sleep 20                                    # events keep arriving while the job is down
RESTORE_FROM=$SP mise run -q submit
docker compose logs --no-log-prefix submit-jobs | grep -E "restoring|submitting|skipped"
curl -s $FLINK/jobs/$(fjid ec2-lakehouse)/checkpoints | jq -c '{restored: .counts.restored, restored_from: .latest.restored.external_path}'
```

**Predict (P5):** the job was down for about 20 s. After a minute, does the raw table have any Kafka offset twice, or any
offset missing?

```bash
mise run -q lab:duck -- "select count(*) - count(distinct (kafka_partition, kafka_offset)) as replayed_offsets,
  (select sum(max_o - min_o + 1 - n) from (select kafka_partition, min(kafka_offset) min_o, max(kafka_offset) max_o,
   count(distinct kafka_offset) n from lake.ec2_stream.state_changes_raw group by 1)) as missing_offsets
  from lake.ec2_stream.state_changes_raw"
```

## Check your predictions

**P1.** Equal: 13 snapshots for 13 completed checkpoints when I ran it. The committer makes exactly one Iceberg commit
per checkpoint. Each commit added 2 files (26 data files, one per writer subtask at parallelism 2) of about 128 rows
each. That's the small-files problem in numbers: at this rate a day is about 5,800 files of a few KB per table.
Compaction (`rewrite_data_files`) and snapshot expiry exist to fix exactly this ([docs/08](../docs/08-streaming-lakehouse.md)).

**P2.** `raw_rows = 3679`, `clean_rows = 3322`. Raw rows are written as they arrive. Clean rows wait in the temporal
join until the watermark passes them (30 s plus a bit), then wait for the next checkpoint commit. At 10 events a second
that's 300-400 rows still in flight. The gap is lag, not loss: it stays roughly constant as both grow.

**P3.** **Zero.** 1,133 settled rows, `state_differs = 0`, `owner_differs = 1` (the simulator had changed 8 CMDB rows
in the meantime, and the replay correctly picked up the newest owner). The JDBC sink upserts by primary key, so
writing the same history again produces the same table. An idempotent sink makes replay a safe recovery tool.

**P4.** Every Kafka record would be appended a second time. With no state to restore, the source starts from
`earliest-offset`, and the Iceberg tables are append-only. The raw table would then show `replayed_offsets` equal to
its old row count, and the clean table would have each event twice, because the new job's dedup state starts empty.
(This lab doesn't run it, because it would double the tables. If it happens, the fix is an Iceberg rollback to the
snapshot before the replay, then restarting the job from a savepoint.) Same action, opposite outcome: the difference is
the sink.

**P5.** `replayed_offsets = 0`, `missing_offsets = 0`. The savepoint was `s3://flink/savepoints/savepoint-295eb3-...`,
the new job reported `restored: 1` from it, and it resumed at the exact Kafka offsets the savepoint recorded. The
20 s of events that arrived during the gap were read on restart, not skipped and not duplicated. On Kubernetes, the
Flink operator does these steps for you with `upgradeMode: savepoint`.

## What to remember

- One checkpoint = one Iceberg commit = a few small files. Schedule compaction.
- Downstream of an event-time join, data lags by the watermark plus one checkpoint.
- Replay is safe into idempotent sinks and dangerous into append sinks. Know which you have.
- Upgrades go through savepoints: stop with savepoint, deploy, restore.

Next: [Lab 08 · Serving tables and alerts](08-serving-and-alerts.md).

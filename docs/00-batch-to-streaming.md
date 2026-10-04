# 00. From batch to streaming

You already think in batch: a Spark job wakes up every 2 hours, reads everything that landed since last time,
computes, writes a new Iceberg snapshot, and exits. Streaming changes one assumption: **the input never ends**.
Almost everything else in this course follows from that.

## The one-sentence difference

- **Batch:** bounded input. The job knows when it has seen everything, so "count instances per region" has one
  right answer, computed once.
- **Streaming:** unbounded input. The job never sees "everything", so it must decide *when* an answer is good
  enough to emit, *what to remember* between events, and *what to do* when an old event shows up late.

Batch is a special case of streaming (a stream that happens to end). That is why Flink runs the same SQL in batch
mode and streaming mode, and why Spark has Structured Streaming.

## Your pipeline, both ways

| Question | Batch today (every 2 h) | Streaming (this project) |
|---|---|---|
| Is i-0abc running? | Answer up to 2 h old | Answer seconds old (`instance_current_state`) |
| Did it flap 3 times in 10 min? | Invisible: snapshots miss everything between them | Detected as it happens (`ec2.alerts`) |
| Who owned it when it stopped? | Owner at snapshot time, maybe wrong | Owner **as of the event time** (temporal join) |
| A collector fails for 4 hours | Gap, then a big catch-up run | Events wait in Kafka; the job catches up from its offset |
| Reprocess last week with new logic | Re-run Spark on the Iceberg partitions | Replay Kafka (if retained) or backfill from Iceberg |

Snapshots answer "what is the state now?". Events answer "what happened, and when?". You can always rebuild state
from events, but you can never rebuild events from snapshots. That asymmetry is why event streams are worth it.

## Concept map: what you know, what it is called here

| Batch / Spark / Iceberg | Streaming equivalent | Lesson |
|---|---|---|
| S3 landing folder, files arrive | Kafka topic, records arrive | 02 |
| A file's schema, Glue table schema | Schema Registry subject, compatibility rules | 03 |
| Airflow schedule triggers the job | The job runs forever; data triggers computation | 06 |
| Spark job checkpointing nothing; rerun the whole DAG on failure | Flink **checkpoints** state + offsets every 30 s; restart from the last one | 06 |
| `GROUP BY` over the whole partition | `GROUP BY` over **windows** (tumbling, hopping, session) | 07 |
| `JOIN` two tables at rest | Stream-table joins: regular, **temporal** (as-of), lookup, interval | 07 |
| `ROW_NUMBER() ... = 1` to dedup | Same SQL, but Flink keeps **state** per key forever (or until TTL) | 07 |
| Partition by `snapshot_date` | Partition by `day(event_time)`, plus watermarks for completeness | 06, 08 |
| Late-arriving file: rerun the partition | **Late event**: dropped, side-output, or corrected downstream | 06 |
| `MERGE INTO` for upserts | **Changelog streams**: insert / update-before / update-after / delete | 07 |
| Idempotent overwrite of a partition | **Exactly-once**: checkpoints + transactional or idempotent sinks | 06 |
| Spark UI stages, skew | Flink UI: backpressure, busy time, watermark lag, checkpoint size | 11 |

## The four ideas that make streaming hard (and this course)

1. **Time has two meanings.** *Event time* is when the instance actually stopped (from AWS). *Processing time* is
   when your code sees it. They drift apart: networks retry, SQS redelivers, a collector reconnects after an hour.
   Correct answers use event time. Lesson 06.
2. **Completeness is a guess.** A 10:00-10:01 window can't wait forever for stragglers. A **watermark** is the job's
   estimate that "everything up to time T has arrived"; it decides when windows close. Lesson 06.
3. **State is the product.** "Latest state per instance", "events seen per id", "CMDB version per instance" are all
   state the job keeps, possibly for days, and must survive crashes. Lesson 06.
4. **Delivery has guarantees.** At-most-once (may lose), at-least-once (may duplicate), exactly-once (effectively
   once, end to end). Every hop in this pipeline is labelled with which one it gives and why. Lessons 04 and 06.

## What "real time" means here

| Path | Typical latency in this stack | Set by |
|---|---|---|
| AWS event to Kafka | < 1 s | SQS long polling + producer `linger.ms=20` |
| Kafka to Postgres serving table | 1 to 2 s | JDBC sink flush interval (1 s) |
| Kafka to an alert | 30 to 60 s | Pattern completes, then the exactly-once sink commits on the next checkpoint (30 s) |
| Kafka to Iceberg | 30 to 60 s | Iceberg commits once per checkpoint |
| Alert to triage | 20 s to a few min | The LLM, and the hourly budget |

Every number is a dial you trade against something else (cost, file count, duplicates). That trade is the daily
work of a streaming engineer.

Next: [01. The 2026 landscape](01-the-2026-landscape.md)

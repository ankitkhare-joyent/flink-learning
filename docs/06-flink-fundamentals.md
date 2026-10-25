# 06. Flink fundamentals

## The runtime, compared to Spark

| Spark | Flink | Here |
|---|---|---|
| Driver | **JobManager**: schedules, coordinates checkpoints, serves the UI/REST | `jobmanager` (http://localhost:18081) |
| Executor | **TaskManager**: runs the operators, holds state | `taskmanager`, 8 **task slots** |
| Stage / task | **Operator** / **subtask** (one per parallel instance) | `parallelism.default: 2` |
| Shuffle between stages | **Network exchange** (hash by key, broadcast, forward) | Shown as edges in the job graph |
| Job ends | Job runs forever | Until cancelled or upgraded |

A Flink job is a graph of operators (source, filter, keyBy, aggregate, sink) running *continuously*, with records
flowing through in-memory network buffers. There is no "stage boundary" where data lands on disk.

**Session vs application mode.** Locally we run a *session cluster*: one long-lived JobManager and TaskManager that
our three jobs share. In production on Kubernetes you usually run *application mode*: one small cluster per job,
created by the Flink Kubernetes Operator from a `FlinkDeployment` resource, so jobs fail and upgrade independently.

## State

Most interesting streaming logic needs memory:

- de-duplication remembers every `event_id` seen (for 36 hours, via TTL),
- "latest state per instance" remembers one row per instance,
- the temporal join remembers every CMDB version per instance,
- `MATCH_RECOGNIZE` remembers partial pattern matches per instance.

This is **keyed state**: partitioned by key exactly like the data, so each parallel subtask owns its keys' state
locally. No remote lookups, which is why Flink is fast.

The **state backend** decides where it lives. We use **RocksDB** (`state.backend.type: rocksdb`): an embedded
key-value store on local disk, so state can be far larger than memory. The alternative keeps state on the JVM heap
(faster, limited by memory). Flink 2.x also adds *disaggregated state* (ForSt), with state primarily in object
storage, aimed at very large state on Kubernetes.

**State TTL** (`table.exec.state.ttl = 36 h` in [`jobs/00_session.sql`](../jobs/00_session.sql)) expires keys nobody touched for 36 hours.
Without it, the dedup state grows forever, because a new `event_id` arrives every few milliseconds. Choosing TTLs is
a correctness decision: a duplicate arriving after 36 hours won't be caught.

## Checkpoints: how Flink survives crashes

Every 30 seconds (`execution.checkpointing.interval: 30s`) the JobManager injects a **checkpoint barrier** into
every source. Barriers flow through the graph with the data. When an operator has received the barrier on all
inputs, it snapshots its state, asynchronously, to `s3://flink/checkpoints` (SeaweedFS here). When every operator has
done so, the checkpoint is complete. It contains:

- the Kafka offsets each source had reached at the barrier, and
- the state of every operator *exactly as of those offsets*.

On failure, Flink restarts all operators from the last completed checkpoint and rewinds the sources to its
offsets. State and input position are consistent, so nothing is lost or counted twice **inside Flink**. This is
the Chandy-Lamport snapshot algorithm, and it is the core idea of Flink.

`state.backend.incremental: true` uploads only the RocksDB files that changed since the last checkpoint, which keeps
checkpoints small when state is large.

## Exactly-once end to end

Checkpoints make Flink's *internal* state exactly-once. Output is a different matter: if a sink wrote records
after the last checkpoint and the job crashes, replay writes them again. Two ways out:

| Sink pattern | How it achieves exactly-once | Used for |
|---|---|---|
| **Transactional** (two-phase commit) | Write in a transaction; commit only when the checkpoint completes | Kafka `ec2.alerts` (`sink.delivery-guarantee = exactly-once`), Iceberg (commits per checkpoint) |
| **Idempotent upsert** | Replay writes the same key with the same value again; the final result is identical | Postgres via JDBC with a primary key (`INSERT ... ON CONFLICT`) |

The price of transactional sinks is latency: output becomes visible only at checkpoint commit, so up to 30 s
later. Shorter checkpoints mean lower latency but more overhead (and more small Iceberg files).

## Savepoints: upgrades without losing state

A **savepoint** is a checkpoint you trigger on purpose, in a portable format, kept until you delete it. Upgrade a
job by: stop with savepoint, deploy new code, start from the savepoint. Its state (dedup ids, latest states, partial
pattern matches) carries over. Operators are matched by ID; for SQL jobs, big query changes may make state
incompatible, which is a real constraint to plan for (lesson 11).

## Event time and watermarks

Each event carries `event_time` (when AWS says it happened). Arrival order differs from event order: the simulator
delays 3% of events by 2-15 s and 0.3% by 3-6 minutes, and SQS reorders on its own.

A **watermark** is a timestamp flowing with the data that says "no more events older than this should arrive".
Ours is `WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND` (`event_ts` is `event_time` as an instant): the watermark trails the largest event time
seen by 30 s. An event older than the current watermark when it arrives is **late**.

What watermarks drive:

- **Windows close** when the watermark passes their end. A 10:00-10:01 window emits once the watermark reaches
  10:01, which is when an event from about 10:01:30 has arrived.
- **Event-time joins** emit when both sides' watermarks pass the row's time.
- **Pattern matching and OVER windows** process rows in event-time order, buffering until the watermark says the
  order is final.

The trade-off: a bigger bound (30 s) tolerates more disorder but delays every result by that much. Late events are
dropped by windows and patterns, but still reach the lakehouse (job 1 has no window), so batch can correct later.

Parallel sources each have a watermark; an operator uses the **minimum** of its inputs. One idle partition (no
events) would freeze that minimum forever and stall every window. `table.exec.source.idle-timeout = 10 s` marks an
input idle after 10 s of silence so it stops holding the others back. The CMDB topic is quiet for long stretches,
which is exactly why this setting matters for the temporal join (lesson 07).

## Backpressure

If a sink slows down (Postgres is busy), its input buffers fill, then the operator before it blocks, and so on back
to the source, which simply reads Kafka more slowly. Nothing is dropped and nothing runs out of memory; lag grows
in Kafka instead. The Flink UI colours each operator by busy/backpressured time. Backpressure is how you find the
bottleneck.

## Read the config

`FLINK_PROPERTIES` in [`compose.yaml`](../compose.yaml), key by key:

| Key | Value | Why |
|---|---|---|
| `taskmanager.numberOfTaskSlots` | 8 | 3 jobs x parallelism 2, plus 2 spare for ad-hoc queries |
| `state.backend.type` / `incremental` | rocksdb / true | Large state, small checkpoints |
| `execution.checkpointing.interval` | 30s | Latency of transactional sinks vs overhead |
| `execution.checkpointing.mode` | EXACTLY_ONCE | Aligned barriers |
| `execution.checkpointing.min-pause` | 10s | Leave time for processing between checkpoints |
| `externalized-checkpoint-retention` | RETAIN_ON_CANCELLATION | Keep the last checkpoint if a job is cancelled, to restart from it |
| `restart-strategy.type` | exponential-delay | Back off when something downstream is down, instead of crash-looping |
| `metrics.reporter.prom.*` | port 9249 | Prometheus scraping (lesson 11) |

## Try it

Open http://localhost:18081, click `ec2-alerts`, then:

- **Overview**: the operator graph. Find the `MATCH_RECOGNIZE` operator and the two sinks.
- **Checkpoints**: completed count, size, duration. Size grows as state grows.
- **Watermarks** tab on an operator: how far event time has progressed.
- **BackPressure** tab: should be OK (green) most of the time.

Next: [07. The Flink SQL jobs, line by line](07-flink-sql-walkthrough.md)

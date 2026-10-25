# Lab 05 · The Flink runtime: job graphs, slots, checkpoints and a crash

One sitting, about 35 minutes. Background: [docs/06](../docs/06-flink-fundamentals.md). Code:
[compose.yaml](../compose.yaml) (`FLINK_PROPERTIES`), [jobs/submit.sh](../jobs/submit.sh), [jobs/30_alerts.sql](../jobs/30_alerts.sql).

Three SQL jobs run on one JobManager and one TaskManager. After this lab you can explain:

- how a SQL file turns into a graph of operators, and what one subtle duplication in it costs
- where a slot goes, and why the TaskManager has none free
- what a checkpoint contains and how big it is
- what happens to state, Kafka offsets and alerts when a TaskManager dies

The Flink UI at http://localhost:18081 shows all of this. Open a job and keep it next to the terminal.

## Before you start

```bash
source lab/env.sh
fjobs
```

## 1. Slots

```bash
curl -s $FLINK/taskmanagers | jq -c '.taskmanagers[] | {slots: .slotsNumber, free: .freeSlots}'
```

**Predict (P1):** 3 jobs, `parallelism.default: 2`, 8 slots. How many slots are free? What happens if you submit two
more jobs at parallelism 2?

## 2. The job graph

```bash
J=$(fjid ec2-alerts)
curl -s $FLINK/jobs/$J | jq -r '.vertices[] | "\(.parallelism)  \(.name[0:110])"'
```

**Predict (P2):** `30_alerts.sql` reads `ec2_state_changes_dedup` twice (once for the flapping pattern, once for
terminations). How many `Deduplicate` operators do you expect in the graph?

## 3. Checkpoints

```bash
curl -s $FLINK/jobs/$J/checkpoints | jq -c '.latest.completed | {id, duration_ms: .end_to_end_duration, state_kb: (.state_size/1024|floor), checkpointed_kb: (.checkpointed_size/1024|floor), path: .external_path}'
curl -s $FLINK/jobs/$(fjid ec2-lakehouse)/checkpoints/config | jq -c '{mode, interval, timeout, min_pause, externalization, state_backend}'
```

**Predict (P3):** why is `checkpointed_kb` smaller than `state_kb`?

## 4. Transactions in the alert topic

```bash
tot() { kafka kafka-console-consumer.sh --topic ec2.alerts --from-beginning --isolation-level "$1" --max-messages 100000 --timeout-ms 6000 \
  | jq -r .alert_id | sort | awk -v m="$1" '{n++; if (!s[$0]++) u++} END {print m ": records=" n " distinct_alert_ids=" u}'; }
tot read_committed; tot read_uncommitted; offsets ec2.alerts | tail -1
```

**Predict (P4):** three numbers: records a `read_committed` consumer sees, records a `read_uncommitted` one sees,
and the sum of the end offsets. Are they the same?

## Break it, then fix it

**Break: kill the TaskManager**, the process that holds all the state.

**Predict (P5):** what do the jobs do, how much work is lost, and does any alert get published twice?

```bash
psql_su serving "select count(*), max(updated_at) from instance_current_state"
docker compose kill taskmanager && sleep 20
fjobs | cut -f1-3
```

**Fix:** start a TaskManager again (Kubernetes would do this for you).

```bash
docker compose up -d taskmanager
until [ "$(curl -s $FLINK/jobs/overview | jq '[.jobs[] | select(.state == "RUNNING")] | length')" = 3 ] && \
      [ "$(curl -s $FLINK/taskmanagers | jq '.taskmanagers[0].freeSlots')" = 2 ]; do sleep 3; done
sleep 60 && fjobs | cut -f1-3
psql_su serving "select count(*), max(updated_at) from instance_current_state"
tot read_committed; tot read_uncommitted; offsets ec2.alerts | tail -1
```

## Check your predictions

**P1.** `{"slots":8,"free":2}`: each job takes 2 slots, and 2 are kept spare for the ad-hoc queries in Lab 06. One
more job fits; the one after that waits for slots and then fails with `NoResourceAvailableException: Could not acquire
the minimum required resources`. This repo first ran with exactly 6 slots, and the first ad-hoc batch query in Lab 06
simply hung, waiting for a slot that never came. Sizing slots is a session cluster's capacity plan; application mode
on Kubernetes gives each job its own TaskManagers instead.

**P2.** Two. The graph had `Deduplicate[4]` and `Deduplicate[11]`, each followed by its own branch (`Match[7]` for the
pattern, `TemporalJoin[18]` for terminations), then one `ec2_alerts[21]: Writer -> Committer`. A view is a named query,
not a stored result: every reference to it is planned again, and here the two branches project different columns, so
the planner didn't merge them. Each `Deduplicate` keeps its own copy of 36 hours of event ids. Fine at this size,
and worth knowing when state grows: put both consumers behind one deduplicated stream if it matters.

**P3.** `state_kb` (2,426) is the total state; `checkpointed_kb` (834) is what this checkpoint uploaded. With
incremental RocksDB checkpoints, only SST files that changed since the last checkpoint are sent, so a checkpoint
stays small even when state grows. It took 1.5 s end to end, stored at `s3://flink/checkpoints/<job id>/chk-35`, with
`exactly_once` mode, a 30 s interval, a 10 s minimum pause and `delete_on_cancellation: false` (retained after cancel).

**P4.** No: 92 committed, 99 uncommitted, end offsets 170. `read_uncommitted` also sees the records in the transaction
that is still open (they'll become visible at the next checkpoint) and any aborted ones. The end offsets are higher
still because every Kafka transaction writes a **commit marker** that takes an offset but is never delivered. So with
transactions, offsets stop counting records. Never compute "records = end - start" on a transactional topic.

**P5.** For about 20 seconds after the kill, `fjobs` still said RUNNING: the JobManager notices a lost TaskManager by
missed heartbeats, not instantly. Then the jobs restarted (exponential-delay restart strategy) and waited for slots.
`docker compose up -d taskmanager` brought them back within seconds, and `restored` went up on every job (0 to 4 on
`ec2-serving` and `ec2-lakehouse`, 4 to 6 on `ec2-alerts`: each restart attempt restores from the latest checkpoint).

Lost work: at most the 30 s since the last checkpoint, and it wasn't lost, only redone. Each job rewound its Kafka
offsets to the checkpoint and reprocessed. The serving table kept updating (1,984 to 2,011 rows, `max(updated_at)`
current). Alerts: 108 committed records with 108 distinct ids. **No duplicates.** The open transaction at the time of
the kill was aborted, and the replay produced the same deterministic `alert_id`s in a new transaction. That's
exactly-once end to end: replayable source, checkpointed state, transactional sink.

## What to remember

- A slot is a unit of TaskManager capacity. Parallelism x jobs must fit, or jobs wait.
- Every reference to a view is its own sub-plan with its own state. Read the job graph.
- Checkpoints are incremental and consistent: offsets and state from the same moment.
- A crash costs one checkpoint interval of reprocessing, never correctness, if the sinks are transactional or idempotent.

Next: [Lab 06 · Flink SQL: time, joins and patterns](06-flink-sql-semantics.md).

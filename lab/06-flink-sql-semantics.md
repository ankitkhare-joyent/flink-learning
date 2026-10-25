# Lab 06 · Flink SQL: changelogs, watermarks, joins and late data

One sitting, about 45 minutes. Background: [docs/06](../docs/06-flink-fundamentals.md) (event time, watermarks),
[docs/07](../docs/07-flink-sql-walkthrough.md) (the jobs line by line). Code: [jobs/00_session.sql](../jobs/00_session.sql),
[jobs/10_lakehouse.sql](../jobs/10_lakehouse.sql), [jobs/20_serving.sql](../jobs/20_serving.sql),
[lab/sql/](sql) (the scripts this lab runs).

This is the lab where streaming stops being batch with extra steps. After it you can explain:

- what kind of stream a query produces (append or updates), and why a sink or operator cares
- how far behind real time a watermark runs, and why
- the difference between "the owner when it happened" and "the owner now", in two joins
- exactly where a late event ends up, and where it doesn't

`mise run lab:sql -- FILE.sql` runs a script from `lab/sql/` with the session (sources, views, catalog) preloaded. Each
run starts a SQL client, so give it 30-60 seconds. `mise run sql` opens the same client interactively.

## Before you start

```bash
source lab/env.sh
fjobs | cut -f1-3
```

## 1. What does each query emit?

Read [lab/sql/changelog.sql](sql/changelog.sql): four queries, each under `EXPLAIN CHANGELOG_MODE`.

**Predict (P1):** for each of (a) a filter, (b) the dedup view, (c) latest state per instance, (d) a running count per
state: is the output insert-only (`[I]`), or does it update rows it already emitted?

```bash
mise run -q lab:sql -- changelog.sql | grep -E "^[A-Za-z]+\(.*changelogMode"
```

## 2. Watermarks

Each operator reports the watermark it has received. Compare them with the wall clock:

```bash
J=$(fjid ec2-serving); now=$(date +%s)
for v in $(curl -s $FLINK/jobs/$J | jq -r '.vertices[].id'); do
  n=$(curl -s $FLINK/jobs/$J | jq -r --arg v $v '.vertices[] | select(.id == $v) | .name[0:45]')
  curl -s $FLINK/jobs/$J/vertices/$v/watermarks | jq -r --arg n "$n" --argjson now $now \
    'map(select(.id | test("currentInputWatermark")) | .value | tonumber) | "\($n)\t" + (map(if . < 0 then "none" else "\(($now*1000 - .)/1000 | floor)s behind" end) | join(", "))'
done
```

**Predict (P2):** the watermark is declared as `event_ts - INTERVAL '30' SECOND`. How far behind the clock is it? What do
the two *source* operators report, and why does the CMDB topic not stall the join?

## 3. How many duplicates?

[lab/sql/dedup_count.sql](sql/dedup_count.sql) reads the topic as a bounded batch (up to the latest offset), so it finishes:

```bash
mise run -q lab:sql -- dedup_count.sql | grep -E "^\|"
```

**Predict (P3):** the simulator re-sends 2% of events. How many duplicates do you expect, and how many events arrived
more than 30 s after they happened?

## 4. Two joins, two answers

Pick a busy instance, change its owner, then send an event for it yourself.

```bash
I=$(docker compose exec -T postgres psql -U postgres -d serving -At -c "select instance_id from instance_current_state order by transitions_1h desc limit 1"); echo $I
psql_su serving "select state, owner_email, transitions_1h from instance_current_state where instance_id='$I'"
psql_su cmdb "update ec2_ci set owner_email='temporal@example.com', updated_at=now() where instance_id='$I'"
mise run -q lab:sqs -- send-event $I stopping
```

**Predict (P4):** after about a minute, which owner does the lakehouse record for the instance's *older* events, and for
the one you just sent? Which owner does the serving table show? How long after the event's time does the serving row
change?

```bash
psql_su serving "select state, state_since, owner_email, updated_at from instance_current_state where instance_id='$I'"
mise run -q lab:duck -- "select event_time, state, owner_email from lake.ec2_stream.state_changes where instance_id='$I' order by event_time desc limit 4"
```

## Break it, then fix it

**Break 1: send an event that is 10 minutes late.**

```bash
mise run -q lab:sqs -- send-event $I stopped 600
```

**Predict (P5):** wait a minute. Is the late `stopped` event in (a) `state_changes_raw`, (b) `state_changes`, (c) the
serving table? If it's in (b), which owner does it have?

```bash
mise run -q lab:duck -- "select event_time, state from lake.ec2_stream.state_changes_raw where event_id like 'lab-%'" \
                       "select event_time, state, owner_email from lake.ec2_stream.state_changes where event_id like 'lab-%'"
psql_su serving "select state, state_since from instance_current_state where instance_id='$I'"
```

**Fix:** nothing to fix in the stream; that's the design. The raw table has every event, so a batch job (Spark, or Flink
SQL in batch mode) can recompute anything late data should have changed. Pick the watermark bound for the lateness
you expect, and correct the rest in batch.

**Break 2: switch on mini-batch,** a common "make it faster" setting.
[lab/sql/minibatch.sql](sql/minibatch.sql) enables it, then plans the dedup view and a small pattern on top of it.

**Predict (P6):** what happens to the dedup view's changelog, and can the pattern still be planned?

```bash
mise run -q lab:sql -- minibatch.sql | grep -E "Deduplicate\(|changelogMode=\[I.*Calc|ERROR|doesn't support"
```

**Fix:** keep it off, as [jobs/00_session.sql](../jobs/00_session.sql) does. If you need mini-batch for an
aggregation, put that aggregation in its own job.

## Check your predictions

**P1.** (a) `[I]`: a filter never changes a row it already emitted. (b) `[I]`: keep-first deduplication on processing
time drops later copies, so it's append-only. (c) `[I,UA,D]`: each new event for an instance replaces that instance's
previous row (Flink 2.x sends updates without the "before" image here, plus deletes). (d) `[I,UA]`: every event
updates its state's count. Append-only streams can feed windows, OVER aggregates, pattern matching and Iceberg append
tables. Update streams need upsert sinks (JDBC with a primary key, upsert-kafka) or operators that accept retractions.

**P2.** About 38-41 s behind: the 30 s bound, plus how often the metrics refresh. The two sources show nothing in this
view: they *generate* watermarks rather than receive them. The CMDB source is idle most of the time, and
`table.exec.source.idle-timeout = 10 s` removes it from the minimum, so the joins follow the EC2 stream's watermark
instead of freezing at the CMDB's last update.

**P3.** 24 duplicates in 1,429 records (1.7%) when I ran it, a few minutes after a reset, and `arrived_over_30s_late = 0`.
The simulator's late events are held back 3-6 minutes, so on a young stack none has arrived yet. Run it again later to
see them: about 0.3%.

**P4.** Older events keep `hiro@example.com`; the event you sent gets `temporal@example.com`. That's the lakehouse's
event-time temporal join: the CMDB version valid *at event time*. The serving row shows `temporal@example.com` and
`state = stopping`: a regular join on the latest CMDB value. Event time `19:22:40.412`, serving `updated_at
19:23:13.581`: **33 s later**. The OVER aggregate that computes `previous_state` and `transitions_1h` waits for the
watermark to pass the event. The 30 s bound costs 30 s of latency on everything ordered by event time.

**P5.** (a) Yes. The raw insert has no time logic. (b) Yes, but with **`owner_email = NULL`**:

```
│ 2026-10-25 19:12:44.084+00 │ stopped  │ NULL                 │
│ 2026-10-25 19:22:40.412+00 │ stopping │ temporal@example.com │
```

The temporal join keeps old CMDB versions only until the watermark passes them. Once the owner changed and the
watermark moved on, the version valid at 19:12 (hiro) was cleaned out of state, so the late event found no version and
the `LEFT JOIN` filled NULLs. (c) No: the serving row stayed `stopping` from 19:22:40. The OVER aggregate drops rows
behind the watermark, and "latest by event time" wouldn't pick a 19:12 event over a 19:22 one anyway. A late event
is kept where you asked for completeness (raw), degraded where you asked for time-correct enrichment (clean), and
ignored where you asked for "now" (serving). Each of those is a choice you can see in the SQL.

**P6.** `Deduplicate(keep=[FirstRow], key=[event_id], order=[PROCTIME], outputInsertOnly=[false])` with
`changelogMode=[I,UA,D]`, then `Match Recognize doesn't support consuming update and delete changes which is produced
by node Rank(...)`. Mini-batch makes the dedup emit updates, and pattern matching (also OVER aggregates and windows)
can't consume them. This exact failure happened while this repo was being built; [docs/07](../docs/07-flink-sql-walkthrough.md)
shows how `EXPLAIN CHANGELOG_MODE` found it.

## What to remember

- Every query is either append-only or an update stream. `EXPLAIN CHANGELOG_MODE` tells you which.
- Watermark bound = disorder you tolerate = latency you pay on anything ordered by event time.
- Temporal join: the value then. Regular join: the value now. Choose per question.
- Late data is a policy: keep raw everything, then decide per output what lateness means.

Next: [Lab 07 · The streaming lakehouse](07-streaming-lakehouse.md).

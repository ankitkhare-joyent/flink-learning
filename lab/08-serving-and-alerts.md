# Lab 08 · Serving tables and alerts: what apps and agents actually read

One sitting, about 35 minutes. Background: [docs/07](../docs/07-flink-sql-walkthrough.md) (jobs 2 and 3),
[docs/00](../docs/00-batch-to-streaming.md) (the latency table). Code: [jobs/20_serving.sql](../jobs/20_serving.sql),
[jobs/30_alerts.sql](../jobs/30_alerts.sql), [stack/postgres/init.sql](../stack/postgres/init.sql) (serving tables).

Flink's outputs are what people use: a Postgres table with the current state of every instance, per-minute counts,
and an alert topic. After this lab you can explain:

- how fresh each output is, and what sets that number
- how to check an alert against the raw facts
- why event order on the wire doesn't matter to the pattern, and when timing does
- what an alert rule's precision means for whoever consumes it

## Before you start

```bash
source lab/env.sh
fjobs | cut -f1-3
```

## 1. Freshness

```bash
psql_su serving "select round(extract(epoch from (now() at time zone 'utc') - max(updated_at))::numeric, 1) as serving_age_s,
  (select max(window_end) from state_change_counts_1m) as latest_window_end, now() at time zone 'utc' as now from instance_current_state"
psql_su serving "select state, count(*) from instance_current_state group by 1 order by 2 desc"
```

**Predict (P1):** how old is the newest serving row, and how far behind the clock is the newest one-minute window?

## 2. Check an alert against the facts

```bash
kafka kafka-console-consumer.sh --topic ec2.alerts --from-beginning --isolation-level read_committed --max-messages 2000 --timeout-ms 6000 | jq -r .alert_type | sort | uniq -c
A=$(kafka kafka-console-consumer.sh --topic ec2.alerts --from-beginning --isolation-level read_committed --max-messages 2000 --timeout-ms 6000 --property print.timestamp=true | grep FLAPPING | tail -1)
echo "$A" | cut -f2 | jq -c .; echo "$A" | cut -f1
I=$(echo "$A" | cut -f2 | jq -r .instance_id); WS=$(echo "$A" | cut -f2 | jq -r .window_start); WE=$(echo "$A" | cut -f2 | jq -r .window_end)
mise run -q lab:duck -- "select event_time, state from lake.ec2_stream.state_changes where instance_id='$I' and state in ('running','stopped') and event_time between timestamptz '$WS' and timestamptz '$WE' order by event_time"
```

**Predict (P2):** what will the lakehouse show between `window_start` and `window_end`? The `CreateTime` is when Flink
wrote the record. How long after `detected_at` is it?

**Predict (P3):** how many FLAPPING alerts are there, against how many deliberate flapping episodes the simulator ran?

```bash
kafka kafka-console-consumer.sh --topic ec2.alerts --from-beginning --isolation-level read_committed --max-messages 2000 --timeout-ms 6000 \
  | jq -r 'select(.alert_type == "FLAPPING") | .instance_id' | sort -u | wc -l
docker compose logs --no-log-prefix simulator | grep -c "flapping episode"
```

## 3. Make an instance flap yourself, out of order

Send five events for a quiet instance: stopped, running, stopped, running, stopped, 5 seconds apart in event time, but
**sent in shuffled order**, all within the last 25 seconds.

```bash
I=$(docker compose exec -T postgres psql -U postgres -d serving -At -c "select instance_id from instance_current_state where state='running' order by transitions_1h, state_since limit 1"); echo $I
for spec in "stopped 5" "running 20" "stopped 25" "running 10" "stopped 15"; do set -- $spec; mise run -q lab:sqs -- send-event $I $1 $2; done
date -u +%T
until kafka kafka-console-consumer.sh --topic ec2.alerts --from-beginning --isolation-level read_committed --max-messages 5000 --timeout-ms 3000 | grep -q "$I"; do sleep 2; done; date -u +%T
kafka kafka-console-consumer.sh --topic ec2.alerts --from-beginning --isolation-level read_committed --max-messages 5000 --timeout-ms 3000 | grep "$I" | jq -c '{alert_type, window_start, window_end}'
```

**Predict (P4):** does the pattern match even though the events arrived shuffled? How long after the last event time
does the alert become visible?

## Break it, then fix it

**Break: the same five events, all older than the watermark.** Same order, but 50-90 seconds in the past:

```bash
I2=$(docker compose exec -T postgres psql -U postgres -d serving -At -c "select instance_id from instance_current_state where state='running' and instance_id <> '$I' order by transitions_1h, state_since limit 1")
for spec in "stopped 90" "running 80" "stopped 70" "running 60" "stopped 50"; do set -- $spec; mise run -q lab:sqs -- send-event $I2 $1 $2; done
sleep 100
kafka kafka-console-consumer.sh --topic ec2.alerts --from-beginning --isolation-level read_committed --max-messages 5000 --timeout-ms 3000 | grep -c "$I2"
mise run -q lab:duck -- "select count(*) as in_lakehouse from lake.ec2_stream.state_changes where instance_id='$I2' and event_id like 'lab-%'"
```

**Predict (P5):** alert or no alert? Are the events in the lakehouse?

**Fix:** decide what lateness the alert must tolerate. A bigger watermark bound catches later events but delays every
alert by that much. Or keep the bound and run the same pattern in batch over the lakehouse (for example hourly) to
catch the stragglers. Both are legitimate; silently missing them is not.

## Check your predictions

**P1.** `serving_age_s = 1.0`: the JDBC sink flushes every second, so with 10 events a second the newest row is about a
second old. The newest window ended at `19:31:00` when the clock said `19:31:58`: a window is emitted only once the
watermark (30 s behind) passes its end, so a minute's count appears 30-60 s after that minute closes. Same data,
different freshness, because one is a point lookup and the other waits for completeness.

**P2.** Exactly the pattern: `stopped, running, stopped, running, stopped` between `19:23:25.286` and `19:31:13.025`,
all within 10 minutes. `detected_at` is the third stop's event time; the record's `CreateTime` was `19:31:46.469`, 33 s
later: the watermark, then the transaction commit at the next checkpoint. Check alerts against the raw facts like this
whenever someone asks "why did this fire?".

**P3.** 41 FLAPPING alerts from 41 different instances, against 5 deliberate flapping episodes. The other 36 are
instances whose random stop/start activity happened to fit the pattern. The rule is correct and still noisy. Before you
put an LLM call (Lab 09) or a pager behind an alert, measure its precision. The fix here is a stricter pattern (four
cycles, or a shorter `WITHIN`) or a CMDB filter (prod only), not more triage capacity.

**P4.** It matches: `window_start 19:39:12.136`, `window_end 19:39:30.287`, even though the events went out in the
order 5, 20, 25, 10, 15 seconds ago. `MATCH_RECOGNIZE ... ORDER BY event_time` buffers rows until the watermark
passes them, then evaluates them in event-time order. The alert was visible at `19:40:28`, 58 s after the last event
time: up to 30 s of watermark, plus up to 30 s until the next checkpoint commits the Kafka transaction.

**P5.** No alert (`0`), but all 5 events are in the lakehouse. When they arrived, the watermark was already about 30 s
behind the newest events, and these were 50-90 s old: late. Pattern matching, windows and OVER aggregates drop late
rows; the raw and clean tables keep them. (The first time I tried the in-order version of this lab, I made exactly this
mistake by back-dating the events 40-50 s and wondered why nothing fired.)

## What to remember

- Point-lookup outputs are seconds fresh; anything waiting for completeness lags by the watermark.
- Every alert should be traceable to raw facts in a query or two.
- Arrival order doesn't matter for event-time logic; arrival *lateness* does.
- Alert precision is a design input. Measure it before scaling what consumes alerts.

Next: [Lab 09 · MCP and the triage agent](09-mcp-and-agent.md).

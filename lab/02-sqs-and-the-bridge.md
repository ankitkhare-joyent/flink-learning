# Lab 02 · SQS and the bridge: at-least-once at the edge

One sitting, about 30 minutes. Background: [docs/04](../docs/04-sqs-and-the-bridge.md),
[docs/03](../docs/03-events-and-contracts.md) (the contract and the DLQ). Code:
[services/ingest/sqs_bridge.py](../services/ingest/sqs_bridge.py), [services/ingest/events.py](../services/ingest/events.py),
[stack/elasticmq/elasticmq.conf](../stack/elasticmq/elasticmq.conf).

Every EC2 event enters here: the simulator sends EventBridge-shaped JSON to an SQS queue, and two bridge replicas
move it to Kafka. After this lab you can explain:

- why the queue is almost always empty, and what that tells you
- what "delete only after the Kafka ack" buys you, and what it costs
- where a malformed message goes, and why not to SQS's own DLQ
- what the edge does when Kafka disappears

## Before you start

```bash
source lab/env.sh
mise run -q lab:sqs -- depth
```

## 1. A healthy edge

```bash
for p in 19400 19401; do curl -s localhost:$p/metrics | grep -E '^bridge_(sqs_messages_received|sqs_messages_deleted|invalid_events)_total' | tr '\n' ' '; echo; done
curl -s localhost:19400/metrics | grep -E '^bridge_event_age_seconds_bucket'
```

**Predict (P1):** the queue receives about 10 events a second. What depth do you expect, and how do the two replicas
split the work?

**Predict (P2):** `bridge_event_age_seconds` measures AWS event time to Kafka write. Most events are under 0.1 s.
Why do a few land in the 10-30 s and 300 s buckets?

## 2. Stop the bridge

```bash
docker compose stop sqs-bridge && sleep 30
mise run -q lab:sqs -- depth
mise run -q lab:sqs -- peek 3
mise run -q lab:sqs -- peek 3
```

**Predict (P3):** how many messages pile up in 30 s? Do the two `peek` calls show the same three messages, and what is
their `receive_count`?

Start it again and time the catch-up:

```bash
docker compose start sqs-bridge; t0=$(date +%s)
until mise run -q lab:sqs -- depth | head -1 | grep -q "visible=    0"; do sleep 1; done; echo "drained in $(( $(date +%s)-t0 )) s"
sleep 3; curl -s localhost:19400/metrics | grep -E '^bridge_event_age_seconds_bucket\{le="(10.0|30.0|60.0|300.0)"\}'
```

**Predict (P4):** the events waited in SQS for up to 30 s. What `event_time` do they carry into Kafka, and where do they
land in the age histogram?

## Break it, then fix it

**Break 1: a poison message.**

```bash
offsets ec2.state-changes.dlq | tail -1
mise run -q lab:sqs -- send-junk && sleep 3
offsets ec2.state-changes.dlq | tail -1
kafka kafka-console-consumer.sh --topic ec2.state-changes.dlq --from-beginning --timeout-ms 6000 --property print.headers=true | grep lab-junk
mise run -q lab:sqs -- depth
```

**Predict (P5):** does the junk end up in SQS's DLQ (`ec2-state-change-dlq`) or in Kafka's `ec2.state-changes.dlq`? What
does the bridge record about it? (The DLQ already has a few entries: the simulator sends about 0.3% junk on purpose.)

**Break 2: Kafka goes away for 75 seconds.**

**Predict (P6):** while Kafka is down, what happens to SQS's `visible` and `in_flight` counts? Is anything lost?

```bash
before=$(offsets ec2.state-changes | tail -1)
docker compose stop kafka && sleep 40
mise run -q lab:sqs -- depth | head -1
docker compose logs --no-log-prefix --since 40s sqs-bridge | grep '^{' | jq -r .event | sort | uniq -c
sleep 35 && mise run -q lab:sqs -- depth | head -1
docker compose start kafka && sleep 30
mise run -q lab:sqs -- depth
echo "before: $before   after: $(offsets ec2.state-changes | tail -1)"
fjobs | cut -f1-3
```

**Fix:** none needed. Starting Kafka is the fix, and the edge recovers on its own. Make sure that's what you saw.

## Check your predictions

**P1.** `visible=0 in_flight=0`. The bridges long-poll (`WaitTimeSeconds=20`), so a message is picked up within
milliseconds of arriving. An empty queue with steadily rising `received`/`deleted` counters is the healthy picture;
a growing `visible` count is the alarm. The two replicas split the work roughly in half (1351 and 1397 messages after a
few minutes when I ran it), because SQS hands each receive call whatever is visible. `received == deleted` on each
replica: everything received was acked by Kafka and then deleted.

**P2.** Those are the simulator's deliberately delayed events: 3% are held back 2-15 s ("out of order") and 0.3% for
3-6 minutes ("late"). Their `event_time` is when they happened, so their age is large when they finally arrive. This
histogram is how you'd spot a slow producer or a clogged queue in production.

**P3.** About 300 (`visible=295` after 30 s). The two peeks show **different** messages: a standard queue makes no
ordering promise, and with `VisibilityTimeout=0` each receive just picks whatever it finds. `receive_count` is `1`:
peeking counts as a receive. Do it often enough and you'd push a message toward `maxReceiveCount=5` and the DLQ
yourself.

**P4.** Their original `event_time`. The bridge never replaces it, and Flink orders by it. The backlog drained in about
5 seconds, and the new process's histogram showed most of those messages in the 30-60 s bucket (262 of 266 under 60 s,
4 more under 300 s). The counters restart with the process; in production Prometheus keeps the history.

**P5.** Kafka's `ec2.state-changes.dlq`, and the SQS message is deleted. The DLQ count went from 11 to 12, and the record's
headers say why:

```
error:not JSON: Unterminated string starting at,source.queue:ec2-state-change,receive.count:1	{"version":"0","id":"lab-junk","detail-ty
```

The other entries were the simulator's junk: `bad state: 'rebooting'` and `unexpected event type: ... EC2 Spot Instance
Interruption Warning`. A malformed message will never succeed, so retrying it five times via SQS redrive only wastes
time. The SQS DLQ is for the other kind of failure: messages the bridge *couldn't* deliver (Kafka down for too long).
That one stayed at 0.

**P6.** Nothing is lost. After 40 s, `visible=415 in_flight=22`; after 75 s, `visible=776 in_flight=40`. The in-flight
messages are batches a bridge received but couldn't get acked, logged as `kafka did not ack in time; messages stay in
SQS`. They were never deleted, so after the 60 s visibility timeout SQS would hand them out again. When Kafka came
back, the backlog drained within 30 s and the topic grew by more than 1,100 records (3776 to 4932). The Flink jobs
stayed RUNNING; `ec2-alerts` showed restored checkpoints because its exactly-once sink couldn't commit while Kafka was
down, so it restarted from its last checkpoint (Lab 05 does this on purpose). Had Kafka been down for more than about
5 minutes (5 receives x 60 s), SQS would have moved messages to its own DLQ. That's the case for an alert on the SQS
DLQ depth.

## What to remember

- An empty queue with rising counters is healthy. Watch depth and age, not just errors.
- Delete after the ack: crashes cost duplicates, never losses. Downstream dedup makes it effectively once.
- Poison goes to a DLQ immediately with its reason. Transient failure goes back to the queue to be retried.
- Event time travels with the event, however long it waited.

Next: [Lab 03 · Kafka and the Schema Registry](03-kafka-and-schemas.md).

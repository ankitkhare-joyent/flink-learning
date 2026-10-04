# 04. SQS and the bridge: queues vs logs, and delivery guarantees

## How the real thing works on AWS

```
EC2 state change --> EventBridge (default bus) --rule: source=aws.ec2, detail-type=EC2 Instance State-change Notification--> SQS queue
```

AWS emits an event for every transition. An EventBridge **rule** matches it and delivers it to a **target**, here an
SQS queue. That's how most AWS-native events reach you (S3 notifications, CloudTrail, Config, Health). Locally,
[`services/ingest/simulator.py`](../services/ingest/simulator.py) produces byte-for-byte the same JSON and ElasticMQ plays SQS.

## Queue vs log

| | SQS (queue) | Kafka (log) |
|---|---|---|
| After a consumer reads | Message is deleted (after you call DeleteMessage) | Record stays until retention |
| Many independent readers | No (use SNS fan-out to several queues) | Yes, one consumer group each |
| Replay yesterday | Impossible | Reset the group's offset |
| Ordering | None (standard), per message-group (FIFO) | Per partition (per key) |
| Scaling consumers | Add consumers, any number | Up to the partition count (or share groups) |
| Ops | None | You run (or rent) a cluster |
| Delivery | At-least-once, duplicates possible | At-least-once by default; exactly-once with transactions |

So: SQS to *receive* from AWS, Kafka to *share* with everything downstream. The bridge connects them.

## SQS semantics you must know

- **Visibility timeout** (we use 60 s): a received message becomes invisible to others. If you don't delete it in
  time, it reappears and someone processes it again.
- **At-least-once**: standard queues can deliver a message twice even if you did everything right.
- **Long polling** (`WaitTimeSeconds=20`): one request waits up to 20 s for messages instead of hammering the API.
- **Redrive policy** (`maxReceiveCount=5`): after 5 receives without a delete, SQS moves the message to the
  dead-letter queue `ec2-state-change-dlq`. See [`stack/elasticmq/elasticmq.conf`](../stack/elasticmq/elasticmq.conf).
- **Batching**: receive up to 10, send up to 10, delete up to 10 per call. Cost and throughput both improve.

## The bridge, step by step ([`services/ingest/sqs_bridge.py`](../services/ingest/sqs_bridge.py))

```
receive_message(10, wait 20 s, visibility 60 s)
   for each message:
      normalize()  -- invalid? --> produce raw to ec2.state-changes.dlq
      produce(ec2.state-changes, key=instance_id, value=Avro)
   producer.flush()  -- waits for Kafka acks; delivery callbacks record which messages succeeded
   delete_message_batch(only the acked ones)
```

The order is the whole point:

- **Delete after the Kafka ack** gives *at-least-once*. Crash after producing but before deleting, and SQS
  redelivers: a duplicate in Kafka, never a loss.
- **Delete before producing** would give *at-most-once*: crash in between and the event is gone forever.
- Duplicates are handled downstream: Flink de-duplicates on `event_id` (lesson 07). This "at-least-once delivery +
  idempotent processing = effectively once" pattern is everywhere in streaming. Learn to spot it.

Other production details in the file: an idempotent Kafka producer (no duplicates from producer retries), zstd
compression with a 20 ms batching window, exponential backoff when a cycle fails, graceful shutdown on SIGTERM
(finish the batch, flush, then exit), Prometheus metrics (`bridge_event_age_seconds` is the edge latency), and
structured JSON logs.

## Scaling it

The bridge is stateless, so it scales by running more copies: [`compose.yaml`](../compose.yaml) runs 2 replicas against one queue.
SQS hands each message to one of them. On Kubernetes you would scale it with KEDA on queue depth.

Per-instance order is *not* guaranteed through a standard queue, even with one bridge: SQS may hand messages out in
any order. We don't fight that at the edge. Event time plus watermarks in Flink put events back in order (lesson 06).
If you needed order at the edge, you would use a FIFO queue with `MessageGroupId=instance_id`.

## Alternatives you'll see in the wild

- **Kafka Connect SQS source connector** instead of custom code (less code, less control over validation).
- **EventBridge Pipes** from SQS to Kinesis or an API destination.
- **Lambda** triggered by SQS writing to MSK (fine at low volume; watch concurrency and per-invocation connection cost).
- Skip SQS and have producers write to Kafka directly, when you own the producers.

## Try it

Queue depth and in-flight messages (ElasticMQ stats UI):

```bash
open http://localhost:19325
```

Bridge metrics (replica 1):

```bash
curl -s localhost:19400/metrics | grep ^bridge_
```

Next: [05. CDC with Debezium](05-cdc-with-debezium.md)

# 02. Kafka: the log everything else reads

## The model in five sentences

1. A **topic** is a named, append-only log, split into **partitions** (`ec2.state-changes` has 6).
2. A record has a **key**, a **value**, headers and a timestamp; the key decides the partition (`hash(key) % 6`).
3. Inside one partition records are strictly ordered and each gets an **offset** (0, 1, 2, ...). Across partitions
   there is no order.
4. Consumers in the same **consumer group** split the partitions between them; each group tracks its own committed
   offsets, so groups never affect each other.
5. Records are kept by **retention** (time or size), not deleted when read. Rewinding = moving your offset back.

Compare to S3 landing folders: a partition is like an ever-growing file, the offset is your byte position, and a
consumer group is "which job has processed how far". Kafka just makes that fast, ordered and shared.

## Keys and ordering: the most important design decision

We key EC2 events by `instance_id`. Consequences:

- All events of one instance land in one partition, in order. The Flink pattern "stopped, running, stopped..." per
  instance works because of this.
- Different instances spread across partitions, so 6 consumers can work in parallel.
- Hot keys hurt: one instance producing 90% of events would overload one partition. (Skew, like in Spark.)
- You can add partitions later, but `hash(key) % n` changes, breaking per-key order across the change. Pick the
  count up front (rule of thumb: target throughput / per-partition throughput, then round up for growth).

## Topics in this project

Created on purpose by [`scripts/init_streaming.py`](../scripts/init_streaming.py) (auto-creation is off on the broker, as in production):

| Topic | Partitions | Retention / cleanup | Key | Written by |
|---|---|---|---|---|
| `ec2.state-changes` | 6 | 7 days, delete | instance_id | sqs-bridge (Avro) |
| `ec2.state-changes.dlq` | 1 | 30 days | SQS message id | sqs-bridge (raw bad payloads) |
| `cmdb.public.ec2_ci` | 1 | compacted | instance_id | Debezium |
| `ec2.alerts` | 3 | 14 days | alert_id | Flink (exactly-once) |
| `ec2.alerts.triaged` | 3 | 30 days | alert_id | triage agent |
| `ec2.alerts.dlq` | 1 | 30 days | none | triage agent |

**Compaction** (`cleanup.policy=compact`) keeps at least the latest record per key forever. The CMDB topic is
compacted so a brand-new Flink job can rebuild the full CMDB table by reading the topic from the start. That is how
a stream becomes a table.

## Durability: replication and acks

Locally there is one broker (replication factor 1). In production every partition has 3 replicas on brokers in
different availability zones. One is the **leader**; the others follow. The **ISR** (in-sync replicas) are the
followers that are caught up.

- `acks=all` + `min.insync.replicas=2`: a write succeeds only when 2 replicas have it. Lose a broker, lose nothing.
- `acks=1`: faster, but a leader crash right after the ack loses the record.

The bridge uses `acks=all` and **`enable.idempotence=true`**: the broker de-duplicates producer retries using a
producer id and sequence numbers, so a network blip never writes a record twice or out of order.

## Transactions and exactly-once

A Kafka transaction makes writes to several partitions (and the consumer's offset commit) atomic. Consumers with
`isolation.level=read_committed` only see committed transactions. Flink's exactly-once Kafka sink uses this: it
opens a transaction per checkpoint and commits it when the checkpoint completes (lesson 06). That is why alerts
appear in 30-second bursts and why the agent reads with `read_committed`.

## KRaft, and what changed in Kafka 4.x

- **ZooKeeper is gone** (removed in 4.0). Kafka stores its own metadata in a Raft quorum of controllers. Our
  container runs as both broker and controller (`KAFKA_PROCESS_ROLES: broker,controller`); production separates them.
- **Kafka Queues (share groups, KIP-932)**, production-ready since 4.2: many consumers share one partition with
  per-record acknowledgement and redelivery, like SQS. Use it when consumers must scale past the partition count
  and order does not matter (for example, one LLM call per record).
- **New consumer rebalance protocol (KIP-848)**: rebalances coordinated by the broker, without stop-the-world pauses.

## Consumer lag: the number you will watch most

Lag = latest offset in a partition minus the group's committed offset. Growing lag means consumers are slower than
producers. Look at it in Kafka UI (http://localhost:18080 > Consumers). Our Flink jobs commit their offsets to groups
`flink-ec2-lakehouse`, `flink-ec2-serving` and `flink-ec2-alerts` on every checkpoint, for monitoring only: Flink
itself restarts from its checkpoint, not from those committed offsets.

## Try it

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic ec2.state-changes
```

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group flink-ec2-serving
```

Read alerts as JSON (they are plain JSON; state changes are Avro, so read those in Kafka UI, which decodes them
with the Schema Registry):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ec2.alerts --from-beginning --isolation-level read_committed --max-messages 5
```

Next: [03. Events and contracts](03-events-and-contracts.md)

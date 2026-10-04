# 01. The 2026 landscape: what to learn and why this stack

Researched October 2026. Versions are what this repo pins; sources are at the bottom.

## What employers ask for

- Streaming is one of the strongest pockets of data-engineering demand. UK recruiters report that roughly a third of
  senior data-engineering roles list Kafka, Flink, Kinesis or Pub/Sub as a hard requirement, with a pay premium over
  batch-only roles.
- **Kafka is the backbone** (the most-cited streaming skill in postings) and **Flink is the standard processor**
  for stateful, exactly-once, event-time work. "Kafka + Flink" is the combination that shows up together.
- CDC (Debezium), schema governance (Avro/Protobuf + a registry), Iceberg as the analytics sink, and Kubernetes
  operations around all of it complete the typical job description.
- New in 2025-26: **streaming for AI**. Flink added `ML_PREDICT` (2.1) and `VECTOR_SEARCH` (2.2), the Flink
  community started **Flink Agents** (0.3 preview, June 2026), and vendors pitch "streaming agents". Knowing how
  agents consume and act on streams is a differentiator, which is why lesson 10 exists.

Your existing skills transfer directly: Spark SQL to Flink SQL, Iceberg to the streaming lakehouse, Airflow
thinking to "what runs continuously vs what still runs on a schedule", EMR on EKS to Flink/Strimzi on Kubernetes.

## The stack, layer by layer

| Layer | Picked | Version | Why this one | Main alternatives |
|---|---|---|---|---|
| Event log | **Apache Kafka** (KRaft) | 4.3.1 | The industry standard and the skill employers list most. 4.x removed ZooKeeper; 4.2 made Kafka Queues (share groups) production-ready | Redpanda (Kafka API, C++, single binary), AutoMQ/WarpStream (Kafka on S3), Pulsar, AWS Kinesis |
| Schemas | **Avro + Karapace** | 6.2.4 | Apache-2.0 and API-compatible with Confluent Schema Registry, so the same clients and Flink format work | Confluent Schema Registry (community licence), Apicurio, AWS Glue Schema Registry |
| CDC | **Debezium** on Kafka Connect | 3.7 | The default open-source CDC; reads the Postgres WAL | Flink CDC 3.x (CDC straight into Flink), AWS DMS |
| Queue | **SQS** (ElasticMQ locally) | 1.7.1 | It's what AWS integrations deliver to (EventBridge, S3 notifications); you will meet it at work | SNS+SQS fan-out, Kafka share groups, RabbitMQ |
| Processing | **Apache Flink** (SQL) | 2.2.1 | Stateful, event-time, exactly-once; SQL first, Java/Python when needed. Also Amazon Managed Service for Apache Flink 2.2 | Spark Structured Streaming (micro-batch, great if you live in Spark), Kafka Streams (a library inside your app), RisingWave/Materialize (streaming databases), Arroyo |
| Lakehouse sink | **Apache Iceberg** via REST catalog | 1.12 runtime | Same tables Spark and Trino already read; Flink commits per checkpoint | Apache Paimon (stream-native table format), Apache Fluss (sub-second streaming storage, tiers to Paimon/Iceberg), Delta |
| Serving | **Postgres**, upserted by Flink | 18 | What apps and agents query: millisecond lookups of current state | Redis, DynamoDB, Pinot/Druid/ClickHouse for analytics, a streaming database |
| Secrets | **OpenBao** | 2.7.1 | Linux Foundation fork of Vault under MPL-2.0 after Vault moved to the BSL in 2023; same API, CLI and concepts | HashiCorp Vault (BSL), AWS Secrets Manager, Infisical |
| AI | **MCP server + Claude agent** | MCP SDK 2.3 | MCP is how agents get tools; the agent is a normal Kafka consumer | Flink `ML_PREDICT`, Flink Agents (preview) |

## Why Flink and not Spark Structured Streaming, given you know Spark?

Both are fine and both are in demand. Use Spark Structured Streaming when the team is already all-in on Spark and
latency of seconds to minutes is fine. Learn Flink because:

- It processes record by record with true event-time semantics; Spark's model is micro-batches.
- Its state handling (RocksDB, incremental checkpoints, savepoints for upgrades) is what large stateful jobs need.
- Flink SQL covers temporal joins, `MATCH_RECOGNIZE` pattern matching and changelog streams that Spark SQL does not.
- Job postings that say "real-time" mostly say Flink, and every major cloud sells managed Flink.

## Why Kafka *and* SQS?

They solve different problems, and production systems use both (lesson 04):

- **SQS is a queue**: each message goes to one consumer and is deleted. No replay, no ordering (standard queues),
  infinite scale, zero ops. Perfect for "AWS, deliver me this event".
- **Kafka is a log**: records stay for days, many consumer groups read independently, per-key ordering, replay.
  Perfect as the shared backbone that Flink, the lakehouse and agents all read.

## What this course leaves out (and where to go next)

- **Kafka Streams**: a Java library for stream processing inside your service. Learn it if you join a JVM shop.
- **Paimon and Fluss**: watch them. Fluss + Paimon is the Flink community's "streamhouse" for sub-second analytics.
- **Streaming databases** (RisingWave, Materialize): Postgres-compatible SQL over streams with built-in serving.
  Simpler than Flink for many use cases, less flexible for complex ones.
- **Managed services**: Amazon MSK, Amazon Managed Service for Apache Flink, Confluent Cloud. Lesson 11 maps every
  local component to its AWS counterpart.

## Sources

- [Top 5 skills every real-time/streaming data engineer needs in 2026 (Bristow Holland)](https://www.bristowholland.com/insights/industry-insights/top-5-skills-every-real-time-streaming-data-engineer-needs-in-2026/)
- [Streaming data engineer jobs UK 2026](https://dataengineeringjobs.co.uk/career-advice/streaming-data-engineer-jobs-uk-2026)
- [Apache Flink 2.2.0 release announcement](https://flink.apache.org/2025/12/04/apache-flink-2.2.0-advancing-real-time-data--ai-and-empowering-stream-processing-for-the-ai-era/)
- [Amazon Managed Service for Apache Flink 2.2](https://docs.aws.amazon.com/managed-flink/latest/java/flink-2-2.html)
- [Apache Flink Agents 0.3.0 release announcement](https://flink.apache.org/2026/06/19/apache-flink-agents-0.3.0-release-announcement/)
- [Apache Kafka 4.2.0 release announcement](https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/)
- [Kafka 4.1 overview (OpenLogic)](https://www.openlogic.com/blog/kafka-4-1-overview)
- [Real-time lakehouse patterns with Flink and Iceberg](https://datalakehousehub.com/blog/2026-05-real-time-lakehouse-flink/)
- [Apache Fluss vs Apache Paimon (Flink Forward)](https://asia.flink-forward.org/blog/apache-fluss-vs.-apache-paimon-two-engines-for-the-real-time-lakehouse)
- [OpenBao vs Vault comparison (Jorijn)](https://jorijn.com/en/blog/hashicorp-vault-vs-openbao/)
- [IBM engineers hatch Linux Foundation Vault fork (TechTarget)](https://techtarget.com/searchitoperations/news/366563095/IBM-engineers-hatch-Linux-Foundation-HashiCorp-Vault-fork)

Next: [02. Kafka](02-kafka.md)

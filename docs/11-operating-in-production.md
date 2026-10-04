# 11. Operating in production

Running a streaming system is mostly operations: it never finishes, so everything that can drift, will.

## From this repo to AWS

| Local | Self-managed on EKS | Fully managed on AWS |
|---|---|---|
| `apache/kafka` (1 broker) | **Strimzi** operator: `Kafka`, `KafkaTopic`, `KafkaUser` resources | **Amazon MSK** (provisioned or Serverless), or Confluent Cloud |
| Karapace | Karapace / Apicurio as a Deployment | Confluent Schema Registry, AWS Glue Schema Registry |
| Debezium on Kafka Connect | Strimzi `KafkaConnect` + `KafkaConnector` | **MSK Connect** running Debezium |
| ElasticMQ | n/a | **SQS** + EventBridge rule |
| Flink session cluster | **Flink Kubernetes Operator**: one `FlinkDeployment` per job (application mode) | **Amazon Managed Service for Apache Flink** (2.2 available) |
| SeaweedFS + Lakekeeper | S3 + Lakekeeper/Polaris | S3 + **Glue Iceberg REST catalog** or **S3 Tables** |
| Postgres serving | RDS / Aurora | RDS / Aurora |
| OpenBao (1 node, file-based unseal) | OpenBao Helm chart, 3-node Raft, KMS auto-unseal, Kubernetes auth | AWS Secrets Manager (+ External Secrets Operator), or HCP Vault |
| `docker compose` | Argo CD / Flux syncing the manifests from git | Terraform/OpenTofu |

Your EMR on EKS experience maps cleanly: the Flink Kubernetes Operator plays the role of the EMR on EKS job
controller, but for jobs that never end.

## Deploying and upgrading Flink jobs

A `FlinkDeployment` looks like this (abridged):

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata: { name: ec2-alerts }
spec:
  image: <registry>/ec2-streaming/flink:2.2.1-<git sha>
  flinkVersion: v2_2
  flinkConfiguration:
    execution.checkpointing.interval: 30s
    state.backend.type: rocksdb
  job:
    jarURI: local:///opt/flink/usrlib/sql-runner.jar   # tiny jar that executes a .sql file (the operator repo's
    args: ["/opt/flink/jobs/30_alerts.sql"]           # flink-sql-runner example)
    upgradeMode: savepoint     # stop-with-savepoint, deploy, restore
    parallelism: 2
```

Upgrade flow: change the image or SQL in git, the operator takes a savepoint, stops the job, starts the new version
from that savepoint. Things that break state compatibility (renaming operators in Java, materially changing a SQL
query's shape) need a plan: run old and new side by side, or start fresh and backfill.

**Autoscaling**: the operator's autoscaler adjusts parallelism per operator from busy time and lag. Kafka partitions
cap source parallelism, so size partitions for the peak you expect.

## What to monitor

| Signal | Where | Alert when |
|---|---|---|
| Consumer lag (records and **time**) | Kafka groups `flink-*`, `triage-agent` | Growing for 10+ minutes |
| Checkpoint age / failures | Flink REST `/jobs/<id>/checkpoints` | No completed checkpoint for 3+ intervals |
| Checkpoint duration and size | same | Duration near the interval; size growing without bound (missing TTL) |
| Backpressure / busy time | Flink metrics | An operator busy > 80% sustained |
| Watermark lag (now - watermark) | Flink metrics | Minutes behind: a stuck or idle source |
| Restarts | `numRestarts` | Any increase |
| DLQ size | `ec2.state-changes.dlq`, SQS DLQ | Any growth |
| Edge latency | `bridge_event_age_seconds` | p99 above SLO |
| Serving freshness | `max(updated_at)` of serving tables | Older than 2 minutes |
| CDC slot lag | `pg_replication_slots` | WAL retained above a threshold |

Flink exposes Prometheus metrics on `:9249` (configured in [`compose.yaml`](../compose.yaml)), and the Python services on `:9400+`. A
Prometheus + Grafana stack scrapes them; the MCP tool `pipeline_health` is a small version of the same checks that an
agent can call. Alert on **time-based lag** ("we are 4 minutes behind") rather than record counts, which mean
nothing to a human.

## Reprocessing and backfills

Something will be wrong eventually (a bug, a schema mistake, a new metric needed for last month). Options:

1. **Replay Kafka**: start a new version of the job from `earliest-offset` while Kafka still retains the data (7 days
   here). Simple, exact, limited by retention.
2. **Backfill from Iceberg**: run the same Flink SQL in **batch mode** over `state_changes_raw` (or use Spark, which
   you know), then switch to the stream. Iceberg is the long-term memory; Kafka is the short-term one.
3. **Kafka tiered storage** (KIP-405, production-ready in Kafka 3.9+) keeps old segments in S3 so replay windows can
   be months long at low cost.

Keeping a raw, append-only table (`state_changes_raw`) is what makes option 2 possible. Never stream *only* derived data.

## Testing strategy

| Level | What | In this repo |
|---|---|---|
| Contract | Parser + schema without infrastructure | [`tests/test_events.py`](../tests/test_events.py), [`tests/test_triage_contract.py`](../tests/test_triage_contract.py) |
| Tool surface | MCP tools, annotations, input limits | [`tests/test_mcp_server.py`](../tests/test_mcp_server.py) |
| Job logic | Flink SQL against bounded test inputs (MiniCluster, or Testcontainers with Kafka) | Exercise 7 in lesson 12 |
| End to end | The whole compose stack with the simulator's mess | `docker compose up` + checks in lesson 12 |
| Schema evolution | Register the new schema against the registry in CI before merging | `BACKWARD` compatibility check |

## Cost and sizing notes

- Kafka cost is mostly storage (retention x throughput x replication 3) and cross-AZ traffic. Compression (zstd) and
  sensible retention matter more than broker count.
- Flink cost is TaskManager memory for state. RocksDB + incremental checkpoints keep it on local disk and S3 instead of RAM.
- Iceberg streaming creates a commit per checkpoint per table: compact files and expire snapshots on a schedule (lesson 08).
- LLM cost scales with alerts, not events. Filter hard before the agent and cap the rate.

## Security checklist

TLS on every listener (Kafka, Connect, Registry, OpenBao, Postgres); SASL/SCRAM or mTLS or IAM auth for Kafka clients,
with ACLs per topic; no plaintext secrets in config (OpenBao or Secrets Manager); least-privilege database users; a
schema registry that rejects unreviewed changes; audit logs on the secret store; network policies so only the bridge
can write `ec2.state-changes`.

Next: [12. Exercises](12-exercises.md)

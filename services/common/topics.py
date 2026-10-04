"""Kafka topic contracts. One place, so producers, Flink jobs, MCP tools and init agree.

Naming: <domain>.<entity>[.<qualifier>]  e.g. ec2.state-changes, ec2.state-changes.dlq
Partition key decides ordering: everything about one instance lands on one partition, in order.
"""
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Topic:
    name: str
    partitions: int
    config: dict[str, str] = field(default_factory=dict)


DAY_MS = 24 * 3600 * 1000

STATE_CHANGES = Topic("ec2.state-changes", 6, {"retention.ms": str(7 * DAY_MS), "compression.type": "producer"})
STATE_CHANGES_DLQ = Topic("ec2.state-changes.dlq", 1, {"retention.ms": str(30 * DAY_MS)})
# Debezium writes the CMDB change log here. Compacted: Kafka keeps at least the latest row per key forever,
# so a brand-new consumer can rebuild the whole table from the topic alone.
CMDB_CI = Topic("cmdb.public.ec2_ci", 1, {"cleanup.policy": "compact", "min.compaction.lag.ms": "60000"})
ALERTS = Topic("ec2.alerts", 3, {"retention.ms": str(14 * DAY_MS)})
ALERTS_TRIAGED = Topic("ec2.alerts.triaged", 3, {"retention.ms": str(30 * DAY_MS)})
ALERTS_DLQ = Topic("ec2.alerts.dlq", 1, {"retention.ms": str(30 * DAY_MS)})

ALL = (STATE_CHANGES, STATE_CHANGES_DLQ, CMDB_CI, ALERTS, ALERTS_TRIAGED, ALERTS_DLQ)

STATE_CHANGES_SUBJECT = f"{STATE_CHANGES.name}-value"   # Schema Registry TopicNameStrategy

# Architecture

The full diagram, with the numbered trace of one event and the secrets matrix, is
[architecture.html](architecture.html) (open it in a browser; also published at https://claude.ai/artifact/G6uwaopNAM6WmHrFUShyXR). This is the same picture as a Mermaid diagram, which
renders in VS Code and GitHub.

```mermaid
flowchart LR
  subgraph sources[Sources]
    EC2[EC2 fleet<br/>EventBridge state-change events]
    CMDB[(ServiceNow CMDB<br/>Postgres cmdb.ec2_ci)]
  end
  subgraph ingest[Ingest]
    SQS[[SQS queue<br/>ElasticMQ · redrive 5x]]
    BR[sqs-bridge x2<br/>validate · ack, then delete]
    DBZ[Debezium<br/>Kafka Connect]
  end
  subgraph kafka[Kafka 4.3 · KRaft]
    T1[ec2.state-changes<br/>Avro · key=instance_id]
    DLQ[ec2.state-changes.dlq]
    T3[cmdb.public.ec2_ci<br/>compacted]
    AL[ec2.alerts<br/>exactly-once]
    TR[ec2.alerts.triaged / .dlq]
    SR[(Schema Registry<br/>Karapace)]
  end
  subgraph flink[Flink 2.2 · SQL]
    J1[ec2-lakehouse<br/>dedup · temporal join]
    J2[ec2-serving<br/>OVER · Top-1 · TUMBLE]
    J3[ec2-alerts<br/>MATCH_RECOGNIZE]
  end
  ICE[(Iceberg<br/>Lakekeeper + SeaweedFS)]
  PG[(Postgres serving)]
  subgraph ai[AI on the stream]
    AG[Triage agent<br/>Kafka consumer]
    C[(Claude Opus 5.5)]
    MCP[MCP server<br/>6 read-only tools]
    YOU[You in Claude Code]
  end
  BAO{{OpenBao<br/>AppRole · KV · dynamic + rotated DB users}}

  EC2 -- "1 JSON" --> SQS --> BR
  BR -- "2 Avro, acks=all" --> T1
  BR -- invalid --> DLQ
  BR -.schema id.- SR
  CMDB -- WAL --> DBZ -- changes --> T3
  T1 -- "3" --> J1 & J2 & J3
  T3 --> J1 & J2 & J3
  J1 -- "4 commit per checkpoint" --> ICE
  J2 -- "5 upsert" --> PG
  J3 -- "6 Kafka transaction" --> AL
  AL -- "7 read_committed" --> AG
  AG -- "8 tools" --> MCP
  AG <-->|"9 tool loop"| C
  AG -- triage --> TR
  AG -- alert_triage --> PG
  YOU --> MCP
  MCP -- SELECT --> PG
  MCP -- DuckDB --> ICE
  MCP -- health --> flink
  BAO -. credentials .-> DBZ & flink & MCP & AG & CMDB
```

| # | Step | Latency | Guarantee |
|---|------|---------|-----------|
| 1 | AWS emits the state change; EventBridge delivers it to SQS | < 1 s | at-least-once |
| 2 | Bridge validates, writes Avro to Kafka, deletes from SQS after the ack | < 1 s | at-least-once |
| 3 | Flink reads it; duplicates dropped on `event_id` | ms | exactly-once state |
| 4 | Joined with the CMDB as of event time, committed to Iceberg | 30-60 s | exactly-once |
| 5 | Upserted into `instance_current_state` | 1-2 s | idempotent upsert |
| 6 | Flapping pattern completes; alert published in a Kafka transaction | 30-60 s | exactly-once |
| 7 | Agent reads committed alerts only | ms | at-least-once + idempotent writes |
| 8 | Agent investigates through MCP tools | seconds | read-only |
| 9 | Claude returns a structured triage; offset committed after it is stored | ~30 s | |

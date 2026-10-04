# 03. Events and contracts

In batch, a bad file fails the job and someone fixes it in the morning. In streaming, a bad record arrives at
3 a.m. among millions of good ones, and the pipeline must keep going. So streams need explicit **contracts**.

## Designing an event

Our event ([`schemas/ec2_state_change.avsc`](../schemas/ec2_state_change.avsc)) follows rules worth copying:

| Field | Rule it illustrates |
|---|---|
| `event_id` | Every event has a unique id. Delivery is at-least-once somewhere, so consumers de-duplicate on it. |
| `event_time` | When it **happened** (from AWS). Never overwrite it with "now". |
| `ingest_time` | When we **received** it. `ingest_time - event_time` measures delay and spots late data. |
| `instance_id` | The entity id, also the Kafka key, so per-entity order holds. |
| `state` | A fact ("is now stopped"), not a command ("stop it"). Events describe what happened. |
| `source`, `trace_id` | Where it came from (SQS message id), for tracing a record end to end. |

**Events vs snapshots.** Our event says "i-0abc entered *stopped* at 07:00:03". Your batch snapshot says "at 08:00
i-0abc was stopped". Events are smaller, ordered and tell you *when*; snapshots are self-contained. Many systems
emit both: events for real-time, periodic snapshots for reconciliation ("did we miss any events?").

**Thin vs fat events.** EventBridge sends a thin event (id and state only). We enrich it in Flink with the CMDB
instead of making the producer look things up. Thin at the edge, enriched in the stream, is the common pattern.

## Why Avro and a Schema Registry

JSON has no schema, so every consumer guesses. With Avro + Schema Registry:

1. The schema is registered once under a **subject** (`ec2.state-changes-value`) and gets an id.
2. Each Kafka message carries 5 bytes of header (magic byte + schema id) and compact binary Avro.
3. Consumers (Flink, Kafka UI) fetch the schema by id and decode. Nobody guesses.
4. The registry refuses incompatible changes. That is the contract.

Protobuf and JSON Schema work the same way with the registry; Avro is the most common with Kafka and Flink.

## Compatibility modes

We set `BACKWARD` (the default and most common):

| Mode | Guarantee | Allowed change | Deploy order |
|---|---|---|---|
| BACKWARD | New consumers can read old data | Delete a field; add a field **with a default** | Consumers first |
| FORWARD | Old consumers can read new data | Add a field; delete a field that had a default | Producers first |
| FULL | Both | Add/delete only fields with defaults | Any order |

Try it: add `{"name": "instance_type", "type": ["null", "string"], "default": null}` to the schema and register it
(allowed). Then try adding a field **without** a default (rejected with HTTP 409):

```bash
curl -s localhost:18085/subjects/ec2.state-changes-value/versions
```

```bash
curl -s localhost:18085/config/ec2.state-changes-value
```

In production, schemas live in git and CI registers them (here [`scripts/init_streaming.py`](../scripts/init_streaming.py) plays CI). Producers
run with `auto.register.schemas=False`, so an unreviewed schema cannot sneak in from application code. Look at
`AvroSerializer(..., conf={"auto.register.schemas": False})` in [`services/ingest/sqs_bridge.py`](../services/ingest/sqs_bridge.py).

## Validation at the edge, and dead-letter queues

[`services/ingest/events.py`](../services/ingest/events.py) turns EventBridge JSON into a record and raises `InvalidEvent` when it can't: bad JSON,
wrong event type, an unknown state like `rebooting`, a time without a timezone. The bridge then:

- writes the **raw** payload to `ec2.state-changes.dlq`, with the reason in a header,
- deletes it from SQS, because retrying a malformed message never helps.

Two kinds of failure, two treatments:

| Failure | Example | Treatment |
|---|---|---|
| Permanent (poison) | Truncated JSON | DLQ immediately, keep going |
| Transient | Kafka unavailable | Do not ack; let SQS redeliver; after 5 tries SQS moves it to its own DLQ |

Someone must own the DLQ: alert on its size (the MCP tool `pipeline_health` reports it), inspect, fix the producer
or the parser, and replay. The simulator sends about 0.3% junk on purpose, so the DLQ always has something in it.

## Contract tests

[`tests/test_events.py`](../tests/test_events.py) pins the contract without any infrastructure: a real EventBridge payload normalizes to the
expected record, the record round-trips through the Avro schema, and thirteen kinds of bad input are rejected.
Run it after every schema or parser change:

```bash
mise exec -- uv run pytest tests/test_events.py -q
```

Next: [04. SQS and the bridge](04-sqs-and-the-bridge.md)

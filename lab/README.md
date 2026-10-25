# Hands-on lab: the streaming stack, one component per sitting

The lessons in [docs/](../docs) explain the design. This track has you poke each running piece until you could debug
it yourself. It follows the data: secrets first (nothing starts without them), then the edge where events enter (SQS
and the bridge), the log they land in (Kafka and its schemas), the CMDB changes that join them (Debezium), the engine
that processes them (the Flink runtime, then Flink SQL's semantics), the outputs (the lakehouse, then the serving
tables and alerts), and finally the AI on top (MCP and the triage agent).

## Fresh start (do this once, before Lab 01)

```bash
cd ~/workstuff/flink-learning && git pull
docker ps --format '{{.Names}}' | grep -v ec2-lakehouse   # should print nothing: one lab stack at a time
cp -n .env.example .env       # then put ANTHROPIC_API_KEY in .env (only Lab 09 needs it)
mise install                  # Python, uv and the `bao` CLI, pinned in mise.toml
mise run setup                # Python deps for the host-side scripts and lab helpers
mise run reset                # DELETES this stack's volumes: Kafka, Postgres, Iceberg, OpenBao (keys, root token). .env stays
mise run up                   # build, OpenBao init, topics, tables, CDC, then the three Flink jobs. ~2-4 min
source lab/env.sh             # once per terminal
fjobs | cut -f1-3             # ec2-alerts, ec2-serving, ec2-lakehouse: RUNNING
```

When you finish a sitting, `mise run down` stops the stack and keeps its data. The next `mise run up` unseals OpenBao
again (Lab 01 shows why it has to). Needs about 8 GB of Docker memory.

Every lab has the same shape:

1. **Look**: real commands against your running stack.
2. **Predict** before you run: each step asks a question (P1, P2, ...). Write your guess down first. The answers, with
   the output I got running every command on a freshly reset stack, are at the bottom of each file.
3. **Break it, then fix it**: at least one failure on purpose, so you've seen it before it happens for real.

A tmux split works well: the lab on one side, a shell on the other, the Flink UI (http://localhost:18081) and Kafka UI
(http://localhost:18080) in a browser.

| Lab | Component | You'll be able to explain | Changes anything? |
|---|---|---|---|
| [01](01-openbao.md) | OpenBao | sealed vs unsealed, policies, dynamic vs static vs KV secrets, the Agent sidecar | seals OpenBao; rotates Debezium's password |
| [02](02-sqs-and-the-bridge.md) | SQS and the bridge | at-least-once at the edge, poison messages vs transient failures | stops the bridge, then Kafka, for about a minute each |
| [03](03-kafka-and-schemas.md) | Kafka and the Schema Registry | keys and partitions, lag, the Avro wire format, compatibility | deletes the schema subject, then re-registers it |
| [04](04-cdc-with-debezium.md) | CDC with Debezium | replication slots, change events, regular vs temporal join | edits CMDB rows; stops Kafka Connect for a minute |
| [05](05-flink-runtime.md) | The Flink runtime | slots, job graphs, incremental checkpoints, crash recovery | kills the TaskManager |
| [06](06-flink-sql-semantics.md) | Flink SQL semantics | changelog modes, watermarks, late data, the mini-batch trap | changes one owner; injects two events |
| [07](07-streaming-lakehouse.md) | The streaming lakehouse | commits per checkpoint, small files, replay, savepoint upgrades | replays the serving job; upgrades the lakehouse job |
| [08](08-serving-and-alerts.md) | Serving tables and alerts | freshness per output, checking an alert, alert precision | injects ten events |
| [09](09-mcp-and-agent.md) | MCP and the triage agent | what agents read, cost and lag control, surviving revocation | revokes DB leases; calls Claude (a few cents) |

Each lab is one sitting of 30-45 minutes. Do them in order: later labs reuse things you saw earlier.

## Helpers

| | What | Where |
|---|---|---|
| `source lab/env.sh` | `$BAO_ADDR`, `$BAO_TOKEN` (the root token, local only), `$FLINK`, `$SR`, `$CONNECT`, `$MCP_URL`, and the functions below | [env.sh](env.sh) |
| `bao ...` | the OpenBao CLI (mise installs it for this folder; otherwise `mise exec -- bao ...`) | |
| `vol_cat PATH` | read a file from OpenBao's volumes (`init/init.json`, `approle/mcp/role_id`, `rendered/flink-jobs.env`) | |
| `role_token NAME` | an AppRole login token for `init`, `simulator`, `bao-agent`, `mcp` or `agent`, for `$( ... )` | |
| `bao_keys PATH`, `bao_field PATH FIELD` | a KV secret's field names and version; one value, for `$( ... )` only | |
| `psql_su DB "SQL"` | psql as the Postgres superuser on `serving`, `cmdb` or `postgres` (break-glass; services never use it) | |
| `pg_as USER PASS DB "SQL"` | psql over the container network, so the password is really checked | |
| `kafka TOOL ARGS` | any Kafka CLI tool, pointed at the broker (`kafka kafka-topics.sh --describe --topic ec2.alerts`) | |
| `offsets TOPIC`, `lag GROUP` | end offsets per partition; a consumer group's lag per partition | |
| `fjobs`, `fjid NAME` | Flink jobs with checkpoint counts; one job's id | |
| `mise run -q lab:sqs -- depth\|peek\|send-junk\|send-event` | SQS from your laptop, including hand-made (and back-dated) EC2 events | [sqs.py](sqs.py) |
| `mise run -q lab:avro [-- PARTITION]` | one Kafka record as raw bytes, wire-format header and decoded Avro | [avro_peek.py](avro_peek.py) |
| `mise run -q lab:duck [-- "SQL" ...]` | DuckDB on your laptop, attached to the Iceberg tables | [duck.py](duck.py) |
| `mise run -q lab:sql -- FILE.sql` | a Flink SQL script from `lab/sql/`, with sources and views preloaded | [sql.sh](sql.sh), [sql/](sql) |
| `mise run -q lab:mcp [-- INSTANCE_ID]` | talk to the MCP server the way an MCP client does | [mcp_call.py](mcp_call.py) |
| `mise run sql` | the Flink SQL client, interactive | |
| `mise run bootstrap`, `mise run submit` | re-run the OpenBao bootstrap; submit any Flink job that isn't running (`RESTORE_FROM=...` to restore) | [mise.toml](../mise.toml) |

None of the labs print a secret value. The ones that need a password read it into a shell variable.

## If something is off

- `lab/env.sh: no root token yet`: run `mise run up` first, from the repo root.
- `bao: command not found`: `mise install`, or prefix with `mise exec --`.
- `mise run up` fails on `bao-bootstrap`, `init` or `submit-jobs`: `docker compose logs <that service>`. After
  `mise run reset` everything starts from scratch; if you deleted only some volumes, reset fully.
- A Flink job is missing after you changed `compose.yaml` or `jobs/`: the image changed, so `mise run up` recreated the
  JobManager, and a session cluster without HA forgets its jobs. `mise run submit` starts them again **from scratch**,
  which appends Kafka's history to Iceberg a second time (Lab 07 explains). On a lab stack, `mise run reset` is cleaner.
- `mise run lab:sql` hangs: an ad-hoc query needs a free slot. `fjobs` and `curl -s $FLINK/taskmanagers | jq '.taskmanagers[0].freeSlots'`
  should show 2 free. Cancel leftover queries in the Flink UI.
- Everything is slow: another lab's stack is probably running. `docker ps` and stop it.
- Your numbers differ: offsets, counts, timings, ids and anything Claude wrote will differ, because the simulator is
  random and the clock moves. The shape and the reasoning should match: which side of the watermark, which sink keeps
  what, which call fails and how it recovers.

## The stack, with the lab that covers each part

```
  01 OpenBao :18200  raft · Shamir seal · AppRole · KV · dynamic + static Postgres users · Agent sidecar · audit
        │ credentials for everything below (the bridge needs none)
        ▼
  simulator ──► 02 SQS (ElasticMQ :19324) ──► 02 sqs-bridge ×2 ──► 03 Kafka :19092 ec2.state-changes (Avro, 6 parts)
  simulator ──► CMDB (Postgres cmdb) ──WAL──► 04 Debezium (Connect :18083) ──► 03 Kafka cmdb.public.ec2_ci (compacted)
                                                                                │
                              05/06 Flink :18081  ec2-lakehouse · ec2-serving · ec2-alerts  (checkpoints → S3)
                                     │                       │                        │
                          07 Iceberg (Lakekeeper :18181   08 Postgres serving    08 Kafka ec2.alerts
                             + SeaweedFS :19000)             instance_current_state   (exactly-once)
                                     │                       │                        │
                                     └──────── 09 MCP server :18002 (6 read-only tools) ◄── 09 triage agent ──► Claude
```

The full architecture diagram, with one event traced step by step, is in [docs/architecture.md](../docs/architecture.md).

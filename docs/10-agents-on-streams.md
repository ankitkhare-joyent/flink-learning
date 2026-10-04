# 10. Agents on streams: MCP tools and event-driven agents

There are three useful ways to combine AI agents with streaming, and this project builds the first two.

## Pattern 1: agents query views that the stream keeps fresh (MCP)

An agent should never query Kafka or Flink state directly. Flink's job is to **materialize** query-friendly views,
and the agent reads those through tools:

| MCP tool | Reads | Fresh to within | Why this source |
|---|---|---|---|
| `get_instance_state` | Postgres `instance_current_state` | ~1-2 s | Point lookup in milliseconds |
| `find_instances` | same | ~1-2 s | Filters on current state |
| `fleet_activity` | Postgres `state_change_counts_1m` | ~1.5 min (window + watermark) | Pre-aggregated by Flink |
| `instance_timeline` | Iceberg `state_changes` via DuckDB | ~30-60 s | Full history, owner as of each event |
| `recent_alerts` | Kafka `ec2.alerts` + triage table | ~30 s | Reads the stream itself, without moving any consumer group |
| `pipeline_health` | Flink REST API, Kafka lag, DLQ size, serving freshness | live | Can the data be trusted right now? |

Two rules matter more with streams than with batch:

- **Every answer carries its freshness** (`freshness_seconds`, `latest_window_end`, notes). "Nothing happened" and
  "the pipeline is broken" look identical unless you check, which is why `pipeline_health` exists and the agent's
  system prompt says to call it first.
- **Read-only by design**: every tool is annotated `read_only_hint=True`, and the server's database user (minted by
  OpenBao) can only `SELECT`. The agent can investigate anything and change nothing.

The server ([`services/mcp_server/server.py`](../services/mcp_server/server.py)) follows the same rules as [`../learn-ai-agent/mcp_server`](../../learn-ai-agent/mcp_server): few
task-shaped tools, typed and bounded inputs (instance ids must match `^i-[0-9a-f]{8,17}$`, `limit <= 50`), compact
structured outputs with an output schema, and errors phrased so the model can recover. Over HTTP it runs stateless
with JSON responses, so replicas can scale behind a load balancer. It accepts one bearer token from OpenBao; the
sibling project shows the full OAuth 2.1 setup you'd use for user-facing access.

### Use it from Claude Code

With the stack running, add the server over HTTP (the token comes from OpenBao):

```bash
claude mcp add --transport http ec2-streaming http://localhost:18002/mcp --header "Authorization: Bearer $(docker compose run --rm --no-deps bao-bootstrap python -m scripts.local_creds mcp-token)"
```

Then ask things like "Which tier1 prod instances changed state in the last 10 minutes?" or "Is the streaming
pipeline healthy?".

## Pattern 2: the agent is a stream consumer (event-driven agent)

[`services/agent/triage.py`](../services/agent/triage.py) is a Kafka consumer whose processing step happens to be an LLM agent:

```
ec2.alerts --(read_committed)--> triage agent --> Claude + MCP tools --> ec2.alerts.triaged + Postgres alert_triage
                                      \--> ec2.alerts.dlq after 3 failed attempts
```

The agent loop itself is small: the Anthropic SDK's tool runner drives Claude through the MCP tools and returns a
`TriageResult` (severity, summary, likely cause, evidence, next steps, who to notify, data freshness) enforced by
structured outputs. What makes it production-grade is the streaming discipline around it:

| Concern | How it's handled |
|---|---|
| Crash mid-triage | Offset committed only after outputs are written: at-least-once |
| Redelivery after a crash | `already_triaged()` skips alert ids already in Postgres; writes are upserts |
| Alert storm / cost | Sequential processing with a per-hour budget (`AGENT_TRIAGES_PER_HOUR=20`). Excess becomes consumer lag, not spend |
| Stale alerts | Alerts older than 30 minutes are skipped; a late triage helps nobody |
| Poison alert | Three attempts with backoff, then the DLQ, then commit, so one alert can't block the partition |
| A new agent deployment | `auto.offset.reset=latest`: it doesn't triage the whole history on first start |
| Model refusal | Server-side fallback (`fallbacks="default"`) retries on a fallback model; a final refusal goes to the DLQ |
| Secrets | Anthropic key and MCP token from OpenBao; DB user minted per hour |
| Observability | Prometheus counters for outcomes and tokens, a latency histogram, structured logs per alert |

Agents are slow (seconds to minutes) and expensive compared with stream operators (microseconds). Never put an LLM
call *inside* a high-volume path. Put it after a filter that reduces volume by orders of magnitude, as here:
thousands of state changes per hour become a handful of alerts.

**Kafka share groups** (lesson 02) fit this workload well: several agent replicas could take alerts from the same
partition with per-record acknowledgements, so one slow triage doesn't hold up the rest of its partition.

## Pattern 3: AI inside the stream processor

Flink itself is growing AI features. They are worth knowing, though this project doesn't use them:

- **`ML_PREDICT`** (Flink SQL, 2.1+; Table API in 2.2): `CREATE MODEL` registers an LLM or ML endpoint; `ML_PREDICT`
  calls it per row, asynchronously. Good for classifying or enriching every event (sentiment, categorization).
- **`VECTOR_SEARCH`** (2.2): similarity search against a vector store from inside a streaming query, for real-time
  retrieval (RAG) on each event.
- **Apache Flink Agents** (sub-project, 0.3 preview): event-driven agents that run *as* Flink jobs, with Flink's
  state, checkpointing and exactly-once for agent memory and actions. Not yet production-stable (APIs may change).

When to use which: per-event model calls at scale belong in the stream processor (pattern 3). Investigations that
need many tool calls and judgement belong in a separate agent service fed by a filtered stream (pattern 2). Questions
from humans go through tools over materialized views (pattern 1).

## Try it

Start the agent (needs `ANTHROPIC_API_KEY` in `.env`):

```bash
docker compose --profile agent up -d
```

```bash
docker compose logs -f triage-agent
```

Read the triages:

```bash
docker compose exec postgres psql -U postgres -d serving -c "SELECT alert_type, severity, summary, notify FROM alert_triage ORDER BY triaged_at DESC LIMIT 5"
```

Next: [11. Operating in production](11-operating-in-production.md)

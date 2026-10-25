# Lab 09 · MCP and the triage agent: AI on top of the stream

One sitting, about 40 minutes. Background: [docs/10](../docs/10-agents-on-streams.md). Code:
[services/mcp_server/server.py](../services/mcp_server/server.py), [services/agent/triage.py](../services/agent/triage.py),
[services/common/secrets.py](../services/common/secrets.py) (`DynamicDsn`), [lab/mcp_call.py](mcp_call.py).

The last stop: an MCP server that turns Flink's outputs into tools, and an agent that consumes alerts and investigates
them with those tools. After this lab you can explain:

- what an MCP client sees, and which data each tool reads
- why the history tool lags the current-state tool
- what one triage costs, and how the agent keeps cost and lag under control
- what happens to the tools when someone revokes their database credentials

The agent part calls Claude: about 3 triages, a few cents each. Everything before section 3 is free.

## Before you start

```bash
source lab/env.sh
grep -c '^ANTHROPIC_API_KEY=.' .env     # 1 means the key is there; then: mise run bootstrap
```

## 1. Be an MCP client

```bash
mise run -q lab:mcp
```

[lab/mcp_call.py](mcp_call.py) connects over Streamable HTTP with the bearer token from OpenBao, lists the tools, calls
each one, then makes one bad call and one call without a token.

**Predict (P1):** how many tools, how many of them can change anything, and what does the request without a token get?

**Predict (P2):** `get_instance_state` and `instance_timeline` describe the same instance. Can the timeline's newest
event be *older* than the current state's `since`? Why?

```bash
I=<an instance id from the output>
mise run -q lab:mcp -- $I | grep -E "get_instance_state|instance_timeline"
mise run -q lab:duck -- "select max(event_time) from lake.ec2_stream.state_changes where instance_id='$I'"
```

## 2. What the server costs the database

```bash
psql_su postgres "select rolname, rolvaliduntil from pg_roles where rolname like 'v-approle-mcp-read%'"
```

**Predict (P3):** every tool call opens a new Postgres connection. How many database *users* exist for the MCP server?

## 3. The agent

Start it with a budget of 3 triages per hour:

```bash
AGENT_TRIAGES_PER_HOUR=3 mise run -q agent
docker compose logs -f --no-log-prefix triage-agent | grep '^{' | jq -c '{event, alert_type, severity, seconds, input, output, turns, wait_seconds}'
```

(Ctrl+C when you see a "budget used" line.)

**Predict (P4):** how long does one triage take, how many model turns, how many input tokens? What does the agent do
after the third one?

```bash
psql_su serving "select alert_type, severity, left(summary, 200) as summary, notify from alert_triage order by triaged_at"
kafka kafka-console-consumer.sh --topic ec2.alerts.triaged --from-beginning --max-messages 1 --timeout-ms 5000 \
  | jq -c '{evidence: (.triage.evidence | length), next_steps: (.triage.next_steps | length), data_freshness: .triage.data_freshness}'
lag triage-agent | tail -1
docker compose stop triage-agent
```

**Predict (P5):** what does `lag triage-agent` show while the agent waits on its budget, and why is that the right
behaviour?

## Break it, then fix it

**Break: revoke the MCP server's database credentials** (the incident-response move, as in Lab 01).

```bash
mise run -q lab:mcp | grep -E "pipeline_health:|get_instance_state i-"
bao lease revoke -prefix database/creds/mcp-reader
mise run -q lab:mcp | grep -E "pipeline_health:|get_instance_state i-|Error executing"
docker compose logs --no-log-prefix --since 2m mcp | grep '^{' | jq -c 'select(.event | test("credentials")) | {event, user}'
```

**Predict (P6):** do the tools fail after the revocation? For how long?

**The fix** is already in the code; this lab found the bug. Read `DynamicDsn.invalidate()` in
[services/common/secrets.py](../services/common/secrets.py), and `Backends.sql()` in
[services/mcp_server/server.py](../services/mcp_server/server.py).

## Check your predictions

**P1.** Six tools, all `read-only=True`: `get_instance_state`, `find_instances`, `instance_timeline`, `fleet_activity`,
`recent_alerts`, `pipeline_health`. None can change anything, and the database user behind them can only `SELECT`. The
bad call (`i-not-real`) returns `isError = True` with the pydantic validation message, before any query runs. No token:
`401` with `Bearer error="invalid_token"`. The server is an OAuth-style resource server even with a static token.

**P2.** Yes. When I ran it, the current state was `stopping since 19:43:45`, but the timeline's newest event was
`19:41:01`. The current state comes from Postgres (about a second behind). The timeline comes from Iceberg, which is
behind by the watermark plus one checkpoint, up to about a minute. A minute later both showed `19:43:55`. That's why
every tool reports freshness, and why the agent's prompt asks it to say how current its data was.

**P3.** One. `DynamicDsn` caches the OpenBao credentials and hands the same user to every new connection until five
minutes before its lease expires. One user per server process per hour, however many tool calls it serves.

**P4.** 21-45 s per triage, 3 model turns, about 17,000-18,000 input and 1,800-2,300 output tokens. The first triage was
a `CRITICAL_TERMINATION` of a tier1 prod `payments-api` instance, rated `high`. The two FLAPPING alerts were rated
`low`: one was a staging tier3 batch instance, the other had since been terminated. One triage noticed that many
instances of the same app were churning, which is the noise Lab 08 measured. Each triage cited specific times and
reported its data freshness (`serving data 5.2 s old, ... 43 records in the DLQ`). After the third one, the log says
`hourly triage budget used; waiting (consumer lag will grow)` with `wait_seconds` close to an hour.

**P5.** Lag grows (7 alerts when I looked, and climbing). Waiting is correct: the alerts stay safely in Kafka, nothing is
dropped, and cost stays capped. If you raise the budget, the agent works through the backlog, skipping any alert older
than `max_alert_age_minutes` (30), because a triage that old helps nobody.

**P6.** With the original code, yes, for about 55 minutes. Every call failed with `psycopg.OperationalError ...
password authentication failed for user "v-approle-mcp-read-..."` and the client saw `Error executing tool
pipeline_health`. `DynamicDsn` kept handing out the cached, revoked user until five minutes before its lease would have
expired. With the fix, the next call succeeds, and the log shows the whole recovery:

```
{"event":"database credentials issued","user":"v-approle-mcp-read-kq921MvmyNMNmWkQkcNc-1792957840"}
{"event":"database credentials rejected; fetching new ones","user":"v-approle-mcp-read-kq921MvmyNMNmWkQkcNc-1792957840"}
{"event":"database credentials issued","user":"v-approle-mcp-read-jUGHwVhd4749j1kQPJdc-1792957854"}
```

On an authentication failure the server forgets the cached credentials, asks OpenBao for a new user, and retries once.
The agent's own database writes do the same. Revocation is only a safe incident-response tool if every client
recovers from it, so test it before you need it.

## What to remember

- An agent reads what Flink materialized, through read-only tools that report freshness.
- Different tools sit at different points of the pipeline, so they lag by different amounts.
- An event-driven agent is a Kafka consumer: budget it, let lag absorb bursts, skip what's stale.
- Credential revocation must be survivable. Make clients re-fetch on an authentication failure.

That's the track. [docs/12](../docs/12-exercises.md) has open-ended exercises once you've done all nine.

# Lab 01 · OpenBao: seal, policies, and three kinds of database password

One sitting, about 35 minutes. Background: [docs/09](../docs/09-secrets-with-openbao.md). Code:
[scripts/bao_bootstrap.py](../scripts/bao_bootstrap.py) (policies, roles), [services/common/secrets.py](../services/common/secrets.py)
(how services log in), [stack/openbao/agent.hcl](../stack/openbao/agent.hcl) (the sidecar's templates).

Secrets come first because nothing else starts without them: Debezium, Flink, the simulator, the MCP server and the
agent all get their credentials from OpenBao. After this lab you can explain:

- what "sealed" means, and what keeps working while OpenBao is sealed
- how a policy limits a token, checked on every request
- the difference between a dynamic user, a rotated static user and a KV secret
- what happens to a connector when its password rotates, and how the Agent sidecar fixes it

## Before you start

```bash
source lab/env.sh
bao status | grep -E "Sealed|Total Shares|Storage Type"
```

(`bao` comes from mise. If your shell isn't mise-activated, use `mise exec -- bao ...`.)

## 1. What is configured

```bash
bao auth list -format=json | jq -r 'keys[]'
bao secrets list -format=json | jq -r 'to_entries[] | "\(.key) \(.value.type) \(.value.options.version // "")"'
bao policy list
bao policy read mcp
```

**Predict (P1):** the MCP server's policy is three lines. Which paths do you expect, and can it read the Anthropic key?

## 2. Be the MCP server for a minute

Log in with the MCP server's AppRole, exactly as [services/common/secrets.py](../services/common/secrets.py) does.

```bash
T=$(role_token mcp)
BAO_TOKEN=$T bao token lookup -format=json | jq -c '.data | {policies, ttl, renewable}'
TOKEN=$T bao_keys ec2-streaming/mcp
TOKEN=$T bao_keys ec2-streaming/anthropic
```

**Predict (P2):** what TTL does the token have, and what does the second `bao_keys` return?

Now ask for a database user, the way the MCP server does on its first tool call.

**Predict (P3):** before you ask, how many `v-...` users exist in Postgres? (The stack has been up a few minutes and
nobody has called an MCP tool yet.)

```bash
psql_su postgres "select rolname, rolvaliduntil from pg_roles where rolname like 'v-%'"
C=$(curl -s -H "X-Vault-Token: $T" $BAO_ADDR/v1/database/creds/mcp-reader)
jq -c '{lease_duration, renewable, username: .data.username}' <<<"$C"
U=$(jq -r .data.username <<<"$C"); P=$(jq -r .data.password <<<"$C"); L=$(jq -r .lease_id <<<"$C")
psql_su postgres "select rolname, rolvaliduntil from pg_roles where rolname like 'v-%'"
```

**Predict (P4):** can that user count rows in the serving table? Delete them?

```bash
pg_as "$U" "$P" serving "select current_user, count(*) from instance_current_state"
pg_as "$U" "$P" serving "delete from instance_current_state"
```

**Predict (P5):** you revoke the lease. What happens to the user?

```bash
bao lease revoke "$L"
pg_as "$U" "$P" serving "select 1" | tail -1
```

## 3. Static roles and the sidecar

Debezium can't swap usernames mid-stream, so it uses a *static role*: a fixed user whose password OpenBao owns.

```bash
bao read -format=json database/static-creds/debezium | jq -c '.data | {username, ttl, rotation_period, last_vault_rotation}'
```

**Predict (P6):** `stack/postgres/init.sql` creates `debezium` with no password. Does the old dev password
`debezium-local` work?

```bash
pg_as debezium debezium-local cmdb "select 1" | tail -1
```

Kafka Connect and Flink can't talk to OpenBao, so the `bao-agent` container renders files for them. Field names only:

```bash
for f in debezium.properties flink-s3.yaml flink-jobs.env; do echo "$f: $(vol_cat rendered/$f | sed -E 's/[=:].*//' | tr '\n' ' ')"; done
```

The audit log shows every secret that was handed out, with the values replaced by HMACs:

```bash
docker compose logs --no-log-prefix openbao | grep '^{' \
  | jq -c 'select(.type=="response" and .request.path=="database/creds/mcp-reader") | {time, policies: .auth.policies, password: .response.data.password}' | head -1
```

## Break it, then fix it

**Break 1: OpenBao restarts.** This stack uses one Shamir key share, not auto-unseal.

**Predict (P7):** after a restart, is OpenBao sealed? Does the pipeline (bridge, Flink, serving table) stop?

```bash
docker compose restart openbao && sleep 3
bao status | grep -E "^Sealed|Unseal Progress"
role_token mcp | wc -c
curl -s localhost:19400/metrics | grep '^bridge_kafka_records_produced_total{topic="ec2.state-changes"}'; sleep 5
curl -s localhost:19400/metrics | grep '^bridge_kafka_records_produced_total{topic="ec2.state-changes"}'
psql_su serving "select max(updated_at) as last_write, now() at time zone 'utc' as now from instance_current_state"
```

**Fix:** re-run the idempotent bootstrap, which unseals with the key it stored at init:

```bash
mise run bootstrap && docker wait ec2-streaming-bao-bootstrap-1
docker compose logs --no-log-prefix bao-bootstrap | grep -E "unsealed|complete"
```

**Break 2: rotate Debezium's password.** The incident-response move: "rotate the CDC password now".

**Predict (P8):** right after the rotation, does CDC keep working? And after you restart the connector?

```bash
bao write -f database/rotate-role/debezium
curl -s $CONNECT/connectors/cmdb-source/status | jq -c '[.connector.state, .tasks[0].state]'
curl -s -X POST "$CONNECT/connectors/cmdb-source/restart?includeTasks=true" -o /dev/null && sleep 15
curl -s $CONNECT/connectors/cmdb-source/status | jq -c '[.connector.state, .tasks[0].state]'
curl -s $CONNECT/connectors/cmdb-source/status | jq -r '.tasks[0].trace' | grep -m1 FATAL
```

**Fix:** make the sidecar re-render now instead of in up to 5 minutes, then restart only the failed task:

```bash
docker compose restart bao-agent && sleep 6
curl -s -X POST "$CONNECT/connectors/cmdb-source/restart?includeTasks=true&onlyFailed=true" -o /dev/null && sleep 15
curl -s $CONNECT/connectors/cmdb-source/status | jq -c '[.connector.state, .tasks[0].state]'
```

## Check your predictions

**P1.** Read `database/creds/mcp-reader`, `secret/data/ec2-streaming/mcp` and `secret/data/ec2-streaming/seaweedfs`.
Nothing else, so no Anthropic key. Mounts on a fresh stack: `approle/` and `token/` auth; `database/`, `secret/`
(kv version 2) plus the built-ins `cubbyhole/`, `identity/`, `sys/`. Policies: `agent bao-agent default init mcp
simulator root`.

**P2.** `{"policies":["default","mcp"],"ttl":3599,"renewable":true}`: one hour, from `token_ttl="1h"` on the AppRole.
`bao_keys ec2-streaming/mcp` gives `{"fields":["bearer_token"],"version":1}`. The Anthropic one gives
`permission denied`. The policy is checked on every request, not at login.

**P3.** **Zero.** The MCP server asks for database credentials lazily, on its first tool call
(`DynamicDsn.get()`), so before anyone calls a tool no `v-` user exists. After your request there is one:

```
 v-approle-mcp-read-ZlQzfwIOuJfugxGLnkHm-1792953726 | 2026-10-25 19:42:11+00
```

`lease_duration: 3600`, `renewable: true`. The name encodes the auth method, the role, a random part and the Unix time.
`rolvaliduntil` is one hour out: Postgres itself refuses the password after that, even if OpenBao never revoked it.

**P4.** Count: yes (`414` instances at that moment). Delete: `ERROR: permission denied for table
instance_current_state`. The role's creation SQL grants `SELECT` only.

**P5.** `All revocation operations queued successfully!`, then `FATAL: password authentication failed` for that user.
OpenBao ran the revocation SQL (`REVOKE ALL ... ; DROP ROLE`).

**P6.** No: `FATAL: password authentication failed for user "debezium"`. The bootstrap created the static role, and
OpenBao set a random password straight away. `ttl` counts down from 30 days (`2591810` seconds left when I ran it).
The rendered files contain `password`, `s3.access-key s3.secret-key`, and `SERVING_DB_USER SERVING_DB_PASSWORD
S3_ACCESS_KEY S3_SECRET_KEY`. The audit entry shows `"password":"hmac-sha256:135dd6..."`: you can tell *that* a secret
was issued, and to which policy, without the log ever containing it.

**P7.** Sealed: `Sealed true`, `Unseal Progress 0/1`, and `/v1/sys/health` returns 503. A new login gets nothing
(`role_token` prints an empty string). But the pipeline doesn't notice: the bridge counter kept climbing (705 to 726
records in 5 seconds), and the serving table's last write was one second old. Running services already hold their
credentials (the bridge needs none at all; Flink and Debezium read rendered files at start; the MCP server caches its
DB user until 5 minutes before expiry). OpenBao is on the critical path for *starting* things and for renewals, not
for traffic in flight. That's why production runs 3-5 nodes with KMS auto-unseal, so a restart never needs a human.

**P8.** Right after the rotation: still `["RUNNING","RUNNING"]`. Debezium's replication connection was authenticated
when it opened, and rotating a password doesn't kill existing sessions. After the restart:
`["RUNNING","FAILED"]`, with `FATAL: password authentication failed for user "debezium"` in the trace. The restarted
task read `/run/secrets/debezium.properties`, which still held the old password, because `bao-agent` re-renders
static secrets only every 5 minutes (`static_secret_render_interval`). After restarting `bao-agent` and the task:
`["RUNNING","RUNNING"]`. In production you'd pair every rotation with a re-render and a rolling restart, or wait out
the render interval before restarting anything.

## What to remember

- Sealed means OpenBao can't decrypt anything. Running services carry on; new logins and new credentials stop.
- A policy is a list of paths, checked on every request. Least privilege is visible in three lines.
- Dynamic users exist for a lease; static roles keep a username and rotate its password; KV holds everything else.
- A rotation only reaches a running app when it reconnects *and* reads the new value. Test that path.

Next: [Lab 02 · SQS and the bridge](02-sqs-and-the-bridge.md).

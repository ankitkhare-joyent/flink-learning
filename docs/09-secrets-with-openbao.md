# 09. Secrets with OpenBao

## Why OpenBao, not Vault

HashiCorp Vault is the industry's reference secret manager. In August 2023 it moved from the open-source MPL-2.0
licence to the Business Source License (BSL), which forbids offering it as a competing product. The Linux Foundation
forked the last open version as **OpenBao** (MPL-2.0, governed by the OpenSSF). The API, the CLI (`bao` instead of
`vault`), the HCL policies and the concepts are the same, and client libraries such as `hvac` work against either.
Everything you learn here applies to Vault at work.

## The problem it solves

Look at where secrets *were* in this repo before OpenBao: passwords in [`compose.yaml`](../compose.yaml), in the Debezium connector
JSON, in Flink SQL. Each copy is a leak waiting to happen, nobody rotates them, and when one leaks you can't tell who
used it. A secret manager fixes this with four ideas:

1. **One place** holds secrets, encrypted at rest, with an audit log of every read.
2. **Identity, not passwords**: a service proves *what it is* (AppRole, a Kubernetes service account, an AWS IAM
   role) and gets a short-lived token. No long-lived password to the secret store exists in config.
3. **Least privilege**: policies say exactly which paths each identity may read.
4. **Short-lived and dynamic secrets**: credentials that expire on their own, or are created per request.

## Core concepts

| Concept | What it is | In this repo |
|---|---|---|
| **Seal / unseal** | Data is encrypted with a key that is itself encrypted. Starting the server leaves it *sealed* (can't decrypt anything) until unseal key shares are provided | `bao-bootstrap` unseals on every start. Production: auto-unseal with AWS KMS |
| **Storage** | Where encrypted data lives | Integrated Raft storage, one node ([`stack/openbao/server.hcl`](../stack/openbao/server.hcl)). Production: 3 or 5 nodes |
| **Auth method** | How a client proves identity | `approle` (role_id + secret_id). Production: `kubernetes` or `aws` |
| **Token** | What a login returns; carries policies and a TTL | 1 h TTL, renewable to 24 h |
| **Policy** | HCL listing paths and capabilities | `POLICIES` in [`scripts/bao_bootstrap.py`](../scripts/bao_bootstrap.py) |
| **Secrets engine** | A plug-in mounted at a path that stores or generates secrets | `secret/` (KV v2), `database/` |
| **Lease** | TTL attached to a dynamic secret; revoked at expiry | Dynamic Postgres users live 1 h |
| **Audit device** | Logs every request; secret values are HMAC'd | `audit "file" "stdout"` in `server.hcl` |
| **Agent** | A sidecar that logs in, keeps tokens fresh and renders secrets into files | `bao-agent` container |

## The three kinds of secret used here

**1. Static secrets (KV v2)** at `secret/ec2-streaming/*`: the SeaweedFS keys, the MCP bearer token (generated once,
never printed) and the Anthropic API key (copied from your `.env`). KV v2 keeps versions, so a bad write can be
rolled back.

**2. Dynamic database credentials** (`database/creds/mcp-reader`, `database/creds/agent-writer`). When the MCP server
needs Postgres, it asks OpenBao, which runs `CREATE ROLE "v-approle-mcp-reader-..." ... VALID UNTIL ...` and grants
`SELECT`. The user exists for 1 hour, then OpenBao drops it. A leaked password is useless an hour later, and the
audit log shows which service got which user. See `DynamicDsn` in [`services/common/secrets.py`](../services/common/secrets.py): it asks for new
credentials five minutes before the old ones expire.

**3. Static roles** (`database/static-creds/debezium`, `flink`, `app`). These users exist up front (created *without*
a password in [`stack/postgres/init.sql`](../stack/postgres/init.sql)); OpenBao sets a random password immediately and rotates it every 30 days.
Long-running consumers (Debezium's replication connection, a Flink job's sink) can't swap usernames mid-stream, so
they get a stable username with a rotating password.

## Who gets what (least privilege)

| Identity (AppRole) | Can read | Used by |
|---|---|---|
| `init` | S3 keys | one-shot setup |
| `simulator` | `app` static role | writing CMDB rows |
| `bao-agent` | `debezium` and `flink` static roles, S3 keys | rendering files for Connect and Flink |
| `mcp` | dynamic `mcp-reader` (SELECT only), MCP token, S3 keys | the MCP server |
| `agent` | dynamic `agent-writer` (insert/update `alert_triage` only), MCP token, Anthropic key | the triage agent |

The SQS bridge has **no** secrets at all. On AWS it would get SQS and MSK access from its IAM role (IRSA / EKS Pod
Identity). The best secret is the one that doesn't exist.

## Two ways an app consumes secrets

**The app talks to OpenBao** (MCP server, agent, simulator, init). [`services/common/secrets.py`](../services/common/secrets.py) logs in with AppRole,
reads KV, requests DB credentials and logs in again before its token expires. Best when the app is yours.

**A sidecar renders files** (Kafka Connect, Flink). They know nothing about OpenBao, so `bao-agent` logs in and renders
templates ([`stack/openbao/agent.hcl`](../stack/openbao/agent.hcl)):

```
{{ with secret "database/static-creds/debezium" }}password={{ .Data.password }}{{ end }}
```

into `/rendered/debezium.properties`, which Connect reads through its `FileConfigProvider`. Flink's S3 keys and the
JDBC password for the SQL jobs come the same way. The agent re-renders every 5 minutes, so a rotation reaches the files.
On Kubernetes the same pattern is the Vault/OpenBao Agent Injector, the Secrets Store CSI driver, or the External
Secrets Operator syncing into a Kubernetes Secret.

A rotated password in a *file* does not reach a connection that is already open: Debezium and the Flink JDBC sink
pick it up on their next restart. That's why those roles rotate monthly rather than hourly, and why rotation is
usually paired with a rolling restart.

## Local shortcuts (never do these in production)

[`scripts/bao_bootstrap.py`](../scripts/bao_bootstrap.py) lists them at the top: one unseal key share stored next to the root token in a Docker
volume, AppRole secret_ids in plain files with no TTL, and no TLS. Production uses KMS auto-unseal, revokes the root
token after setup, uses platform identity (Kubernetes or IAM auth) instead of AppRole files, and TLS everywhere.

## Try it

Sign in to the UI at http://localhost:18200/ui with the root token (local only):

```bash
docker compose run --rm --no-deps bao-bootstrap python -m scripts.local_creds root-token
```

Watch a dynamic user appear in Postgres when the MCP server starts (names begin with `v-`):

```bash
docker compose exec postgres psql -U postgres -c "SELECT rolname, rolvaliduntil FROM pg_roles WHERE rolname LIKE 'v-%'"
```

Rotate Debezium's password by hand, then restart the connector task so it picks it up:

```bash
docker compose exec -e BAO_ADDR=http://127.0.0.1:8200 -e BAO_TOKEN=<root token> openbao bao write -f database/rotate-role/debezium
```

```bash
curl -s -X POST "localhost:18083/connectors/cmdb-source/restart?includeTasks=true"
```

Read the audit log (secret values appear as `hmac-sha256:...`):

```bash
docker compose logs openbao | grep '"type":"response"' | tail -3
```

Next: [10. Agents on streams](10-agents-on-streams.md)

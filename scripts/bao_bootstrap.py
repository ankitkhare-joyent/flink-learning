"""Initialise, unseal and configure OpenBao for the streaming stack. Idempotent; runs on every `up`.

Each step names the equivalent `bao` CLI command, which is what you would type (or put in Terraform/OpenTofu
with the vault provider) against a real cluster.

LOCAL-ONLY SHORTCUTS, called out so you never copy them to production:
  * 1 unseal key share and the root token are written to a Docker volume. In production: auto-unseal with
    AWS KMS (`seal "awskms"`), several key shares held by different people, and the root token revoked
    right after setup (`bao token revoke <root>`).
  * AppRole secret_ids are written to files with no TTL. In production the platform (Kubernetes auth,
    AWS IAM auth) proves identity, or a trusted orchestrator delivers a response-wrapped secret_id.
  * TLS is off. In production every listener has TLS.
"""
import json
import os
import secrets
import time
from pathlib import Path

import hvac

ADDR = os.environ.get("BAO_ADDR", "http://openbao:8200")
INIT_FILE = Path(os.environ.get("BAO_INIT_FILE", "/bao-init/init.json"))
APPROLE_DIR = Path(os.environ.get("BAO_APPROLE_OUT", "/bao-approle"))
PG_ADMIN_USER = os.environ.get("PG_ADMIN_USER", "postgres")
PG_ADMIN_PASSWORD = os.environ["PG_ADMIN_PASSWORD"]
KV = "secret"

# ------------------------------------------------------------------ who may read what (least privilege)
POLICIES = {
    # One-shot setup job: needs the S3 keys to create buckets and the Iceberg warehouse.
    "init": 'path "secret/data/ec2-streaming/seaweedfs" { capabilities = ["read"] }',
    # The simulator writes CMDB rows as the `app` user, whose password OpenBao rotates.
    "simulator": 'path "database/static-creds/app" { capabilities = ["read"] }',
    # Sidecar that renders config files for Debezium (Kafka Connect) and Flink.
    "bao-agent": """
path "database/static-creds/debezium" { capabilities = ["read"] }
path "database/static-creds/flink"    { capabilities = ["read"] }
path "secret/data/ec2-streaming/seaweedfs" { capabilities = ["read"] }
""",
    "mcp": """
path "database/creds/mcp-reader" { capabilities = ["read"] }
path "secret/data/ec2-streaming/mcp"       { capabilities = ["read"] }
path "secret/data/ec2-streaming/seaweedfs" { capabilities = ["read"] }
""",
    "agent": """
path "database/creds/agent-writer" { capabilities = ["read"] }
path "secret/data/ec2-streaming/mcp"       { capabilities = ["read"] }
path "secret/data/ec2-streaming/anthropic" { capabilities = ["read"] }
""",
}

# ------------------------------------------------------------------ Postgres credentials
# Static roles: users that already exist (created in stack/postgres/init.sql *without* a password).
# OpenBao sets a random password immediately and rotates it every rotation_period. Long-running consumers
# (Debezium, Flink) use these because a connector cannot swap users mid-stream.
STATIC_ROLES = {"debezium": "cmdb", "flink": "serving", "app": "cmdb"}
# Dynamic roles: a brand-new user per request, dropped when the lease expires. Best for short sessions.
DYNAMIC_ROLES = {
    "mcp-reader": ("serving", ['CREATE ROLE "{{name}}" WITH LOGIN PASSWORD \'{{password}}\' VALID UNTIL \'{{expiration}}\';',
                               'GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{{name}}";']),
    "agent-writer": ("serving", ['CREATE ROLE "{{name}}" WITH LOGIN PASSWORD \'{{password}}\' VALID UNTIL \'{{expiration}}\';',
                                 'GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{{name}}";',
                                 'GRANT INSERT, UPDATE ON alert_triage TO "{{name}}";']),
}
REVOKE = ['REVOKE ALL ON ALL TABLES IN SCHEMA public FROM "{{name}}";', 'DROP ROLE IF EXISTS "{{name}}";']


def wait_for_server(c: hvac.Client) -> None:
    for _ in range(60):
        try:
            c.sys.read_health_status(method="GET")
            return
        except Exception:  # noqa: BLE001
            time.sleep(1)
    raise SystemExit("openbao not reachable")


def init_and_unseal(c: hvac.Client) -> str:
    if not c.sys.is_initialized():                       # bao operator init -key-shares=1 -key-threshold=1
        out = c.sys.initialize(secret_shares=1, secret_threshold=1)
        INIT_FILE.parent.mkdir(parents=True, exist_ok=True)
        INIT_FILE.write_text(json.dumps({"keys": out["keys"], "root_token": out["root_token"]}))
        INIT_FILE.chmod(0o600)
        print("openbao: initialised (1 key share; local only)")
    init = json.loads(INIT_FILE.read_text())
    if c.sys.is_sealed():                                # bao operator unseal <key>
        c.sys.submit_unseal_key(init["keys"][0])
        print("openbao: unsealed")
    for _ in range(30):                                  # raft needs a moment to elect itself leader
        try:
            c.token = init["root_token"]
            c.sys.list_mounted_secrets_engines()
            return init["root_token"]
        except Exception:  # noqa: BLE001
            time.sleep(1)
    raise SystemExit("openbao: not active after unseal")


def ensure_mount(c: hvac.Client, path: str, kind: str, **kw) -> None:
    if f"{path}/" not in c.sys.list_mounted_secrets_engines()["data"]:
        c.sys.enable_secrets_engine(backend_type=kind, path=path, **kw)   # bao secrets enable -path=.. kind
        print(f"secrets engine {kind} at {path}/: enabled")


def kv_secrets(c: hvac.Client) -> None:
    ensure_mount(c, KV, "kv", options={"version": "2"})
    kv = c.secrets.kv.v2

    def read(name):
        for _ in range(30):   # a freshly enabled KV v2 mount answers "upgrading" for a few seconds
            try:
                return kv.read_secret_version(path=f"ec2-streaming/{name}", mount_point=KV,
                                              raise_on_deleted_version=True)["data"]["data"]
            except hvac.exceptions.InvalidPath:
                return None
            except hvac.exceptions.InvalidRequest as e:
                if "Upgrading" not in str(e):
                    raise
                time.sleep(1)
        raise SystemExit("kv mount never became ready")

    def put(name, data):                                 # bao kv put secret/ec2-streaming/<name> k=v
        kv.create_or_update_secret(path=f"ec2-streaming/{name}", secret=data, mount_point=KV)
        print(f"kv secret/ec2-streaming/{name}: written (version history kept)")

    s3 = {"access_key": os.environ["S3_ACCESS_KEY"], "secret_key": os.environ["S3_SECRET_KEY"]}
    if read("seaweedfs") != s3:
        put("seaweedfs", s3)
    if read("mcp") is None:                              # generated once, never printed
        put("mcp", {"bearer_token": secrets.token_urlsafe(32)})
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    current = read("anthropic")
    if key and (current or {}).get("api_key") != key:
        put("anthropic", {"api_key": key})
    elif not key and current is None:
        print("kv secret/ec2-streaming/anthropic: skipped (no ANTHROPIC_API_KEY in .env)")


def database_engine(c: hvac.Client) -> None:
    ensure_mount(c, "database", "database")
    db = c.secrets.database
    for name in ("cmdb", "serving"):                     # bao write database/config/<db> ...
        db.configure(name=name, plugin_name="postgresql-database-plugin", verify_connection=True,
                     connection_url=f"postgresql://{{{{username}}}}:{{{{password}}}}@postgres:5432/{name}?sslmode=disable",
                     username=PG_ADMIN_USER, password=PG_ADMIN_PASSWORD, allowed_roles=["*"],
                     password_authentication="scram-sha-256")
    existing = set((db.list_static_roles().get("data") or {}).get("keys", [])) if _has_static(c) else set()
    for role, database in STATIC_ROLES.items():          # bao write database/static-roles/<role> ...
        if role not in existing:
            db.create_static_role(name=role, db_name=database, username=role, rotation_period=30 * 24 * 3600,
                                  rotation_statements=["ALTER ROLE \"{{name}}\" WITH PASSWORD '{{password}}';"])
            print(f"database static role {role}: created (password rotated now, then every 30 days)")
    for role, (database, creation) in DYNAMIC_ROLES.items():   # bao write database/roles/<role> ...
        db.create_role(name=role, db_name=database, creation_statements=creation, revocation_statements=REVOKE,
                       default_ttl="1h", max_ttl="24h")
    print("database dynamic roles: mcp-reader, agent-writer (1 h TTL)")


def _has_static(c: hvac.Client) -> bool:
    try:
        c.secrets.database.list_static_roles()
        return True
    except hvac.exceptions.InvalidPath:
        return False


def approles(c: hvac.Client) -> None:
    if "approle/" not in c.sys.list_auth_methods()["data"]:
        c.sys.enable_auth_method("approle")              # bao auth enable approle
    for name, hcl in POLICIES.items():
        c.sys.create_or_update_policy(name=name, policy=hcl)   # bao policy write <name> <file>
        c.auth.approle.create_or_update_approle(role_name=name, token_policies=[name], token_ttl="1h",
                                                token_max_ttl="24h", secret_id_ttl="0")
        out = APPROLE_DIR / name
        out.mkdir(parents=True, exist_ok=True)
        role_id = c.auth.approle.read_role_id(name)["data"]["role_id"]
        (out / "role_id").write_text(role_id)
        if not (out / "secret_id").exists():
            (out / "secret_id").write_text(c.auth.approle.generate_secret_id(name)["data"]["secret_id"])
        for f in out.iterdir():
            f.chmod(0o644)   # local: containers run as different uids. Production: per-pod tmpfs, mode 0400.
    print(f"approle: {', '.join(POLICIES)}")


def main() -> None:
    c = hvac.Client(url=ADDR, timeout=60)   # init on Raft can take a while
    wait_for_server(c)
    init_and_unseal(c)
    kv_secrets(c)
    database_engine(c)
    approles(c)
    print("openbao bootstrap complete")


if __name__ == "__main__":
    main()

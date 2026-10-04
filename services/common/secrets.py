"""Read secrets from OpenBao (the open-source, Vault-API-compatible fork of HashiCorp Vault).

How a service gets a secret, step by step (docs/09-secrets-with-openbao.md has the long version):
  1. Prove who it is. Here: AppRole (a role_id + secret_id pair mounted as files). On Kubernetes you would
     use the Kubernetes auth method (the pod's service-account token); on EC2/EKS the AWS IAM method.
     Either way the service never holds a long-lived password for the secret store itself.
  2. Get a short-lived OpenBao token back (1 h here), scoped by a policy to only the paths it needs.
  3. Read what it needs:
       * KV v2 static secrets   secret/data/ec2-streaming/<name>   (API keys, tokens, S3 keys)
       * dynamic DB credentials database/creds/<role>              (a brand-new Postgres user, 1 h TTL)
       * static-role DB creds   database/static-creds/<role>       (an existing user whose password OpenBao rotates)
Without BAO_ADDR (unit tests, running a script from your laptop) it falls back to local dev defaults.
"""
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import hvac
import structlog

log = structlog.get_logger()
KV_MOUNT, KV_PREFIX = "secret", "ec2-streaming"

# Local-only fallbacks so scripts and tests run without OpenBao. Never used inside docker compose.
_DEV_FALLBACK = {
    "seaweedfs": {"access_key": "s3admin", "secret_key": "s3admin-local-secret"},
    "mcp": {"bearer_token": os.environ.get("MCP_BEARER_TOKEN", "local-dev-mcp-token")},
    "anthropic": {"api_key": os.environ.get("ANTHROPIC_API_KEY", "")},
}


@dataclass
class DbCredentials:
    username: str
    password: str
    expires_at: float          # epoch seconds; static roles report time until the next rotation
    lease_id: str | None = None

    def fresh(self, margin: float = 300) -> bool:
        return time.time() < self.expires_at - margin


class OpenBao:
    """Thread-safe AppRole client that logs in again before its token expires."""

    def __init__(self, addr: str, role_dir: str):
        self.client = hvac.Client(url=addr, timeout=10)
        self.role_dir = Path(role_dir)
        self._token_expires = 0.0
        self._lock = threading.Lock()

    def _login(self) -> None:
        if time.time() < self._token_expires - 120:
            return
        role_id = (self.role_dir / "role_id").read_text().strip()
        secret_id = (self.role_dir / "secret_id").read_text().strip()
        auth = self.client.auth.approle.login(role_id=role_id, secret_id=secret_id)["auth"]
        self._token_expires = time.time() + auth["lease_duration"]
        log.info("openbao login", policies=auth["token_policies"], ttl_seconds=auth["lease_duration"])

    def kv(self, name: str) -> dict:
        with self._lock:
            self._login()
            return self.client.secrets.kv.v2.read_secret_version(
                path=f"{KV_PREFIX}/{name}", mount_point=KV_MOUNT, raise_on_deleted_version=True)["data"]["data"]

    def db_creds(self, role: str) -> DbCredentials:
        """Dynamic credentials: OpenBao creates a Postgres user just for us and drops it when the lease ends."""
        with self._lock:
            self._login()
            r = self.client.secrets.database.generate_credentials(name=role)
        return DbCredentials(r["data"]["username"], r["data"]["password"], time.time() + r["lease_duration"],
                             r["lease_id"])

    def static_db_creds(self, role: str) -> DbCredentials:
        with self._lock:
            self._login()
            r = self.client.secrets.database.get_static_credentials(name=role)["data"]
        return DbCredentials(r["username"], r["password"], time.time() + r["ttl"])


_client: OpenBao | None = None


def openbao() -> OpenBao | None:
    global _client
    addr = os.environ.get("BAO_ADDR")
    if not addr:
        return None
    if _client is None:
        _client = OpenBao(addr, os.environ.get("BAO_APPROLE_DIR", "/run/approle"))
    return _client


def load_secrets(name: str) -> dict:
    bao = openbao()
    if bao is None:
        return _DEV_FALLBACK[name]
    for attempt in range(30):   # OpenBao may still be unsealing on a cold start
        try:
            return bao.kv(name)
        except (hvac.exceptions.VaultError, OSError) as e:
            log.info("waiting for openbao", secret=name, error=type(e).__name__, attempt=attempt)
            time.sleep(2)
    raise RuntimeError(f"could not read secret {name!r} from OpenBao")


class DynamicDsn:
    """A Postgres DSN whose user/password come from OpenBao and are replaced before they expire.

    Call .get() for every new connection; existing connections keep working until the lease ends.
    """

    def __init__(self, host_db: str, role: str, static: bool = False, fallback_dsn: str | None = None):
        self.host_db, self.role, self.static, self.fallback = host_db, role, static, fallback_dsn
        self._creds: DbCredentials | None = None
        self._lock = threading.Lock()

    def get(self) -> str:
        bao = openbao()
        if bao is None:
            if not self.fallback:
                raise RuntimeError("BAO_ADDR is not set and no fallback DSN was given")
            return self.fallback
        with self._lock:
            if self._creds is None or not self._creds.fresh():
                self._creds = bao.static_db_creds(self.role) if self.static else bao.db_creds(self.role)
                log.info("database credentials issued", role=self.role, user=self._creds.username,
                         ttl_seconds=int(self._creds.expires_at - time.time()))
            c = self._creds
        return f"postgresql://{c.username}:{c.password}@{self.host_db}"

"""DynamicDsn: credentials are cached until near expiry, and replaced at once after a rejection (revoked lease)."""
import time

from services.common import secrets
from services.common.secrets import DbCredentials, DynamicDsn


class FakeBao:
    def __init__(self):
        self.issued = 0

    def db_creds(self, role):
        self.issued += 1
        return DbCredentials(f"v-{role}-{self.issued}", "pw", time.time() + 3600)


def test_caches_then_refreshes_after_invalidate(monkeypatch):
    bao = FakeBao()
    monkeypatch.setattr(secrets, "openbao", lambda: bao)
    dsn = DynamicDsn("postgres:5432/serving", "mcp-reader")
    first = dsn.get()
    assert dsn.get() == first and bao.issued == 1          # cached: OpenBao asked once
    dsn.invalidate()                                        # Postgres said "password authentication failed"
    assert dsn.get() != first and bao.issued == 2           # a new user, without waiting for the lease to expire


def test_auth_failure_detection():
    assert secrets.is_auth_failure(Exception('FATAL:  password authentication failed for user "v-x"'))
    assert not secrets.is_auth_failure(Exception("connection timed out"))

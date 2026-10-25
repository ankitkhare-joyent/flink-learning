"""MCP server over the *streaming* EC2 platform: live state, recent history, alerts and pipeline health.

    python -m services.mcp_server.server --transport stdio   # Claude Code / Claude Desktop spawn it locally
    python -m services.mcp_server.server --transport http    # Streamable HTTP on :8002/mcp, bearer-token protected

Where each tool reads from, and why (docs/10-agents-on-streams.md):
  get_instance_state, find_instances, fleet_activity -> Postgres serving tables Flink keeps up to date
                                                        (milliseconds to answer, seconds behind reality)
  instance_timeline                                  -> Iceberg via DuckDB (full history, ~30 s behind)
  recent_alerts                                      -> the ec2.alerts Kafka topic itself + triage table
  pipeline_health                                    -> Flink REST API + Kafka consumer-group lag
An agent never talks to Flink state directly: Flink *writes* query-friendly views and the agent reads those.

Design rules (same as ../learn-ai-agent/mcp_server): few task-shaped tools, all read-only and annotated so,
typed and bounded inputs, compact bounded outputs, errors phrased so the model can correct itself, and
every answer says how fresh it is - with streams, "as of when?" is half the answer.
"""
import argparse
import asyncio
import hmac
import json
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal

import duckdb
import httpx
import psycopg
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from mcp.server import MCPServer
from mcp.server.auth.provider import AccessToken
from mcp.server.auth.settings import AuthSettings
from mcp.server.mcpserver import Context
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
from psycopg.rows import dict_row
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from starlette.requests import Request
from starlette.responses import JSONResponse

from services.common.observability import configure_logging
from services.common.secrets import DynamicDsn, is_auth_failure, load_secrets
from services.common.topics import ALERTS, STATE_CHANGES, STATE_CHANGES_DLQ

log = configure_logging("mcp")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MCP_", extra="ignore")
    serving_host_db: str = "localhost:15432/serving"
    flink_url: str = "http://localhost:18081"
    kafka_bootstrap_servers: str = Field("localhost:19092", validation_alias="KAFKA_BOOTSTRAP_SERVERS")
    iceberg_rest_uri: str = Field("http://localhost:18181/catalog", validation_alias="ICEBERG_REST_URI")
    iceberg_warehouse: str = Field("lakehouse", validation_alias="ICEBERG_WAREHOUSE")
    s3_endpoint: str = Field("http://localhost:19000", validation_alias="S3_ENDPOINT")
    resource_url: str = "http://localhost:18002/mcp"
    allowed_hosts: list[str] = ["localhost:18002", "127.0.0.1:18002", "mcp:8002", "localhost:8002"]


settings = Settings()
READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=False)
INSTANCE_ID = Annotated[str, Field(pattern=r"^i-[0-9a-f]{8,17}$", description="EC2 instance ID, e.g. i-0abc...")]
STATE = Literal["pending", "running", "stopping", "stopped", "shutting-down", "terminated"]
FLINK_JOBS = ("ec2-lakehouse", "ec2-serving", "ec2-alerts")


# ------------------------------------------------------------------ data access
class Backends:
    """One instance per server process. Postgres credentials are short-lived users minted by OpenBao."""

    def __init__(self):
        self.pg = DynamicDsn(settings.serving_host_db, "mcp-reader",
                             fallback_dsn=f"postgresql://postgres:postgres-local@{settings.serving_host_db}")
        self.http = httpx.AsyncClient(timeout=10)
        self.admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
        self._duck: duckdb.DuckDBPyConnection | None = None
        self._duck_lock = asyncio.Lock()

    async def sql(self, query: str, params: tuple = ()) -> list[dict]:
        for attempt in (1, 2):
            dsn = await asyncio.to_thread(self.pg.get)
            try:
                conn = await psycopg.AsyncConnection.connect(dsn, row_factory=dict_row, connect_timeout=5)
            except psycopg.OperationalError as e:
                if attempt == 1 and is_auth_failure(e):   # lease revoked: get a new user and try once more
                    self.pg.invalidate()
                    continue
                raise
            async with conn:
                await conn.execute("SET statement_timeout = '5s'; SET default_transaction_read_only = on")
                cur = await conn.execute(query, params)
                return await cur.fetchall()
        raise RuntimeError("unreachable")

    def _duckdb(self) -> duckdb.DuckDBPyConnection:
        if self._duck is None:
            s3 = load_secrets("seaweedfs")
            db = duckdb.connect(config={"memory_limit": "512MB", "threads": 2})
            db.execute("LOAD iceberg; LOAD httpfs;")
            db.execute(f"""CREATE OR REPLACE SECRET lake_s3 (TYPE s3, KEY_ID '{s3["access_key"]}',
                SECRET '{s3["secret_key"]}', ENDPOINT '{settings.s3_endpoint.split("://")[1]}',
                URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')""")
            db.execute(f"""ATTACH IF NOT EXISTS '{settings.iceberg_warehouse}' AS lake (TYPE iceberg,
                ENDPOINT '{settings.iceberg_rest_uri}', AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')""")
            self._duck = db
        return self._duck

    async def lake(self, query: str, params: list) -> list[dict]:
        async with self._duck_lock:   # one DuckDB connection; Iceberg reads are short and bounded
            def run():
                return self._duckdb().cursor().execute(query, params).to_arrow_table().to_pylist()
            return await asyncio.to_thread(run)

    def read_topic_tail(self, topic: str, per_partition: int) -> list[dict]:
        """Last N records of each partition. No consumer group: reading never moves anyone's offsets."""
        c = Consumer({"bootstrap.servers": settings.kafka_bootstrap_servers, "group.id": "mcp-reader-unused",
                      "enable.auto.commit": False, "isolation.level": "read_committed"})
        try:
            parts = c.list_topics(topic, timeout=5).topics[topic].partitions
            assignments, ends = [], {}
            for p in parts:
                lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=5)
                if hi > lo:
                    assignments.append(TopicPartition(topic, p, max(lo, hi - per_partition)))
                    ends[p] = hi
            if not assignments:
                return []
            c.assign(assignments)
            out, deadline = [], time.time() + 5
            while ends and time.time() < deadline:
                m = c.poll(0.5)
                if m is None or m.error():
                    continue
                out.append(json.loads(m.value()))
                if m.offset() + 1 >= ends.get(m.partition(), 0):
                    ends.pop(m.partition(), None)
            return out
        finally:
            c.close()

    def consumer_lag(self, group: str, topic: str) -> int | None:
        c = Consumer({"bootstrap.servers": settings.kafka_bootstrap_servers, "group.id": group,
                      "enable.auto.commit": False})
        try:
            parts = c.list_topics(topic, timeout=5).topics[topic].partitions
            committed = c.committed([TopicPartition(topic, p) for p in parts], timeout=5)
            if all(tp.offset < 0 for tp in committed):
                return None   # the job has not completed a checkpoint yet
            lag = 0
            for tp in committed:
                _, hi = c.get_watermark_offsets(tp, timeout=5)
                lag += hi - max(tp.offset, 0)
            return lag
        finally:
            c.close()

    def topic_size(self, topic: str) -> int:
        c = Consumer({"bootstrap.servers": settings.kafka_bootstrap_servers, "group.id": "mcp-size-unused"})
        try:
            parts = c.list_topics(topic, timeout=5).topics[topic].partitions
            return sum(hi - lo for lo, hi in (c.get_watermark_offsets(TopicPartition(topic, p), timeout=5)
                                              for p in parts))
        finally:
            c.close()


def _iso(v: Any) -> str | None:
    return v.astimezone(UTC).isoformat(timespec="seconds") if isinstance(v, datetime) else v


def _utc(v: datetime | None) -> datetime | None:
    """Serving tables store UTC in `timestamp` (no zone) columns; make that explicit."""
    return v.replace(tzinfo=UTC) if v is not None and v.tzinfo is None else v


# ------------------------------------------------------------------ output models (become outputSchema)
class InstanceState(BaseModel):
    instance_id: str
    state: str
    previous_state: str | None
    state_since: str = Field(description="UTC time of the last transition (event time)")
    account_id: str
    region: str
    app_name: str | None
    environment: str | None
    owner_email: str | None
    criticality: str | None
    transitions_1h: int = Field(description="State changes in the hour before the last one; >6 is unusual")
    freshness_seconds: float = Field(description="How long ago Flink last updated this row")


class FindResult(BaseModel):
    total_matches: int
    returned: int
    instances: list[InstanceState]


class Timeline(BaseModel):
    instance_id: str
    events: list[dict]
    note: str


class FleetActivity(BaseModel):
    minutes: int
    group_by: str
    groups: list[dict]
    latest_window_end: str | None


class Alerts(BaseModel):
    alerts: list[dict]
    note: str


class PipelineHealth(BaseModel):
    status: Literal["healthy", "degraded", "down"]
    problems: list[str]
    flink_jobs: list[dict]
    kafka: dict
    serving_freshness_seconds: float | None


def _row(r: dict) -> InstanceState:
    now = datetime.now(UTC)
    return InstanceState(
        instance_id=r["instance_id"], state=r["state"], previous_state=r["previous_state"],
        state_since=_iso(_utc(r["state_since"])), account_id=r["account_id"], region=r["region"],
        app_name=r["app_name"], environment=r["environment"], owner_email=r["owner_email"],
        criticality=r["criticality"], transitions_1h=r["transitions_1h"],
        freshness_seconds=round((now - _utc(r["updated_at"])).total_seconds(), 1))


# ------------------------------------------------------------------ auth
class StaticBearerVerifier:
    """HTTP mode accepts one shared bearer token, stored in OpenBao (secret/ec2-streaming/mcp).

    Deliberately simpler than ../learn-ai-agent, which runs a full OAuth 2.1 resource server with token
    exchange. A shared token is fine for one internal agent; anything user-facing should use OAuth.
    """

    def __init__(self, token: str):
        self._token = token

    async def verify_token(self, token: str) -> AccessToken | None:
        if not hmac.compare_digest(token.encode(), self._token.encode()):
            return None
        return AccessToken(token=token, client_id="triage-agent", scopes=["ec2:read"], resource=settings.resource_url)


# ------------------------------------------------------------------ server
@asynccontextmanager
async def lifespan(server: MCPServer):
    backends = Backends()
    try:
        yield {"b": backends}
    finally:
        await backends.http.aclose()


def build_server(mode: str) -> MCPServer:
    auth_kwargs = {}
    if mode == "http":
        auth_kwargs = {
            "token_verifier": StaticBearerVerifier(load_secrets("mcp")["bearer_token"]),
            "auth": AuthSettings(issuer_url=settings.resource_url, resource_server_url=settings.resource_url,
                                 required_scopes=["ec2:read"], validate_token_resource=True)}
    mcp = MCPServer(
        name="ec2-streaming",
        title="EC2 Streaming Platform",
        version="1.0.0",
        instructions=(
            "Real-time, read-only view of the company's EC2 fleet, built from EC2 state-change events. "
            "get_instance_state answers 'what is it doing now'; instance_timeline gives its recent history with "
            "the owner at each moment; find_instances searches current state; fleet_activity shows event rates; "
            "recent_alerts lists what the alerting job flagged; pipeline_health says whether the data itself can "
            "be trusted right now. Results carry freshness: always mention how current the data is."),
        lifespan=lifespan,
        **auth_kwargs,
    )

    def b(ctx: Context) -> Backends:
        return ctx.request_context.lifespan_context["b"]

    @mcp.tool(annotations=READ_ONLY, title="Current state of one instance")
    async def get_instance_state(instance_id: INSTANCE_ID, ctx: Context) -> InstanceState:
        """Current state of one EC2 instance, seconds behind reality: state, previous state, since when,
        app/owner/criticality from the CMDB, and how many transitions it made in the last hour."""
        rows = await b(ctx).sql("SELECT * FROM instance_current_state WHERE instance_id = %s", (instance_id,))
        if not rows:
            raise ToolError(f"no events seen for {instance_id}; check the ID, or it has been quiet since the "
                            "stream started (try instance_timeline)")
        return _row(rows[0])

    @mcp.tool(annotations=READ_ONLY, title="Find instances by current state")
    async def find_instances(
        ctx: Context,
        state: STATE | None = None,
        app_name: str | None = None,
        environment: Literal["prod", "staging", "dev"] | None = None,
        criticality: Literal["tier1", "tier2", "tier3"] | None = None,
        min_transitions_1h: Annotated[int | None, Field(ge=1, description="Only instances this busy")] = None,
        limit: Annotated[int, Field(ge=1, le=50)] = 10,
    ) -> FindResult:
        """Search the live fleet. For "unstable" or "flapping" instances use min_transitions_1h (e.g. 6).
        Results are sorted by most recent transition first."""
        where, params = [], []
        for col, val in (("state", state), ("app_name", app_name), ("environment", environment),
                         ("criticality", criticality)):
            if val is not None:
                where.append(f"{col} = %s")
                params.append(val)
        if min_transitions_1h:
            where.append("transitions_1h >= %s")
            params.append(min_transitions_1h)
        clause = f"WHERE {' AND '.join(where)}" if where else ""
        rows = await b(ctx).sql(f"SELECT *, count(*) OVER () AS total FROM instance_current_state {clause} "
                                f"ORDER BY state_since DESC LIMIT %s", (*params, limit))
        return FindResult(total_matches=rows[0]["total"] if rows else 0, returned=len(rows),
                          instances=[_row(r) for r in rows])

    @mcp.tool(annotations=READ_ONLY, title="Recent history of one instance")
    async def instance_timeline(
        instance_id: INSTANCE_ID,
        ctx: Context,
        hours: Annotated[int, Field(ge=1, le=168)] = 6,
        limit: Annotated[int, Field(ge=1, le=100)] = 40,
    ) -> Timeline:
        """Every state transition of one instance in the last N hours, newest first, each with the app and owner
        recorded in the CMDB *at that moment*. Read from the Iceberg lakehouse, so it lags ~30-60 s."""
        since = datetime.now(UTC) - timedelta(hours=hours)
        rows = await b(ctx).lake(
            "SELECT event_time, state, owner_email, app_name, environment, criticality, "
            "date_diff('millisecond', event_time, ingest_time) / 1000.0 AS ingest_delay_s "
            "FROM lake.ec2_stream.state_changes WHERE instance_id = ? AND event_time >= ? "
            "ORDER BY event_time DESC LIMIT ?", [instance_id, since, limit])
        for r in rows:
            r["event_time"] = _iso(r["event_time"])
        return Timeline(instance_id=instance_id, events=rows,
                        note="Lakehouse data commits every Flink checkpoint (30 s); the last minute may be missing.")

    @mcp.tool(annotations=READ_ONLY, title="Fleet activity")
    async def fleet_activity(
        ctx: Context,
        minutes: Annotated[int, Field(ge=1, le=240)] = 15,
        group_by: Literal["region", "state"] = "state",
    ) -> FleetActivity:
        """How many state changes happened across the fleet in the last N minutes, grouped by region or by
        target state, from 1-minute event-time windows. Use it to tell one bad instance from a fleet-wide event."""
        rows = await b(ctx).sql(
            f"SELECT {group_by} AS key, sum(events)::bigint AS events, max(window_end) AS latest "
            f"FROM state_change_counts_1m WHERE window_end > (now() AT TIME ZONE 'UTC') - make_interval(mins => %s) "
            f"GROUP BY {group_by} ORDER BY events DESC", (minutes,))
        latest = max((r["latest"] for r in rows), default=None)
        return FleetActivity(minutes=minutes, group_by=group_by,
                             groups=[{group_by: r["key"], "events": r["events"]} for r in rows],
                             latest_window_end=_iso(_utc(latest)))

    @mcp.tool(annotations=READ_ONLY, title="Recent alerts")
    async def recent_alerts(
        ctx: Context,
        alert_type: Literal["FLAPPING", "CRITICAL_TERMINATION"] | None = None,
        instance_id: INSTANCE_ID | None = None,
        limit: Annotated[int, Field(ge=1, le=50)] = 10,
    ) -> Alerts:
        """Alerts the streaming job raised (read straight from the ec2.alerts topic), newest first, with the
        triage the agent wrote for each one, if any."""
        alerts = await asyncio.to_thread(b(ctx).read_topic_tail, ALERTS.name, 200)
        alerts = [a for a in alerts if (alert_type is None or a["alert_type"] == alert_type)
                  and (instance_id is None or a["instance_id"] == instance_id)]
        alerts.sort(key=lambda a: a["detected_at"], reverse=True)
        alerts = alerts[:limit]
        if alerts:
            triage = await b(ctx).sql("SELECT alert_id, summary, likely_cause, triaged_at FROM alert_triage "
                                      "WHERE alert_id = ANY(%s)", ([a["alert_id"] for a in alerts],))
            by_id = {t["alert_id"]: t for t in triage}
            for a in alerts:
                t = by_id.get(a["alert_id"])
                a["triage"] = {"summary": t["summary"], "likely_cause": t["likely_cause"],
                               "triaged_at": _iso(t["triaged_at"])} if t else None
        return Alerts(alerts=alerts, note="Alerts are published when a Flink checkpoint commits (every 30 s).")

    @mcp.tool(annotations=READ_ONLY, title="Pipeline health")
    async def pipeline_health(ctx: Context) -> PipelineHealth:
        """Is the streaming pipeline itself healthy? Flink job status and checkpoint age, Kafka consumer lag per
        job, dead-letter queue size, and how fresh the serving tables are. Check this before trusting
        'nothing happened' - silence can mean a broken pipeline."""
        bk, problems = b(ctx), []
        jobs = []
        try:
            overview = (await bk.http.get(f"{settings.flink_url}/jobs/overview")).json()["jobs"]
            running = {j["name"]: j for j in overview if j["state"] == "RUNNING"}
            for name in FLINK_JOBS:
                j = running.get(name)
                if j is None:
                    problems.append(f"Flink job {name} is not running")
                    jobs.append({"name": name, "state": "NOT RUNNING"})
                    continue
                cp = (await bk.http.get(f"{settings.flink_url}/jobs/{j['jid']}/checkpoints")).json()
                last = (cp.get("latest") or {}).get("completed") or {}
                age = round(time.time() - last["latest_ack_timestamp"] / 1000, 1) if last else None
                if age is None or age > 120:
                    problems.append(f"{name}: no completed checkpoint in the last 2 minutes (sinks are not committing)")
                lag = await asyncio.to_thread(bk.consumer_lag, f"flink-{name}", STATE_CHANGES.name)
                if lag is not None and lag > 5000:
                    problems.append(f"{name}: {lag} records behind on {STATE_CHANGES.name}")
                jobs.append({"name": name, "state": "RUNNING", "last_checkpoint_age_s": age,
                             "failed_checkpoints": cp["counts"]["failed"], "kafka_lag": lag})
        except httpx.HTTPError as e:
            problems.append(f"Flink REST API unreachable: {type(e).__name__}")
        dlq = await asyncio.to_thread(bk.topic_size, STATE_CHANGES_DLQ.name)
        fresh = await bk.sql("SELECT max(updated_at) AS t FROM instance_current_state")
        freshness = (datetime.now(UTC) - _utc(fresh[0]["t"])).total_seconds() if fresh and fresh[0]["t"] else None
        if freshness is None or freshness > 120:
            problems.append("serving tables have not been updated in over 2 minutes")
        status = "healthy" if not problems else ("down" if len(problems) >= 3 else "degraded")
        return PipelineHealth(status=status, problems=problems, flink_jobs=jobs,
                              kafka={"dlq_records": dlq, "dlq_topic": STATE_CHANGES_DLQ.name},
                              serving_freshness_seconds=round(freshness, 1) if freshness is not None else None)

    @mcp.resource("ec2stream://alert-types", title="Alert types", mime_type="text/markdown")
    def alert_types() -> str:
        """What each alert means and how it is detected."""
        return ("# Alert types\n"
                "- FLAPPING (warning): stopped->running->stopped->running->stopped within 10 minutes of event time, "
                "detected with MATCH_RECOGNIZE. Usual causes: failing health checks with auto-recovery, an ASG "
                "replacing an unhealthy node, a scheduler or script fighting a human, spot capacity churn.\n"
                "- CRITICAL_TERMINATION (critical): a tier1 instance in prod was terminated (CMDB as of event time). "
                "Check whether it was planned (ASG scale-in, replacement launched) before paging.\n")

    @mcp.prompt(title="Triage an alert")
    def triage_alert(alert_id: str) -> str:
        """Ready-made prompt for investigating one alert end to end."""
        return (f"Triage alert {alert_id}. Find it with recent_alerts, check pipeline_health first, then look at "
                "the instance with get_instance_state and instance_timeline, and use fleet_activity to see whether "
                "it is isolated. Say what happened, the likely cause, who to notify, and how fresh the data is.")

    @mcp.custom_route("/healthz", methods=["GET"])
    async def healthz(_: Request) -> JSONResponse:
        return JSONResponse({"status": "ok"})

    return mcp


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--transport", choices=["stdio", "http"], default="stdio")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=8002)
    args = p.parse_args()
    mcp = build_server(args.transport)
    if args.transport == "stdio":
        mcp.run("stdio")
    else:
        # Stateless + JSON responses: any replica can answer any request (scales behind a plain load balancer).
        mcp.run("streamable-http", host=args.host, port=args.port, stateless_http=True, json_response=True,
                transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=True,
                                                             allowed_hosts=settings.allowed_hosts,
                                                             allowed_origins=[]))


if __name__ == "__main__":
    main()

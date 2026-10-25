"""Event-driven triage agent: a Kafka consumer whose "processing step" is a Claude agent with MCP tools.

    ec2.alerts --> [this service] --> ec2.alerts.triaged  (+ serving.alert_triage for humans and the MCP server)
                         |  \\-> ec2.alerts.dlq           (alerts that could not be triaged after retries)
                         \\-- MCP tools (services/mcp_server): live state, history, fleet activity, pipeline health

The agent part is small. The streaming part is what makes it production-grade:
  * At-least-once: the Kafka offset is committed only after the triage is written. A crash mid-triage means
    the alert is processed again, so writes are idempotent (keyed by alert_id; already-triaged ids skipped).
  * Backpressure and cost: alerts are handled one at a time with a per-hour budget. When alerts arrive faster
    than the budget, consumer lag grows (visible in Kafka UI) instead of the bill. Alerts older than
    max_alert_age_minutes are skipped: a stale triage helps nobody.
  * Poison messages: an alert that fails `max_attempts` times goes to a DLQ topic and is committed, so one bad
    alert cannot block the partition forever.
  * Read-only tools: the agent can look at anything the MCP server exposes and change nothing. Its only
    writes are its own outputs, with a Postgres user OpenBao created for it an hour ago.
"""
import asyncio
import json
import time
from datetime import UTC, datetime
from typing import Literal

import httpx2
import psycopg
from anthropic import APIConnectionError, APIStatusError, AsyncAnthropic, RateLimitError
from anthropic.lib.tools.mcp import async_mcp_tool
from confluent_kafka import Consumer, KafkaException, Producer
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from prometheus_client import Counter, Histogram
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from services.common.observability import Shutdown, configure_logging, start_metrics_server
from services.common.secrets import DynamicDsn, is_auth_failure, load_secrets
from services.common.topics import ALERTS, ALERTS_DLQ, ALERTS_TRIAGED


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AGENT_", extra="ignore")
    model: str = "claude-opus-5-5"
    effort: Literal["low", "medium", "high", "xhigh", "max"] = "medium"
    max_tool_iterations: int = 12
    triages_per_hour: int = 20
    max_alert_age_minutes: int = 30
    max_attempts: int = 3
    mcp_url: str = "http://localhost:18002/mcp"
    serving_host_db: str = "localhost:15432/serving"
    kafka_bootstrap_servers: str = Field("localhost:19092", validation_alias="KAFKA_BOOTSTRAP_SERVERS")
    metrics_port: int = 9402


class TriageResult(BaseModel):
    """The structured output the agent must return (enforced by structured outputs, validated by pydantic)."""
    severity: Literal["low", "medium", "high", "critical"]
    summary: str = Field(description="One or two sentences a human on call can act on")
    likely_cause: str
    evidence: list[str] = Field(description="Facts from the tools that support the cause, each with its time")
    next_steps: list[str]
    notify: str | None = Field(description="Owner email or support group to notify, from the CMDB, if any")
    data_freshness: str = Field(description="How current the data used was, e.g. 'serving 4 s old, pipeline healthy'")


SYSTEM = """You are the on-call triage assistant for a company's AWS EC2 fleet.
You receive one alert produced by a real-time stream processing job and investigate it with read-only tools
over live data. Work like a careful SRE:
1. Check pipeline_health first. If the pipeline is degraded, say so: the alert or the evidence may be wrong.
2. Look at the instance itself (get_instance_state, instance_timeline) and at the wider fleet (fleet_activity)
   to tell an isolated problem from a fleet-wide event.
3. Base every claim on tool output and give times in UTC. If the data cannot tell you the cause, say what
   would, rather than guessing.
Keep tool calls purposeful; you rarely need more than six. Return the triage in the required format."""

TRIAGES = Counter("agent_triages_total", "Alerts handled", ["outcome"])
TRIAGE_SECONDS = Histogram("agent_triage_seconds", "Wall time per triage", buckets=(5, 10, 20, 30, 60, 120, 300))
TOKENS = Counter("agent_tokens_total", "Claude tokens used", ["kind"])


class TriageFailed(Exception):
    pass


class TriageAgent:
    def __init__(self, s: Settings, log):
        self.s, self.log = s, log
        api_key = load_secrets("anthropic").get("api_key")
        if not api_key:
            raise SystemExit("no Anthropic API key: add ANTHROPIC_API_KEY to .env and re-run bao-bootstrap")
        self.claude = AsyncAnthropic(api_key=api_key, max_retries=4)   # SDK retries 429/5xx with backoff
        self.mcp_token = load_secrets("mcp")["bearer_token"]
        self.db = DynamicDsn(s.serving_host_db, "agent-writer")
        self.producer = Producer({"bootstrap.servers": s.kafka_bootstrap_servers, "enable.idempotence": True,
                                  "acks": "all", "client.id": "triage-agent"})
        self.consumer = Consumer({
            "bootstrap.servers": s.kafka_bootstrap_servers,
            "group.id": "triage-agent",
            "enable.auto.commit": False,                  # commit only after the triage is safely written
            "auto.offset.reset": "latest",                # a new agent does not triage the whole backlog
            "isolation.level": "read_committed",          # only alerts from committed Flink checkpoints
            "max.poll.interval.ms": 900000,               # one triage can take minutes; do not get kicked out
        })
        self._window_start, self._window_count = time.time(), 0

    # ------------------------------------------------------------------ the agent
    async def triage(self, alert: dict, session: ClientSession) -> tuple[TriageResult, dict]:
        tools = [async_mcp_tool(t, session) for t in (await session.list_tools()).tools]
        runner = self.claude.beta.messages.tool_runner(
            model=self.s.model,
            max_tokens=16000,
            max_iterations=self.s.max_tool_iterations,
            system=SYSTEM,
            thinking={"type": "adaptive"},
            output_config={"effort": self.s.effort},
            output_format=TriageResult,
            # If a safety classifier declines, the API retries on a fallback model inside the same call.
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",
            tools=tools,
            messages=[{"role": "user", "content": f"Triage this alert:\n{json.dumps(alert, indent=2)}"}],
        )
        usage = {"input": 0, "output": 0, "turns": 0}
        final = None
        async for message in runner:
            final = message
            usage["turns"] += 1
            usage["input"] += message.usage.input_tokens
            usage["output"] += message.usage.output_tokens
        if final is None or final.stop_reason == "refusal":
            raise TriageFailed(f"model declined or returned nothing (stop_reason={getattr(final, 'stop_reason', None)})")
        if final.stop_reason == "max_tokens" or final.parsed_output is None:
            raise TriageFailed(f"no structured triage returned (stop_reason={final.stop_reason})")
        return final.parsed_output, usage

    # ------------------------------------------------------------------ outputs (idempotent)
    def connect(self) -> psycopg.Connection:
        try:
            return psycopg.connect(self.db.get(), connect_timeout=5)
        except psycopg.OperationalError as e:
            if not is_auth_failure(e):
                raise
            self.db.invalidate()                 # lease revoked: get a new user and try once more
            return psycopg.connect(self.db.get(), connect_timeout=5)

    def already_triaged(self, alert_id: str) -> bool:
        with self.connect() as c:
            return c.execute("SELECT 1 FROM alert_triage WHERE alert_id = %s", (alert_id,)).fetchone() is not None

    def write(self, alert: dict, result: TriageResult) -> None:
        with self.connect() as c:
            c.execute(
                "INSERT INTO alert_triage (alert_id, instance_id, alert_type, severity, summary, likely_cause, "
                "next_steps, notify, model) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (alert_id) DO UPDATE SET "
                "severity = EXCLUDED.severity, summary = EXCLUDED.summary, likely_cause = EXCLUDED.likely_cause, "
                "next_steps = EXCLUDED.next_steps, notify = EXCLUDED.notify, model = EXCLUDED.model, triaged_at = now()",
                (alert["alert_id"], alert["instance_id"], alert["alert_type"], result.severity, result.summary,
                 result.likely_cause, json.dumps(result.next_steps), result.notify, self.s.model))
        out = {"alert": alert, "triage": result.model_dump(), "model": self.s.model,
               "triaged_at": datetime.now(UTC).isoformat(timespec="seconds")}
        self.producer.produce(ALERTS_TRIAGED.name, key=alert["alert_id"], value=json.dumps(out))
        if self.producer.flush(30):
            raise TriageFailed("kafka did not acknowledge the triage record")

    def dead_letter(self, raw: bytes, reason: str) -> None:
        self.producer.produce(ALERTS_DLQ.name, value=raw, headers={"error": reason[:500]})
        self.producer.flush(30)

    # ------------------------------------------------------------------ budget
    async def respect_budget(self, shutdown: Shutdown) -> None:
        if time.time() - self._window_start > 3600:
            self._window_start, self._window_count = time.time(), 0
        while self._window_count >= self.s.triages_per_hour and not shutdown.requested:
            wait = self._window_start + 3600 - time.time()
            self.log.info("hourly triage budget used; waiting (consumer lag will grow)", wait_seconds=int(wait))
            await asyncio.sleep(min(wait, 30))
            if time.time() - self._window_start > 3600:
                self._window_start, self._window_count = time.time(), 0

    # ------------------------------------------------------------------ one message
    async def handle(self, msg, session: ClientSession) -> None:
        raw = msg.value()
        try:
            alert = json.loads(raw)
            alert_id = alert["alert_id"]
        except (ValueError, KeyError, TypeError):
            TRIAGES.labels("invalid").inc()
            self.dead_letter(raw, "not a valid alert")
            return
        log = self.log.bind(alert_id=alert_id, alert_type=alert.get("alert_type"), instance_id=alert.get("instance_id"))
        age_min = (datetime.now(UTC) - datetime.fromisoformat(alert["detected_at"])
                   ).total_seconds() / 60
        if age_min > self.s.max_alert_age_minutes:
            TRIAGES.labels("skipped_stale").inc()
            log.info("alert too old; skipped", age_minutes=round(age_min, 1))
            return
        if await asyncio.to_thread(self.already_triaged, alert_id):
            TRIAGES.labels("skipped_duplicate").inc()
            log.info("already triaged (redelivery); skipped")
            return
        for attempt in range(1, self.s.max_attempts + 1):
            t0 = time.perf_counter()
            try:
                result, usage = await self.triage(alert, session)
                await asyncio.to_thread(self.write, alert, result)
                self._window_count += 1
                TRIAGES.labels("ok").inc()
                TRIAGE_SECONDS.observe(time.perf_counter() - t0)
                TOKENS.labels("input").inc(usage["input"])
                TOKENS.labels("output").inc(usage["output"])
                log.info("triaged", severity=result.severity, seconds=round(time.perf_counter() - t0, 1), **usage)
                return
            except (TriageFailed, RateLimitError, APIConnectionError, APIStatusError, psycopg.Error) as e:
                log.warning("triage attempt failed", attempt=attempt, error=f"{type(e).__name__}: {e}"[:300])
                await asyncio.sleep(2 ** attempt)
        TRIAGES.labels("dead_lettered").inc()
        self.dead_letter(raw, f"failed after {self.s.max_attempts} attempts")
        log.error("triage failed; sent to DLQ")

    # ------------------------------------------------------------------ main loop
    async def run(self, shutdown: Shutdown) -> None:
        self.consumer.subscribe([ALERTS.name])
        self.log.info("triage agent started", model=self.s.model, budget_per_hour=self.s.triages_per_hour)
        while not shutdown.requested:
            try:
                http = httpx2.AsyncClient(headers={"Authorization": f"Bearer {self.mcp_token}"}, timeout=60)
                async with http, streamable_http_client(self.s.mcp_url, http_client=http) as (read, write), \
                        ClientSession(read, write) as session:
                    await session.initialize()
                    self.log.info("connected to MCP server", url=self.s.mcp_url)
                    while not shutdown.requested:
                        msg = await asyncio.to_thread(self.consumer.poll, 1.0)
                        if msg is None:
                            continue
                        if msg.error():
                            self.log.warning("kafka error", error=str(msg.error()))
                            continue
                        await self.respect_budget(shutdown)
                        await self.handle(msg, session)
                        self.consumer.commit(message=msg, asynchronous=False)   # after the outputs are durable
            except (OSError, httpx2.HTTPError, KafkaException, ExceptionGroup) as e:
                self.log.warning("connection problem; reconnecting", error=f"{type(e).__name__}: {e}"[:300])
                await asyncio.sleep(5)
        self.consumer.close()   # leaves the group cleanly so partitions rebalance at once
        self.log.info("triage agent stopped")


def main() -> None:
    s = Settings()
    log = configure_logging("triage-agent")
    start_metrics_server(s.metrics_port)
    shutdown = Shutdown()
    asyncio.run(TriageAgent(s, log).run(shutdown))


if __name__ == "__main__":
    main()

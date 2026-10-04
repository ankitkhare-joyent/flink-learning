"""Simulates the two source systems so the pipeline has realistic, messy input.

1. AWS: an EC2 fleet whose instances start, stop, launch and terminate. Every transition is sent to SQS
   as the exact JSON EventBridge produces ("EC2 Instance State-change Notification").
2. ServiceNow CMDB: one row per instance (app, owner, criticality) in Postgres. New instances are inserted
   and owners occasionally change; Debezium turns those writes into a change stream.

Real event streams are messy, so this one is too (rates are configurable):
  * duplicates      - SQS standard queues deliver at-least-once; the same event id arrives twice
  * out of order    - an event is sent a few seconds after later events (network/retry delays)
  * late            - an event is sent minutes after it happened, after the watermark has passed
  * malformed       - junk that must go to a dead-letter queue, not crash the pipeline
  * flapping        - an instance stopping/starting over and over (what the alert job detects)
"""
import heapq
import json
import random
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import boto3
import psycopg
from prometheus_client import Counter
from pydantic_settings import BaseSettings, SettingsConfigDict

from services.common.observability import Shutdown, configure_logging, start_metrics_server
from services.common.secrets import DynamicDsn
from services.ingest.events import eventbridge_event


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SIM_")
    fleet_size: int = 2000
    events_per_second: float = 10.0
    duplicate_rate: float = 0.02
    out_of_order_rate: float = 0.03
    late_rate: float = 0.003
    malformed_rate: float = 0.003
    terminate_share: float = 0.04          # of user actions on a running instance
    flap_every_seconds: float = 180.0      # start a flapping episode this often
    cmdb_update_every_seconds: float = 20.0
    seed: int | None = None
    metrics_port: int = 9401


class Sources(BaseSettings):
    sqs_endpoint: str | None = "http://localhost:19324"
    sqs_queue_name: str = "ec2-state-change"
    cmdb_host_db: str = "localhost:15432/cmdb"


SENT = Counter("sim_events_sent_total", "Events sent to SQS", ["kind"])
CMDB_WRITES = Counter("sim_cmdb_writes_total", "CMDB rows written", ["op"])

ACCOUNTS = ("111122223333", "222233334444", "333344445555", "444455556666")
REGIONS = ("us-east-1", "us-west-2", "eu-west-1", "ap-south-1")
APPS = {  # app -> (support group, criticality)
    "payments-api": ("sre-payments", "tier1"), "checkout-web": ("sre-commerce", "tier1"),
    "search-indexer": ("search-platform", "tier2"), "inventory-sync": ("data-platform", "tier2"),
    "dcim-collector": ("infra-ops", "tier2"), "ansible-runner": ("infra-ops", "tier3"),
    "reporting-batch": ("data-platform", "tier3"), "ml-training": ("ml-platform", "tier3"),
}
OWNERS = ("asha", "ben", "carla", "dev", "elena", "farid", "gwen", "hiro")
# How long AWS takes to finish a transition (seconds).
DURATION = {"pending": (5, 15), "stopping": (3, 10), "shutting-down": (5, 20)}
NEXT = {"pending": "running", "stopping": "stopped", "shutting-down": "terminated"}


@dataclass
class Instance:
    instance_id: str
    account_id: str
    region: str
    state: str = "running"
    busy: bool = False       # mid-transition or flapping: no random actions


def new_instance_id(rng: random.Random) -> str:
    return "i-" + "".join(rng.choice("0123456789abcdef") for _ in range(17))


class Simulator:
    def __init__(self, cfg: Settings, src: Sources, log):
        self.cfg, self.log = cfg, log
        self.rng = random.Random(cfg.seed)
        self.sqs = boto3.client("sqs", endpoint_url=src.sqs_endpoint)
        self.queue_url = self.sqs.get_queue_url(QueueName=src.sqs_queue_name)["QueueUrl"]
        # The `app` user's password is owned and rotated by OpenBao (static role); we never see it in config.
        dsn = DynamicDsn(src.cmdb_host_db, "app", static=True, fallback_dsn=f"postgresql://app@{src.cmdb_host_db}")
        self.db = psycopg.connect(dsn.get(), autocommit=True)
        self.fleet: dict[str, Instance] = {}
        self.timeline: list[tuple[float, int, str, str]] = []   # (due, seq, instance_id, new_state)
        self.delayed: list[tuple[float, int, str]] = []        # (send_at, seq, json body)
        self.outbox: list[str] = []
        self._seq = 0

    # ------------------------------------------------------------------ CMDB (ServiceNow stand-in)
    def load_or_seed_fleet(self) -> None:
        rows = self.db.execute("SELECT instance_id, account_id, region FROM ec2_ci").fetchall()
        if rows:
            self.fleet = {r[0]: Instance(*r) for r in rows}
            self.log.info("fleet loaded from CMDB", instances=len(rows))
            return
        long_ago = datetime.now(UTC) - timedelta(days=30)
        with self.db.cursor() as cur:
            for _ in range(self.cfg.fleet_size):
                inst = Instance(new_instance_id(self.rng), self.rng.choice(ACCOUNTS), self.rng.choice(REGIONS))
                self.fleet[inst.instance_id] = inst
                cur.execute(*self._ci_insert(inst, long_ago))
        CMDB_WRITES.labels("insert").inc(len(self.fleet))
        self.log.info("CMDB seeded", instances=len(self.fleet))

    def _ci_insert(self, inst: Instance, ts: datetime):
        app = self.rng.choice(list(APPS))
        group, crit = APPS[app]
        env = self.rng.choices(("prod", "staging", "dev"), weights=(5, 2, 3))[0]
        return (("INSERT INTO ec2_ci (instance_id, account_id, region, app_name, environment, owner_email, "
                 "support_group, criticality, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                 "ON CONFLICT (instance_id) DO NOTHING"),
                (inst.instance_id, inst.account_id, inst.region, app, env,
                 f"{self.rng.choice(OWNERS)}@example.com", group, crit if env == "prod" else "tier3", ts))

    def cmdb_reassign_owner(self) -> None:
        iid = self.rng.choice(list(self.fleet))
        self.db.execute("UPDATE ec2_ci SET owner_email=%s, updated_at=now() WHERE instance_id=%s",
                        (f"{self.rng.choice(OWNERS)}@example.com", iid))
        CMDB_WRITES.labels("update").inc()

    # ------------------------------------------------------------------ fleet behaviour
    def schedule(self, delay: float, inst: Instance, state: str) -> None:
        self._seq += 1
        heapq.heappush(self.timeline, (time.monotonic() + delay, self._seq, inst.instance_id, state))

    def random_action(self) -> None:
        inst = self.fleet[self.rng.choice(list(self.fleet))]
        if inst.busy:
            return
        if inst.state == "running":
            inst.busy = True
            nxt = "shutting-down" if self.rng.random() < self.cfg.terminate_share else "stopping"
            self.schedule(0, inst, nxt)
        elif inst.state == "stopped":
            inst.busy = True
            self.schedule(0, inst, "pending")

    def launch_replacement(self, old: Instance) -> None:
        inst = Instance(new_instance_id(self.rng), old.account_id, old.region, state="pending", busy=True)
        self.db.execute(*self._ci_insert(inst, datetime.now(UTC) - timedelta(seconds=1)))
        CMDB_WRITES.labels("insert").inc()
        self.fleet[inst.instance_id] = inst
        self.schedule(self.rng.uniform(1, 3), inst, "pending")

    def start_flapping(self) -> None:
        candidates = [i for i in self.fleet.values() if i.state == "running" and not i.busy]
        if not candidates:
            return
        inst = self.rng.choice(candidates)
        inst.busy = True
        t = 0.0
        for _ in range(4):   # four stop/start cycles in about two minutes
            for state in ("stopping", "stopped", "pending", "running"):
                self.schedule(t, inst, state)
                t += self.rng.uniform(4, 9)
        self._seq += 1
        heapq.heappush(self.timeline, (time.monotonic() + t + 1, self._seq, inst.instance_id, "_release"))
        self.log.info("flapping episode", instance_id=inst.instance_id)

    def apply_due_transitions(self) -> None:
        now = time.monotonic()
        while self.timeline and self.timeline[0][0] <= now:
            _, _, iid, state = heapq.heappop(self.timeline)
            inst = self.fleet.get(iid)
            if inst is None:
                continue
            if state == "_release":
                inst.busy = False
                continue
            inst.state = state
            self.emit(inst)
            if state in NEXT and not self._has_pending(iid):   # finish the transition later
                lo, hi = DURATION[state]
                self.schedule(self.rng.uniform(lo, hi), inst, NEXT[state])
            elif state == "terminated":
                del self.fleet[iid]
                self.launch_replacement(inst)
            elif state in ("running", "stopped") and not self._has_pending(iid):
                inst.busy = False

    def _has_pending(self, iid: str) -> bool:
        return any(t[2] == iid for t in self.timeline)

    # ------------------------------------------------------------------ emitting, with realistic mess
    def emit(self, inst: Instance) -> None:
        evt = eventbridge_event(str(uuid.uuid4()), datetime.now(UTC), inst.account_id, inst.region,
                                inst.instance_id, inst.state)
        body = json.dumps(evt)
        r = self.rng.random()
        if r < self.cfg.late_rate:
            self._delay(body, self.rng.uniform(180, 360), "late")
        elif r < self.cfg.late_rate + self.cfg.out_of_order_rate:
            self._delay(body, self.rng.uniform(2, 15), "out_of_order")
        else:
            self.outbox.append(body)
            SENT.labels("normal").inc()
        if self.rng.random() < self.cfg.duplicate_rate:
            self.outbox.append(body)
            SENT.labels("duplicate").inc()
        if self.rng.random() < self.cfg.malformed_rate:
            bad = self.rng.choice([
                body[: len(body) // 2],                                         # truncated JSON
                json.dumps({**evt, "detail": {"instance-id": inst.instance_id, "state": "rebooting"}}),
                json.dumps({**evt, "detail-type": "EC2 Spot Instance Interruption Warning"}),
            ])
            self.outbox.append(bad)
            SENT.labels("malformed").inc()

    def _delay(self, body: str, seconds: float, kind: str) -> None:
        self._seq += 1
        heapq.heappush(self.delayed, (time.monotonic() + seconds, self._seq, body))
        SENT.labels(kind).inc()

    def flush(self) -> None:
        now = time.monotonic()
        while self.delayed and self.delayed[0][0] <= now:
            self.outbox.append(heapq.heappop(self.delayed)[2])
        while self.outbox:
            batch, self.outbox = self.outbox[:10], self.outbox[10:]   # SQS batch limit is 10
            out = self.sqs.send_message_batch(QueueUrl=self.queue_url, Entries=[
                {"Id": str(i), "MessageBody": b} for i, b in enumerate(batch)])
            if out.get("Failed"):
                self.log.warning("sqs send failures", failed=out["Failed"])

    # ------------------------------------------------------------------ main loop
    def run(self, shutdown: Shutdown) -> None:
        self.load_or_seed_fleet()
        tick = 0.1
        actions_per_tick = self.cfg.events_per_second / 2 * tick   # each action yields ~2 events
        next_flap = time.monotonic() + 20
        next_cmdb = time.monotonic() + self.cfg.cmdb_update_every_seconds
        self.log.info("simulator running", eps=self.cfg.events_per_second, fleet=len(self.fleet))
        while not shutdown.requested:
            budget = actions_per_tick
            while budget > 0:
                if self.rng.random() < min(budget, 1):
                    self.random_action()
                budget -= 1
            now = time.monotonic()
            if now >= next_flap:
                self.start_flapping()
                next_flap = now + self.cfg.flap_every_seconds
            if now >= next_cmdb:
                self.cmdb_reassign_owner()
                next_cmdb = now + self.cfg.cmdb_update_every_seconds
            self.apply_due_transitions()
            self.flush()
            shutdown.wait(tick)
        self.log.info("simulator stopped")


def main() -> None:
    cfg = Settings()
    log = configure_logging("simulator")
    start_metrics_server(cfg.metrics_port)
    shutdown = Shutdown()
    for attempt in range(30):
        try:
            sim = Simulator(cfg, Sources(), log)
            break
        except Exception as e:  # noqa: BLE001
            log.info("waiting for dependencies", error=str(e), attempt=attempt)
            time.sleep(2)
    else:
        raise SystemExit("dependencies never became ready")
    sim.run(shutdown)


if __name__ == "__main__":
    main()

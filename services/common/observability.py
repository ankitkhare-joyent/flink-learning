"""Structured JSON logs + Prometheus metrics + graceful shutdown, shared by every Python service.

Production habits, not decoration:
  * JSON logs with stable keys, so Loki/CloudWatch/Datadog can index them (no f-string soup).
  * A /metrics endpoint per process; dashboards and alerts are built on these, not on logs.
  * SIGTERM is the normal way Kubernetes and Docker stop a pod. Services stop *taking* new work,
    finish what is in flight, commit/ack it, then exit 0. That is what makes at-least-once real.
"""
import logging
import signal
import sys
import threading

import structlog
from prometheus_client import start_http_server


def configure_logging(service: str, level: str = "INFO") -> structlog.stdlib.BoundLogger:
    logging.basicConfig(stream=sys.stdout, level=level, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    return structlog.get_logger(service=service)


def start_metrics_server(port: int) -> None:
    if port:
        start_http_server(port)


class Shutdown:
    """Set when SIGTERM/SIGINT arrives. Loops check `.requested` and `.wait()` instead of sleeping."""

    def __init__(self) -> None:
        self._event = threading.Event()
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self._handle)

    def _handle(self, signum, _frame) -> None:
        structlog.get_logger().info("shutdown requested", signal=signal.Signals(signum).name)
        self._event.set()

    @property
    def requested(self) -> bool:
        return self._event.is_set()

    def wait(self, seconds: float) -> bool:
        """Sleep up to `seconds`; returns True if shutdown was requested meanwhile."""
        return self._event.wait(seconds)

"""
Structured JSON logging and step timing.

Every record is one JSON object carrying the run id, so logs from local runs
and Kubernetes pods can be filtered per run. ``log_step`` records start time,
end time, duration, exit status and caller-supplied record counts for each step.
"""

from __future__ import annotations

import json
import logging
import sys
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, TextIO

_HANDLER_NAME = "talent_etl_json"


def new_run_id() -> str:
    """Return a sortable, unique run id such as ``20261014T130622Z-1a2b3c4d``."""
    return f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def utc_now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string with millisecond precision."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class JsonFormatter(logging.Formatter):
    """Format log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        """Serialize the record, including ``extra={"context": {...}}`` and exceptions."""
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "logger": record.name,
            "run_id": getattr(record, "run_id", None),
            "message": record.getMessage(),
        }
        context = getattr(record, "context", None)
        if context:
            payload["context"] = context
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class _RunIdFilter(logging.Filter):
    """Attach the current run id to every record passing through the handler."""

    def __init__(self, run_id: str | None) -> None:
        super().__init__()
        self.run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = self.run_id
        return True


def configure_logging(
    level: str = "INFO", run_id: str | None = None, stream: TextIO | None = None
) -> None:
    """
    Install a JSON handler on the root logger.

    Safe to call repeatedly: the handler installed by a previous call is replaced.

    Args:
        level: Root log level name.
        run_id: Pipeline run id added to every record.
        stream: Output stream; defaults to stdout so Kubernetes captures it.
    """
    root = logging.getLogger()
    for handler in list(root.handlers):
        if handler.get_name() == _HANDLER_NAME:
            root.removeHandler(handler)
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.set_name(_HANDLER_NAME)
    handler.setFormatter(JsonFormatter())
    handler.addFilter(_RunIdFilter(run_id))
    root.addHandler(handler)
    root.setLevel(level)
    for noisy in ("botocore", "boto3", "urllib3", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


@contextmanager
def log_step(logger: logging.Logger, step: str, **context: Any) -> Iterator[dict[str, Any]]:
    """
    Log the start, end, duration and status of a pipeline step.

    The yielded dict collects metrics (for example ``rows_read``) that are logged
    on completion. Exceptions are logged with status ``FAILED`` and re-raised.

    Args:
        logger: Logger to write to.
        step: Step name, for example ``bronze.employees``.
        **context: Static fields to include on every record for this step.
    """
    metrics: dict[str, Any] = {}
    started_at = utc_now_iso()
    start = time.perf_counter()
    logger.info("step started", extra={"context": {"step": step, "started_at": started_at, **context}})
    try:
        yield metrics
    except BaseException:
        logger.error(
            "step failed",
            exc_info=True,
            extra={"context": {
                "step": step, "status": "FAILED", "started_at": started_at,
                "ended_at": utc_now_iso(),
                "duration_s": round(time.perf_counter() - start, 3),
                **context, "metrics": metrics,
            }},
        )
        raise
    logger.info(
        "step succeeded",
        extra={"context": {
            "step": step, "status": "SUCCEEDED", "started_at": started_at,
            "ended_at": utc_now_iso(),
            "duration_s": round(time.perf_counter() - start, 3),
            **context, "metrics": metrics,
        }},
    )

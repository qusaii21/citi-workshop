"""Unit tests for talent_etl.logging_utils."""

import io
import json
import logging
import re

import pytest

from talent_etl.logging_utils import configure_logging, log_step, new_run_id


@pytest.fixture
def log_stream():
    stream = io.StringIO()
    configure_logging("INFO", run_id="run-123", stream=stream)
    yield stream
    configure_logging("WARNING")


def records(stream):
    return [json.loads(line) for line in stream.getvalue().splitlines()]


def test_new_run_id_is_sortable_and_unique():
    first, second = new_run_id(), new_run_id()
    assert re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-f]{8}", first)
    assert first != second


def test_records_are_json_with_run_id_and_context(log_stream):
    logging.getLogger("t").warning("hello %s", "world", extra={"context": {"rows": 3}})
    (record,) = records(log_stream)
    assert record["level"] == "WARNING"
    assert record["message"] == "hello world"
    assert record["run_id"] == "run-123"
    assert record["context"] == {"rows": 3}


def test_exceptions_are_serialized(log_stream):
    try:
        raise ValueError("bad row")
    except ValueError:
        logging.getLogger("t").error("failed", exc_info=True)
    (record,) = records(log_stream)
    assert "ValueError: bad row" in record["exception"]


def test_configure_logging_is_idempotent(log_stream):
    configure_logging("INFO", run_id="run-456", stream=log_stream)
    logging.getLogger("t").info("once")
    lines = records(log_stream)
    assert len(lines) == 1
    assert lines[0]["run_id"] == "run-456"


def test_log_step_success_records_duration_status_and_metrics(log_stream):
    with log_step(logging.getLogger("t"), "bronze.employees", table="employees") as metrics:
        metrics["rows_read"] = 2000
    started, finished = records(log_stream)
    assert started["message"] == "step started"
    assert started["context"]["table"] == "employees"
    ctx = finished["context"]
    assert finished["message"] == "step succeeded"
    assert ctx["status"] == "SUCCEEDED"
    assert ctx["metrics"] == {"rows_read": 2000}
    assert ctx["duration_s"] >= 0
    assert ctx["started_at"] <= ctx["ended_at"]


def test_log_step_failure_logs_error_and_reraises(log_stream):
    with pytest.raises(RuntimeError), log_step(logging.getLogger("t"), "silver.events") as metrics:
        metrics["rows_read"] = 10
        raise RuntimeError("schema drift")
    finished = records(log_stream)[-1]
    assert finished["level"] == "ERROR"
    assert finished["context"]["status"] == "FAILED"
    assert finished["context"]["metrics"] == {"rows_read": 10}
    assert "schema drift" in finished["exception"]

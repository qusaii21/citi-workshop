"""Unit tests for talent_etl.retry."""

import logging

import pytest

from talent_etl.config import RetryConfig
from talent_etl.retry import RetryPolicy


class Flaky:
    """Callable that fails a fixed number of times before returning a value."""

    def __init__(self, failures, error=ConnectionError("boom")):
        self.failures = failures
        self.error = error
        self.calls = 0

    def __call__(self):
        self.calls += 1
        if self.calls <= self.failures:
            raise self.error
        return "ok"


def make_policy(attempts=4, sleeps=None, **kwargs):
    recorded = [] if sleeps is None else sleeps
    return RetryPolicy(attempts=attempts, base_delay=1.0, max_delay=5.0, jitter=False,
                       sleep=recorded.append, **kwargs), recorded


def test_returns_immediately_on_success():
    policy, sleeps = make_policy()
    assert policy.call(lambda: 42, description="op") == 42
    assert sleeps == []


def test_retries_transient_errors_with_exponential_backoff():
    policy, sleeps = make_policy(attempts=4)
    op = Flaky(failures=3)
    assert policy.call(op, description="op") == "ok"
    assert op.calls == 4
    assert sleeps == [1.0, 2.0, 4.0]


def test_backoff_is_capped_at_max_delay():
    policy, _ = make_policy()
    assert [policy.delay_for(n) for n in range(1, 6)] == [1.0, 2.0, 4.0, 5.0, 5.0]


def test_jitter_scales_delay_between_half_and_full():
    policy = RetryPolicy(base_delay=2.0, max_delay=10.0, jitter=True)
    for _ in range(50):
        assert 1.0 <= policy.delay_for(1) <= 2.0


def test_raises_last_error_when_attempts_exhausted(caplog):
    policy, sleeps = make_policy(attempts=3)
    op = Flaky(failures=10)
    with caplog.at_level(logging.WARNING), pytest.raises(ConnectionError):
        policy.call(op, description="op")
    assert op.calls == 3
    assert len(sleeps) == 2
    assert [r.message for r in caplog.records] == [
        "transient error, retrying", "transient error, retrying", "retries exhausted",
    ]


def test_non_retryable_error_fails_without_sleeping(caplog):
    policy, sleeps = make_policy()
    op = Flaky(failures=1, error=PermissionError("AccessDenied"))
    with caplog.at_level(logging.DEBUG), pytest.raises(PermissionError):
        policy.call(op, description="op", is_retryable=lambda exc: not isinstance(exc, PermissionError))
    assert op.calls == 1
    assert sleeps == []
    assert caplog.records[-1].levelname == "DEBUG"
    assert caplog.records[-1].context["error"] == "PermissionError: AccessDenied"


def test_single_attempt_policy_never_sleeps():
    policy, sleeps = make_policy(attempts=1)
    with pytest.raises(ConnectionError):
        policy.call(Flaky(failures=1), description="op")
    assert sleeps == []


def test_from_config():
    policy = RetryPolicy.from_config(RetryConfig(attempts=2, base_delay=0.1, max_delay=0.4))
    assert (policy.attempts, policy.base_delay, policy.max_delay, policy.jitter) == (2, 0.1, 0.4, True)

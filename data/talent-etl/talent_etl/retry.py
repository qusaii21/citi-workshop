"""
Retry with exponential backoff for transient PostgreSQL and S3 failures.

Callers decide which errors are transient through ``is_retryable``; permanent
errors such as access denied fail immediately instead of burning retries.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TypeVar

from talent_etl.config import RetryConfig

T = TypeVar("T")

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RetryPolicy:
    """
    Exponential backoff policy: delay = min(max_delay, base_delay * 2 ** (attempt - 1)).

    With ``jitter`` enabled the delay is scaled by a random factor in [0.5, 1.0]
    so that parallel jobs do not retry in lockstep.
    """

    attempts: int = 4
    base_delay: float = 1.0
    max_delay: float = 30.0
    jitter: bool = True
    sleep: Callable[[float], None] = field(default=time.sleep, repr=False, compare=False)

    @classmethod
    def from_config(cls, config: RetryConfig) -> RetryPolicy:
        """Build a policy from the ``RETRY_*`` settings."""
        return cls(attempts=config.attempts, base_delay=config.base_delay, max_delay=config.max_delay)

    def delay_for(self, attempt: int) -> float:
        """Return the delay to wait after failed attempt number ``attempt`` (1-based)."""
        delay = min(self.max_delay, self.base_delay * 2 ** (attempt - 1))
        return delay * random.uniform(0.5, 1.0) if self.jitter else delay  # nosec B311

    def call(
        self,
        operation: Callable[[], T],
        *,
        description: str,
        is_retryable: Callable[[BaseException], bool] = lambda _exc: True,
    ) -> T:
        """
        Run ``operation`` until it succeeds or the attempts are exhausted.

        Args:
            operation: Zero-argument callable to run.
            description: Human-readable name used in log messages.
            is_retryable: Returns True for errors worth retrying.

        Returns:
            The value returned by ``operation``.

        Raises:
            Exception: The last error, once it is not retryable or attempts run out.
        """
        for attempt in range(1, self.attempts + 1):
            try:
                return operation()
            except Exception as exc:
                context = {
                    "operation": description, "attempt": attempt,
                    "max_attempts": self.attempts, "error": f"{type(exc).__name__}: {exc}",
                }
                if not is_retryable(exc):
                    # Expected for e.g. "not found" probes; callers decide whether it is a failure.
                    logger.debug("non-retryable error", extra={"context": context})
                    raise
                if attempt == self.attempts:
                    logger.error("retries exhausted", extra={"context": context})
                    raise
                delay = self.delay_for(attempt)
                logger.warning(
                    "transient error, retrying",
                    extra={"context": {**context, "retry_in_s": round(delay, 3)}},
                )
                self.sleep(delay)
        raise AssertionError("unreachable: attempts must be >= 1")

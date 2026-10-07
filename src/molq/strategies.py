"""Pluggable polling strategies for job monitoring.

This module provides different strategies for determining the interval
between status polls. The default ExponentialBackoffStrategy starts fast
and slows down, reducing unnecessary scheduler queries for long jobs.
"""

from typing import Protocol


class PollingStrategy(Protocol):
    """Protocol for polling interval strategies."""

    def next_interval(self, elapsed: float, poll_count: int) -> float:
        """Return seconds to sleep before the next poll.

        Args:
            elapsed: Total seconds since monitoring started.
            poll_count: Number of polls completed so far.

        Returns:
            Seconds to wait before next poll.
        """
        ...


class FixedStrategy:
    """Fixed interval between polls.

    Simple but wasteful for long-running jobs.
    """

    def __init__(self, interval: float = 5.0):
        self._interval = interval

    def next_interval(self, elapsed: float, poll_count: int) -> float:
        return self._interval


class ExponentialBackoffStrategy:
    """Start with short intervals, gradually increase.

    Default strategy. For a 24h job, reduces poll count from ~43k
    (at 2s fixed) to ~60 calls.
    """

    def __init__(
        self,
        initial: float = 1.0,
        maximum: float = 60.0,
        factor: float = 1.5,
    ):
        self._initial = initial
        self._maximum = maximum
        self._factor = factor
        # Polls after which the interval is capped at `maximum`. Clamping the
        # exponent keeps ``factor ** poll_count`` from overflowing for jobs that
        # run many hours (default backoff saturates after ~11 polls).
        if factor > 1.0:
            import math

            self._saturate_at = math.ceil(
                math.log(maximum / initial) / math.log(factor)
            )
        else:
            self._saturate_at = float("inf")

    def next_interval(self, elapsed: float, poll_count: int) -> float:
        if poll_count >= self._saturate_at:
            return self._maximum
        return min(self._initial * (self._factor**poll_count), self._maximum)


class AdaptiveStrategy:
    """Adapts interval based on expected job duration.

    Uses ~1% of expected duration as the poll interval, clamped to [1s, 300s].
    Falls back to exponential backoff if no duration estimate is available.
    """

    def __init__(self, expected_duration: float | None = None):
        self._expected = expected_duration
        self._fallback = ExponentialBackoffStrategy()

    def next_interval(self, elapsed: float, poll_count: int) -> float:
        if self._expected is None:
            return self._fallback.next_interval(elapsed, poll_count)
        return max(1.0, min(self._expected * 0.01, 300.0))

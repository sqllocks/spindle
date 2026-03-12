"""Token-bucket rate limiter with Poisson inter-arrival time support."""

from __future__ import annotations

import time
from typing import Callable


class TokenBucket:
    """Token bucket rate limiter.

    Tokens accumulate at ``rate`` tokens per second, capped at ``burst_capacity``.
    Each call to :meth:`consume` deducts one token.  If no token is available,
    the caller should wait the returned duration before emitting the next event.

    Args:
        rate: Target events per second.
        burst_capacity: Maximum tokens that can accumulate.  Defaults to ``2 * rate``
            (allows brief bursts without stalling).
        clock: Monotonic time function — injectable for testing.
    """

    def __init__(
        self,
        rate: float,
        burst_capacity: float | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._rate = max(rate, 0.001)
        self._capacity = burst_capacity if burst_capacity is not None else max(self._rate * 2, 1.0)
        self._tokens = self._capacity
        self._clock = clock or time.monotonic
        self._last_refill = self._clock()

    def update_rate(self, rate: float) -> None:
        """Update the token accumulation rate."""
        self._rate = max(rate, 0.001)

    def _refill(self) -> None:
        now = self._clock()
        elapsed = now - self._last_refill
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        self._last_refill = now

    def consume(self) -> float:
        """Consume one token.

        Returns:
            ``0.0`` if a token was available immediately, otherwise the number of
            seconds the caller should sleep before the next token arrives.
        """
        self._refill()
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return 0.0
        wait = (1.0 - self._tokens) / self._rate
        return wait

    def wait_and_consume(self) -> None:
        """Block until a token is available, then consume it."""
        wait = self.consume()
        if wait > 0:
            time.sleep(wait)


def poisson_interarrival(rate: float, rng) -> float:
    """Draw a single inter-arrival time from the exponential distribution.

    In a Poisson process inter-arrival times are Exponentially distributed
    with mean ``1 / rate``.  This produces realistic arrival jitter rather than
    perfectly uniform spacing.

    Args:
        rate: Expected events per second (must be > 0).
        rng: ``numpy.random.Generator`` instance.

    Returns:
        Seconds to wait before emitting the next event.
    """
    if rate <= 0:
        return float("inf")
    return float(rng.exponential(1.0 / rate))

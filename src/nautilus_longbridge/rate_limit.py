from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


class SlidingWindowRateLimiter:
    """Async sliding-window limiter with cancellation-safe waits."""

    def __init__(self, limit: int, period_secs: float, *, min_interval_secs: float = 0.0) -> None:
        self.limit = limit
        self.period_secs = period_secs
        self.min_interval_secs = min_interval_secs
        self._timestamps: deque[float] = deque()
        self._last = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                while self._timestamps and now - self._timestamps[0] >= self.period_secs:
                    self._timestamps.popleft()
                wait_window = (
                    self.period_secs - (now - self._timestamps[0])
                    if len(self._timestamps) >= self.limit
                    else 0.0
                )
                wait_interval = max(0.0, self.min_interval_secs - (now - self._last))
                wait_for = max(wait_window, wait_interval)
                if wait_for <= 0:
                    self._timestamps.append(now)
                    self._last = now
                    return
            await asyncio.sleep(wait_for)


class ConcurrencyLimiter:
    def __init__(self, limit: int) -> None:
        self._semaphore = asyncio.Semaphore(limit)

    async def run(self, operation: Callable[[], Awaitable[T]]) -> T:
        async with self._semaphore:
            return await operation()

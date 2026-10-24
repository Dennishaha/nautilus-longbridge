from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")


@dataclass
class _Waiter:
    future: asyncio.Future[None]
    cancelled: bool = False


class SlidingWindowRateLimiter:
    """FIFO async sliding-window limiter with cancellation-safe waits."""

    def __init__(self, limit: int, period_secs: float, *, min_interval_secs: float = 0.0) -> None:
        if limit < 1:
            raise ValueError("limit must be positive")
        if period_secs <= 0:
            raise ValueError("period_secs must be positive")
        if min_interval_secs < 0:
            raise ValueError("min_interval_secs cannot be negative")
        self.limit = limit
        self.period_secs = period_secs
        self.min_interval_secs = min_interval_secs
        self._timestamps: deque[float] = deque()
        self._last = 0.0
        self._queue: deque[_Waiter] = deque()
        self._queue_lock = asyncio.Lock()
        self._processor: asyncio.Task[None] | None = None

    async def acquire(self) -> None:
        loop = asyncio.get_running_loop()
        waiter = _Waiter(loop.create_future())
        async with self._queue_lock:
            self._queue.append(waiter)
            if self._processor is None or self._processor.done():
                self._processor = asyncio.create_task(
                    self._process_waiters(), name="longbridge-rate-limit"
                )
        try:
            await waiter.future
        except asyncio.CancelledError:
            waiter.cancelled = True
            async with self._queue_lock:
                with suppress(ValueError):
                    self._queue.remove(waiter)
            raise

    async def _process_waiters(self) -> None:
        current = asyncio.current_task()
        try:
            while True:
                async with self._queue_lock:
                    if not self._queue:
                        return
                    waiter = self._queue[0]
                    if waiter.cancelled or waiter.future.cancelled():
                        self._queue.popleft()
                        continue
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
                        self._queue.popleft()
                        self._timestamps.append(now)
                        self._last = now
                        waiter.future.set_result(None)
                        continue
                await asyncio.sleep(wait_for)
        finally:
            async with self._queue_lock:
                if self._processor is current:
                    self._processor = None
                    if current is not None and current.cancelled():
                        while self._queue:
                            waiter = self._queue.popleft()
                            waiter.cancelled = True
                            if not waiter.future.done():
                                waiter.future.cancel()


class ConcurrencyLimiter:
    def __init__(self, limit: int) -> None:
        self._semaphore = asyncio.Semaphore(limit)

    async def run(self, operation: Callable[[], Awaitable[T]]) -> T:
        async with self._semaphore:
            return await operation()

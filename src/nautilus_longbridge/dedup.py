from __future__ import annotations

import time
from collections import deque


class TimedDeduplicator:
    """Bounded, time-based deduplication for SDK snapshots and push events.

    The complete caller-supplied key is retained. Entries expire from the
    monotonic TTL window, and the capacity bound prevents an unhealthy feed
    from growing process memory without limit.
    """

    def __init__(self, ttl_secs: float = 300.0, max_entries: int = 200_000) -> None:
        if ttl_secs <= 0:
            raise ValueError("ttl_secs must be positive")
        if max_entries < 1:
            raise ValueError("max_entries must be positive")
        self.ttl_secs = ttl_secs
        self.max_entries = max_entries
        self._seen: dict[str, float] = {}
        self._expiry: deque[tuple[float, str]] = deque()

    def _purge(self, now: float) -> None:
        expiry_cutoff = now - self.ttl_secs
        while self._expiry and self._expiry[0][0] <= expiry_cutoff:
            timestamp, key = self._expiry.popleft()
            if self._seen.get(key) == timestamp:
                self._seen.pop(key, None)
        while len(self._seen) > self.max_entries and self._expiry:
            timestamp, key = self._expiry.popleft()
            if self._seen.get(key) == timestamp:
                self._seen.pop(key, None)

    def add(self, key: str) -> bool:
        """Return ``True`` for a new key and ``False`` for a duplicate."""

        now = time.monotonic()
        self._purge(now)
        if key in self._seen:
            return False
        self._seen[key] = now
        self._expiry.append((now, key))
        self._purge(now)
        return True

    def __len__(self) -> int:
        self._purge(time.monotonic())
        return len(self._seen)

    def clear(self) -> None:
        self._seen.clear()
        self._expiry.clear()

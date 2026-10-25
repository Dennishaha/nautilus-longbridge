from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal

from nautilus_longbridge.mapping import aggressor_side, price, timestamp_ns
from nautilus_longbridge.rate_limit import SlidingWindowRateLimiter


def test_timestamp_is_utc_nanoseconds() -> None:
    value = datetime(2026, 1, 1, tzinfo=UTC)
    assert timestamp_ns(value, 0) == 1_767_225_600_000_000_000


def test_decimal_price_preserves_requested_precision() -> None:
    converted = price(Decimal("12.34567"), 4)
    assert converted.as_decimal() == Decimal("12.3457")


def test_aggressor_mapping() -> None:
    from longbridge import openapi

    assert aggressor_side(openapi.TradeDirection.Up).name == "BUY"
    assert aggressor_side(openapi.TradeDirection.Down).name == "SELL"


def test_rate_limiter_enforces_window() -> None:
    async def scenario() -> float:
        limiter = SlidingWindowRateLimiter(1, 0.02)
        await limiter.acquire()
        start = asyncio.get_running_loop().time()
        await limiter.acquire()
        return asyncio.get_running_loop().time() - start

    assert asyncio.run(scenario()) >= 0.015


def test_sdk_local_naive_timestamp_does_not_get_mislabeled_as_utc(monkeypatch) -> None:
    import time

    with monkeypatch.context() as patch:
        patch.setenv("TZ", "Asia/Shanghai")
        time.tzset()
        try:
            # SDK fromtimestamp(..., None) on a Shanghai host.
            summer = datetime(2025, 3, 10, 21, 30)
            winter = datetime(2025, 3, 7, 22, 30)
            assert timestamp_ns(summer, 0) == int(
                datetime(2025, 3, 10, 13, 30, tzinfo=UTC).timestamp() * 1e9
            )
            assert timestamp_ns(winter, 0) == int(
                datetime(2025, 3, 7, 14, 30, tzinfo=UTC).timestamp() * 1e9
            )
        finally:
            patch.undo()
            time.tzset()

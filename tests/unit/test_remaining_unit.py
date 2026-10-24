from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from nautilus_longbridge.dedup import TimedDeduplicator
from nautilus_longbridge.rate_limit import SlidingWindowRateLimiter


def test_timed_deduplicator_is_bounded_and_keeps_complete_keys(monkeypatch) -> None:
    now = 100.0
    monkeypatch.setattr("nautilus_longbridge.dedup.time.monotonic", lambda: now)
    deduplicator = TimedDeduplicator(ttl_secs=10.0, max_entries=2)

    assert deduplicator.add("a|same-prefix|1")
    assert not deduplicator.add("a|same-prefix|1")
    assert deduplicator.add("a|same-prefix|2")
    assert deduplicator.add("a|same-prefix|3")
    assert len(deduplicator) == 2
    assert deduplicator.add("a|same-prefix|1")

    now = 111.0
    assert deduplicator.add("expired")
    assert len(deduplicator) == 1


def test_timed_deduplicator_stays_bounded_under_high_frequency_input() -> None:
    deduplicator = TimedDeduplicator(ttl_secs=300.0, max_entries=4096)
    for index in range(100_000):
        assert deduplicator.add(f"symbol-{index % 1000}|event|{index}")
    assert len(deduplicator) == 4096
    assert not deduplicator.add("symbol-999|event|99999")


@pytest.mark.asyncio
async def test_rate_limiter_grants_waiters_in_fifo_order() -> None:
    limiter = SlidingWindowRateLimiter(100, 1.0)
    granted: list[int] = []

    async def worker(index: int) -> None:
        await limiter.acquire()
        granted.append(index)

    await asyncio.gather(*(worker(index) for index in range(100)))
    assert granted == list(range(100))


@pytest.mark.asyncio
async def test_rate_limiter_removes_cancelled_waiter() -> None:
    limiter = SlidingWindowRateLimiter(1, 0.03)
    await limiter.acquire()
    cancelled = asyncio.create_task(limiter.acquire())
    next_waiter = asyncio.create_task(limiter.acquire())
    await asyncio.sleep(0)
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    await asyncio.wait_for(next_waiter, timeout=0.2)


def test_typed_security_metadata_accepts_stock_and_etf_but_rejects_unknown() -> None:
    from nautilus_longbridge.errors import UnsupportedOperation
    from nautilus_longbridge.instruments import LongbridgeInstrumentProvider

    provider = LongbridgeInstrumentProvider()
    base = dict(symbol="700.HK", currency="HKD", lot_size=100)
    assert provider._to_equity(SimpleNamespace(**base, security_type="STOCK"))
    assert provider._to_equity(SimpleNamespace(**base, security_type="ETF"))
    with pytest.raises(UnsupportedOperation, match="does not support"):
        provider._to_equity(SimpleNamespace(**base, security_type="WARRANT"))
    with pytest.raises(UnsupportedOperation, match="security type"):
        provider._to_equity(SimpleNamespace(**base, security_type="FUND"))


def test_sdk_static_info_without_security_type_remains_compatible() -> None:
    from nautilus_longbridge.instruments import LongbridgeInstrumentProvider

    instrument = LongbridgeInstrumentProvider()._to_equity(
        SimpleNamespace(symbol="700.HK", currency="HKD", lot_size=100)
    )
    assert instrument.raw_symbol.value == "700.HK"


def test_sdk_board_metadata_rejects_warrants() -> None:
    from longbridge import openapi

    from nautilus_longbridge.errors import UnsupportedOperation
    from nautilus_longbridge.instruments import LongbridgeInstrumentProvider

    with pytest.raises(UnsupportedOperation, match="warrant"):
        LongbridgeInstrumentProvider()._to_equity(
            SimpleNamespace(
                symbol="12345.HK",
                currency="HKD",
                lot_size=100,
                board=openapi.SecurityBoard.HKWarrant,
            )
        )


def test_trade_key_distinguishes_direction_and_trade_type() -> None:
    from nautilus_longbridge.data import LongbridgeDataClient

    timestamp = 1_800_000_000_000_000_000
    buy = SimpleNamespace(price=Decimal("10"), volume=100, direction="Buy", trade_type="A")
    sell = SimpleNamespace(price=Decimal("10"), volume=100, direction="Sell", trade_type="A")
    assert LongbridgeDataClient._trade_key("700.HK", buy, timestamp) != LongbridgeDataClient._trade_key(
        "700.HK", sell, timestamp
    )


def test_synthetic_execution_id_is_stable() -> None:
    from nautilus_longbridge.execution import LongbridgeExecutionClient

    value = SimpleNamespace(
        order_id="123",
        symbol="700.HK",
        price=Decimal("10.20"),
        quantity=Decimal("100"),
        side="Buy",
        trade_done_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    first = LongbridgeExecutionClient._execution_id(object.__new__(LongbridgeExecutionClient), value)
    second = LongbridgeExecutionClient._execution_id(object.__new__(LongbridgeExecutionClient), value)
    assert first == second
    assert first.startswith("SYN-")

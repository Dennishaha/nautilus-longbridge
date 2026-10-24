from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from tests.fixtures.fakes import FakeContextFactory, FakeQuoteContext, FakeTradeContext

from nautilus_longbridge.config import LongbridgeDataClientConfig, LongbridgeExecutionClientConfig
from nautilus_longbridge.data import LongbridgeDataClient
from nautilus_longbridge.execution import LongbridgeExecutionClient
from nautilus_longbridge.instruments import LongbridgeInstrumentProvider
from nautilus_longbridge.lifecycle import ConnectionState
from nautilus_longbridge.rate_limit import ConcurrencyLimiter, SlidingWindowRateLimiter


class FixedClock:
    def __init__(self, value: int) -> None:
        self.value = value

    def timestamp_ns(self) -> int:
        return self.value


def _provider() -> LongbridgeInstrumentProvider:
    provider = LongbridgeInstrumentProvider()
    provider.add(
        provider._to_equity(
            SimpleNamespace(symbol="700.HK", currency="HKD", lot_size=100)
        )
    )
    return provider


@pytest.mark.asyncio
async def test_reconnect_backfills_trades_and_deduplicates_live_push() -> None:
    start_ns = 1_800_000_000_000_000_000
    end_ns = start_ns + 30_000_000_000
    item = SimpleNamespace(
        timestamp=datetime.fromtimestamp((start_ns + 10_000_000_000) / 1e9, UTC),
        price=Decimal("10.20"),
        volume=100,
        direction="Up",
        trade_type="Automatch",
    )
    factory = FakeContextFactory()
    factory.quote_sequence = [FakeQuoteContext()]
    factory.quote_sequence[0].trade_values["700.HK"] = [item]
    client = object.__new__(LongbridgeDataClient)
    client._lb_config = LongbridgeDataClientConfig(
        reconnect_initial_secs=0.001,
        reconnect_max_secs=0.001,
        reconnect_max_attempts=1,
    )
    client._context_factory = factory
    client._quote = FakeQuoteContext()
    client._desired_quotes = set()
    client._desired_trades = {"700.HK"}
    client._desired_bars = {}
    client._desired_depth = set()
    client._last_quotes = {}
    client._last_depth = {}
    client._quote_rate = SlidingWindowRateLimiter(100, 1.0)
    client._quote_concurrency = ConcurrencyLimiter(5)
    client._reconnect_lock = __import__("asyncio").Lock()
    client._reconnect_task = None
    client._connection_state = ConnectionState.RECONNECTING
    client._disconnect_started_ns = start_ns
    client.clock = FixedClock(end_ns)
    client.instrument_provider = _provider()
    output: list[object] = []
    client._handle_data = output.append

    await client._reconnect_with_backoff()
    await client._publish_trades("700.HK", SimpleNamespace(trades=[item]))

    assert len(output) == 1
    assert client._connection_state is ConnectionState.CONNECTED
    assert factory.quote_context.subscription_calls
    assert factory.quote_context.subscription_calls[0][0] == ["700.HK"]


@pytest.mark.asyncio
async def test_health_reconnect_is_single_task_and_failed_state_is_terminal() -> None:
    factory = FakeContextFactory()
    client = object.__new__(LongbridgeExecutionClient)
    client._lb_config = LongbridgeExecutionClientConfig(
        account_id="ACC-1",
        reconnect_initial_secs=0.001,
        reconnect_max_secs=0.001,
        reconnect_max_attempts=2,
    )
    client._context_factory = factory
    client._trade = factory.trade_context
    client._trade_rate = SlidingWindowRateLimiter(100, 1.0)
    client._reconnect_lock = __import__("asyncio").Lock()
    client._reconnect_task = None
    client._connection_state = ConnectionState.CONNECTED
    factory.trade_sequence = [FakeTradeContext()]
    first = client._start_reconnect()
    second = client._start_reconnect()
    assert first is second
    assert first is not None
    await first
    assert client._connection_state is ConnectionState.CONNECTED

    failing = FakeTradeContext()
    failing.subscribe_exception = RuntimeError("offline")
    client._context_factory.trade_sequence = [failing, failing]
    client._connection_state = ConnectionState.CONNECTED
    failed_task = client._start_reconnect()
    assert failed_task is not None
    await failed_task
    assert client._connection_state is ConnectionState.FAILED


@pytest.mark.asyncio
async def test_missing_execution_id_is_deduplicated_with_synthetic_id() -> None:
    context = FakeTradeContext()
    execution = SimpleNamespace(
        order_id="123",
        symbol="700.HK",
        quantity=Decimal("100"),
        price=Decimal("10.20"),
        side="Buy",
        trade_done_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    context.executions = [execution]
    client = object.__new__(LongbridgeExecutionClient)
    client._lb_config = LongbridgeExecutionClientConfig(account_id="ACC-1")
    client._trade = context
    client._trade_rate = SlidingWindowRateLimiter(100, 1.0)
    client._seen_fills = set()
    client._venue_by_client = {}
    client._last_filled = {}
    client.instrument_provider = _provider()
    client.clock = FixedClock(1_800_000_000_000_000_000)
    reports: list[object] = []
    generated: list[object] = []
    client._handle_report = reports.append
    client.generate_order_filled = lambda *args: generated.append(args)
    order = SimpleNamespace(client_order_id="O-1")

    await client._emit_fills_for_order("123", order)
    await client._emit_fills_for_order("123", order)

    assert len(reports) == 1
    assert len(generated) == 1
    assert reports[0].trade_id.value.startswith("SYN-")

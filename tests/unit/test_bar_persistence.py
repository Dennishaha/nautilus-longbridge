from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from longbridge.openapi import Period
from nautilus_trader.model import BarType
from tests.fixtures.fakes import FakeQuoteContext
from tests.unit.test_data_conversion import _client

from nautilus_longbridge.data import _bar_type, _period_for_bar_type
from nautilus_longbridge.lifecycle import ConnectionState
from nautilus_longbridge.symbols import instrument_id_from_symbol


def candle(period=Period.Min_1, *, confirmed=True, offset=0, **changes):
    values = dict(
        timestamp=datetime(2026, 1, 2, 14, 30, tzinfo=UTC) + timedelta(minutes=offset),
        open="10",
        high="11",
        low="9",
        close="10.5",
        volume=100,
    )
    values.update(changes)
    return SimpleNamespace(
        period=period, is_confirmed=confirmed, candlestick=SimpleNamespace(**values)
    )


@pytest.mark.parametrize(
    ("period", "spec"),
    [(Period.Min_1, "1-MINUTE"), (Period.Min_60, "1-HOUR"), (Period.Day, "1-DAY")],
)
def test_sdk_periods_round_trip_without_name_attribute(period, spec):
    iid = instrument_id_from_symbol("700.HK")
    bar_type = BarType.from_str(f"{iid}-{spec}-LAST-EXTERNAL")
    assert _period_for_bar_type(bar_type) == period
    assert _bar_type(iid, str(period).split(".")[-1]) == bar_type


@pytest.mark.parametrize("spec", ["1-MINUTE-BID-EXTERNAL", "1-MINUTE-LAST-INTERNAL"])
def test_non_vendor_bar_types_are_rejected(spec):
    with pytest.raises(ValueError):
        _period_for_bar_type(BarType.from_str(f"700.HK.LONGBRIDGE-{spec}"))


@pytest.mark.asyncio
async def test_unknown_sdk_period_is_never_persisted_as_another_period():
    client = _client()
    client._handle_data = lambda _: pytest.fail("unsupported period was published")
    with pytest.raises(ValueError, match="period"):
        await client._publish_bar("700.HK", candle(Period.Min_5))


@pytest.mark.asyncio
async def test_final_update_survives_identical_unconfirmed_and_high_low_changes():
    client = _client()
    output = []
    client._handle_data = output.append
    await client._publish_bar("700.HK", candle(confirmed=False))
    await client._publish_bar("700.HK", candle(confirmed=False, high="12"))
    await client._publish_bar("700.HK", candle(high="12"))
    await client._publish_bar("700.HK", candle(high="12"))
    assert len(output) == 3
    assert str(output[-1].high) == "12.0000"


@pytest.mark.asyncio
async def test_completed_only_suppresses_replay_after_ttl_and_conflicting_final(caplog):
    client = _client()
    client._lb_config.completed_bars_only = True
    output = []
    client._handle_data = output.append
    await client._publish_bar("700.HK", candle(confirmed=False))
    await client._publish_bar("700.HK", candle())
    client._deduplicator.clear()
    await client._publish_bar("700.HK", candle())
    await client._publish_bar("700.HK", candle(high="12"))
    await client._publish_bar("700.HK", candle(offset=1))
    await client._publish_bar("700.HK", candle(offset=-1))
    assert len(output) == 2
    assert "Conflicting confirmed candle" in caplog.text


@pytest.mark.asyncio
async def test_two_periods_for_one_symbol_restore_and_unsubscribe_independently():
    client = _client()
    client._quote = FakeQuoteContext()
    client._desired_bars = {}
    client._desired_quotes = set()
    client._desired_trades = set()
    commands = [
        SimpleNamespace(bar_type=BarType.from_str(f"700.HK.LONGBRIDGE-1-{p}-LAST-EXTERNAL"))
        for p in ("MINUTE", "HOUR")
    ]
    for command in commands:
        await client._subscribe_bars(command)
    client._quote.bar_subscriptions.clear()
    await client._restore_subscriptions()
    assert [entry[1] for entry in client._quote.bar_subscriptions] == [Period.Min_1, Period.Min_60]
    await client._unsubscribe_bars(commands[0])
    assert [entry[1] for entry in client._quote.bar_subscriptions] == [Period.Min_60]
    assert len(client._desired_bars) == 1


@pytest.mark.asyncio
async def test_live_all_session_subscription_keeps_night_selection_after_reconnect():
    from longbridge.openapi import TradeSessions

    client = _client()
    client._lb_config.live_trade_sessions = "All"
    client._quote = FakeQuoteContext()
    client._quote.static_info = AsyncMock(
        return_value=[SimpleNamespace(symbol="AAPL.US", currency="USD", lot_size=1)]
    )
    client.instrument_provider.set_quote_context(client._quote)
    client._handle_instrument = lambda _: None
    client._desired_bars, client._desired_quotes, client._desired_trades = {}, set(), set()
    selections = []

    async def subscribe(symbol, period, trade_sessions):
        selections.append(trade_sessions)
        return []

    client._quote.subscribe_candlesticks = subscribe
    command = SimpleNamespace(bar_type=BarType.from_str("AAPL.US.LONGBRIDGE-1-MINUTE-LAST-EXTERNAL"))
    await client._subscribe_bars(command)
    await client._restore_subscriptions()
    assert selections == [TradeSessions.All, TradeSessions.All]


@pytest.mark.asyncio
async def test_disconnect_drains_accepted_candles_before_cancelling_consumer():
    client = _client()
    client._quote = FakeQuoteContext()
    client._desired_bars = {}
    client._desired_quotes = set()
    client._desired_trades = set()
    client._health_task = None
    client._queue = asyncio.Queue()
    client._connection_state = ConnectionState.CONNECTED
    client._lb_config.completed_bars_only = True
    output = []
    client._handle_data = output.append
    client._consumer = asyncio.create_task(client._consume())
    for offset in range(3):
        await client._on_candlestick("700.HK", candle(offset=offset))
    await client._disconnect()
    assert len(output) == 3
    assert client._queue is None and client._consumer is None

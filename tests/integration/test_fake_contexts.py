from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from longbridge import openapi
from nautilus_trader.model import BarType, BookType
from tests.fixtures.fakes import FakeContextFactory

from nautilus_longbridge.config import LongbridgeDataClientConfig
from nautilus_longbridge.data import LongbridgeDataClient
from nautilus_longbridge.instruments import LongbridgeInstrumentProvider
from nautilus_longbridge.rate_limit import ConcurrencyLimiter, SlidingWindowRateLimiter
from nautilus_longbridge.symbols import instrument_id_from_symbol


def _subscription_client(factory: FakeContextFactory) -> LongbridgeDataClient:
    client = object.__new__(LongbridgeDataClient)
    client._lb_config = LongbridgeDataClientConfig()
    client._quote = factory.quote_context
    client._desired_quotes = set()
    client._desired_trades = set()
    client._desired_bars = {}
    client._desired_depth = set()
    client._desired_book_depth = {}
    client._desired_book_deltas = set()
    client._last_quotes = {}
    client._depth_received_ns = {}
    provider = LongbridgeInstrumentProvider()
    provider.add(
        provider._to_equity(SimpleNamespace(symbol="700.HK", currency="HKD", lot_size=100))
    )
    client.instrument_provider = provider
    client._handle_instrument = lambda _: None
    return client


@pytest.mark.asyncio
async def test_quote_subscription_is_replayed_after_local_reconnect() -> None:
    factory = FakeContextFactory()
    client = object.__new__(LongbridgeDataClient)
    client._lb_config = LongbridgeDataClientConfig()
    client._context_factory = factory
    client._quote = factory.quote_context
    client._desired_quotes = {"700.HK"}
    client._desired_trades = {"AAPL.US"}
    client._desired_bars = {}

    await client._restore_subscriptions()

    assert factory.quote_context.subscription_calls
    assert factory.quote_context.subscription_calls[0][0] == ["700.HK"]
    assert factory.quote_context.subscription_calls[1][0] == ["AAPL.US"]


@pytest.mark.asyncio
async def test_quote_command_updates_desired_state() -> None:
    factory = FakeContextFactory()
    client = object.__new__(LongbridgeDataClient)
    client._lb_config = LongbridgeDataClientConfig()
    client._quote = factory.quote_context
    client._desired_quotes = set()
    client._desired_depth = set()
    async def static_info(symbols):
        return [
            SimpleNamespace(
                symbol=symbol,
                currency="HKD" if symbol.endswith(".HK") else "USD",
                lot_size=100 if symbol.endswith(".HK") else 1,
            )
            for symbol in symbols
        ]

    client._quote.static_info = AsyncMock(side_effect=static_info)
    client.instrument_provider = LongbridgeInstrumentProvider()
    client.instrument_provider.set_quote_context(client._quote)
    instruments = []
    client._handle_instrument = instruments.append
    command = SimpleNamespace(
        instrument_ids=[
            instrument_id_from_symbol("700.HK"),
            instrument_id_from_symbol("AAPL.US"),
        ]
    )

    await client._subscribe_quotes(command)

    assert client._desired_quotes == {"700.HK", "AAPL.US"}
    assert client._desired_depth == {"700.HK", "AAPL.US"}
    assert {str(value.id) for value in instruments} == {
        "700.HK.LONGBRIDGE",
        "AAPL.US.LONGBRIDGE",
    }
    assert factory.quote_context.subscription_calls


@pytest.mark.asyncio
async def test_duplicate_quote_subscriptions_use_one_provider_lease() -> None:
    factory = FakeContextFactory()
    client = _subscription_client(factory)
    command = SimpleNamespace(instrument_id=instrument_id_from_symbol("700.HK"))

    await client._subscribe_quotes(command)
    await client._subscribe_quotes(command)
    assert len(factory.quote_context.subscription_calls) == 1

    await client._unsubscribe_quotes(command)
    assert factory.quote_context.unsubscriptions == []
    assert client._desired_quotes == {"700.HK"}

    await client._unsubscribe_quotes(command)
    assert factory.quote_context.unsubscriptions == [
        (["700.HK"], [openapi.SubType.Quote, openapi.SubType.Depth])
    ]


@pytest.mark.asyncio
async def test_quote_and_book_depth_subscriptions_share_depth_lease() -> None:
    factory = FakeContextFactory()
    client = _subscription_client(factory)
    instrument_id = instrument_id_from_symbol("700.HK")
    depth = SimpleNamespace(instrument_id=instrument_id, book_type=BookType.L2_MBP, depth=10)
    quote = SimpleNamespace(instrument_id=instrument_id)

    await client._subscribe_book_depth(depth)
    await client._subscribe_quotes(quote)
    assert factory.quote_context.subscription_calls == [
        (["700.HK"], [openapi.SubType.Depth]),
        (["700.HK"], [openapi.SubType.Quote]),
    ]

    await client._unsubscribe_quotes(quote)
    assert factory.quote_context.unsubscriptions == [
        (["700.HK"], [openapi.SubType.Quote])
    ]
    await client._unsubscribe_book_depth(depth)
    assert factory.quote_context.unsubscriptions[-1] == (
        ["700.HK"],
        [openapi.SubType.Depth],
    )


@pytest.mark.asyncio
async def test_duplicate_trade_and_bar_subscriptions_unsubscribe_last_reference() -> None:
    factory = FakeContextFactory()
    client = _subscription_client(factory)
    instrument_id = instrument_id_from_symbol("700.HK")
    trade = SimpleNamespace(instrument_id=instrument_id)
    bar = SimpleNamespace(
        bar_type=BarType.from_str("700.HK.LONGBRIDGE-1-MINUTE-LAST-EXTERNAL")
    )

    await client._subscribe_trades(trade)
    await client._subscribe_trades(trade)
    assert len(factory.quote_context.subscription_calls) == 1
    await client._unsubscribe_trades(trade)
    assert factory.quote_context.unsubscriptions == []
    await client._unsubscribe_trades(trade)
    assert factory.quote_context.unsubscriptions == [
        (["700.HK"], [openapi.SubType.Trade])
    ]

    await client._subscribe_bars(bar)
    await client._subscribe_bars(bar)
    assert len(factory.quote_context.bar_subscriptions) == 1
    await client._unsubscribe_bars(bar)
    assert factory.quote_context.bar_subscriptions
    await client._unsubscribe_bars(bar)
    assert factory.quote_context.bar_subscriptions == []


@pytest.mark.asyncio
async def test_explicit_instrument_request_publishes_real_definition_and_rejects_missing() -> None:
    factory = FakeContextFactory()
    client = object.__new__(LongbridgeDataClient)
    client.instrument_provider = LongbridgeInstrumentProvider()
    factory.quote_context.static_info = AsyncMock(return_value=[
        SimpleNamespace(symbol="AAPL.US", currency="USD", lot_size=1)
    ])
    client.instrument_provider.set_quote_context(factory.quote_context)
    instruments = []
    client._handle_instrument = instruments.append
    request = SimpleNamespace(instrument_id=instrument_id_from_symbol("AAPL.US"))
    await client._request_instrument(request)
    assert len(instruments) == 1 and instruments[0].id == request.instrument_id
    factory.quote_context.static_info.assert_awaited_once_with(["AAPL.US"])
    factory.quote_context.static_info.return_value = []
    with pytest.raises(ValueError, match="no instrument"):
        await client._request_instrument(SimpleNamespace(instrument_id=instrument_id_from_symbol("NVDA.US")))
    assert len(instruments) == 1


@pytest.mark.asyncio
async def test_disconnect_unsubscribes_and_releases_quote_context() -> None:
    factory = FakeContextFactory()
    client = object.__new__(LongbridgeDataClient)
    client._lb_config = LongbridgeDataClientConfig()
    client._quote = factory.quote_context
    client._context_factory = factory
    client._desired_quotes = {"700.HK"}
    client._desired_trades = {"AAPL.US"}
    client._desired_bars = {}
    client._desired_depth = {"700.HK"}
    client._quote_rate = SlidingWindowRateLimiter(100, 1.0)
    client._quote_concurrency = ConcurrencyLimiter(5)
    client._health_task = None
    client._consumer = None
    client.instrument_provider = None

    await client._disconnect()

    assert factory.quote_context.unsubscriptions == [
        (["700.HK"], [openapi.SubType.Quote, openapi.SubType.Depth]),
        (["AAPL.US"], [openapi.SubType.Trade]),
    ]
    assert client._quote is None

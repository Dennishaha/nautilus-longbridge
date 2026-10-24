from __future__ import annotations

from types import SimpleNamespace

import pytest
from longbridge import openapi
from tests.fixtures.fakes import FakeContextFactory

from nautilus_longbridge.config import LongbridgeDataClientConfig
from nautilus_longbridge.data import LongbridgeDataClient
from nautilus_longbridge.rate_limit import ConcurrencyLimiter, SlidingWindowRateLimiter
from nautilus_longbridge.symbols import instrument_id_from_symbol


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

    assert factory.quote_context.subscriptions
    assert factory.quote_context.subscriptions[0][0] == ["700.HK"]
    assert factory.quote_context.subscriptions[1][0] == ["AAPL.US"]


@pytest.mark.asyncio
async def test_quote_command_updates_desired_state() -> None:
    factory = FakeContextFactory()
    client = object.__new__(LongbridgeDataClient)
    client._lb_config = LongbridgeDataClientConfig()
    client._quote = factory.quote_context
    client._desired_quotes = set()
    client._desired_depth = set()
    command = SimpleNamespace(instrument_id=instrument_id_from_symbol("700.HK"))

    await client._subscribe_quotes(command)

    assert client._desired_quotes == {"700.HK"}
    assert client._desired_depth == {"700.HK"}
    assert factory.quote_context.subscriptions


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

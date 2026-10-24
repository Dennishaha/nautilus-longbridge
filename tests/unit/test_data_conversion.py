from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from nautilus_longbridge.config import LongbridgeDataClientConfig
from nautilus_longbridge.data import LongbridgeDataClient
from nautilus_longbridge.instruments import LongbridgeInstrumentProvider


class FixedClock:
    def timestamp_ns(self) -> int:
        return 1_800_000_000_000_000_000


def _client() -> LongbridgeDataClient:
    client = object.__new__(LongbridgeDataClient)
    client._lb_config = LongbridgeDataClientConfig()
    client.clock = FixedClock()
    client._last_quotes = {}
    client._last_depth = {}
    provider = LongbridgeInstrumentProvider()
    provider.add(
        provider._to_equity(
            SimpleNamespace(
                symbol="700.HK",
                currency="HKD",
                lot_size=100,
                name_cn="测试",
                name_en="Test",
                name_hk="測試",
                exchange="SEHK",
            )
        )
    )
    client.instrument_provider = provider
    return client


@pytest.mark.asyncio
async def test_depth_and_quote_are_mapped_without_fabricating_bbo() -> None:
    client = _client()
    output: list[object] = []
    client._handle_data = output.append
    depth = SimpleNamespace(
        bids=[SimpleNamespace(position=1, price="10.1", volume=100, order_num=2)],
        asks=[SimpleNamespace(position=1, price="10.2", volume=80, order_num=3)],
    )
    quote = SimpleNamespace(last_done="10.15", timestamp=datetime(2026, 1, 1, tzinfo=UTC))

    await client._publish_depth("700.HK", depth)
    assert output[0].bids[0].price.as_decimal() == Decimal("10.1000")
    await client._publish_quote("700.HK", quote)
    assert output[-1].bid_price.as_decimal() == Decimal("10.1000")
    assert output[-1].ask_price.as_decimal() == Decimal("10.2000")

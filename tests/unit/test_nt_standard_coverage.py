from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from longbridge import openapi
from nautilus_trader.core import UUID4
from nautilus_trader.live import (
    BookDeltasResponse,
    BookDepthResponse,
    BookResponse,
    InstrumentResponse,
)
from nautilus_trader.model import BookType, OrderBookDepth
from tests.fixtures.fakes import FakeQuoteContext, FakeTradeContext

from nautilus_longbridge.config import LongbridgeDataClientConfig, LongbridgeExecutionClientConfig
from nautilus_longbridge.data import LongbridgeDataClient
from nautilus_longbridge.execution import LongbridgeExecutionClient
from nautilus_longbridge.instruments import LongbridgeInstrumentProvider
from nautilus_longbridge.rate_limit import SlidingWindowRateLimiter
from nautilus_longbridge.symbols import instrument_id_from_symbol


class FixedClock:
    def timestamp_ns(self) -> int:
        return 1_800_000_000_000_000_000


def _quote_client() -> LongbridgeDataClient:
    client = object.__new__(LongbridgeDataClient)
    client._lb_config = LongbridgeDataClientConfig()
    client._quote = FakeQuoteContext()
    client.clock = FixedClock()
    client._quote_rate = None
    client._quote_concurrency = None
    client._response_sink = None
    provider = LongbridgeInstrumentProvider()
    provider.set_quote_context(client._quote)
    client.instrument_provider = provider
    client._handle_instrument = lambda _: None
    async def quote_call(operation):
        return await operation()

    client._quote_call = quote_call
    return client


def _depth() -> SimpleNamespace:
    return SimpleNamespace(
        bids=[
            SimpleNamespace(position=1, price="10.1", volume=100, order_num=2),
            SimpleNamespace(position=2, price="10.0", volume=200, order_num=3),
        ],
        asks=[
            SimpleNamespace(position=1, price="10.2", volume=80, order_num=4),
            SimpleNamespace(position=2, price="10.3", volume=90, order_num=5),
        ],
    )


@pytest.mark.asyncio
async def test_standard_instrument_and_book_requests_emit_typed_responses() -> None:
    client = _quote_client()
    client._quote.static_info = AsyncMock(
        return_value=[SimpleNamespace(symbol="700.HK", currency="HKD", lot_size=100)]
    )
    client._quote.depth = AsyncMock(return_value=_depth())
    responses: list[object] = []
    client._response_sink = responses.append
    instrument_id = instrument_id_from_symbol("700.HK")

    await client._request_instrument(
        SimpleNamespace(instrument_id=instrument_id, request_id=UUID4())
    )
    await client._request_book_snapshot(
        SimpleNamespace(instrument_id=instrument_id, request_id=UUID4())
    )
    await client._request_book_depth(
        SimpleNamespace(instrument_id=instrument_id, depth=1, request_id=UUID4())
    )
    await client._request_book_deltas(
        SimpleNamespace(instrument_id=instrument_id, limit=1, request_id=UUID4())
    )

    assert isinstance(responses[0], InstrumentResponse)
    assert isinstance(responses[1], BookResponse)
    assert isinstance(responses[2], BookDepthResponse)
    assert isinstance(responses[3], BookDeltasResponse)
    assert len(responses[2].data[0].bids) == 1
    assert responses[3].data[-1].flags


@pytest.mark.asyncio
async def test_book_depth_and_delta_subscriptions_publish_only_requested_shapes() -> None:
    client = _quote_client()
    client._quote.static_info = AsyncMock(
        return_value=[SimpleNamespace(symbol="700.HK", currency="HKD", lot_size=100)]
    )
    client._quote.subscribe = AsyncMock()
    client._desired_quotes = set()
    client._desired_trades = set()
    client._desired_bars = {}
    client._desired_depth = set()
    client._desired_book_depth = {}
    client._desired_book_deltas = set()
    client._last_depth = {}
    client._last_quotes = {}
    client._depth_received_ns = {}
    output: list[object] = []
    client._handle_data = output.append
    command = SimpleNamespace(
        instrument_id=instrument_id_from_symbol("700.HK"),
        depth=1,
        book_type=BookType.L2_MBP,
    )

    await client._subscribe_book_depth(command)
    await client._subscribe_book_deltas(command)
    await client._publish_depth("700.HK", _depth())

    assert any(isinstance(value, OrderBookDepth) for value in output)
    assert any(value.__class__.__name__ == "OrderBookDeltas" for value in output)
    assert client._desired_book_depth["700.HK"] == 1
    assert "700.HK" in client._desired_book_deltas


def _execution_client(context: FakeTradeContext) -> LongbridgeExecutionClient:
    client = object.__new__(LongbridgeExecutionClient)
    client._lb_config = LongbridgeExecutionClientConfig(account_id="ACC-1")
    client._trade = context
    client._trade_rate = SlidingWindowRateLimiter(100, 1.0)
    client._venue_by_client = {"O-1": "V-1"}
    client._pending_orders = {}
    client._seen_fills = set()
    client._last_filled = {}
    client._last_status = {}
    client.clock = FixedClock()
    provider = LongbridgeInstrumentProvider()
    provider.add(provider._to_equity(SimpleNamespace(symbol="700.HK", currency="HKD", lot_size=100)))
    client.instrument_provider = provider
    return client


@pytest.mark.asyncio
async def test_execution_report_filters_history_and_resolves_client_order_id() -> None:
    context = FakeTradeContext()
    updated = datetime(2026, 1, 2, tzinfo=UTC)
    context.orders = [
        SimpleNamespace(
            order_id="V-1",
            remark="NT-O-1",
            status=openapi.OrderStatus.Filled,
            symbol="700.HK",
            side=openapi.OrderSide.Buy,
            order_type=openapi.OrderType.LO,
            time_in_force=openapi.TimeInForceType.Day,
            quantity=Decimal("100"),
            executed_quantity=Decimal("100"),
            executed_price=Decimal("10.2"),
            price=Decimal("10.2"),
            submitted_at=updated,
            updated_at=updated,
        )
    ]
    client = _execution_client(context)
    report = await client._generate_order_status_report(
        SimpleNamespace(
            venue_order_id=None,
            client_order_id="O-1",
            instrument_id=None,
        )
    )

    assert report is not None
    assert report.client_order_id.value == "O-1"
    assert report.avg_px == Decimal("10.2")


@pytest.mark.asyncio
async def test_fill_report_reads_paginated_all_executions() -> None:
    context = FakeTradeContext()
    context.all_executions = AsyncMock(
        side_effect=[
            SimpleNamespace(
                trades=[
                    SimpleNamespace(
                        order_id="V-1",
                        trade_id="T-1",
                        symbol="700.HK",
                        quantity=Decimal("100"),
                        price=Decimal("10.1"),
                        side=openapi.OrderSide.Buy,
                        trade_done_at=datetime(2026, 1, 2, tzinfo=UTC),
                    )
                ],
                has_more=True,
            ),
            SimpleNamespace(
                trades=[
                    SimpleNamespace(
                        order_id="V-2",
                        trade_id="T-2",
                        symbol="700.HK",
                        quantity=Decimal("100"),
                        price=Decimal("10.2"),
                        side=openapi.OrderSide.Buy,
                        trade_done_at=datetime(2026, 1, 2, tzinfo=UTC),
                    )
                ],
                has_more=False,
            ),
        ]
    )
    client = _execution_client(context)

    reports = await client._generate_fill_reports(SimpleNamespace())

    assert [report.trade_id.value for report in reports] == ["T-1", "T-2"]
    assert [call.kwargs["page"] for call in context.all_executions.await_args_list] == [1, 2]


def test_status_mapping_does_not_turn_pending_cancel_into_canceled() -> None:
    from nautilus_trader.model import OrderStatus

    from nautilus_longbridge.execution import map_status

    assert map_status("PendingCancel", Decimal("0"), Decimal("100")) is OrderStatus.ACCEPTED
    assert map_status("NotReported", Decimal("0"), Decimal("100")) is OrderStatus.SUBMITTED

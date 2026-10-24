from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from nautilus_trader.core import UUID4
from nautilus_trader.model import (
    ClientOrderId,
    MarketOrder,
    OrderSide,
    OrderStatus,
    Quantity,
    StrategyId,
    TimeInForce,
    TraderId,
)
from tests.fixtures.fakes import FakeTradeContext

from nautilus_longbridge.config import LongbridgeExecutionClientConfig
from nautilus_longbridge.execution import LongbridgeExecutionClient
from nautilus_longbridge.instruments import LongbridgeInstrumentProvider
from nautilus_longbridge.rate_limit import SlidingWindowRateLimiter
from nautilus_longbridge.symbols import instrument_id_from_symbol


class FixedClock:
    def timestamp_ns(self) -> int:
        return 1_800_000_000_000_000_000


def _order(quantity: int = 100) -> MarketOrder:
    return MarketOrder(
        TraderId("TRADER-001"),
        StrategyId("S-001"),
        instrument_id_from_symbol("700.HK"),
        ClientOrderId("O-1"),
        OrderSide.BUY,
        Quantity.from_int(quantity),
        UUID4(),
        1,
        TimeInForce.DAY,
        False,
        False,
    )


def _client(context: FakeTradeContext) -> LongbridgeExecutionClient:
    client = object.__new__(LongbridgeExecutionClient)
    client._lb_config = LongbridgeExecutionClientConfig(
        account_id="ACC-1", paper_trading=True, dry_run=False
    )
    client._trade = context
    client._trade_rate = SlidingWindowRateLimiter(100, 1.0)
    client._pending_orders = {}
    client._venue_by_client = {}
    client._seen_fills = set()
    client._last_filled = defaultdict(Decimal)
    client._last_status = {}
    provider = LongbridgeInstrumentProvider()
    provider.add(
        provider._to_equity(
            SimpleNamespace(symbol="700.HK", currency="HKD", lot_size=100)
        )
    )
    client.instrument_provider = provider
    client._quote = None
    client._reconciliation_tasks = {}
    client.clock = FixedClock()
    return client


@pytest.mark.asyncio
async def test_submit_uses_idempotency_key_and_deduplicates_push_fills(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LONGBRIDGE_ENABLE_LIVE_SMOKE", "1")
    monkeypatch.setenv("LONGBRIDGE_ENABLE_ORDER_SMOKE", "1")
    context = FakeTradeContext()
    client = _client(context)
    order = _order()
    generated: list[tuple[str, object]] = []
    reports: list[object] = []
    client.generate_order_submitted = lambda value: generated.append(("submitted", value))
    client.generate_order_accepted = lambda *args: generated.append(("accepted", args))
    client.generate_order_filled = lambda *args: generated.append(("filled", args))
    client._handle_report = reports.append

    await client._submit_order(SimpleNamespace(order=order))

    assert context.submitted[0]["client_request_id"] == "NT-O-1"
    assert generated[0][0] == "submitted"
    assert generated[1][0] == "accepted"

    event = SimpleNamespace(
        order_id="123",
        status="PartialFilled",
        submitted_quantity=Decimal("100"),
        executed_quantity=Decimal("40"),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    context.executions = [
        SimpleNamespace(
            order_id="123",
            trade_id="T-1",
            symbol="700.HK",
            quantity=Decimal("40"),
            price=Decimal("10.25"),
            side="Buy",
            trade_done_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
    ]
    await client._process_order_event(event)
    await client._process_order_event(event)
    assert len(reports) == 1
    assert reports[0].trade_id.value == "T-1"

    context.executions.append(
        SimpleNamespace(
            order_id="123",
            trade_id="T-2",
            symbol="700.HK",
            quantity=Decimal("60"),
            price=Decimal("10.30"),
            side="Buy",
            trade_done_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
    )
    await client._process_order_event(
        SimpleNamespace(
            order_id="123",
            status="Filled",
            submitted_quantity=Decimal("100"),
            executed_quantity=Decimal("100"),
            updated_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
    )
    await client._process_order_event(event)
    assert [report.trade_id.value for report in reports] == ["T-1", "T-2"]
    assert [kind for kind, _ in generated].count("filled") == 2


@pytest.mark.asyncio
async def test_timeout_reconciles_without_resubmitting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LONGBRIDGE_ENABLE_LIVE_SMOKE", "1")
    monkeypatch.setenv("LONGBRIDGE_ENABLE_ORDER_SMOKE", "1")
    context = FakeTradeContext()
    context.submit_exception = TimeoutError()
    context.orders = [
        SimpleNamespace(
            order_id="456",
            remark="NT-O-1",
            status="New",
            symbol="700.HK",
            side="Buy",
            quantity=Decimal("100"),
            executed_quantity=Decimal("0"),
            order_type="MO",
            time_in_force="Day",
            submitted_at=datetime(2026, 1, 1, tzinfo=UTC),
            updated_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
    ]
    client = _client(context)
    order = _order()
    generated: list[tuple[str, object]] = []
    client.generate_order_submitted = lambda value: generated.append(("submitted", value))
    client.generate_order_accepted = lambda *args: generated.append(("accepted", args))

    await client._submit_order(SimpleNamespace(order=order))
    reconciliation = client._reconciliation_tasks["O-1"]
    assert await reconciliation is True

    assert len(context.submitted) == 1
    assert client._venue_by_client["O-1"] == "456"
    assert [kind for kind, _ in generated] == ["submitted", "accepted"]


@pytest.mark.asyncio
async def test_hk_order_quantity_must_match_board_lot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LONGBRIDGE_ENABLE_LIVE_SMOKE", "1")
    monkeypatch.setenv("LONGBRIDGE_ENABLE_ORDER_SMOKE", "1")
    client = _client(FakeTradeContext())
    denied: list[str] = []
    client.generate_order_denied = lambda order, reason: denied.append(reason)

    await client._submit_order(SimpleNamespace(order=_order(quantity=150)))

    assert denied
    assert "multiple of lot size" in denied[0]
    assert client._trade.submitted == []


@pytest.mark.asyncio
async def test_cancel_waits_for_terminal_confirmation() -> None:
    context = FakeTradeContext()
    context.orders = [
        SimpleNamespace(
            order_id="123",
            status="Canceled",
            symbol="700.HK",
            side="Buy",
            quantity=Decimal("100"),
            executed_quantity=Decimal("0"),
            order_type="MO",
            time_in_force="Day",
            updated_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
    ]
    client = _client(context)
    order = _order()
    client._pending_orders["O-1"] = order
    client._venue_by_client["O-1"] = "123"
    client._last_status["O-1"] = OrderStatus.ACCEPTED
    canceled: list[object] = []
    client.generate_order_canceled = lambda *args: canceled.append(args)

    await client._cancel_order(SimpleNamespace(order=order))

    assert context.canceled == ["123"]
    assert len(canceled) == 1


@pytest.mark.asyncio
async def test_reconciliation_maps_orders_fills_positions_and_account_state() -> None:
    from longbridge import openapi

    context = FakeTradeContext()
    client = _client(context)
    client._venue_by_client["O-1"] = "123"
    context.orders = [
        SimpleNamespace(
            order_id="123",
            status=openapi.OrderStatus.New,
            symbol="700.HK",
            side=openapi.OrderSide.Buy,
            order_type=openapi.OrderType.LO,
            time_in_force=openapi.TimeInForceType.Day,
            quantity=Decimal("100"),
            executed_quantity=Decimal("40"),
            price=Decimal("10.20"),
            submitted_at=datetime(2026, 1, 1, tzinfo=UTC),
            updated_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
    ]
    context.executions = [
        SimpleNamespace(
            order_id="123",
            trade_id="T-1",
            symbol="700.HK",
            quantity=Decimal("40"),
            price=Decimal("10.25"),
            side=openapi.OrderSide.Buy,
            trade_done_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
    ]
    context.positions = SimpleNamespace(
        channels=[
            SimpleNamespace(
                positions=[
                    SimpleNamespace(
                        symbol="700.HK", quantity=Decimal("100"), cost_price=Decimal("9.80")
                    )
                ]
            )
        ]
    )
    context.balances = [
        SimpleNamespace(currency="HKD", net_assets=Decimal("10000"), total_cash=Decimal("9000"))
    ]
    account_states: list[object] = []
    client.generate_account_state = lambda *args: account_states.append(args)

    orders = await client._generate_order_status_reports(SimpleNamespace())
    fills = await client._generate_fill_reports(SimpleNamespace())
    positions = await client._generate_position_status_reports(SimpleNamespace())
    await client._emit_account_state()

    assert orders[0].client_order_id.value == "O-1"
    assert orders[0].price.as_decimal() == Decimal("10.2000")
    assert fills[0].trade_id.value == "T-1"
    assert fills[0].client_order_id.value == "O-1"
    assert positions[0].quantity.as_decimal() == Decimal("100")
    assert account_states

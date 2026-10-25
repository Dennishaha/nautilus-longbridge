from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from nautilus_trader.model import BarType, OrderStatus, QuoteTick
from tests.fixtures.fakes import FakeTradeContext
from tests.integration.test_execution_fake import _client as execution_client
from tests.integration.test_execution_fake import _order
from tests.unit.test_data_conversion import _client as data_client

from nautilus_longbridge.config import LongbridgeExecutionClientConfig
from nautilus_longbridge.data import _period_for_bar_type
from nautilus_longbridge.errors import ConfigurationError


class Clock:
    def __init__(self, stamp=1_800_000_000_000_000_000):
        self.stamp = stamp

    def timestamp_ns(self):
        return self.stamp


def depth():
    return SimpleNamespace(
        bids=[SimpleNamespace(position=1, price="10.10", volume=100, order_num=1)],
        asks=[SimpleNamespace(position=1, price="10.20", volume=200, order_num=1)],
    )


@pytest.mark.asyncio
async def test_depth_drives_quotes_and_last_trade_does_not_refresh_stale_bbo():
    client = data_client()
    client.clock = Clock()
    client._desired_quotes = {"700.HK"}
    output = []
    client._handle_data = output.append
    await client._publish_depth("700.HK", depth())
    ticks = [x for x in output if isinstance(x, QuoteTick)]
    assert len(ticks) == 1 and ticks[0].ts_event == client.clock.stamp
    client.clock.stamp += 3_000_000_000
    await client._publish_quote(
        "700.HK", SimpleNamespace(last_done="11", timestamp=datetime.now(UTC))
    )
    assert len([x for x in output if isinstance(x, QuoteTick)]) == 1
    await client._publish_depth("700.HK", depth())
    assert len([x for x in output if isinstance(x, QuoteTick)]) == 2


@pytest.mark.asyncio
async def test_queue_receipt_time_is_not_replaced_after_delay():
    client = data_client()
    client.clock = Clock()
    client._desired_quotes = {"700.HK"}
    client._queue = asyncio.Queue()
    client._handle_data = lambda value: output.append(value)
    output = []
    await client._on_depth("700.HK", depth())
    kind, symbol, value, received = client._queue.get_nowait()
    client.clock.stamp += 5_000_000_000
    await client._publish_depth(symbol, value, received_ns=received)
    assert kind == "depth" and not any(isinstance(x, QuoteTick) for x in output)


@pytest.mark.asyncio
async def test_confirmed_minutes_only_and_unsupported_period_rejected():
    client = data_client()
    client._lb_config.completed_bars_only = True
    output = []
    client._handle_data = output.append
    event = SimpleNamespace(
        is_confirmed=False,
        period=SimpleNamespace(name="Min_1"),
        candlestick=SimpleNamespace(
            timestamp=datetime(2026, 1, 2, 14, 30, tzinfo=UTC),
            open="10",
            high="11",
            low="9",
            close="10.5",
            volume=100,
        ),
    )
    await client._publish_bar("700.HK", event)
    assert not output
    event.is_confirmed = True
    await client._publish_bar("700.HK", event)
    assert len(output) == 1
    assert _period_for_bar_type(BarType.from_str("700.HK.LONGBRIDGE-1-MINUTE-LAST-EXTERNAL"))
    with pytest.raises(ValueError):
        _period_for_bar_type(BarType.from_str("700.HK.LONGBRIDGE-5-MINUTE-LAST-EXTERNAL"))


@pytest.mark.asyncio
async def test_cash_balance_uses_cash_infos_not_net_assets_and_snapshot_paper_only():
    context = FakeTradeContext()
    client = execution_client(context)
    context.balances = [
        SimpleNamespace(
            currency="USD",
            net_assets=Decimal("10000"),
            total_cash=Decimal("9000"),
            cash_infos=[
                SimpleNamespace(
                    currency="USD",
                    available_cash=Decimal("100"),
                    frozen_cash=Decimal("50"),
                    settling_cash=Decimal("20"),
                )
            ],
        )
    ]
    context.positions = SimpleNamespace(
        channels=[
            SimpleNamespace(positions=[SimpleNamespace(symbol="AAPL.US", quantity=3)]),
            SimpleNamespace(positions=[SimpleNamespace(symbol="AAPL.US", quantity=2)]),
        ]
    )
    states = []
    client.generate_account_state = lambda *args: states.append(args)
    await client._emit_account_state()
    balance = states[0][0][0]
    assert balance.total.as_decimal() == 170 and balance.free.as_decimal() == 100
    snapshot = await client.paper_snapshot()
    assert snapshot["equity"] == 10000 and snapshot["available_cash"] == 100
    assert snapshot["positions"] == {"AAPL.US": 5} and not snapshot["can_short"]
    client._lb_config = LongbridgeExecutionClientConfig(account_id="ACC-1", paper_trading=False)
    with pytest.raises(ConfigurationError):
        await client.paper_snapshot()


@pytest.mark.asyncio
async def test_partial_cancel_waits_for_execution_then_emits_fill_before_terminal():
    context = FakeTradeContext()
    client = execution_client(context)
    order = _order()
    client._pending_orders = {"O-1": order}
    client._venue_by_client = {"O-1": "123"}
    client._last_status = {"O-1": OrderStatus.ACCEPTED}
    sequence = []
    client.generate_order_filled = lambda *args: sequence.append("fill")
    client.generate_order_canceled = lambda *args: sequence.append("cancel")
    client._handle_report = lambda report: None
    event = SimpleNamespace(
        order_id="123",
        status="Canceled",
        executed_quantity=Decimal(40),
        submitted_quantity=Decimal(100),
    )
    await client._process_order_event(event)
    assert not sequence and client._last_status["O-1"] == OrderStatus.ACCEPTED
    context.executions = [
        SimpleNamespace(
            order_id="123",
            trade_id="T1",
            symbol="700.HK",
            side="Buy",
            quantity=Decimal(40),
            price=Decimal("10.2"),
        )
    ]
    await client._process_order_event(event)
    await client._process_order_event(event)
    assert sequence == ["fill", "cancel"] and client._last_filled["O-1"] == 40


@pytest.mark.asyncio
async def test_order_fee_snapshot_does_not_assume_missing_commission_is_zero():
    context = FakeTradeContext()
    client = execution_client(context)
    context.balances = [SimpleNamespace(currency="USD", net_assets=10000, available_cash=9000)]
    context.orders = [
        SimpleNamespace(
            order_id="123", status="Filled", submitted_quantity=10, executed_quantity=10
        )
    ]
    client._venue_by_client = {"O-1": "123"}
    # No cached order object: snapshot can still collect fees by known ID.
    first = await client.paper_snapshot()
    assert first["order_fees"] == {}

    async def order_detail(order_id):
        return SimpleNamespace(
            charge_detail=SimpleNamespace(total_amount=Decimal("1.75"), currency="USD")
        )

    context.order_detail = order_detail
    second = await client.paper_snapshot()
    assert second["order_fees"] == {"O-1": 1.75}

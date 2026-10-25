from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest
from longbridge.openapi import OrderType as LBOrderType
from nautilus_trader.core import UUID4
from nautilus_trader.model import (
    ClientOrderId,
    MarketOrder,
    OrderSide,
    OrderStatus,
    Price,
    Quantity,
    QuoteTick,
    StrategyId,
    TimeInForce,
    TraderId,
)
from tests.fixtures.fakes import FakeTradeContext
from tests.integration.test_execution_fake import _client

from nautilus_longbridge.accounts import paper_margin_balance
from nautilus_longbridge.config import LongbridgeExecutionClientConfig
from nautilus_longbridge.errors import ConfigurationError
from nautilus_longbridge.symbols import instrument_id_from_symbol


def balance(**updates):
    fields = dict(
        currency="USD",
        net_assets=Decimal("100000"),
        buy_power=Decimal("100000"),
        init_margin=Decimal("0"),
        maintenance_margin=Decimal("0"),
        margin_call=Decimal("0"),
        max_finance_amount=Decimal("400000"),
        remaining_finance_amount=Decimal("400000"),
        risk_level=0,
        cash_infos=[
            SimpleNamespace(
                currency="HKD",
                available_cash=Decimal("800000"),
                frozen_cash=Decimal("0"),
                settling_cash=Decimal("400000"),
            )
        ],
    )
    fields.update(updates)
    return SimpleNamespace(**fields)


def order(qty=100, *, symbol="AAPL.US", side=OrderSide.BUY, key="O-1"):
    return MarketOrder(
        TraderId("TRADER-001"),
        StrategyId("S-001"),
        instrument_id_from_symbol(symbol),
        ClientOrderId(key),
        side,
        Quantity.from_int(qty),
        UUID4(),
        1,
        TimeInForce.DAY,
        False,
        False,
    )


def margin_client(monkeypatch):
    monkeypatch.setenv("LONGBRIDGE_ENABLE_LIVE_SMOKE", "1")
    monkeypatch.setenv("LONGBRIDGE_ENABLE_ORDER_SMOKE", "1")
    context = FakeTradeContext()
    context.balances = [balance()]
    context.positions = SimpleNamespace(channels=[])
    client = _client(context)
    client._lb_config = LongbridgeExecutionClientConfig(
        account_id="ACC-1",
        paper_trading=True,
        dry_run=False,
        account_mode="MARGIN",
    )
    client._margin_submit_lock = asyncio.Lock()
    client._margin_exit_lock = asyncio.Lock()
    client._margin_preparing, client._margin_cancelled = set(), set()
    for symbol in ("AAPL.US", "MSFT.US", "SPY.US", "NVDA.US"):
        client.instrument_provider.add(
            client.instrument_provider._to_equity(
                SimpleNamespace(
                    symbol=symbol,
                    currency="USD",
                    lot_size=1,
                )
            )
        )
    stamp = client.clock.timestamp_ns()
    ticks = {
        symbol: QuoteTick(
            instrument_id_from_symbol(symbol),
            Price.from_str("99.98"),
            Price.from_str("100.00"),
            Quantity.from_int(1000),
            Quantity.from_int(1000),
            stamp,
            stamp,
        )
        for symbol in ("AAPL.US", "MSFT.US", "SPY.US", "NVDA.US")
    }
    client.cache = SimpleNamespace(quote=lambda iid: ticks[str(iid.symbol)])
    calls = []

    async def estimate(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(cash_max_qty=Decimal(0), margin_max_qty=Decimal(2000))

    context.estimate_max_purchase_quantity = estimate
    output = []
    client.generate_account_state = lambda *args: output.append(("account", args))
    client.generate_order_denied = lambda *args: output.append(("denied", args))
    client.generate_order_submitted = lambda *args: output.append(("submitted", args))
    client.generate_order_accepted = lambda *args: output.append(("accepted", args))
    return client, context, output, calls, ticks


def test_margin_config_is_explicit_and_paper_only():
    assert LongbridgeExecutionClientConfig(account_id="A-1").account_mode == "CASH"
    for kwargs in (dict(paper_trading=False), dict(base_currency="HKD")):
        with pytest.raises(ConfigurationError, match="USD-base paper"):
            LongbridgeExecutionClientConfig(account_id="A-1", account_mode="MARGIN", **kwargs)


@pytest.mark.asyncio
async def test_hkd_collateral_snapshot_and_market_buy_pass_without_usd_cash(monkeypatch):
    client, context, output, calls, _ = margin_client(monkeypatch)
    snapshot = await client.paper_snapshot()
    assert snapshot["available_cash"] is None and snapshot["account_mode"] == "MARGIN"
    assert snapshot["currency_cash"]["HKD"]["available_cash"] == "800000"
    state = output[0][1]
    assert state[0][0].total.as_decimal() == 100000
    assert state[1][0].initial.as_decimal() == 0
    assert state[4]["balance_semantics"] == "BROKER_VALUED_COLLATERAL_EQUITY"
    await client._submit_order(SimpleNamespace(order=order()))
    assert len(context.submitted) == 1
    assert calls[-1]["order_type"] == LBOrderType.MO and calls[-1]["currency"] == "USD"
    assert calls[-1]["fractional_shares"] is False
    assert not any(kind == "denied" for kind, _ in output)


def test_negative_usd_cash_is_retained_as_cash_deficit_not_negative_equity():
    b = balance()
    b.cash_infos.append(
        SimpleNamespace(
            currency="USD",
            available_cash=Decimal("-10000"),
            frozen_cash=Decimal("0"),
            settling_cash=Decimal("-200"),
        )
    )
    result = paper_margin_balance(b)
    assert result["equity"] == 100000 and result["available_cash"] == -10000
    assert result["negative_available_cash"] == {"USD": "10000"}


@pytest.mark.parametrize(
    "field,value",
    [
        ("buy_power", None),
        ("net_assets", Decimal("NaN")),
        ("init_margin", Decimal("-1")),
        ("risk_level", None),
        ("cash_infos", []),
    ],
)
def test_incomplete_or_invalid_margin_state_fails_closed(field, value):
    with pytest.raises(ConfigurationError):
        paper_margin_balance(balance(**{field: value}))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", ["capacity", "timeout", "stale", "risk", "margin_call", "exposure"]
)
async def test_margin_entry_guards_do_not_send_an_order(monkeypatch, case):
    client, context, output, _, ticks = margin_client(monkeypatch)

    async def zero_capacity(**kwargs):
        return SimpleNamespace(cash_max_qty=Decimal(0), margin_max_qty=Decimal(0))

    async def timeout(**kwargs):
        raise TimeoutError()

    if case == "capacity":
        context.estimate_max_purchase_quantity = zero_capacity
    elif case == "timeout":
        context.estimate_max_purchase_quantity = timeout
    elif case == "stale":
        old = ticks["AAPL.US"]
        ticks["AAPL.US"] = QuoteTick(
            old.instrument_id,
            old.bid_price,
            old.ask_price,
            old.bid_size,
            old.ask_size,
            old.ts_event - 3_000_000_000,
            old.ts_init - 3_000_000_000,
        )
    elif case == "risk":
        context.balances[0].risk_level = 2
    elif case == "margin_call":
        context.balances[0].margin_call = Decimal(100)
    await client._submit_order(SimpleNamespace(order=order(1000 if case == "exposure" else 100)))
    assert not context.submitted and any(kind == "denied" for kind, _ in output)


@pytest.mark.asyncio
async def test_broker_positions_and_local_pending_buys_reserve_exposure(monkeypatch):
    client, context, output, _, _ = margin_client(monkeypatch)
    await client._submit_order(SimpleNamespace(order=order(600)))
    assert len(context.submitted) == 1
    # Broker orders view still empty; local submission must reserve 60,000.
    await client._submit_order(SimpleNamespace(order=order(500, symbol="MSFT.US", key="O-2")))
    assert len(context.submitted) == 1 and output[-1][0] == "denied"


@pytest.mark.asyncio
async def test_lagging_broker_position_does_not_erase_local_filled_exposure(monkeypatch):
    client, context, output, _, _ = margin_client(monkeypatch)
    previous = order(600)
    client._pending_orders["O-1"] = previous
    client._last_filled["O-1"] = Decimal(600)
    client._last_status["O-1"] = OrderStatus.FILLED
    await client._submit_order(SimpleNamespace(order=order(500, symbol="MSFT.US", key="O-2")))
    assert not context.submitted and output[-1][0] == "denied"


@pytest.mark.asyncio
async def test_fourth_symbol_is_blocked(monkeypatch):
    client, context, output, _, _ = margin_client(monkeypatch)
    context.positions = SimpleNamespace(
        channels=[
            SimpleNamespace(
                positions=[
                    SimpleNamespace(symbol=symbol, quantity=1)
                    for symbol in ("MSFT.US", "SPY.US", "NVDA.US")
                ]
            )
        ]
    )
    await client._submit_order(SimpleNamespace(order=order()))
    assert not context.submitted and output[-1][0] == "denied"


@pytest.mark.asyncio
async def test_risk_halt_allows_long_reduction_but_never_short_opening(monkeypatch):
    client, context, output, _, _ = margin_client(monkeypatch)
    context.balances[0].risk_level = 3
    context.positions = SimpleNamespace(
        channels=[
            SimpleNamespace(
                positions=[
                    SimpleNamespace(symbol="AAPL.US", quantity=20),
                ]
            )
        ]
    )
    await client._submit_order(SimpleNamespace(order=order(20, side=OrderSide.SELL)))
    assert len(context.submitted) == 1
    await client._submit_order(SimpleNamespace(order=order(21, side=OrderSide.SELL, key="O-2")))
    assert len(context.submitted) == 1 and output[-1][0] == "denied"
    await client._submit_order(SimpleNamespace(order=order(1, side=OrderSide.SELL, key="O-3")))
    assert len(context.submitted) == 1 and output[-1][0] == "denied"


@pytest.mark.asyncio
async def test_cancellation_during_slow_preflight_prevents_late_submission(monkeypatch):
    client, context, output, _, _ = margin_client(monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()

    async def slow(**kwargs):
        entered.set()
        await release.wait()
        return SimpleNamespace(cash_max_qty=Decimal(0), margin_max_qty=Decimal(2000))

    context.estimate_max_purchase_quantity = slow
    command = SimpleNamespace(order=order())
    task = asyncio.create_task(client._submit_order(command))
    await entered.wait()
    await client._cancel_order(command)
    release.set()
    await task
    assert not context.submitted and output[-1][0] == "denied"
    assert "canceled during" in output[-1][1][1]


@pytest.mark.asyncio
async def test_cancellation_while_submit_is_inflight_is_sent_after_ack(monkeypatch):
    client, context, output, _, _ = margin_client(monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()

    async def slow_submit(**kwargs):
        context.submitted.append(kwargs)
        entered.set()
        await release.wait()
        return SimpleNamespace(order_id="123")

    context.submit_order = slow_submit
    context.orders = [
        SimpleNamespace(
            order_id="123",
            status="Canceled",
            symbol="AAPL.US",
            side="Buy",
            quantity=Decimal(100),
            executed_quantity=Decimal(0),
        )
    ]
    client.generate_order_canceled = lambda *args: output.append(("canceled", args))
    command = SimpleNamespace(order=order())
    task = asyncio.create_task(client._submit_order(command))
    await entered.wait()
    await client._cancel_order(command)
    assert not context.canceled
    release.set()
    await task
    assert len(context.submitted) == 1 and context.canceled == ["123"]
    assert output[-1][0] == "canceled"


@pytest.mark.asyncio
async def test_uncertain_submit_keeps_cancellation_until_reconciliation(monkeypatch):
    client, context, output, _, _ = margin_client(monkeypatch)
    context.submit_exception = TimeoutError()
    client.generate_order_canceled = lambda *args: output.append(("canceled", args))
    command = SimpleNamespace(order=order())
    await client._submit_order(command)
    await client._cancel_order(command)
    assert "O-1" in client._margin_cancelled
    pending = SimpleNamespace(
        order_id="123",
        status="New",
        symbol="AAPL.US",
        side="Buy",
        remark="NT-O-1",
        quantity=Decimal(100),
        executed_quantity=Decimal(0),
    )
    context.orders = [pending]

    async def cancel(oid, **kwargs):
        context.canceled.append(oid)
        pending.status = "Canceled"

    context.cancel_order = cancel
    assert await client._reconciliation_tasks["O-1"] is True
    assert context.canceled == ["123"] and "O-1" not in client._margin_cancelled
    assert len(context.submitted) == 1

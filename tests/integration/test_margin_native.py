"""Native risk engine -> connector -> fake broker, with HKD-only cash."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from nautilus_trader.common import Environment, LogLevel
from nautilus_trader.config import LoggerConfig, StrategyConfig
from nautilus_trader.live import LiveNode
from nautilus_trader.model import MarginAccount, OrderSide, Quantity, TimeInForce, TraderId
from nautilus_trader.trading import Strategy
from tests.fixtures.fakes import FakeContextFactory
from tests.unit.test_paper_margin import balance

from nautilus_longbridge.config import LongbridgeDataClientConfig, LongbridgeExecutionClientConfig
from nautilus_longbridge.data import LongbridgeDataClient
from nautilus_longbridge.execution import LongbridgeExecutionClient
from nautilus_longbridge.factories import (
    LongbridgeDataClientFactory,
    LongbridgeExecutionClientFactory,
)
from nautilus_longbridge.instruments import LongbridgeInstrumentProvider
from nautilus_longbridge.symbols import instrument_id_from_symbol


def test_native_margin_account_routes_buy_through_preflight_without_usd_cash(monkeypatch):
    monkeypatch.setenv("LONGBRIDGE_ENABLE_LIVE_SMOKE", "1")
    monkeypatch.setenv("LONGBRIDGE_ENABLE_ORDER_SMOKE", "1")
    factory = FakeContextFactory()
    factory.trade_context.balances = [balance()]
    factory.trade_context.positions = SimpleNamespace(channels=[])

    async def capacity(**kwargs):
        return SimpleNamespace(cash_max_qty=0, margin_max_qty=1000)

    async def static_info(symbols):
        return [SimpleNamespace(symbol=s, currency="USD", lot_size=1) for s in symbols]

    factory.trade_context.estimate_max_purchase_quantity = capacity
    factory.quote_context.static_info = static_info
    clients = []
    fill_tasks = []

    async def submit(**kwargs):
        trade = factory.trade_context
        trade.submitted.append(kwargs)
        oid = str(len(trade.submitted))

        async def fill():
            await asyncio.sleep(0.01)
            now = datetime.now(UTC)
            buy = str(kwargs["side"]).endswith("Buy")
            trade.positions = SimpleNamespace(
                channels=[
                    SimpleNamespace(
                        positions=[
                            SimpleNamespace(symbol="AAPL.US", quantity=50 if buy else 0),
                        ]
                    )
                ]
            )
            trade.executions.append(
                SimpleNamespace(
                    order_id=oid,
                    trade_id=f"T-{oid}",
                    symbol="AAPL.US",
                    quantity=Decimal(50),
                    price=Decimal("100"),
                    side=kwargs["side"],
                    trade_done_at=now,
                )
            )
            event = SimpleNamespace(
                order_id=oid,
                status="Filled",
                symbol="AAPL.US",
                side=kwargs["side"],
                quantity=Decimal(50),
                submitted_quantity=Decimal(50),
                executed_quantity=Decimal(50),
                order_type="MO",
                time_in_force="Day",
                submitted_at=now,
                updated_at=now,
                remark=kwargs["remark"],
            )
            trade.orders.append(event)
            await trade.order_callback(event)

        fill_tasks.append(asyncio.create_task(fill()))
        return SimpleNamespace(order_id=oid)

    factory.trade_context.submit_order = submit

    class DataFactory(LongbridgeDataClientFactory):
        @staticmethod
        def create(**kwargs):
            client = LongbridgeDataClient(
                context_factory=factory,
                instrument_provider=LongbridgeInstrumentProvider(),
                **kwargs,
            )
            subscribe = client._subscribe_quotes

            async def with_quote(command):
                await subscribe(command)
                await client._publish_depth(
                    "AAPL.US",
                    SimpleNamespace(
                        bids=[SimpleNamespace(position=1, price="99.98", volume=1000, order_num=1)],
                        asks=[
                            SimpleNamespace(position=1, price="100.00", volume=1000, order_num=1)
                        ],
                    ),
                )

            client._subscribe_quotes = with_quote
            return client

    class ExecFactory(LongbridgeExecutionClientFactory):
        @staticmethod
        def create(**kwargs):
            client = LongbridgeExecutionClient(context_factory=factory, **kwargs)
            clients.append(client)
            return client

    class SubmitOnce(Strategy):
        def __new__(cls):
            return super().__new__(cls, StrategyConfig())

        def __init__(self):
            super().__init__(StrategyConfig())
            self.attempted = False
            self.accepted, self.denied = [], []
            self.fills = []

        def on_start(self):
            self.subscribe_quotes(instrument_id_from_symbol("AAPL.US"))
            self.clock.set_timer(
                "test-stop", timedelta(seconds=3), callback=lambda _: node.handle().stop()
            )

        def on_quote(self, tick):
            if not self.attempted:
                self.attempted = True
                self.submit_order(
                    self.order_factory.market(
                        tick.instrument_id,
                        OrderSide.BUY,
                        Quantity.from_int(50),
                        time_in_force=TimeInForce.DAY,
                    )
                )

        def on_order_accepted(self, event):
            self.accepted.append(event)

        def on_order_denied(self, event):
            self.denied.append(event)

        def on_order_filled(self, event):
            self.fills.append(event)
            if len(self.fills) == 1:
                self.submit_order(
                    self.order_factory.market(
                        instrument_id_from_symbol("AAPL.US"),
                        OrderSide.SELL,
                        Quantity.from_int(50),
                        time_in_force=TimeInForce.DAY,
                    )
                )
            else:
                node.handle().stop()

        def on_stop(self):
            self.clock.cancel_timer("test-stop")

    node = (
        LiveNode.builder("margin-native-fake", TraderId("MARGINTST-001"), Environment.LIVE)
        .with_logging(LoggerConfig(stdout_level=LogLevel.ERROR))
        .add_data_client("LONGBRIDGE_DATA", DataFactory, LongbridgeDataClientConfig())
        .add_exec_client(
            "LONGBRIDGE_EXEC",
            ExecFactory,
            LongbridgeExecutionClientConfig(
                account_id="MARGINTST-001",
                account_mode="MARGIN",
                paper_trading=True,
                dry_run=False,
            ),
        )
        .build()
    )
    strategy = SubmitOnce()
    node.add_strategy(strategy)
    try:
        node.run()
        assert strategy.attempted and not strategy.denied
        assert (
            len(strategy.accepted)
            == len(factory.trade_context.submitted)
            == len(strategy.fills)
            == 2
        )
        assert all(task.done() and task.exception() is None for task in fill_tasks)
        assert not node.cache.positions_open()
        assert isinstance(node.cache.account(clients[0].account_id), MarginAccount)
        assert clients[0]._last_margin_preflight["quantity"] == "50"
    finally:
        node.dispose()

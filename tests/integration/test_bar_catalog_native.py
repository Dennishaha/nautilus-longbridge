"""Exercise adapter -> native data engine -> streaming -> actual Parquet reads."""

from datetime import timedelta
from types import SimpleNamespace

import pytest
from nautilus_trader.common import Clock, Environment, LogLevel
from nautilus_trader.config import LoggerConfig, StrategyConfig
from nautilus_trader.live import LiveNodeBuilder, LiveNodeConfig
from nautilus_trader.model import Bar, BarType, NautilusDataType, Price, Quantity, TraderId
from nautilus_trader.persistence import ParquetDataCatalog, StreamingWriter
from nautilus_trader.trading import Strategy
from tests.fixtures.fakes import FakeContextFactory
from tests.unit.test_bar_persistence import candle

from nautilus_longbridge import LongbridgeDataClientConfig, bar_catalog_streaming
from nautilus_longbridge.data import LongbridgeDataClient
from nautilus_longbridge.factories import LongbridgeDataClientFactory
from nautilus_longbridge.instruments import LongbridgeInstrumentProvider


@pytest.mark.parametrize("periodic", [False, True])
def test_native_node_promotes_completed_bars_and_instruments(tmp_path, periodic):
    factory = FakeContextFactory()

    async def static_info(symbols):
        return [SimpleNamespace(symbol=s, currency="HKD", lot_size=100) for s in symbols]

    factory.quote_context.static_info = static_info
    bar_types = [
        BarType.from_str(f"700.HK.LONGBRIDGE-1-{p}-LAST-EXTERNAL")
        for p in ("MINUTE", "HOUR", "DAY")
    ]

    class DataFactory(LongbridgeDataClientFactory):
        @staticmethod
        def create(**kwargs):
            client = LongbridgeDataClient(
                context_factory=factory,
                instrument_provider=LongbridgeInstrumentProvider(),
                **kwargs,
            )
            subscribe = client._subscribe_bars

            async def with_candles(command):
                await subscribe(command)
                period = factory.quote_context.bar_subscriptions[-1][1]
                await client._on_candlestick("700.HK", candle(period, confirmed=False))
                await client._on_candlestick("700.HK", candle(period))
                await client._on_candlestick("700.HK", candle(period))

            client._subscribe_bars = with_candles
            return client

    class Recorder(Strategy):
        def __new__(cls):
            return super().__new__(cls, StrategyConfig())

        def __init__(self):
            super().__init__(StrategyConfig())
            self.bars = []
            self.visible_before_stop = False

        def on_start(self):
            for bar_type in bar_types:
                self.subscribe_bars(bar_type)
            self.clock.set_timer(
                "test-stop", timedelta(seconds=3), callback=lambda _: handle.stop()
            )
            if periodic:
                self.clock.set_timer(
                    "check-catalog", timedelta(milliseconds=50), callback=self.check_catalog
                )

        def check_catalog(self, _):
            catalog = ParquetDataCatalog(str(tmp_path))
            if len(catalog.query_bars()) == 3:
                self.visible_before_stop = True
                handle.stop()

        def on_bar(self, bar):
            self.bars.append(bar)
            if len(self.bars) == 3 and not periodic:
                handle.stop()

        def on_stop(self):
            self.clock.cancel_timer("test-stop")
            if periodic:
                self.clock.cancel_timer("check-catalog")

    node = (
        LiveNodeBuilder.from_config(
            "bar-catalog-fake",
            LiveNodeConfig(
                environment=Environment.LIVE,
                trader_id=TraderId("CATALOG-001"),
                logging=LoggerConfig(stdout_level=LogLevel.ERROR),
                streaming=bar_catalog_streaming(
                    tmp_path, promotion_interval_ms=10 if periodic else 60_000
                ),
                delay_post_stop_secs=0,
            ),
        )
        .add_data_client(
            "LONGBRIDGE_DATA", DataFactory, LongbridgeDataClientConfig(completed_bars_only=True)
        )
        .build()
    )
    handle = node.handle()
    recorder = Recorder()
    node.add_strategy(recorder)
    try:
        node.run()
        assert len(recorder.bars) == 3
        assert recorder.visible_before_stop == periodic
    finally:
        node.dispose()
        del node
    catalog = ParquetDataCatalog(str(tmp_path))
    for bar_type in bar_types:
        rows = catalog.query_bars(identifiers=[str(bar_type)])
        assert len(rows) == 1
        expected = next(b for b in recorder.bars if b.bar_type == bar_type)
        assert rows[0] == expected
        assert rows[0].ts_init == expected.ts_init
    assert [str(i.id) for i in catalog.instruments()] == ["700.HK.LONGBRIDGE"]
    assert list(tmp_path.glob("data/bars/**/*.parquet"))
    assert list(tmp_path.glob("live/**/*.feather"))
    # rc8 makes promotion idempotent for an already-promoted source.
    for instance_id in catalog.list_live_runs():
        catalog.convert_stream_to_data(instance_id, NautilusDataType.Bar, environment=Environment.LIVE)
    assert len(catalog.query_bars()) == 3


def test_unpromoted_live_bar_source_can_be_recovered_with_native_catalog(tmp_path):
    instance_id = "unpromoted-run"
    writer = StreamingWriter(str(tmp_path / "live" / instance_id), Clock.new_test())
    bar = Bar(
        BarType.from_str("700.HK.LONGBRIDGE-1-MINUTE-LAST-EXTERNAL"),
        Price.from_str("10.0000"),
        Price.from_str("11.0000"),
        Price.from_str("9.0000"),
        Price.from_str("10.5000"),
        Quantity.from_int(100),
        1_800_000_000_000_000_000,
        1_800_000_060_000_000_000,
    )
    try:
        writer.write(bar)
    finally:
        writer.close()
    catalog = ParquetDataCatalog(str(tmp_path))
    assert not catalog.query_bars()
    catalog.convert_stream_to_data(instance_id, NautilusDataType.Bar, environment=Environment.LIVE)
    assert catalog.query_bars() == [bar]

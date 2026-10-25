"""Record confirmed Longbridge bars through Nautilus' native catalog streaming."""

from __future__ import annotations

import argparse
from pathlib import Path

from nautilus_trader.common import Environment
from nautilus_trader.config import StrategyConfig
from nautilus_trader.live import LiveNodeBuilder, LiveNodeConfig
from nautilus_trader.model import BarType, TraderId
from nautilus_trader.trading import Strategy

from nautilus_longbridge import (
    LongbridgeDataClientConfig,
    LongbridgeDataClientFactory,
    bar_catalog_streaming,
)
from nautilus_longbridge.symbols import instrument_id_from_symbol


class BarRecorder(Strategy):
    def __new__(cls, bar_types: list[BarType]):
        return super().__new__(cls, StrategyConfig())

    def __init__(self, bar_types: list[BarType]) -> None:
        super().__init__(StrategyConfig())
        self.bar_types = bar_types

    def on_start(self) -> None:
        for bar_type in self.bar_types:
            self.subscribe_bars(bar_type)

    def on_stop(self) -> None:
        for bar_type in self.bar_types:
            self.unsubscribe_bars(bar_type)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("symbols", nargs="+", help="e.g. AAPL.US 700.HK")
    parser.add_argument("--catalog", type=Path, default=Path("data/catalog"))
    parser.add_argument("--period", choices=("minute", "hour", "day"), default="minute")
    parser.add_argument("--promotion-interval-ms", type=int, default=60_000)
    args = parser.parse_args()
    bar_types = [
        BarType.from_str(
            f"{instrument_id_from_symbol(symbol)}-1-{args.period.upper()}-LAST-EXTERNAL"
        )
        for symbol in dict.fromkeys(args.symbols)
    ]
    config = LiveNodeConfig(
        environment=Environment.LIVE,
        trader_id=TraderId("RECORDER-001"),
        streaming=bar_catalog_streaming(
            args.catalog, promotion_interval_ms=args.promotion_interval_ms
        ),
    )
    node = (
        LiveNodeBuilder.from_config("longbridge-bar-recorder", config)
        .add_data_client(
            "LONGBRIDGE_DATA",
            LongbridgeDataClientFactory,
            LongbridgeDataClientConfig(completed_bars_only=True),
        )
        .build()
    )
    try:
        node.add_strategy(BarRecorder(bar_types))
        node.run()
    finally:
        # Closing the NT-owned writer promotes the last batch into the catalog.
        node.dispose()


if __name__ == "__main__":
    main()

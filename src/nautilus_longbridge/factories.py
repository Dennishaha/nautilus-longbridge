from __future__ import annotations

from typing import Any

from nautilus_trader.live import DataClientConfig, ExecutionClientConfig
from nautilus_trader.live.clients import DataClientFactory, ExecutionClientFactory

from .config import LongbridgeDataClientConfig, LongbridgeExecutionClientConfig
from .constants import VENUE
from .data import LongbridgeDataClient
from .execution import LongbridgeExecutionClient
from .instruments import LongbridgeInstrumentProvider


class LongbridgeDataClientFactory(DataClientFactory):
    @staticmethod
    def create(
        *, name: str, config: DataClientConfig, cache: Any, clock: Any
    ) -> LongbridgeDataClient:
        if not isinstance(config, LongbridgeDataClientConfig):
            raise TypeError("LongbridgeDataClientFactory requires LongbridgeDataClientConfig")
        provider = LongbridgeInstrumentProvider(
            config.instrument_provider,
            price_precision=config.price_precision,
            price_increment=config.price_increment,
        )
        return LongbridgeDataClient(
            name=name,
            config=config,
            cache=cache,
            clock=clock,
            venue=VENUE,
            instrument_provider=provider,
        )


class LongbridgeExecutionClientFactory(ExecutionClientFactory):
    @staticmethod
    def create(
        *,
        name: str,
        config: ExecutionClientConfig,
        cache: Any,
        clock: Any,
        trader_id: Any,
    ) -> LongbridgeExecutionClient:
        if not isinstance(config, LongbridgeExecutionClientConfig):
            raise TypeError(
                "LongbridgeExecutionClientFactory requires LongbridgeExecutionClientConfig"
            )
        return LongbridgeExecutionClient(
            name=name,
            config=config,
            cache=cache,
            clock=clock,
            trader_id=trader_id,
        )

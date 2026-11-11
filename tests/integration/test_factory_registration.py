from __future__ import annotations

import pytest
from nautilus_trader.common import Clock, Environment
from nautilus_trader.live import InstrumentProviderConfig, LiveNode
from nautilus_trader.model import TraderId

from nautilus_longbridge import (
    LongbridgeDataClientConfig,
    LongbridgeDataClientFactory,
    LongbridgeExecutionClientConfig,
    LongbridgeExecutionClientFactory,
)


@pytest.mark.parametrize("account_id", ["LONGBRIDGE-ACCOUNT", "LBPT00000000"])
def test_rc7_builds_external_longbridge_factories(
    account_id: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LONGBRIDGE_ACCOUNT_ID", account_id)
    builder = LiveNode.builder("longbridge-test", TraderId("TRADER-001"), Environment.LIVE)
    builder = builder.add_data_client(
        "LONGBRIDGE_DATA",
        LongbridgeDataClientFactory,
        LongbridgeDataClientConfig(),
    )
    builder = builder.add_exec_client(
        "LONGBRIDGE_EXEC",
        LongbridgeExecutionClientFactory,
        LongbridgeExecutionClientConfig(),
    )
    node = builder.build()
    try:
        assert str(node.environment) == "LIVE"
        assert str(node.trader_id) == "TRADER-001"
    finally:
        node.dispose()


def test_factories_preserve_native_instrument_provider_config() -> None:
    provider_config = InstrumentProviderConfig(load_ids=["700.HK.LONGBRIDGE"])
    data_config = LongbridgeDataClientConfig(instrument_provider=provider_config)
    exec_config = LongbridgeExecutionClientConfig(
        account_id="ACC-1", instrument_provider=provider_config
    )

    data_client = LongbridgeDataClientFactory.create(
        name="LONGBRIDGE_DATA",
        config=data_config,
        cache=None,
        clock=Clock.new_test(),
    )
    exec_client = LongbridgeExecutionClientFactory.create(
        name="LONGBRIDGE_EXEC",
        config=exec_config,
        cache=None,
        clock=Clock.new_test(),
        trader_id=TraderId("TRADER-001"),
    )

    assert data_client.instrument_provider._config.load_ids == ["700.HK.LONGBRIDGE"]
    assert exec_client.instrument_provider._config.load_ids == ["700.HK.LONGBRIDGE"]

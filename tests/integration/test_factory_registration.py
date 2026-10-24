from __future__ import annotations

import pytest
from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode
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

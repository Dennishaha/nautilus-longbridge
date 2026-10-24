from __future__ import annotations

import json

import pytest

from nautilus_longbridge.config import (
    LongbridgeDataClientConfig,
    LongbridgeExecutionClientConfig,
    assert_order_enabled,
)
from nautilus_longbridge.errors import ConfigurationError
from nautilus_longbridge.symbols import instrument_id_from_symbol, symbol_from_instrument_id


@pytest.mark.parametrize("symbol", ["700.HK", "AAPL.US", "600000.SH", "000001.SZ"])
def test_symbol_round_trip(symbol: str) -> None:
    instrument_id = instrument_id_from_symbol(symbol)
    assert symbol_from_instrument_id(instrument_id) == symbol


def test_symbol_rejects_unknown_market() -> None:
    with pytest.raises(ConfigurationError):
        instrument_id_from_symbol("BTC.USDT")


def test_config_serialization_contains_no_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    secret = "secret-value-that-must-never-appear"
    monkeypatch.setenv("LONGBRIDGE_APP_SECRET", secret)
    config = LongbridgeDataClientConfig()
    serialized = " ".join(
        [repr(config), str(config), json.dumps(config.dict()), config.json().decode()]
    )
    assert secret not in serialized


def test_execution_config_requires_account_id() -> None:
    with pytest.raises(ConfigurationError):
        LongbridgeExecutionClientConfig(account_id=" ")


def test_orders_are_disabled_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LONGBRIDGE_ENABLE_LIVE_SMOKE", raising=False)
    with pytest.raises(ConfigurationError):
        assert_order_enabled(
            LongbridgeExecutionClientConfig(account_id="LONGBRIDGE-ACCOUNT").settings
        )


def test_order_smoke_gate_is_required_after_live_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LONGBRIDGE_ENABLE_LIVE_SMOKE", "1")
    monkeypatch.delenv("LONGBRIDGE_ENABLE_ORDER_SMOKE", raising=False)
    with pytest.raises(ConfigurationError, match="ORDER_SMOKE"):
        assert_order_enabled(
            LongbridgeExecutionClientConfig(
                account_id="LONGBRIDGE-ACCOUNT", dry_run=False
            ).settings
        )


def test_non_secret_settings_load_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LONGBRIDGE_ACCOUNT_ID", "ACCOUNT-FROM-ENV")
    monkeypatch.setenv("LONGBRIDGE_REGION", "cn")
    monkeypatch.setenv("LONGBRIDGE_REQUEST_TIMEOUT_SECS", "7.5")
    monkeypatch.setenv("LONGBRIDGE_DRY_RUN", "false")
    config = LongbridgeExecutionClientConfig()
    assert config.account_id == "ACCOUNT-FROM-ENV"
    assert config.settings.region == "cn"
    assert config.settings.request_timeout_secs == 7.5
    assert config.settings.dry_run is False

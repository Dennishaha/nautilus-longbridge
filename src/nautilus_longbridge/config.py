from __future__ import annotations

import json
import os
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from nautilus_trader.live import DataClientConfig, ExecutionClientConfig

from .constants import SUPPORTED_MARKETS
from .errors import AuthenticationError, ConfigurationError


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_text(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip()


def _env_float(name: str, default: float) -> float:
    value = _env_text(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be a number") from exc


def _env_int(name: str, default: int) -> int:
    value = _env_text(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc


def _explicit_or_env(name: str, value: str | None) -> str | None:
    return value if value is not None else _env_text(name)


def _safe(value: Any) -> Any:
    if hasattr(value, "name"):
        return value.name
    return value


@dataclass(frozen=True)
class LongbridgeSafeSettings:
    """Non-secret settings shared by both client families."""

    region: str = "hk"
    paper_trading: bool = True
    dry_run: bool = True
    http_url: str | None = None
    quote_ws_url: str | None = None
    trade_ws_url: str | None = None
    request_timeout_secs: float = 15.0
    reconnect_initial_secs: float = 1.0
    reconnect_max_secs: float = 30.0
    queue_size: int = 10_000

    def validate(self) -> None:
        if self.region not in {"cn", "hk"}:
            raise ConfigurationError("region must be 'cn' or 'hk'")
        if self.request_timeout_secs <= 0:
            raise ConfigurationError("request_timeout_secs must be positive")
        if (
            self.reconnect_initial_secs <= 0
            or self.reconnect_max_secs < self.reconnect_initial_secs
        ):
            raise ConfigurationError("invalid reconnect backoff")
        if self.queue_size < 1:
            raise ConfigurationError("queue_size must be positive")

    def as_dict(self) -> dict[str, Any]:
        return {key: _safe(value) for key, value in self.__dict__.items()}


class _SafeConfigMixin:
    _safe_values: dict[str, Any]

    def _safe_dict(self) -> dict[str, Any]:
        values = dict(self._safe_values)
        inherited = getattr(super(), "dict", None)
        if inherited is not None:
            with suppress(Exception):
                values.update(inherited())
        return values

    def dict(self) -> dict[str, Any]:
        return self._safe_dict()

    def json(self) -> bytes:
        return json.dumps(self._safe_dict(), sort_keys=True, default=str).encode()

    def __repr__(self) -> str:
        fields = ", ".join(f"{key}={value!r}" for key, value in self._safe_values.items())
        return f"{type(self).__name__}({fields})"

    def __str__(self) -> str:
        return self.__repr__()


class LongbridgeDataClientConfig(_SafeConfigMixin, DataClientConfig):
    """Safe configuration for the Longbridge market-data client."""

    def __init__(
        self,
        *,
        region: str | None = None,
        paper_trading: bool | None = None,
        dry_run: bool | None = None,
        http_url: str | None = None,
        quote_ws_url: str | None = None,
        trade_ws_url: str | None = None,
        request_timeout_secs: float | None = None,
        reconnect_initial_secs: float | None = None,
        reconnect_max_secs: float | None = None,
        queue_size: int | None = None,
        price_precision: int = 4,
        price_increment: str = "0.0001",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        resolved_region = region if region is not None else (_env_text("LONGBRIDGE_REGION", "hk") or "hk")
        self.settings = LongbridgeSafeSettings(
            region=resolved_region.lower(),
            paper_trading=_env_bool("LONGBRIDGE_PAPERTRADING", True)
            if paper_trading is None
            else paper_trading,
            dry_run=_env_bool("LONGBRIDGE_DRY_RUN", True) if dry_run is None else dry_run,
            http_url=_explicit_or_env("LONGBRIDGE_HTTP_URL", http_url),
            quote_ws_url=_explicit_or_env("LONGBRIDGE_QUOTE_WS_URL", quote_ws_url),
            trade_ws_url=_explicit_or_env("LONGBRIDGE_TRADE_WS_URL", trade_ws_url),
            request_timeout_secs=(
                _env_float("LONGBRIDGE_REQUEST_TIMEOUT_SECS", 15.0)
                if request_timeout_secs is None
                else request_timeout_secs
            ),
            reconnect_initial_secs=(
                _env_float("LONGBRIDGE_RECONNECT_INITIAL_SECS", 1.0)
                if reconnect_initial_secs is None
                else reconnect_initial_secs
            ),
            reconnect_max_secs=(
                _env_float("LONGBRIDGE_RECONNECT_MAX_SECS", 30.0)
                if reconnect_max_secs is None
                else reconnect_max_secs
            ),
            queue_size=(
                _env_int("LONGBRIDGE_QUEUE_SIZE", 10_000)
                if queue_size is None
                else queue_size
            ),
        )
        self.price_precision = price_precision
        self.price_increment = price_increment
        self.settings.validate()
        if price_precision < 0 or not price_increment:
            raise ConfigurationError("invalid price precision settings")
        self._safe_values = {
            "region": self.settings.region,
            "paper_trading": self.settings.paper_trading,
            "dry_run": self.settings.dry_run,
            "price_precision": self.price_precision,
            "price_increment": self.price_increment,
        }


class LongbridgeExecutionClientConfig(_SafeConfigMixin, ExecutionClientConfig):
    """Safe configuration for the Longbridge execution client."""

    def __init__(
        self,
        *,
        account_id: str | None = None,
        base_currency: str = "USD",
        region: str | None = None,
        paper_trading: bool | None = None,
        dry_run: bool | None = None,
        http_url: str | None = None,
        quote_ws_url: str | None = None,
        trade_ws_url: str | None = None,
        request_timeout_secs: float | None = None,
        reconnect_initial_secs: float | None = None,
        reconnect_max_secs: float | None = None,
        queue_size: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if account_id is None:
            account_id = _env_text("LONGBRIDGE_ACCOUNT_ID")
        if account_id is None:
            raise ConfigurationError("account_id is required or set LONGBRIDGE_ACCOUNT_ID")
        if not account_id.strip():
            raise ConfigurationError("account_id is required")
        if not base_currency.strip():
            raise ConfigurationError("base_currency is required")
        resolved_region = region if region is not None else (_env_text("LONGBRIDGE_REGION", "hk") or "hk")
        self.settings = LongbridgeSafeSettings(
            region=resolved_region.lower(),
            paper_trading=_env_bool("LONGBRIDGE_PAPERTRADING", True)
            if paper_trading is None
            else paper_trading,
            dry_run=_env_bool("LONGBRIDGE_DRY_RUN", True) if dry_run is None else dry_run,
            http_url=_explicit_or_env("LONGBRIDGE_HTTP_URL", http_url),
            quote_ws_url=_explicit_or_env("LONGBRIDGE_QUOTE_WS_URL", quote_ws_url),
            trade_ws_url=_explicit_or_env("LONGBRIDGE_TRADE_WS_URL", trade_ws_url),
            request_timeout_secs=(
                _env_float("LONGBRIDGE_REQUEST_TIMEOUT_SECS", 15.0)
                if request_timeout_secs is None
                else request_timeout_secs
            ),
            reconnect_initial_secs=(
                _env_float("LONGBRIDGE_RECONNECT_INITIAL_SECS", 1.0)
                if reconnect_initial_secs is None
                else reconnect_initial_secs
            ),
            reconnect_max_secs=(
                _env_float("LONGBRIDGE_RECONNECT_MAX_SECS", 30.0)
                if reconnect_max_secs is None
                else reconnect_max_secs
            ),
            queue_size=(
                _env_int("LONGBRIDGE_QUEUE_SIZE", 10_000)
                if queue_size is None
                else queue_size
            ),
        )
        self.account_id = account_id
        self.base_currency = base_currency.upper()
        self.settings.validate()
        self._safe_values = {
            "account_id": self.account_id,
            "base_currency": self.base_currency,
            "region": self.settings.region,
            "paper_trading": self.settings.paper_trading,
            "dry_run": self.settings.dry_run,
        }


def build_sdk_config(settings: LongbridgeSafeSettings) -> Any:
    """Build an SDK Config from environment credentials without retaining them."""
    try:
        from longbridge.openapi import Config
    except ImportError as exc:  # pragma: no cover - exercised by installation checks
        raise AuthenticationError("longbridge SDK is not installed") from exc

    names = ("LONGBRIDGE_APP_KEY", "LONGBRIDGE_APP_SECRET", "LONGBRIDGE_ACCESS_TOKEN")
    missing = [name for name in names if not os.getenv(name)]
    if missing:
        raise AuthenticationError(
            f"missing Longbridge credential environment variables: {', '.join(missing)}"
        )
    # SDK 5.2.2 exposes one official global endpoint set. Region remains a
    # validated routing label, while explicit endpoint overrides are honored.
    # Keeping the defaults here makes endpoint selection deterministic and
    # prevents a stale process environment from silently changing the client.
    endpoint_defaults_by_region = {
        "hk": {
            "http_url": "https://openapi.longbridge.com",
            "quote_ws_url": "wss://openapi-quote.longbridge.com/v2",
            "trade_ws_url": "wss://openapi-trade.longbridge.com/v2",
        },
        # SDK 5.2.2 documents the same global endpoint set for the supported
        # account regions; keep the region choice explicit for future endpoint
        # changes and for tests that validate routing configuration.
        "cn": {
            "http_url": "https://openapi.longbridge.com",
            "quote_ws_url": "wss://openapi-quote.longbridge.com/v2",
            "trade_ws_url": "wss://openapi-trade.longbridge.com/v2",
        },
    }
    endpoint_defaults = endpoint_defaults_by_region[settings.region]
    return Config.from_apikey(
        os.environ[names[0]],
        os.environ[names[1]],
        os.environ[names[2]],
        http_url=settings.http_url or endpoint_defaults["http_url"],
        quote_ws_url=settings.quote_ws_url or endpoint_defaults["quote_ws_url"],
        trade_ws_url=settings.trade_ws_url or endpoint_defaults["trade_ws_url"],
        enable_papertrading=settings.paper_trading,
    )


def assert_order_enabled(settings: LongbridgeSafeSettings) -> None:
    if settings.dry_run:
        raise ConfigurationError("order submission is disabled while dry_run=True")
    if not _env_bool("LONGBRIDGE_ENABLE_LIVE_SMOKE", False):
        raise ConfigurationError("set LONGBRIDGE_ENABLE_LIVE_SMOKE=1 to enable order submission")
    if not _env_bool("LONGBRIDGE_ENABLE_ORDER_SMOKE", False):
        raise ConfigurationError("set LONGBRIDGE_ENABLE_ORDER_SMOKE=1 to enable order submission")
    if not settings.paper_trading and not _env_bool("LONGBRIDGE_ENABLE_REAL_TRADING", False):
        raise ConfigurationError("real trading requires LONGBRIDGE_ENABLE_REAL_TRADING=1")


def market_for_symbol(symbol: str) -> str:
    try:
        market = symbol.rsplit(".", 1)[1].upper()
    except IndexError as exc:
        raise ConfigurationError(f"symbol must contain a market suffix: {symbol!r}") from exc
    if market not in SUPPORTED_MARKETS:
        raise ConfigurationError(f"unsupported Longbridge market suffix: {market}")
    return market

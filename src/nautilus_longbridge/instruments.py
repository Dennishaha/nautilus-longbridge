from __future__ import annotations

import re
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from nautilus_trader.live.providers import InstrumentProvider
from nautilus_trader.model import (
    AssetClass,
    Currency,
    Equity,
    FuturesContract,
    InstrumentId,
    Price,
    Quantity,
    Symbol,
)

from .config import market_for_symbol
from .errors import ConfigurationError, UnsupportedOperation
from .symbols import instrument_id_from_symbol, symbol_from_instrument_id

SUPPORTED_SECURITY_TYPES = frozenset({"STOCK", "EQUITY", "COMMON_STOCK", "ETF", "ETP"})
SUPPORTED_FUTURES_TYPES = frozenset({"FUTURE", "FUTURES", "FUTURES_CONTRACT"})
UNSUPPORTED_SECURITY_MARKERS = frozenset({"OPTION", "WARRANT", "CBBC", "DERIVATIVE"})


def validate_lot_size(order_quantity: Decimal, lot_size: Decimal, symbol: str) -> None:
    """Validate an equity order quantity against the venue board lot."""
    if order_quantity <= 0:
        raise ConfigurationError("order quantity must be positive")
    if lot_size <= 0:
        raise ConfigurationError(f"invalid lot size {lot_size} for {symbol}")
    if order_quantity % lot_size != 0:
        raise ConfigurationError(
            f"order quantity {order_quantity} for {symbol} must be a multiple of lot size "
            f"{lot_size}"
        )


class LongbridgeInstrumentProvider(InstrumentProvider):
    """Loads real Longbridge instruments from provider static metadata.

    The released Longbridge SDK currently exposes no futures metadata endpoint.
    The conversion path below is deliberately metadata-only: it becomes usable
    when the quote context supplies a futures static response, while missing
    multiplier, tick, currency, exchange, or expiry is rejected instead of
    inventing contract terms.
    """

    def __init__(
        self, config: Any = None, *, price_precision: int = 4, price_increment: str = "0.0001"
    ) -> None:
        super().__init__(config)
        self.price_precision = price_precision
        self.price_increment = Decimal(price_increment)
        self._quote_context: Any = None

    def set_quote_context(self, context: Any) -> None:
        self._quote_context = context

    async def load_all_async(self, filters: dict | None = None) -> None:
        symbols = (filters or {}).get("symbols") if filters else None
        if not symbols:
            raise UnsupportedOperation(
                "Longbridge cannot enumerate all ordinary securities; configure instrument_provider.load_ids"
            )
        await self.load_symbols_async(list(symbols))

    async def load_ids_async(
        self, instrument_ids: list[InstrumentId], filters: dict | None = None
    ) -> None:
        symbols = [symbol_from_instrument_id(instrument_id) for instrument_id in instrument_ids]
        await self.load_symbols_async(symbols)

    async def load_symbols_async(self, symbols: list[str]) -> None:
        if not symbols:
            return
        if self._quote_context is None:
            raise RuntimeError("quote context is not connected")
        requested = {str(symbol).strip().upper() for symbol in symbols}
        infos = await self._load_static_info(sorted(requested))
        returned = [str(self._field(info, "symbol", "code") or "").strip().upper() for info in infos]
        if any(not symbol for symbol in returned):
            raise ConfigurationError("Longbridge static_info returned an empty symbol")
        if set(returned) - requested:
            raise ConfigurationError("Longbridge static_info returned an unrequested symbol")
        if len(returned) != len(set(returned)):
            raise ConfigurationError("Longbridge static_info returned duplicate symbols")
        missing = requested - set(returned)
        if missing:
            if any(self._looks_like_futures_symbol(symbol) for symbol in missing):
                raise UnsupportedOperation(
                    "Longbridge OpenAPI does not expose futures static metadata in the "
                    "installed SDK; a futures-capable official quote context is required"
                )
            raise ConfigurationError(
                f"Longbridge static_info returned no instrument for {sorted(missing)}"
            )
        for info in infos:
            self.add(self._to_instrument(info))

    async def _load_static_info(self, symbols: list[str]) -> list[Any]:
        """Load equity or futures static data without coupling to an SDK name.

        ``future_static_info`` is intentionally an optional context capability.
        It is not present in Longbridge SDK 5.2.2, but keeping the adapter seam
        here allows a future official SDK method to be adopted without moving
        vendor calls into NT data tools or strategy-lab.
        """
        context = self._quote_context
        future_method = getattr(context, "future_static_info", None)
        if callable(future_method) and any(
            self._looks_like_futures_symbol(symbol) for symbol in symbols
        ):
            future_values = await future_method(symbols)
            if future_values:
                return future_values
        return await context.static_info(symbols)

    def _to_instrument(self, info: Any, *, reference: dict[str, Any] | None = None) -> Any:
        security_type = self._explicit_security_type(info)
        if security_type in SUPPORTED_FUTURES_TYPES:
            return self._to_future(info, reference=reference)
        return self._to_equity(info, reference=reference)

    @staticmethod
    def _looks_like_futures_symbol(symbol: str) -> bool:
        ticker = symbol.rsplit(".", 1)[0].upper()
        return ticker.endswith("MAIN") or bool(
            re.fullmatch(r"(?:[A-Z]{1,3}|6[BEJ])[FGHJKMNQUVXZ]\d{1,2}", ticker)
        )

    def _to_equity(self, info: Any, *, reference: dict[str, Any] | None = None) -> Equity:
        raw_symbol = str(info.symbol).upper()
        market_for_symbol(raw_symbol)
        explicit_type = self._explicit_security_type(info)
        board_type = self._board_type(info)
        security_type = explicit_type or board_type
        if security_type in UNSUPPORTED_SECURITY_MARKERS:
            raise UnsupportedOperation(
                f"Longbridge phase 1 does not support {security_type.lower()} instrument {raw_symbol}"
            )
        if explicit_type is not None and explicit_type not in SUPPORTED_SECURITY_TYPES:
            raise UnsupportedOperation(
                f"Longbridge phase 1 supports {sorted(SUPPORTED_SECURITY_TYPES)}, "
                f"got security type {explicit_type!r} for {raw_symbol}"
            )
        if board_type is not None and board_type in UNSUPPORTED_SECURITY_MARKERS:
            raise UnsupportedOperation(
                f"Longbridge phase 1 does not support {board_type.lower()} instrument {raw_symbol}"
            )
        instrument_id = instrument_id_from_symbol(raw_symbol)
        currency = Currency.from_str(str(info.currency).upper())
        raw_lot_size = getattr(info, "lot_size", None)
        if raw_lot_size is None or int(raw_lot_size) <= 0:
            raise ConfigurationError(f"Longbridge returned invalid lot size for {raw_symbol}")
        lot_size = int(raw_lot_size)
        instrument = Equity(
            instrument_id=instrument_id,
            raw_symbol=Symbol(raw_symbol),
            currency=currency,
            price_precision=self.price_precision,
            price_increment=Price.from_decimal_dp(self.price_increment, self.price_precision),
            ts_event=reference["received_ns"] if reference else 0,
            ts_init=reference["received_ns"] if reference else 0,
            lot_size=Quantity.from_int(lot_size),
            info={
                "name_cn": getattr(info, "name_cn", ""),
                "name_en": getattr(info, "name_en", ""),
                "name_hk": getattr(info, "name_hk", ""),
                "exchange": getattr(info, "exchange", ""),
                "longbridge_symbol": raw_symbol,
                "price_increment_source": "connector_config_unverified",
                **({"broker_reference": reference} if reference else {}),
            },
        )
        self.add_currency(currency)
        return instrument

    def _to_future(
        self, info: Any, *, reference: dict[str, Any] | None = None
    ) -> FuturesContract:
        """Convert a complete provider futures record into an NT contract.

        Longbridge has not published these fields in its current Python SDK.
        Every field needed for a meaningful contract is therefore mandatory;
        defaults would make P&L and risk calculations unsafe.
        """
        raw_symbol = str(self._field(info, "symbol", "code") or "").strip().upper()
        market_for_symbol(raw_symbol)
        if not raw_symbol:
            raise ConfigurationError("Longbridge futures metadata returned an empty symbol")

        security_type = self._explicit_security_type(info)
        if security_type not in SUPPORTED_FUTURES_TYPES:
            raise UnsupportedOperation(
                f"Longbridge futures metadata for {raw_symbol} has unsupported type "
                f"{security_type!r}"
            )
        multiplier = self._positive_decimal(
            info, "multiplier", "contract_size", "size", "volume_multiple"
        )
        increment = self._positive_decimal(
            info, "price_increment", "min_change", "tick_size", "min_price_increment"
        )
        currency_text = self._field(info, "price_currency", "currency")
        exchange = self._field(info, "exchange", "exchange_code", "venue")
        root = self._field(info, "root_symbol", "underlying", "owner")
        asset_class_value = self._field(info, "asset_class", "category")
        # Asset classification is part of the provider's static contract
        # metadata. The connector must not infer it from a research universe
        # or a hard-coded root-symbol list.
        asset_class = self._asset_class(asset_class_value)
        expiration_ns = self._timestamp_ns(
            self._field(
                info,
                "expiration_ns",
                "expiration_timestamp",
                "expiration",
                "expiry",
                "last_trade_time",
                "last_trade_date",
            )
        )
        activation_value = self._field(info, "activation_ns", "activation_timestamp", "activation")
        activation_ns = self._timestamp_ns(activation_value) if activation_value else 0
        if multiplier is None or increment is None:
            raise ConfigurationError(
                f"Longbridge futures metadata for {raw_symbol} lacks positive multiplier or tick"
            )
        if not currency_text or not exchange or not root or asset_class is None:
            raise ConfigurationError(
                f"Longbridge futures metadata for {raw_symbol} lacks currency, exchange, "
                "root, or provider asset class"
            )
        if expiration_ns <= 0:
            raise ConfigurationError(
                f"Longbridge futures metadata for {raw_symbol} lacks a valid expiration"
            )
        try:
            currency = Currency.from_str(str(currency_text).upper())
        except (ValueError, TypeError) as exc:
            raise ConfigurationError(
                f"Longbridge futures metadata has invalid currency {currency_text!r}"
            ) from exc
        received_ns = (reference or {}).get("received_ns", 0)
        exponent = increment.as_tuple().exponent
        if not isinstance(exponent, int):
            raise ConfigurationError(
                f"Longbridge futures metadata has an invalid tick size for {raw_symbol}"
            )
        precision = max(0, -exponent)
        info_dict = self._metadata_dict(info)
        info_dict.update(
            {
                "provider": "LONGBRIDGE_OPENAPI",
                "security_type": "FUTURE",
                "root_symbol": str(root).upper(),
                "contract_size": str(multiplier),
                "price_currency": str(currency_text).upper(),
                "metadata_received_ns": received_ns,
            }
        )
        self.add_currency(currency)
        return FuturesContract(
            instrument_id=instrument_id_from_symbol(raw_symbol),
            raw_symbol=Symbol(raw_symbol),
            asset_class=asset_class,
            underlying=str(root).upper(),
            activation_ns=activation_ns,
            expiration_ns=expiration_ns,
            currency=currency,
            price_precision=precision,
            price_increment=Price.from_decimal_dp(increment, precision),
            multiplier=Quantity.from_decimal(multiplier),
            lot_size=Quantity.from_int(1),
            ts_event=received_ns,
            ts_init=received_ns,
            exchange=str(exchange).upper(),
            info=info_dict,
        )

    @staticmethod
    def _field(info: Any, *names: str) -> Any:
        for name in names:
            value = info.get(name) if isinstance(info, dict) else getattr(info, name, None)
            if value not in (None, ""):
                return value
        return None

    @classmethod
    def _positive_decimal(cls, info: Any, *names: str) -> Decimal | None:
        value = cls._field(info, *names)
        if value is None:
            return None
        try:
            result = Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None
        return result if result.is_finite() and result > 0 else None

    @staticmethod
    def _asset_class(value: Any) -> AssetClass | None:
        if value is not None:
            name = str(getattr(value, "name", value)).upper().replace("ASSETCLASS.", "")
            resolved = getattr(AssetClass, name, None)
            if resolved is not None:
                return resolved
        return None

    @staticmethod
    def _timestamp_ns(value: Any) -> int:
        if value in (None, ""):
            return 0
        if isinstance(value, datetime):
            stamp = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
            return int(stamp.timestamp() * 1_000_000_000)
        if isinstance(value, date):
            return int(datetime(value.year, value.month, value.day, tzinfo=UTC).timestamp() * 1e9)
        if isinstance(value, (int, float, Decimal)):
            number = float(value)
            if not number > 0:
                return 0
            if number < 10_000_000_000:  # seconds
                return int(number * 1_000_000_000)
            if number < 10_000_000_000_000:  # milliseconds
                return int(number * 1_000_000)
            if number < 10_000_000_000_000_000:  # microseconds
                return int(number * 1_000)
            return int(number)  # nanoseconds
        text = str(value).strip().replace("Z", "+00:00")
        try:
            stamp = datetime.fromisoformat(text)
        except ValueError:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    stamp = datetime.strptime(text, fmt)
                    break
                except ValueError:
                    continue
            else:
                return 0
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=UTC)
        return int(stamp.timestamp() * 1_000_000_000)

    @staticmethod
    def _metadata_dict(info: Any) -> dict[str, Any]:
        if isinstance(info, dict):
            return {str(key): value for key, value in info.items()}
        values: dict[str, Any] = {}
        for name in dir(info):
            if name.startswith("_"):
                continue
            try:
                value = getattr(info, name)
            except Exception:
                continue
            if callable(value):
                continue
            values[name] = str(value) if isinstance(value, (Decimal, datetime, date)) else value
        return values

    @staticmethod
    def _explicit_security_type(info: Any) -> str | None:
        for field in ("security_type", "asset_type"):
            value = info.get(field) if isinstance(info, dict) else getattr(info, field, None)
            if value is None or not str(value).strip():
                continue
            return str(value).upper().replace("-", "_").replace(" ", "_")
        return None

    @staticmethod
    def _board_type(info: Any) -> str | None:
        value = getattr(info, "board", None)
        if value is None:
            return None
        name = getattr(value, "__name__", None) or getattr(value, "name", None) or str(value)
        normalized = str(name).upper().replace("SECURITYBOARD.", "")
        if "WARRANT" in normalized:
            return "WARRANT"
        if "OPTION" in normalized:
            return "OPTION"
        return None

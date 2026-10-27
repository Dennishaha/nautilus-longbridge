from __future__ import annotations

from decimal import Decimal
from typing import Any

from nautilus_trader.live.providers import InstrumentProvider
from nautilus_trader.model import Currency, Equity, InstrumentId, Price, Quantity, Symbol

from .config import market_for_symbol
from .errors import ConfigurationError, UnsupportedOperation
from .symbols import instrument_id_from_symbol, symbol_from_instrument_id

SUPPORTED_SECURITY_TYPES = frozenset({"STOCK", "EQUITY", "COMMON_STOCK", "ETF", "ETP"})
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
    """Loads ordinary Longbridge equities and ETFs from ``static_info``."""

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
        infos = await self._quote_context.static_info(sorted(requested))
        returned = [str(getattr(info, "symbol", "")).strip().upper() for info in infos]
        if any(not symbol for symbol in returned):
            raise ConfigurationError("Longbridge static_info returned an empty symbol")
        if set(returned) - requested:
            raise ConfigurationError("Longbridge static_info returned an unrequested symbol")
        if len(returned) != len(set(returned)):
            raise ConfigurationError("Longbridge static_info returned duplicate symbols")
        missing = requested - set(returned)
        if missing:
            raise ConfigurationError(
                f"Longbridge static_info returned no instrument for {sorted(missing)}"
            )
        for info in infos:
            self.add(self._to_equity(info))

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

    @staticmethod
    def _explicit_security_type(info: Any) -> str | None:
        for field in ("security_type", "asset_type"):
            if not hasattr(info, field):
                continue
            value = getattr(info, field)
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

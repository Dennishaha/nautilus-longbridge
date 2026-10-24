from __future__ import annotations

from decimal import Decimal
from typing import Any

from nautilus_trader.live.providers import InstrumentProvider
from nautilus_trader.model import Currency, Equity, InstrumentId, Price, Quantity, Symbol

from .config import market_for_symbol
from .errors import ConfigurationError, UnsupportedOperation
from .symbols import instrument_id_from_symbol, symbol_from_instrument_id


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
        infos = await self._quote_context.static_info(symbols)
        for info in infos:
            self.add(self._to_equity(info))

    def _to_equity(self, info: Any) -> Equity:
        raw_symbol = str(info.symbol).upper()
        market_for_symbol(raw_symbol)
        security_type = str(
            getattr(info, "security_type", getattr(info, "asset_type", ""))
        ).upper()
        if any(kind in security_type for kind in ("OPTION", "WARRANT", "CBBC", "DERIVATIVE")):
            raise UnsupportedOperation(
                f"Longbridge phase 1 does not support derivative instrument {raw_symbol}"
            )
        instrument_id = instrument_id_from_symbol(raw_symbol)
        currency = Currency.from_str(str(info.currency).upper())
        lot_size = int(getattr(info, "lot_size", 1) or 1)
        instrument = Equity(
            instrument_id=instrument_id,
            raw_symbol=Symbol(raw_symbol),
            currency=currency,
            price_precision=self.price_precision,
            price_increment=Price.from_decimal_dp(self.price_increment, self.price_precision),
            ts_event=0,
            ts_init=0,
            lot_size=Quantity.from_int(lot_size),
            info={
                "name_cn": getattr(info, "name_cn", ""),
                "name_en": getattr(info, "name_en", ""),
                "name_hk": getattr(info, "name_hk", ""),
                "exchange": getattr(info, "exchange", ""),
                "longbridge_symbol": raw_symbol,
            },
        )
        self.add_currency(currency)
        return instrument

from __future__ import annotations

from nautilus_trader.model import InstrumentId, Symbol, Venue

from .constants import SUPPORTED_MARKETS, VENUE
from .errors import ConfigurationError


def validate_symbol(raw_symbol: str) -> str:
    value = raw_symbol.strip().upper()
    parts = value.rsplit(".", 1)
    if len(parts) != 2 or not parts[0] or parts[1] not in SUPPORTED_MARKETS:
        raise ConfigurationError(f"unsupported Longbridge symbol: {raw_symbol!r}")
    return value


def instrument_id_from_symbol(raw_symbol: str, venue: Venue = VENUE) -> InstrumentId:
    return InstrumentId(Symbol(validate_symbol(raw_symbol)), venue)


def symbol_from_instrument_id(instrument_id: InstrumentId) -> str:
    if str(instrument_id.venue) != str(VENUE):
        raise ConfigurationError(f"instrument belongs to {instrument_id.venue}, not {VENUE}")
    return validate_symbol(str(instrument_id.symbol))

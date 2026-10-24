from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from nautilus_trader.model import AggressorSide, Price, Quantity


def timestamp_ns(value: datetime | None, fallback: int) -> int:
    if value is None:
        return fallback
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return int(value.astimezone(UTC).timestamp() * 1_000_000_000)


def as_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def price(value: Any, precision: int) -> Price:
    return Price.from_decimal_dp(as_decimal(value), precision)


def quantity(value: Any, precision: int | None = None) -> Quantity:
    decimal = as_decimal(value)
    if precision is None:
        return Quantity.from_decimal(decimal)
    return Quantity.from_decimal_dp(decimal, precision)


def enum_name(value: Any) -> str:
    if isinstance(value, str):
        return value.upper()
    name = getattr(value, "name", None)
    if name:
        return str(name).upper()
    text = str(value)
    if "." in text:
        return text.rsplit(".", 1)[-1].upper()
    return type(value).__name__.upper()


def aggressor_side(direction: Any) -> AggressorSide:
    name = enum_name(direction)
    if name in {"UP", "BUY", "BUYER"}:
        return AggressorSide.BUY
    if name in {"DOWN", "SELL", "SELLER"}:
        return AggressorSide.SELL
    return AggressorSide.NO_AGGRESSOR

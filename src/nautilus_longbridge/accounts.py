"""Broker-reported collateral and cash are distinct account inputs."""

from __future__ import annotations

from typing import Any

from .errors import ConfigurationError
from .mapping import as_decimal


def paper_margin_balance(balance: Any) -> dict[str, Any]:
    if str(getattr(balance, "currency", "")).upper() != "USD":
        raise ConfigurationError("paper margin requires broker-reported USD valuation")
    names = (
        "net_assets",
        "buy_power",
        "init_margin",
        "maintenance_margin",
        "margin_call",
        "max_finance_amount",
        "remaining_finance_amount",
    )
    values = {}
    for name in names:
        value = getattr(balance, name, None)
        if value is None:
            raise ConfigurationError(f"missing margin account field: {name}")
        number = as_decimal(value)
        if not number.is_finite() or (name != "net_assets" and number < 0):
            raise ConfigurationError(f"invalid margin account field: {name}")
        values[name] = number
    if values["net_assets"] <= 0:
        raise ConfigurationError("nonpositive margin account equity")
    risk = str(getattr(balance, "risk_level", ""))
    if risk not in {"0", "1", "2", "3"}:
        raise ConfigurationError("missing or invalid broker risk level")
    cash = {}
    for info in getattr(balance, "cash_infos", []):
        currency = str(info.currency).upper()
        if currency in cash:
            raise ConfigurationError("duplicate cash currency")
        row = {}
        for name in ("available_cash", "frozen_cash", "settling_cash"):
            value = getattr(info, name, None)
            if value is None or not as_decimal(value).is_finite():
                raise ConfigurationError("invalid per-currency cash details")
            row[name] = str(value)
        cash[currency] = row
    if not cash:
        raise ConfigurationError("missing per-currency cash details")
    return {
        "account_mode": "MARGIN",
        "valuation_currency": "USD",
        "equity": float(values["net_assets"]),
        "buying_power": float(values["buy_power"]),
        "initial_margin": float(values["init_margin"]),
        "maintenance_margin": float(values["maintenance_margin"]),
        "margin_call": float(values["margin_call"]),
        "max_finance_amount": float(values["max_finance_amount"]),
        "remaining_finance_amount": float(values["remaining_finance_amount"]),
        "risk_level": int(risk),
        "account_healthy": (
            risk == "0"
            and values["margin_call"] == 0
            and values["net_assets"] >= max(values["init_margin"], values["maintenance_margin"])
        ),
        "currency_cash": cash,
        # A missing USD cash row remains unknown, never broker buying power.
        "available_cash": float(cash["USD"]["available_cash"]) if "USD" in cash else None,
        "negative_available_cash": {
            c: str(-as_decimal(v["available_cash"]))
            for c, v in cash.items()
            if as_decimal(v["available_cash"]) < 0
        },
    }

"""Read-only account reference adaptation; no strategy or execution policy."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Any


async def account_reference(context: Any, symbol: str, *, received_ns: int,
                            timeout: float = 15) -> dict[str, Any]:
    """Only margin ratios and completed-order charges are exposed.

    The SDK has no account borrow-pool/APR endpoint. Never substitute market
    short-interest statistics or a maximum sell estimate for a locate.
    """
    result: dict[str, Any] = {
        "schema": "longbridge-account-reference-v1", "symbol": symbol,
        "received_ns": received_ns, "historical_availability_verified": False,
        "borrow_availability": "NOT_EXPOSED_BY_SDK", "borrow_apr": None,
        "available_quantity": None, "fee_schedule": "NOT_EXPOSED_BY_SDK",
        "order_charges": [], "errors": [],
    }
    try:
        ratio = await asyncio.wait_for(context.margin_ratio(symbol), timeout)
        result["margin_ratio"] = {name: str(getattr(ratio, name))
                                  for name in ("im_factor", "mm_factor", "fm_factor")}
    except Exception as exc:
        result["errors"].append({"operation": "margin_ratio", "type": type(exc).__name__})
    try:
        end = datetime.now(UTC)
        orders = await asyncio.wait_for(context.history_orders(
            symbol=symbol, start_at=end - timedelta(days=30), end_at=end), timeout)
        # Bounded, completed executions only. No pending-order details, account
        # balances, client remarks or raw account/order identifiers are published.
        for order in [o for o in orders if getattr(o, "executed_quantity", 0) > 0][:10]:
            detail = await asyncio.wait_for(context.order_detail(str(order.order_id)), timeout)
            charge = detail.charge_detail
            components = []
            for item in charge.items:
                for fee in item.fees:
                    components.append({name: str(getattr(fee, name, ""))
                                       for name in ("code", "name", "amount", "currency")})
            result["order_charges"].append({
                "order_key_sha256": hashlib.sha256(str(order.order_id).encode()).hexdigest(),
                "side": str(detail.side), "quantity": str(detail.executed_quantity),
                "price": str(detail.executed_price), "updated_at": str(detail.updated_at),
                "total": str(charge.total_amount), "currency": str(charge.currency),
                "components": components,
            })
    except Exception as exc:
        result["errors"].append({"operation": "completed_order_charges", "type": type(exc).__name__})
    payload = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
    result["source_sha256"] = hashlib.sha256(payload).hexdigest()
    return result

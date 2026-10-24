from __future__ import annotations

from decimal import Decimal

from nautilus_trader.model import OrderStatus

from nautilus_longbridge.execution import (
    client_request_id,
    map_order_type,
    map_status,
    map_time_in_force,
)


def test_client_request_id_is_stable_and_bounded() -> None:
    value = client_request_id("O-123")
    assert value == "NT-O-123"
    assert len(client_request_id("x" * 200)) <= 64


def test_order_type_and_tif_mapping() -> None:
    from longbridge import openapi

    assert map_order_type("MARKET") is openapi.OrderType.MO
    assert map_order_type("LIMIT") is openapi.OrderType.LO
    assert map_time_in_force("DAY")[0] is openapi.TimeInForceType.Day


def test_status_mapping_is_complete_for_terminal_states() -> None:
    assert map_status("Filled", Decimal("10"), Decimal("10")) is OrderStatus.FILLED
    assert map_status("PartialFilled", Decimal("2"), Decimal("10")) is OrderStatus.PARTIALLY_FILLED
    assert map_status("Canceled", Decimal("0"), Decimal("10")) is OrderStatus.CANCELED
    assert map_status("Rejected", Decimal("0"), Decimal("10")) is OrderStatus.REJECTED
    assert map_status("Expired", Decimal("0"), Decimal("10")) is OrderStatus.EXPIRED

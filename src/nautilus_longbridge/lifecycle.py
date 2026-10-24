from __future__ import annotations

from enum import StrEnum


class ConnectionState(StrEnum):
    """Lifecycle states shared by the market-data and execution clients."""

    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    RECONNECTING = "RECONNECTING"
    FAILED = "FAILED"

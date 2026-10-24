from nautilus_trader.model import Venue

VENUE = Venue("LONGBRIDGE")
CLIENT_ID_DATA = "LONGBRIDGE_DATA"
CLIENT_ID_EXEC = "LONGBRIDGE_EXEC"

SUPPORTED_MARKETS = frozenset({"HK", "US", "SH", "SZ"})
TRADE_MARKETS = frozenset({"HK", "US"})
SUPPORTED_ORDER_TYPES = frozenset({"MARKET", "LIMIT"})
SUPPORTED_TIFS = frozenset({"DAY", "GTC", "GTD"})

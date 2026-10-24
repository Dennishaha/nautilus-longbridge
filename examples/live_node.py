from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import TraderId

from nautilus_longbridge import (
    LongbridgeDataClientConfig,
    LongbridgeDataClientFactory,
    LongbridgeExecutionClientConfig,
    LongbridgeExecutionClientFactory,
)


def build_node() -> LiveNode:
    builder = LiveNode.builder("longbridge", TraderId("TRADER-001"), Environment.LIVE)
    builder = builder.add_data_client(
        "LONGBRIDGE_DATA", LongbridgeDataClientFactory, LongbridgeDataClientConfig()
    )
    builder = builder.add_exec_client(
        "LONGBRIDGE_EXEC",
        LongbridgeExecutionClientFactory,
        LongbridgeExecutionClientConfig(),
    )
    return builder.build()


if __name__ == "__main__":
    node = build_node()
    print(node)
    node.dispose()

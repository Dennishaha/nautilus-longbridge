import asyncio

from nautilus_longbridge import LongbridgeDataClientConfig


async def main() -> None:
    # LiveNode owns the client lifecycle; this example only validates safe configuration.
    print(LongbridgeDataClientConfig())


if __name__ == "__main__":
    asyncio.run(main())

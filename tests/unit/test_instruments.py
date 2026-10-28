from __future__ import annotations

from types import SimpleNamespace

import pytest
from nautilus_trader.model import AssetClass, FuturesContract

from nautilus_longbridge.errors import UnsupportedOperation
from nautilus_longbridge.instruments import LongbridgeInstrumentProvider
from nautilus_longbridge.symbols import instrument_id_from_symbol


@pytest.mark.asyncio
async def test_instrument_provider_loads_requested_equity() -> None:
    provider = LongbridgeInstrumentProvider()
    provider.set_quote_context(
        SimpleNamespace(
            static_info=lambda symbols: _static_info(symbols),
        )
    )
    await provider.load_symbols_async(["700.HK"])
    instrument = provider.find(instrument_id_from_symbol("700.HK"))
    assert instrument is not None
    assert instrument.raw_symbol.value == "700.HK"
    assert instrument.lot_size.as_decimal() == 100


def test_instrument_provider_rejects_derivative_metadata() -> None:
    provider = LongbridgeInstrumentProvider()
    with pytest.raises(UnsupportedOperation):
        provider._to_equity(
            SimpleNamespace(symbol="12345.HK", currency="HKD", lot_size=100, asset_type="WARRANT")
        )


def test_instrument_provider_converts_complete_future_metadata() -> None:
    provider = LongbridgeInstrumentProvider()
    instrument = provider._to_instrument(
        {
            "symbol": "ESZ6.US",
            "security_type": "FUTURE",
            "root_symbol": "ES",
            "asset_class": "INDEX",
            "currency": "USD",
            "exchange": "CME",
            "multiplier": "50",
            "price_increment": "0.25",
            "expiration": "2026-12-18T00:00:00+00:00",
        },
        reference={"received_ns": 1_700_000_000_000_000_000},
    )

    assert isinstance(instrument, FuturesContract)
    assert instrument.asset_class == AssetClass.INDEX
    assert instrument.underlying == "ES"
    assert instrument.multiplier.as_decimal() == 50
    assert instrument.price_increment.as_decimal() == 0.25
    assert instrument.expiration_ns > instrument.ts_init
    assert {
        "historical_execution_metadata",
        "execution_metadata_verified",
        "research_only_until_execution_audit",
    }.isdisjoint(instrument.info)


def test_instrument_provider_rejects_incomplete_future_metadata() -> None:
    with pytest.raises(ValueError, match="lacks positive multiplier or tick"):
        LongbridgeInstrumentProvider()._to_instrument(
            {
                "symbol": "ESZ6.US",
                "security_type": "FUTURE",
                "root_symbol": "ES",
                "asset_class": "INDEX",
                "currency": "USD",
                "exchange": "CME",
                "expiration": "2026-12-18",
            }
        )


def test_instrument_provider_requires_provider_asset_class() -> None:
    with pytest.raises(ValueError, match="provider asset class"):
        LongbridgeInstrumentProvider()._to_instrument(
            {
                "symbol": "ESZ6.US",
                "security_type": "FUTURE",
                "root_symbol": "ES",
                "currency": "USD",
                "exchange": "CME",
                "multiplier": "50",
                "price_increment": "0.25",
                "expiration": "2026-12-18",
            }
        )


@pytest.mark.asyncio
async def test_instrument_provider_reports_missing_sdk_future_capability() -> None:
    class Quote:
        async def static_info(self, symbols):
            return []

    provider = LongbridgeInstrumentProvider()
    provider.set_quote_context(Quote())
    with pytest.raises(UnsupportedOperation, match="does not expose futures static metadata"):
        await provider.load_symbols_async(["ESmain.US"])


@pytest.mark.asyncio
async def test_instrument_provider_uses_optional_future_static_method() -> None:
    class Quote:
        async def future_static_info(self, symbols):
            assert symbols == ["ESZ6.US"]
            return [
                {
                    "symbol": "ESZ6.US",
                    "security_type": "FUTURE",
                    "root_symbol": "ES",
                    "asset_class": "INDEX",
                    "currency": "USD",
                    "exchange": "CME",
                    "multiplier": 50,
                    "price_increment": 0.25,
                    "expiration": "2026-12-18",
                }
            ]

        async def static_info(self, symbols):
            raise AssertionError("equity static_info must not be used when futures metadata exists")

    provider = LongbridgeInstrumentProvider()
    provider.set_quote_context(Quote())
    await provider.load_symbols_async(["ESZ6.US"])
    assert isinstance(provider.list_all()[0], FuturesContract)


async def _static_info(symbols: list[str]) -> list[SimpleNamespace]:
    return [
        SimpleNamespace(
            symbol=symbol,
            currency="HKD",
            lot_size=100,
            name_cn="测试",
            name_en="Test",
            name_hk="測試",
            exchange="SEHK",
        )
        for symbol in symbols
    ]

from __future__ import annotations

from types import SimpleNamespace

import pytest

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

from __future__ import annotations

from datetime import date, datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from longbridge.openapi import AdjustType, Period
from nautilus_trader.core import UUID4
from nautilus_trader.model import BarType, ClientId

from nautilus_longbridge.history import history_candles

NY = ZoneInfo("America/New_York")


def timestamp_ns(value):
    return int(value.timestamp()) * 10**9


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [False, True])
async def test_native_request_records_raw_response_before_validation(tmp_path, invalid):
    import hashlib
    import json

    from nautilus_longbridge.config import LongbridgeDataClientConfig
    from nautilus_longbridge.data import LongbridgeDataClient

    opening = datetime(2024, 1, 2, 9, 30, tzinfo=NY)
    output = []

    class Quote:
        async def history_candlesticks_by_date(self, symbol, period, adjustment, **kwargs):
            assert adjustment == AdjustType.ForwardAdjust
            return [
                SimpleNamespace(
                    timestamp=opening,
                    open="100",
                    high="98" if invalid else "102",
                    low="99",
                    close="101",
                    volume=100,
                )
            ]

    async def invoke(operation):
        return await operation()

    async def ensure(symbol):
        return None

    request_id = UUID4()
    client = SimpleNamespace(
        _quote=Quote(),
        _quote_call=invoke,
        _ensure_instrument=ensure,
        _lb_config=LongbridgeDataClientConfig(
            historical_adjustment="ForwardAdjust", historical_evidence_dir=str(tmp_path)
        ),
        clock=SimpleNamespace(timestamp_ns=lambda: timestamp_ns(datetime(2026, 10, 25, tzinfo=NY))),
        client_id=ClientId("LONGBRIDGE_DATA"),
        _handle_response=output.append,
    )
    request = SimpleNamespace(
        bar_type=BarType.from_str("SPY.US.LONGBRIDGE-1-MINUTE-LAST-EXTERNAL"),
        start_ns=timestamp_ns(opening),
        end_ns=timestamp_ns(opening),
        limit=None,
        request_id=request_id,
    )
    operation = LongbridgeDataClient._request_bars(client, request)
    if invalid:
        with pytest.raises(ValueError, match="OHLCV"):
            await operation
        assert not output
    else:
        await operation
        assert len(output[0].data) == 1
        assert output[0].data[0].ts_init > output[0].data[0].ts_event
    directory = tmp_path / str(request_id)
    manifest = json.loads((directory / "manifest.json").read_text())
    assert manifest["status"] == ("FAILED" if invalid else "COMPLETE")
    raw = directory / manifest["requests"][0]["path"]
    assert hashlib.sha256(raw.read_bytes()).hexdigest() == manifest["requests"][0]["sha256"]
    assert json.loads(raw.read_text())["candles"][0]["high"] == ("98" if invalid else "102")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "symbol,zone",
    [
        ("SPY.US", NY),
        ("700.HK", ZoneInfo("Asia/Hong_Kong")),
        ("600000.SH", ZoneInfo("Asia/Shanghai")),
    ],
)
async def test_history_calendar_bounds_use_market_timezone_and_exact_last_nanosecond(symbol, zone):
    calls = []

    class Quote:
        async def history_candlesticks_by_date(self, *args, **kwargs):
            calls.append((kwargs["start"], kwargs["end"]))
            return []

    first = datetime(2024, 1, 2, tzinfo=zone)
    last = datetime(2024, 1, 3, tzinfo=zone)
    await history_candles(
        Quote(),
        symbol,
        Period.Min_1,
        AdjustType.NoAdjust,
        timestamp_ns(first),
        timestamp_ns(last) - 1,
        delay=0,
    )
    assert calls == [(date(2024, 1, 2), date(2024, 1, 2))]


@pytest.mark.asyncio
async def test_history_paginates_dates_and_filters_requested_bounds():
    calls = []

    class Quote:
        async def history_candlesticks_by_date(self, symbol, period, adjustment, **kwargs):
            calls.append((kwargs["start"], kwargs["end"]))
            return [
                SimpleNamespace(
                    timestamp=datetime.combine(kwargs["start"], datetime.min.time(), NY),
                    open="100",
                    high="102",
                    low="99",
                    close="101",
                    volume=100,
                )
            ]

    first, last = datetime(2015, 1, 2, tzinfo=NY), datetime(2026, 10, 23, tzinfo=NY)
    rows = await history_candles(
        Quote(),
        "SPY.US",
        Period.Day,
        AdjustType.NoAdjust,
        timestamp_ns(first),
        timestamp_ns(last),
        delay=0,
    )
    assert len(calls) == 6 and len(rows) == 6
    assert all((end - start).days < 730 for start, end in calls)
    assert calls[-1][1] == date(2026, 10, 23)


@pytest.mark.asyncio
@pytest.mark.parametrize("defect", ["duplicate", "invalid", "cap"])
async def test_history_preserves_evidence_before_rejecting_source(defect):
    row = SimpleNamespace(
        timestamp=datetime(2024, 1, 2, tzinfo=NY),
        open="100",
        high="102",
        low="99",
        close="101",
        volume=100,
    )
    values = [row, row] if defect == "duplicate" else [row]
    if defect == "invalid":
        row.high = "98"
    elif defect == "cap":
        values = [row] * 1000

    class Quote:
        async def history_candlesticks_by_date(self, *args, **kwargs):
            return values

    retained = []
    stamp = timestamp_ns(row.timestamp)
    with pytest.raises(ValueError):
        await history_candles(
            Quote(),
            "SPY.US",
            Period.Min_1,
            AdjustType.NoAdjust,
            stamp,
            stamp + 60 * 10**9,
            delay=0,
            evidence=lambda first, last, rows: retained.extend(rows),
        )
    assert retained == values


@pytest.mark.asyncio
async def test_native_bar_request_uses_historical_range_and_overall_limit():
    from nautilus_trader.live import BarsResponse

    from nautilus_longbridge.data import LongbridgeDataClient

    opening = datetime(2024, 1, 2, 9, 30, tzinfo=NY)
    responses, calls = [], []

    class Quote:
        async def candlesticks(self, *args):
            pytest.fail("bounded request used the recent-bars endpoint")

        async def history_candlesticks_by_date(self, *args, **kwargs):
            calls.append(kwargs)
            return [
                SimpleNamespace(
                    timestamp=opening + timedelta(minutes=index),
                    open="100",
                    high="102",
                    low="99",
                    close="101",
                    volume=100,
                )
                for index in range(4)
            ]

    async def invoke(operation):
        return await operation()

    async def ensure_instrument(symbol):
        return None

    now = timestamp_ns(datetime(2026, 10, 25, tzinfo=NY))
    client = SimpleNamespace(
        _quote=Quote(),
        _quote_call=invoke,
        _ensure_instrument=ensure_instrument,
        _lb_config=SimpleNamespace(price_precision=4),
        clock=SimpleNamespace(timestamp_ns=lambda: now),
        client_id=ClientId("LONGBRIDGE"),
        _handle_response=responses.append,
    )
    end = timestamp_ns(opening + timedelta(minutes=2))
    request = SimpleNamespace(
        bar_type=BarType.from_str("SPY.US.LONGBRIDGE-1-MINUTE-LAST-EXTERNAL"),
        start_ns=timestamp_ns(opening + timedelta(minutes=1)),
        end_ns=end,
        limit=1,
        request_id=UUID4(),
    )
    await LongbridgeDataClient._request_bars(client, request)
    assert len(calls) == 1 and len(responses) == 1
    response = responses[0]
    assert isinstance(response, BarsResponse)
    assert len(response.data) == 1 and response.data[0].ts_event == end
    assert response.data[0].ts_init == now

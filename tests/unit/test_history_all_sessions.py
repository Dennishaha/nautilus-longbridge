from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from longbridge.openapi import AdjustType, Period, TradeSessions

from nautilus_longbridge.config import LongbridgeDataClientConfig
from nautilus_longbridge.history import history_candles

NY = ZoneInfo("America/New_York")


@pytest.mark.asyncio
async def test_all_sessions_pages_past_a_thousand_rows_without_losing_night_bars():
    start = datetime(2024, 1, 2, tzinfo=NY)
    candles = [SimpleNamespace(timestamp=start + timedelta(minutes=i), open="100", high="101",
                               low="99", close="100", volume="100") for i in range(1440)]
    calls, evidence = [], []

    class Quote:
        async def history_candlesticks_by_offset(self, symbol, period, adjustment, forward, count,
                                               *, time, trade_sessions):
            assert trade_sessions == TradeSessions.All and forward is False
            calls.append(time)
            return [c for c in candles if c.timestamp <= time][-count:]

    bars = await history_candles(Quote(), "AAPL.US", Period.Min_1, AdjustType.NoAdjust,
                                int(start.timestamp()) * 10**9,
                                int((start + timedelta(days=1)).timestamp()) * 10**9 - 1,
                                trade_sessions="All", delay=0,
                                evidence=lambda first, last, values: evidence.extend(values))
    assert len(calls) == 2 and bars == candles
    assert len(evidence) == 1440
    assert bars[0].timestamp.hour == 0 and bars[-1].timestamp.hour == 23


@pytest.mark.asyncio
async def test_all_session_evidence_is_retained_before_invalid_page_is_rejected():
    start = datetime(2024, 1, 2, tzinfo=NY)
    candle = SimpleNamespace(timestamp=start, open="100", high="90", low="99", close="100", volume=1)

    class Quote:
        async def history_candlesticks_by_offset(self, *args, **kwargs):
            return [candle]

    evidence = []
    with pytest.raises(ValueError, match="OHLCV"):
        await history_candles(Quote(), "AAPL.US", Period.Min_1, AdjustType.NoAdjust,
                              int(start.timestamp()) * 10**9, int(start.timestamp()) * 10**9,
                              trade_sessions="All", delay=0,
                              evidence=lambda first, last, values: evidence.extend(values))
    assert evidence == [candle]


def test_history_session_configuration_is_explicit_and_validated():
    assert LongbridgeDataClientConfig().historical_trade_sessions == "Intraday"
    assert LongbridgeDataClientConfig(historical_trade_sessions="All").dict()["historical_trade_sessions"] == "All"
    with pytest.raises(Exception, match="trade sessions"):
        LongbridgeDataClientConfig(historical_trade_sessions="NightOnly")


@pytest.mark.asyncio
@pytest.mark.parametrize("persistent", [False, True])
@pytest.mark.parametrize("error_kind", ["no_quote", "timeout"])
async def test_transient_no_quote_retries_same_anchor_and_persistent_error_still_fails(persistent, error_kind):
    start = datetime(2025, 5, 7, 0, 39, tzinfo=NY)
    candle = SimpleNamespace(timestamp=start, open="100", high="100", low="100", close="100", volume=1)
    attempts, failures = [], []

    class NoQuote(Exception):
        code = 301603

    class Quote:
        async def history_candlesticks_by_offset(self, *args, **kwargs):
            attempts.append(kwargs["time"])
            if persistent or len(attempts) == 1:
                raise NoQuote("no quote") if error_kind == "no_quote" else TimeoutError()
            return [candle]

    async def operation():
        return await history_candles(Quote(), "GOOGL.US", Period.Min_1, AdjustType.NoAdjust,
                                    int(start.timestamp()) * 10**9, int(start.timestamp()) * 10**9,
                                    trade_sessions="All", delay=0,
                                    error_evidence=lambda *args: failures.append(args))
    if persistent:
        with pytest.raises(NoQuote if error_kind == "no_quote" else TimeoutError):
            await operation()
        assert len(attempts) == 3 and len(failures) == 3
    else:
        assert await operation() == [candle]
        assert len(attempts) == 2 and len(failures) == 1
    assert all(anchor == start for anchor in attempts)
    assert all(record[2] == (301603 if error_kind == "no_quote" else None) for record in failures)


@pytest.mark.asyncio
async def test_wrapped_connect_error_retries_same_anchor():
    start = datetime(2026, 10, 26, 10, 0, tzinfo=NY)
    candle = SimpleNamespace(timestamp=start, open="100", high="100", low="100", close="100", volume=1)
    attempts, failures = [], []

    class OpenApiConnectError(Exception):
        pass

    class Quote:
        async def history_candlesticks_by_offset(self, *args, **kwargs):
            attempts.append(kwargs["time"])
            if len(attempts) == 1:
                raise OpenApiConnectError(
                    "OpenApiException: error sending request for url "
                    "https://openapi.longbridge.com/v1/socket/token: client error (Connect)"
                )
            return [candle]

    bars = await history_candles(
        Quote(), "AAPL.US", Period.Min_1, AdjustType.NoAdjust,
        int(start.timestamp()) * 10**9, int(start.timestamp()) * 10**9,
        trade_sessions="All", delay=0,
        error_evidence=lambda *args: failures.append(args),
    )
    assert bars == [candle]
    assert attempts == [start, start]
    assert len(failures) == 1 and failures[0][2] is None

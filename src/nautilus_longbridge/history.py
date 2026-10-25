"""Range-aware history belongs to the NT adapter, never to a research client."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from .config import market_for_symbol
from .mapping import timestamp_ns

NY = ZoneInfo("America/New_York")


def market_timezone(symbol: str) -> ZoneInfo:
    return ZoneInfo(
        {
            "US": "America/New_York",
            "HK": "Asia/Hong_Kong",
            "SH": "Asia/Shanghai",
            "SZ": "Asia/Shanghai",
        }[market_for_symbol(symbol)]
    )


async def history_candles(
    quote: Any,
    symbol: str,
    period: Any,
    adjustment: Any,
    start_ns: int,
    end_ns: int,
    *,
    call: Callable[..., Awaitable[Any]] | None = None,
    evidence: Callable[..., None] | None = None,
    delay: float = 0.6,
) -> list[Any]:
    from longbridge.openapi import TradeSessions

    if start_ns > end_ns:
        raise ValueError("historical range start exceeds end")
    # Floats round 23:59:59.999999999 up to the next calendar day.
    zone = market_timezone(symbol)
    first = datetime.fromtimestamp(start_ns // 1_000_000_000, zone).date()
    last = datetime.fromtimestamp(end_ns // 1_000_000_000, zone).date()
    period_name = str(period).split(".")[-1]
    span = {"Min_1": 1, "Min_60": 100, "Day": 730}.get(period_name)
    if span is None:
        raise ValueError("unsupported history period")
    rows, seen = [], set()
    cursor = first
    while cursor <= last:
        chunk_end = min(cursor + timedelta(days=span - 1), last)

        async def operation(first=cursor, last=chunk_end):
            return await quote.history_candlesticks_by_date(
                symbol,
                period,
                adjustment,
                start=first,
                end=last,
                trade_sessions=TradeSessions.Intraday,
            )

        values = (
            await call(operation) if call is not None else await asyncio.wait_for(operation(), 30)
        )
        if evidence is not None:
            evidence(cursor, chunk_end, values)
        if len(values) >= 1000:
            raise ValueError("history request may be truncated at the provider's 1000-row cap")
        previous = None
        for candle in values:
            stamp = timestamp_ns(candle.timestamp, 0)
            day = datetime.fromtimestamp(stamp // 1_000_000_000, zone).date()
            if not cursor <= day <= chunk_end or previous is not None and stamp < previous:
                raise ValueError("source history boundary/order violation")
            previous = stamp
            if not start_ns <= stamp <= end_ns:
                continue
            if stamp in seen:
                raise ValueError("duplicate history timestamp; source rows are not overwritten")
            seen.add(stamp)
            prices = [
                Decimal(str(getattr(candle, key))) for key in ("open", "high", "low", "close")
            ]
            volume = Decimal(str(candle.volume))
            if (
                any(not value.is_finite() or value <= 0 for value in prices)
                or not volume.is_finite()
                or volume < 0
                or prices[2] > min(prices[0], prices[3])
                or prices[1] < max(prices[0], prices[3])
            ):
                raise ValueError("invalid historical OHLCV")
            rows.append(candle)
        cursor = chunk_end + timedelta(days=1)
        await asyncio.sleep(delay)
    return rows

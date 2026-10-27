"""Range-aware history belongs to the NT adapter, never to a research client."""

from __future__ import annotations

import asyncio
import re
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
    error_evidence: Callable[..., None] | None = None,
    delay: float = 0.6,
    trade_sessions: str = "Intraday",
) -> list[Any]:
    from longbridge.openapi import TradeSessions

    if start_ns > end_ns:
        raise ValueError("historical range start exceeds end")
    if trade_sessions not in {"Intraday", "All"}:
        raise ValueError("unsupported historical trade sessions")
    # Floats round 23:59:59.999999999 up to the next calendar day.
    zone = market_timezone(symbol)
    first = datetime.fromtimestamp(start_ns // 1_000_000_000, zone).date()
    last = datetime.fromtimestamp(end_ns // 1_000_000_000, zone).date()
    period_name = str(period).split(".")[-1]
    span = {"Min_1": 1, "Min_60": 100, "Day": 730}.get(period_name)
    if span is None:
        raise ValueError("unsupported history period")
    if trade_sessions == "All" and period_name == "Min_1":
        # A full civil day can exceed the date endpoint's 1,000-row cap.
        # Page backwards with an explicit anchor instead of truncating a day.
        return await _all_minute_history(
            quote, symbol, period, adjustment, start_ns, end_ns,
            call=call, evidence=evidence, error_evidence=error_evidence, delay=delay,
        )
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
                trade_sessions=getattr(TradeSessions, trade_sessions),
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


async def _all_minute_history(
    quote: Any, symbol: str, period: Any, adjustment: Any, start_ns: int, end_ns: int,
    *, call: Callable[..., Awaitable[Any]] | None,
    evidence: Callable[..., None] | None, delay: float,
    error_evidence: Callable[..., None] | None = None,
) -> list[Any]:
    from longbridge.openapi import TradeSessions

    zone = market_timezone(symbol)
    cursor_ns = end_ns
    rows: dict[int, Any] = {}
    while cursor_ns >= start_ns:
        anchor = datetime.fromtimestamp(cursor_ns // 1_000_000_000, zone)

        async def operation(anchor=anchor):
            return await quote.history_candlesticks_by_offset(
                symbol, period, adjustment, False, 1000,
                time=anchor, trade_sessions=TradeSessions.All,
            )

        for attempt in range(3):
            try:
                values = await call(operation) if call is not None else await asyncio.wait_for(operation(), 30)
                break
            except Exception as exc:
                match = re.search(r"\bcode=(\d+)\b", str(exc))
                code = getattr(exc, "code", None)
                if not isinstance(code, int):
                    code = int(match.group(1)) if match else None
                if error_evidence is not None:
                    error_evidence(anchor, attempt + 1, code, type(exc).__name__)
                # The exact same anchor has been observed to succeed after a
                # 301603 response and transient transport failures. The SDK
                # can wrap a socket failure in OpenApiException, so classify
                # only its explicit transport wording as retryable. Bounded
                # retries never turn absent data into an empty page, move the
                # anchor, or suppress a persistent error.
                message = str(exc).lower()
                wrapped_transport_failure = any(
                    marker in message
                    for marker in (
                        "error sending request",
                        "client error (connect)",
                        "connection reset",
                        "connection refused",
                    )
                )
                if (
                    code != 301603
                    and not isinstance(exc, (TimeoutError, ConnectionError))
                    and not wrapped_transport_failure
                ) or attempt == 2:
                    raise
                await asyncio.sleep(max(delay, .5) * 2 ** attempt)
        if evidence is not None:
            evidence(datetime.fromtimestamp(start_ns // 1_000_000_000, zone).date(),
                     anchor.date(), values)
        if not values:
            break
        if len(values) > 1000:
            raise ValueError("history page exceeds the requested size")
        stamps = [timestamp_ns(candle.timestamp, 0) for candle in values]
        if stamps != sorted(stamps) or len(stamps) != len(set(stamps)):
            raise ValueError("duplicate or unordered history page")
        if stamps[-1] > cursor_ns:
            raise ValueError("history page exceeds its request anchor")
        for stamp, candle in zip(stamps, values, strict=True):
            # Validate and retain all responses in evidence before selecting.
            prices = [Decimal(str(getattr(candle, key))) for key in ("open", "high", "low", "close")]
            volume = Decimal(str(candle.volume))
            if (any(not value.is_finite() or value <= 0 for value in prices)
                    or not volume.is_finite() or volume < 0
                    or prices[2] > min(prices[0], prices[3])
                    or prices[1] < max(prices[0], prices[3])):
                raise ValueError("invalid historical OHLCV")
            if start_ns <= stamp <= end_ns:
                if stamp in rows:
                    raise ValueError("duplicate history timestamp; source rows are not overwritten")
                rows[stamp] = candle
        # Minute bars start at whole seconds. Subtract a second so inclusive
        # SDK endpoints cannot repeat the oldest bar on the next page.
        next_cursor = stamps[0] - 1_000_000_000
        if next_cursor >= cursor_ns:
            raise ValueError("history pagination made no progress")
        cursor_ns = next_cursor
        if cursor_ns >= start_ns:
            await asyncio.sleep(delay)
    return [rows[stamp] for stamp in sorted(rows)]

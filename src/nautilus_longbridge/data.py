from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import random
from collections.abc import Awaitable, Callable
from functools import partial
from types import SimpleNamespace
from typing import Any

from nautilus_trader.live import BarsResponse, QuotesResponse, TradesResponse
from nautilus_trader.live.clients import DataClient
from nautilus_trader.model import (
    AggregationSource,
    Bar,
    BarAggregation,
    BarSpecification,
    BarType,
    BookOrder,
    InstrumentId,
    OrderBookDepth,
    OrderSide,
    PriceType,
    QuoteTick,
    TradeId,
    TradeTick,
)

from .config import LongbridgeDataClientConfig
from .dedup import TimedDeduplicator
from .instruments import LongbridgeInstrumentProvider
from .lifecycle import ConnectionState
from .mapping import aggressor_side, price, quantity, timestamp_ns
from .rate_limit import ConcurrencyLimiter, SlidingWindowRateLimiter
from .symbols import instrument_id_from_symbol, symbol_from_instrument_id
from .transport import ContextFactory, LongbridgeContextFactory, QuoteContext

logger = logging.getLogger(__name__)


def _sdk_enum(name: str, fallback: Any = None) -> Any:
    from longbridge import openapi

    return getattr(openapi, name, fallback)


def _period_for_bar_type(bar_type: BarType) -> Any:
    name = str(bar_type.spec).upper()
    if name.startswith("1-MINUTE-"):
        return _sdk_enum("Period", {}).Min_1
    if name.startswith("1-HOUR-"):
        return _sdk_enum("Period", {}).Hour_1
    if name.startswith("1-DAY-"):
        return _sdk_enum("Period", {}).Day
    raise ValueError(f"unsupported Longbridge bar period: {bar_type.spec}")


def _bar_type(instrument_id: InstrumentId, period_name: str) -> BarType:
    if "MIN" in period_name.upper():
        spec = BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST)
    elif "HOUR" in period_name.upper():
        spec = BarSpecification(1, BarAggregation.HOUR, PriceType.LAST)
    else:
        spec = BarSpecification(1, BarAggregation.DAY, PriceType.LAST)
    return BarType(instrument_id, spec, AggregationSource.EXTERNAL)


class LongbridgeDataClient(DataClient):
    """Async market-data adapter backed by Longbridge ``AsyncQuoteContext``."""

    def __init__(
        self,
        *,
        config: LongbridgeDataClientConfig,
        context_factory: ContextFactory | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(config=config, **kwargs)
        self.config = config
        self._lb_config = config
        self._context_factory = context_factory or LongbridgeContextFactory()
        self._quote: QuoteContext | None = None
        self._queue: asyncio.Queue[tuple[str, str, Any, int]] | None = None
        self._consumer: asyncio.Task[Any] | None = None
        self._health_task: asyncio.Task[Any] | None = None
        self._desired_quotes: set[str] = set()
        self._desired_trades: set[str] = set()
        self._desired_bars: dict[str, Any] = {}
        self._desired_depth: set[str] = set()
        self._last_quotes: dict[str, Any] = {}
        self._last_depth: dict[str, Any] = {}
        self._depth_received_ns: dict[str, int] = {}
        self._deduplicator = TimedDeduplicator()
        self._quote_rate = SlidingWindowRateLimiter(10, 1.0)
        self._quote_concurrency = ConcurrencyLimiter(5)
        self._reconnect_lock = asyncio.Lock()
        self._reconnect_task: asyncio.Task[Any] | None = None
        self._connection_state = ConnectionState.DISCONNECTED
        self._disconnect_started_ns: int | None = None

    @property
    def quote_context(self) -> QuoteContext | None:
        return self._quote

    @property
    def connection_state(self) -> ConnectionState:
        return self._connection_state

    async def _connect(self) -> None:
        self._connection_state = ConnectionState.CONNECTING
        try:
            loop = asyncio.get_running_loop()
            self._queue = asyncio.Queue(maxsize=self._lb_config.settings.queue_size)
            self._quote = self._context_factory.quote(self._lb_config.settings, loop)
            self._configure_quote_context(self._quote)
            self._consumer = self.create_task(self._consume(), "market-data-consumer")
            self._health_task = self.create_task(self._health_loop(), "market-data-health")
            await self._restore_subscriptions()
            self._connection_state = ConnectionState.CONNECTED
        except BaseException:
            self._connection_state = ConnectionState.FAILED
            raise

    async def _disconnect(self) -> None:
        reconnect_task = getattr(self, "_reconnect_task", None)
        self._reconnect_task = None
        if reconnect_task is not None and not reconnect_task.done():
            reconnect_task.cancel()
            await asyncio.gather(reconnect_task, return_exceptions=True)
        self._connection_state = ConnectionState.DISCONNECTED
        self._disconnect_started_ns = None
        getattr(self, "_last_depth", {}).clear()
        getattr(self, "_depth_received_ns", {}).clear()
        quote = self._quote
        if quote is not None:
            from longbridge.openapi import SubType

            try:
                if self._desired_quotes:
                    await self._quote_call(
                        lambda: quote.unsubscribe(
                            sorted(self._desired_quotes), [SubType.Quote, SubType.Depth]
                        )
                    )
                if self._desired_trades:
                    await self._quote_call(
                        lambda: quote.unsubscribe(sorted(self._desired_trades), [SubType.Trade])
                    )
                for symbol, period in self._desired_bars.items():
                    await self._quote_call(partial(quote.unsubscribe_candlesticks, symbol, period))
            except Exception:
                logger.debug("Longbridge quote unsubscribe during disconnect failed", exc_info=True)
            self._clear_quote_callbacks(quote)
        if isinstance(self.instrument_provider, LongbridgeInstrumentProvider):
            self.instrument_provider.set_quote_context(None)
        for task in (self._health_task, self._consumer):
            if task is not None:
                task.cancel()
        tasks = [task for task in (self._health_task, self._consumer) if task is not None]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._health_task = None
        self._consumer = None
        self._queue = None
        self._quote = None

    async def _health_loop(self) -> None:
        while True:
            await asyncio.sleep(max(5.0, self._lb_config.settings.reconnect_initial_secs))
            if self._connection_state is ConnectionState.CONNECTED and self._quote is not None:
                try:
                    await self._quote_call(self._quote.subscriptions)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.warning("Longbridge quote health check failed; scheduling reconnect")
                    self._disconnect_started_ns = self.clock.timestamp_ns()
                    self._start_reconnect()

    def _configure_quote_context(self, quote: QuoteContext) -> None:
        quote.set_on_quote(self._on_quote)
        quote.set_on_depth(self._on_depth)
        quote.set_on_trades(self._on_trades)
        quote.set_on_candlestick(self._on_candlestick)
        if isinstance(self.instrument_provider, LongbridgeInstrumentProvider):
            self.instrument_provider.set_quote_context(quote)

    def _start_reconnect(self) -> asyncio.Task[Any] | None:
        existing = self._reconnect_task
        if existing is not None and not existing.done():
            return existing
        self._connection_state = ConnectionState.RECONNECTING
        self._depth_epoch_ns = self.clock.timestamp_ns()
        getattr(self, "_last_depth", {}).clear()
        getattr(self, "_depth_received_ns", {}).clear()
        task = asyncio.create_task(self._reconnect_loop(), name="longbridge-quote-reconnect")
        self._reconnect_task = task

        def clear(completed: asyncio.Task[Any]) -> None:
            if self._reconnect_task is completed:
                self._reconnect_task = None

        task.add_done_callback(clear)
        return task

    async def _reconnect_with_backoff(self) -> None:
        task = self._start_reconnect()
        if task is not None:
            await task

    async def _reconnect_loop(self) -> None:
        delay = self._lb_config.settings.reconnect_initial_secs
        start_ns = self._disconnect_started_ns or self.clock.timestamp_ns()
        max_attempts = self._lb_config.settings.reconnect_max_attempts
        for attempt in range(max_attempts):
            try:
                async with self._reconnect_lock:
                    old_quote = self._quote
                    if old_quote is not None:
                        self._clear_quote_callbacks(old_quote)
                    loop = asyncio.get_running_loop()
                    quote = self._context_factory.quote(self._lb_config.settings, loop)
                    self._configure_quote_context(quote)
                    self._quote = quote
                    await self._restore_subscriptions()
                    reconnect_ns = self.clock.timestamp_ns()
                    await self._backfill_gap(start_ns, reconnect_ns)
                self._disconnect_started_ns = None
                self._connection_state = ConnectionState.CONNECTED
                logger.info("Longbridge quote connection restored")
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Longbridge quote reconnect attempt %d/%d failed (%s); retrying in %.1fs",
                    attempt + 1,
                    max_attempts,
                    type(exc).__name__,
                    delay,
                )
                if attempt + 1 < max_attempts:
                    await asyncio.sleep(delay * random.uniform(0.8, 1.2))
                    delay = min(self._lb_config.settings.reconnect_max_secs, delay * 2)
        self._connection_state = ConnectionState.FAILED
        logger.error("Longbridge quote reconnection failed after %d attempts", max_attempts)

    @staticmethod
    def _clear_quote_callbacks(quote: QuoteContext) -> None:
        for setter in (
            quote.set_on_quote,
            quote.set_on_depth,
            quote.set_on_trades,
            quote.set_on_candlestick,
        ):
            try:
                setter(lambda *args: None)
            except Exception:
                logger.debug("Longbridge quote callback cleanup failed", exc_info=True)

    async def _backfill_gap(self, start_ns: int, end_ns: int) -> None:
        """Backfill trades during a disconnect, using the same dedup path as pushes."""
        if end_ns <= start_ns or self._quote is None:
            return
        gap_seconds = (end_ns - start_ns) / 1_000_000_000
        logger.warning(
            "Longbridge quote connection gap %.3fs; backfilling trade subscriptions",
            gap_seconds,
        )
        quote = self._quote
        if quote is None:
            return
        for symbol in sorted(self._desired_trades):
            try:

                async def fetch_trades(symbol: str = symbol) -> list[Any]:
                    return await quote.trades(symbol, 1000)

                values = await self._quote_call(fetch_trades)
                missing: list[Any] = []
                for item in values:
                    value = getattr(item, "timestamp", None)
                    if value is None:
                        continue
                    item_ns = timestamp_ns(value, end_ns)
                    if start_ns <= item_ns <= end_ns:
                        missing.append(item)
                if missing:
                    await self._publish_trades(symbol, SimpleNamespace(trades=missing))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "Longbridge trade backfill failed for %s: %s", symbol, type(exc).__name__
                )
        if self._desired_quotes:
            logger.warning(
                "Longbridge quote snapshots cannot be backfilled with an event-time range; "
                "live quote/depth snapshots will resume after reconnect"
            )
        if self._desired_bars:
            logger.warning(
                "Longbridge candlestick gap cannot be backfilled safely without a completed-bar "
                "boundary; live candlesticks will resume after reconnect"
            )

    async def _restore_subscriptions(self) -> None:
        quote = self._quote
        if quote is None:
            return
        from longbridge.openapi import SubType, TradeSessions

        if self._desired_quotes:
            await self._quote_call(
                lambda: quote.subscribe(
                    sorted(self._desired_quotes), [SubType.Quote, SubType.Depth]
                )
            )
        if self._desired_trades:
            await self._quote_call(
                lambda: quote.subscribe(sorted(self._desired_trades), [SubType.Trade])
            )
        for symbol, period in self._desired_bars.items():
            await self._quote_call(
                partial(quote.subscribe_candlesticks, symbol, period, TradeSessions.Intraday)
            )

    async def _quote_call(self, operation: Callable[[], Awaitable[Any]]) -> Any:
        if not hasattr(self, "_quote_rate"):
            self._quote_rate = SlidingWindowRateLimiter(10, 1.0)
            self._quote_concurrency = ConcurrencyLimiter(5)
        await self._quote_rate.acquire()
        return await self._quote_concurrency.run(
            lambda: asyncio.wait_for(operation(), self._lb_config.settings.request_timeout_secs)
        )

    def _enqueue(self, kind: str, symbol: str, event: Any) -> None:
        if self._queue is None:
            return
        try:
            self._queue.put_nowait((kind, symbol, event, self.clock.timestamp_ns()))
        except asyncio.QueueFull:
            logger.warning(
                "Longbridge market-data queue is full; dropping %s event for %s", kind, symbol
            )

    async def _on_quote(self, symbol: str, event: Any) -> None:
        self._enqueue("quote", symbol, event)

    async def _on_depth(self, symbol: str, event: Any) -> None:
        self._enqueue("depth", symbol, event)

    async def _on_trades(self, symbol: str, event: Any) -> None:
        self._enqueue("trades", symbol, event)

    async def _on_candlestick(self, symbol: str, event: Any) -> None:
        self._enqueue("bar", symbol, event)

    async def _consume(self) -> None:
        assert self._queue is not None
        while True:
            kind, symbol, event, received_ns = await self._queue.get()
            try:
                if kind == "quote":
                    await self._publish_quote(symbol, event)
                elif kind == "depth":
                    await self._publish_depth(symbol, event, received_ns=received_ns)
                elif kind == "trades":
                    await self._publish_trades(symbol, event)
                elif kind == "bar":
                    await self._publish_bar(symbol, event)
            finally:
                self._queue.task_done()

    def _dedup(self, key: str) -> bool:
        if not hasattr(self, "_deduplicator"):
            self._deduplicator = TimedDeduplicator()
        return self._deduplicator.add(key)

    @staticmethod
    def _trade_key(symbol: str, item: Any, ts_event: int) -> str:
        return "|".join(
            (
                symbol,
                str(ts_event),
                str(getattr(item, "price", "")),
                str(getattr(item, "volume", "")),
                str(getattr(item, "direction", "")),
                str(getattr(item, "trade_type", "")),
            )
        )

    @classmethod
    def _trade_digest(cls, symbol: str, item: Any, ts_event: int) -> str:
        # TradeId is limited to 36 characters by NautilusTrader. The complete
        # un-hashed key is still used for deduplication; this is a full
        # 128-bit digest rather than a truncated SHA-1 prefix for the ID.
        return hashlib.blake2b(
            cls._trade_key(symbol, item, ts_event).encode(), digest_size=16
        ).hexdigest()

    async def _publish_quote(self, symbol: str, event: Any) -> None:
        self._last_quotes[symbol] = event
        depth = self._last_depth.get(symbol)
        instrument_id = await self._ensure_instrument(symbol)
        tick = self._make_quote_tick(instrument_id, symbol, event, depth)
        if tick is not None:
            self._handle_data(tick)

    def _make_quote_tick(
        self, instrument_id: InstrumentId, symbol: str, event: Any, depth: Any
    ) -> QuoteTick | None:
        if depth is None or not getattr(depth, "bids", None) or not getattr(depth, "asks", None):
            return None
        bid = depth.bids[0]
        ask = depth.asks[0]
        if getattr(bid, "price", None) is None or getattr(ask, "price", None) is None:
            return None
        if not all(math.isfinite(float(v)) and float(v) > 0 for v in (bid.price, ask.price, bid.volume, ask.volume)) or float(bid.price) > float(ask.price):
            return None
        # Depth is the source of BBO. Quote.last_done must never refresh an
        # old depth snapshot's timestamp. SDK PushDepth has no exchange stamp,
        # so its local receipt time is an explicitly approximate event time.
        ts_event = getattr(self, "_depth_received_ns", {}).get(symbol)
        if ts_event is None:
            return None
        if not 0 <= self.clock.timestamp_ns() - ts_event <= self._lb_config.max_depth_age_secs * 1e9:
            return None
        key = f"quote:{symbol}:{ts_event}:{bid.price}:{ask.price}:{bid.volume}:{ask.volume}"
        if not self._dedup(key):
            return None
        return QuoteTick(
            instrument_id,
            price(bid.price, self._lb_config.price_precision),
            price(ask.price, self._lb_config.price_precision),
            quantity(getattr(bid, "volume", 0)),
            quantity(getattr(ask, "volume", 0)),
            ts_event,
            self.clock.timestamp_ns(),
        )

    async def _publish_depth(
        self, symbol: str, event: Any, *, received_ns: int | None = None
    ) -> None:
        if received_ns is not None and received_ns < getattr(self, "_depth_epoch_ns", 0):
            return
        self._last_depth[symbol] = event
        if not hasattr(self, "_depth_received_ns"):
            self._depth_received_ns = {}
        self._depth_received_ns[symbol] = (
            self.clock.timestamp_ns() if received_ns is None else received_ns
        )
        ts_event = timestamp_ns(getattr(event, "timestamp", None), self.clock.timestamp_ns())
        instrument_id = await self._ensure_instrument(symbol)
        bids = [
            row for row in getattr(event, "bids", []) if getattr(row, "price", None) is not None
        ]
        asks = [
            row for row in getattr(event, "asks", []) if getattr(row, "price", None) is not None
        ]
        key = (
            f"depth:{symbol}:{ts_event}:{[(str(x.price), x.volume) for x in bids]}:"
            f"{[(str(x.price), x.volume) for x in asks]}"
        )
        if not self._dedup(key):
            return
        self._handle_data(
            OrderBookDepth(
                instrument_id,
                [
                    BookOrder(
                        OrderSide.BUY,
                        price(row.price, self._lb_config.price_precision),
                        quantity(row.volume),
                        int(row.position),
                    )
                    for row in bids
                ],
                [
                    BookOrder(
                        OrderSide.SELL,
                        price(row.price, self._lb_config.price_precision),
                        quantity(row.volume),
                        int(row.position),
                    )
                    for row in asks
                ],
                [int(row.order_num) for row in bids],
                [int(row.order_num) for row in asks],
                0,
                0,
                ts_event,
                self.clock.timestamp_ns(),
            )
        )
        if symbol in getattr(self, "_desired_quotes", set()) or symbol in self._last_quotes:
            tick = self._make_quote_tick(
                instrument_id, symbol, self._last_quotes.get(symbol), event
            )
            if tick is not None:
                self._handle_data(tick)

    async def _publish_trades(self, symbol: str, event: Any) -> None:
        instrument_id = await self._ensure_instrument(symbol)
        fallback_ns = self.clock.timestamp_ns()
        items = sorted(
            getattr(event, "trades", []),
            key=lambda item: timestamp_ns(getattr(item, "timestamp", None), fallback_ns),
        )
        for item in items:
            ts_event = timestamp_ns(getattr(item, "timestamp", None), fallback_ns)
            digest = self._trade_digest(symbol, item, ts_event)
            if not self._dedup(f"trade:{self._trade_key(symbol, item, ts_event)}"):
                continue
            self._handle_data(
                TradeTick(
                    instrument_id,
                    price(item.price, self._lb_config.price_precision),
                    quantity(item.volume),
                    aggressor_side(getattr(item, "direction", None)),
                    TradeId(digest),
                    ts_event,
                    self.clock.timestamp_ns(),
                )
            )

    async def _publish_bar(self, symbol: str, event: Any) -> None:
        if self._lb_config.completed_bars_only and not getattr(event, "is_confirmed", False):
            return
        candle = getattr(event, "candlestick", event)
        period_name = str(getattr(getattr(event, "period", None), "name", "Min_1"))
        instrument_id = await self._ensure_instrument(symbol)
        bar_type = _bar_type(instrument_id, period_name)
        ts_event = timestamp_ns(getattr(candle, "timestamp", None), self.clock.timestamp_ns())
        key = f"bar:{symbol}:{period_name}:{ts_event}:{candle.close}:{candle.volume}"
        if not self._dedup(key):
            return
        self._handle_data(
            Bar(
                bar_type,
                price(candle.open, self._lb_config.price_precision),
                price(candle.high, self._lb_config.price_precision),
                price(candle.low, self._lb_config.price_precision),
                price(candle.close, self._lb_config.price_precision),
                quantity(candle.volume),
                ts_event,
                self.clock.timestamp_ns(),
            )
        )

    async def _ensure_instrument(self, symbol: str) -> InstrumentId:
        instrument_id = instrument_id_from_symbol(symbol)
        provider = self.instrument_provider
        if (
            isinstance(provider, LongbridgeInstrumentProvider)
            and provider.find(instrument_id) is None
        ):
            await provider.load_symbols_async([symbol])
            instrument = provider.find(instrument_id)
            if instrument is not None:
                self._handle_instrument(instrument)
        return instrument_id

    async def _subscribe_quotes(self, command: Any) -> None:
        symbol = symbol_from_instrument_id(command.instrument_id)
        self._desired_quotes.add(symbol)
        self._desired_depth.add(symbol)
        quote = self._quote
        if quote is not None:
            from longbridge.openapi import SubType

            await self._quote_call(
                lambda: quote.subscribe([symbol], [SubType.Quote, SubType.Depth])
            )

    async def _unsubscribe_quotes(self, command: Any) -> None:
        symbol = symbol_from_instrument_id(command.instrument_id)
        self._desired_quotes.discard(symbol)
        self._desired_depth.discard(symbol)
        quote = self._quote
        if quote is not None:
            from longbridge.openapi import SubType

            await self._quote_call(
                lambda: quote.unsubscribe([symbol], [SubType.Quote, SubType.Depth])
            )

    async def _subscribe_trades(self, command: Any) -> None:
        symbol = symbol_from_instrument_id(command.instrument_id)
        self._desired_trades.add(symbol)
        quote = self._quote
        if quote is not None:
            from longbridge.openapi import SubType

            await self._quote_call(lambda: quote.subscribe([symbol], [SubType.Trade]))

    async def _unsubscribe_trades(self, command: Any) -> None:
        symbol = symbol_from_instrument_id(command.instrument_id)
        self._desired_trades.discard(symbol)
        quote = self._quote
        if quote is not None:
            from longbridge.openapi import SubType

            await self._quote_call(lambda: quote.unsubscribe([symbol], [SubType.Trade]))

    async def _subscribe_bars(self, command: Any) -> None:
        symbol = symbol_from_instrument_id(command.bar_type.instrument_id)
        period = _period_for_bar_type(command.bar_type)
        self._desired_bars[symbol] = period
        quote = self._quote
        if quote is not None:
            from longbridge.openapi import TradeSessions

            await self._quote_call(
                lambda: quote.subscribe_candlesticks(symbol, period, TradeSessions.Intraday)
            )

    async def _unsubscribe_bars(self, command: Any) -> None:
        symbol = symbol_from_instrument_id(command.bar_type.instrument_id)
        period = self._desired_bars.pop(symbol, _period_for_bar_type(command.bar_type))
        quote = self._quote
        if quote is not None:
            await self._quote_call(lambda: quote.unsubscribe_candlesticks(symbol, period))

    async def _request_quotes(self, request: Any) -> None:
        quote = self._quote
        if quote is None:
            return
        symbol = symbol_from_instrument_id(request.instrument_id)
        values = await self._quote_call(lambda: quote.quote([symbol]))
        data: list[Any] = []
        for value in values:
            self._last_quotes[symbol] = value
            depth = await self._quote_call(lambda: quote.depth(symbol))
            self._last_depth[symbol] = depth
            if not hasattr(self, "_depth_received_ns"):
                self._depth_received_ns = {}
            self._depth_received_ns[symbol] = self.clock.timestamp_ns()
            tick = self._make_quote_tick(request.instrument_id, symbol, value, depth)
            if tick is not None:
                data.append(tick)
        self._handle_response(
            QuotesResponse(
                self.client_id,
                request.instrument_id,
                data,
                request.request_id,
                self.clock.timestamp_ns(),
                request.start_ns,
                request.end_ns,
            )
        )

    async def _request_trades(self, request: Any) -> None:
        quote = self._quote
        if quote is None:
            return
        symbol = symbol_from_instrument_id(request.instrument_id)
        values = await self._quote_call(lambda: quote.trades(symbol, request.limit or 1000))
        data: list[Any] = []
        for item in values:
            ts_event = timestamp_ns(getattr(item, "timestamp", None), self.clock.timestamp_ns())
            digest = self._trade_digest(symbol, item, ts_event)
            data.append(
                TradeTick(
                    request.instrument_id,
                    price(item.price, self._lb_config.price_precision),
                    quantity(item.volume),
                    aggressor_side(getattr(item, "direction", None)),
                    TradeId(digest),
                    ts_event,
                    self.clock.timestamp_ns(),
                )
            )
        self._handle_response(
            TradesResponse(
                self.client_id,
                request.instrument_id,
                data,
                request.request_id,
                self.clock.timestamp_ns(),
                request.start_ns,
                request.end_ns,
            )
        )

    async def _request_bars(self, request: Any) -> None:
        quote = self._quote
        if quote is None:
            return
        symbol = symbol_from_instrument_id(request.bar_type.instrument_id)
        from longbridge.openapi import AdjustType, TradeSessions

        period = _period_for_bar_type(request.bar_type)
        values = await self._quote_call(
            lambda: quote.candlesticks(
                symbol,
                period,
                request.limit or 1000,
                AdjustType.NoAdjust,
                TradeSessions.Intraday,
            )
        )
        data: list[Any] = []
        for candle in values:
            ts_event = timestamp_ns(getattr(candle, "timestamp", None), self.clock.timestamp_ns())
            data.append(
                Bar(
                    request.bar_type,
                    price(candle.open, self._lb_config.price_precision),
                    price(candle.high, self._lb_config.price_precision),
                    price(candle.low, self._lb_config.price_precision),
                    price(candle.close, self._lb_config.price_precision),
                    quantity(candle.volume),
                    ts_event,
                    self.clock.timestamp_ns(),
                )
            )
        self._handle_response(
            BarsResponse(
                self.client_id,
                request.bar_type,
                data,
                request.request_id,
                self.clock.timestamp_ns(),
                request.start_ns,
                request.end_ns,
            )
        )

    async def _request_book_snapshot(self, request: Any) -> None:
        quote = self._quote
        if quote is None:
            return
        symbol = symbol_from_instrument_id(request.instrument_id)
        value = await self._quote_call(lambda: quote.depth(symbol))
        await self._publish_depth(symbol, value)

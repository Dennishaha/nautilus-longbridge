from __future__ import annotations

import asyncio
import hashlib
import logging
import random
from collections import deque
from collections.abc import Awaitable, Callable
from functools import partial
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
from .instruments import LongbridgeInstrumentProvider
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
    if "MIN" in name:
        return _sdk_enum("Period", {}).Min_1
    if "HOUR" in name:
        return _sdk_enum("Period", {}).Hour_1
    if "DAY" in name:
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
        self._queue: asyncio.Queue[tuple[str, str, Any]] | None = None
        self._consumer: asyncio.Task[Any] | None = None
        self._health_task: asyncio.Task[Any] | None = None
        self._desired_quotes: set[str] = set()
        self._desired_trades: set[str] = set()
        self._desired_bars: dict[str, Any] = {}
        self._desired_depth: set[str] = set()
        self._last_quotes: dict[str, Any] = {}
        self._last_depth: dict[str, Any] = {}
        self._seen: deque[str] = deque(maxlen=20_000)
        self._seen_set: set[str] = set()
        self._quote_rate = SlidingWindowRateLimiter(10, 1.0)
        self._quote_concurrency = ConcurrencyLimiter(5)
        self._reconnect_lock = asyncio.Lock()

    @property
    def quote_context(self) -> QuoteContext | None:
        return self._quote

    async def _connect(self) -> None:
        loop = asyncio.get_running_loop()
        self._queue = asyncio.Queue(maxsize=self._lb_config.settings.queue_size)
        self._quote = self._context_factory.quote(self._lb_config.settings, loop)
        self._configure_quote_context(self._quote)
        self._consumer = self.create_task(self._consume(), "market-data-consumer")
        self._health_task = self.create_task(self._health_loop(), "market-data-health")
        await self._restore_subscriptions()

    async def _disconnect(self) -> None:
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
                    await self._quote_call(
                        partial(quote.unsubscribe_candlesticks, symbol, period)
                    )
            except Exception:
                logger.debug("Longbridge quote unsubscribe during disconnect failed", exc_info=True)
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
            if self._quote is not None:
                try:
                    await self._quote_call(self._quote.subscriptions)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.warning(
                        "Longbridge quote health check failed; SDK context will be recreated by node lifecycle"
                    )
                    await self._reconnect_with_backoff()

    def _configure_quote_context(self, quote: QuoteContext) -> None:
        quote.set_on_quote(self._on_quote)
        quote.set_on_depth(self._on_depth)
        quote.set_on_trades(self._on_trades)
        quote.set_on_candlestick(self._on_candlestick)
        if isinstance(self.instrument_provider, LongbridgeInstrumentProvider):
            self.instrument_provider.set_quote_context(quote)

    async def _reconnect_with_backoff(self) -> None:
        delay = self._lb_config.settings.reconnect_initial_secs
        while True:
            try:
                async with self._reconnect_lock:
                    loop = asyncio.get_running_loop()
                    quote = self._context_factory.quote(self._lb_config.settings, loop)
                    self._configure_quote_context(quote)
                    self._quote = quote
                    await self._restore_subscriptions()
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Longbridge quote reconnect failed (%s); retrying in %.1fs",
                    type(exc).__name__,
                    delay,
                )
                await asyncio.sleep(delay * random.uniform(0.8, 1.2))
                delay = min(self._lb_config.settings.reconnect_max_secs, delay * 2)

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
            self._queue.put_nowait((kind, symbol, event))
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
            kind, symbol, event = await self._queue.get()
            try:
                if kind == "quote":
                    await self._publish_quote(symbol, event)
                elif kind == "depth":
                    await self._publish_depth(symbol, event)
                elif kind == "trades":
                    await self._publish_trades(symbol, event)
                elif kind == "bar":
                    await self._publish_bar(symbol, event)
            finally:
                self._queue.task_done()

    def _dedup(self, key: str) -> bool:
        if key in self._seen_set:
            return False
        if len(self._seen) == self._seen.maxlen:
            self._seen_set.discard(self._seen[0])
        self._seen.append(key)
        self._seen_set.add(key)
        return True

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
        ts_event = timestamp_ns(getattr(event, "timestamp", None), self.clock.timestamp_ns())
        key = f"quote:{symbol}:{ts_event}:{bid.price}:{ask.price}:{getattr(event, 'last_done', '')}"
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

    async def _publish_depth(self, symbol: str, event: Any) -> None:
        self._last_depth[symbol] = event
        ts_event = self.clock.timestamp_ns()
        instrument_id = await self._ensure_instrument(symbol)
        bids = [
            row for row in getattr(event, "bids", []) if getattr(row, "price", None) is not None
        ]
        asks = [
            row for row in getattr(event, "asks", []) if getattr(row, "price", None) is not None
        ]
        key = f"depth:{symbol}:{[(str(x.price), x.volume) for x in bids]}:{[(str(x.price), x.volume) for x in asks]}"
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

    async def _publish_trades(self, symbol: str, event: Any) -> None:
        instrument_id = await self._ensure_instrument(symbol)
        for item in getattr(event, "trades", []):
            ts_event = timestamp_ns(getattr(item, "timestamp", None), self.clock.timestamp_ns())
            raw_key = (
                f"{symbol}:{ts_event}:{item.price}:{item.volume}:{getattr(item, 'trade_type', '')}"
            )
            digest = hashlib.sha1(raw_key.encode(), usedforsecurity=False).hexdigest()[:20]
            if not self._dedup(f"trade:{digest}"):
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
            digest = hashlib.sha1(
                f"{symbol}:{ts_event}:{item.price}:{item.volume}".encode(), usedforsecurity=False
            ).hexdigest()[:20]
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

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

from nautilus_trader.core import UUID4
from nautilus_trader.live import (
    BarsResponse,
    BookDeltasResponse,
    BookDepthResponse,
    BookResponse,
    InstrumentResponse,
    InstrumentsResponse,
    QuotesResponse,
    TradesResponse,
)
from nautilus_trader.live.clients import DataClient
from nautilus_trader.model import (
    AggregationSource,
    Bar,
    BarAggregation,
    BarSpecification,
    BarType,
    BookAction,
    BookOrder,
    BookType,
    ClientId,
    InstrumentId,
    OrderBook,
    OrderBookDelta,
    OrderBookDeltas,
    OrderBookDepth,
    OrderSide,
    PriceType,
    QuoteTick,
    RecordFlag,
    TradeId,
    TradeTick,
)

from .config import LongbridgeDataClientConfig
from .dedup import TimedDeduplicator
from .errors import ConfigurationError, UnsupportedOperation
from .instruments import LongbridgeInstrumentProvider
from .lifecycle import ConnectionState
from .mapping import aggressor_side, price, quantity, timestamp_ns
from .rate_limit import ConcurrencyLimiter, SlidingWindowRateLimiter
from .symbols import instrument_id_from_symbol, symbol_from_instrument_id
from .transport import ContextFactory, LongbridgeContextFactory, QuoteContext

logger = logging.getLogger(__name__)


def _client_price_precision(client: Any, symbol: str) -> int:
    method = getattr(client, "_price_precision", None)
    if callable(method):
        return int(method(symbol))
    return int(getattr(getattr(client, "_lb_config", None), "price_precision", 4))


def _dispatch_response(client: Any, response: Any) -> None:
    """Dispatch through the connector helper while keeping adapter hooks duck-testable."""
    handler = getattr(client, "_emit_response", None)
    if callable(handler):
        handler(response)
    else:
        client._handle_response(response)


def _sdk_enum(name: str, fallback: Any = None) -> Any:
    from longbridge import openapi

    return getattr(openapi, name, fallback)


def _period_for_bar_type(bar_type: BarType) -> Any:
    if (
        bar_type.aggregation_source != AggregationSource.EXTERNAL
        or bar_type.spec.price_type != PriceType.LAST
    ):
        raise ValueError(f"Longbridge requires external LAST bars: {bar_type}")
    name = str(bar_type.spec).upper()
    if name.startswith("1-MINUTE-"):
        return _sdk_enum("Period", {}).Min_1
    if name.startswith("1-HOUR-"):
        return _sdk_enum("Period", {}).Min_60
    if name.startswith("1-DAY-"):
        return _sdk_enum("Period", {}).Day
    raise ValueError(f"unsupported Longbridge bar period: {bar_type.spec}")


def _bar_type(instrument_id: InstrumentId, period_name: str) -> BarType:
    aggregations = {
        "Min_1": BarAggregation.MINUTE,
        "Min_60": BarAggregation.HOUR,
        "Day": BarAggregation.DAY,
    }
    if period_name not in aggregations:
        raise ValueError(f"unsupported Longbridge candle period: {period_name}")
    spec = BarSpecification(1, aggregations[period_name], PriceType.LAST)
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
        self._desired_bars: dict[tuple[str, str], Any] = {}
        self._last_confirmed_bars: dict[str, Bar] = {}
        self._accepting_events = False
        self._desired_depth: set[str] = set()
        self._desired_book_depth: dict[str, int] = {}
        self._desired_book_deltas: set[str] = set()
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

    def _now_ns(self) -> int:
        clock = getattr(self, "clock", None)
        return clock.timestamp_ns() if clock is not None else 0

    def _emit_response(self, response: Any) -> None:
        sink = getattr(self, "_response_sink", None)
        if sink is not None:
            sink(response)
        elif "_handle_response" in getattr(self, "__dict__", {}) or hasattr(self, "_output"):
            self._handle_response(response)

    def _response_client_id(self) -> Any:
        return getattr(self, "client_id", ClientId("LONGBRIDGE-DATA"))

    def _price_precision(self, symbol: str) -> int:
        """Use the loaded instrument precision, with the equity fallback."""
        provider = getattr(self, "instrument_provider", None)
        if isinstance(provider, LongbridgeInstrumentProvider):
            instrument = provider.find(instrument_id_from_symbol(symbol))
            if instrument is not None:
                return int(getattr(instrument, "price_precision", self._lb_config.price_precision))
        return self._lb_config.price_precision

    @staticmethod
    def _response_request_id(request: Any) -> Any:
        return getattr(request, "request_id", UUID4())

    async def _connect(self) -> None:
        self._connection_state = ConnectionState.CONNECTING
        try:
            loop = asyncio.get_running_loop()
            self._queue = asyncio.Queue(maxsize=self._lb_config.settings.queue_size)
            self._accepting_events = True
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
                depth_only = (
                    getattr(self, "_desired_depth", set())
                    | getattr(self, "_desired_book_deltas", set())
                ) - self._desired_quotes
                if depth_only:
                    await self._quote_call(
                        lambda: quote.unsubscribe(sorted(depth_only), [SubType.Depth])
                    )
                if getattr(self, "_desired_trades", set()):
                    await self._quote_call(
                        lambda: quote.unsubscribe(sorted(self._desired_trades), [SubType.Trade])
                    )
                for (symbol, _), period in getattr(self, "_desired_bars", {}).items():
                    await self._quote_call(partial(quote.unsubscribe_candlesticks, symbol, period))
            except Exception:
                logger.debug("Longbridge quote unsubscribe during disconnect failed", exc_info=True)
            self._clear_quote_callbacks(quote)
        self._accepting_events = False
        try:
            # Keep the consumer alive until already accepted candles reach NT's
            # output queue. The node then owns flushing and catalog promotion.
            queue = getattr(self, "_queue", None)
            if queue is not None and self._consumer is not None:
                if self._consumer.done():
                    await self._consumer  # Surface a failed consumer instead of hanging on join.
                await asyncio.wait_for(
                    queue.join(), self._lb_config.settings.request_timeout_secs
                )
        finally:
            if isinstance(self.instrument_provider, LongbridgeInstrumentProvider):
                self.instrument_provider.set_quote_context(None)
            tasks = [task for task in (self._health_task, self._consumer) if task is not None]
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self._health_task = None
            self._consumer = None
            self._queue = None
            self._quote = None
            self._reference_trade = None

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
        depth_only = (
            getattr(self, "_desired_depth", set())
            | getattr(self, "_desired_book_deltas", set())
        ) - self._desired_quotes
        if depth_only:
            await self._quote_call(
                lambda: quote.subscribe(sorted(depth_only), [SubType.Depth])
            )
        if self._desired_trades:
            await self._quote_call(
                lambda: quote.subscribe(sorted(self._desired_trades), [SubType.Trade])
            )
        for (symbol, _), period in self._desired_bars.items():
            await self._ensure_instrument(symbol)
            await self._quote_call(
                partial(quote.subscribe_candlesticks, symbol, period,
                        getattr(TradeSessions, self._lb_config.live_trade_sessions))
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
        if self._queue is None or not getattr(self, "_accepting_events", True):
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
                    await self._publish_depth(
                        symbol,
                        event,
                        received_ns=received_ns,
                        publish_depth=symbol in getattr(self, "_desired_book_depth", {}),
                        publish_deltas=symbol in getattr(self, "_desired_book_deltas", set()),
                    )
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
            price(bid.price, _client_price_precision(self, symbol)),
            price(ask.price, _client_price_precision(self, symbol)),
            quantity(getattr(bid, "volume", 0)),
            quantity(getattr(ask, "volume", 0)),
            ts_event,
            self.clock.timestamp_ns(),
        )

    async def _publish_depth(
        self,
        symbol: str,
        event: Any,
        *,
        received_ns: int | None = None,
        publish_depth: bool = True,
        publish_deltas: bool | None = None,
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
        if publish_deltas is None:
            publish_deltas = symbol in getattr(self, "_desired_book_deltas", set())
        depth, deltas, _ = self._make_book(
            instrument_id,
            event,
            ts_event,
            depth_limit=getattr(self, "_desired_book_depth", {}).get(symbol),
        )
        key = f"depth:{symbol}:{ts_event}:{depth.bids}:{depth.asks}"
        if not self._dedup(key):
            return
        if publish_depth:
            self._handle_data(depth)
        if publish_deltas:
            self._handle_data(deltas)
        if symbol in getattr(self, "_desired_quotes", set()) or symbol in self._last_quotes:
            tick = self._make_quote_tick(
                instrument_id, symbol, self._last_quotes.get(symbol), event
            )
            if tick is not None:
                self._handle_data(tick)

    def _make_book(
        self,
        instrument_id: InstrumentId,
        event: Any,
        ts_event: int,
        depth_limit: int | None = None,
    ) -> tuple[OrderBookDepth, OrderBookDeltas, OrderBook]:
        bids = [
            row for row in getattr(event, "bids", []) if getattr(row, "price", None) is not None
        ]
        asks = [
            row for row in getattr(event, "asks", []) if getattr(row, "price", None) is not None
        ]
        if depth_limit:
            depth_limit = max(1, int(depth_limit))
            bids = bids[:depth_limit]
            asks = asks[:depth_limit]
        ts_init = self.clock.timestamp_ns()
        flags = RecordFlag.F_SNAPSHOT.value
        depth = OrderBookDepth(
            instrument_id,
            [
                BookOrder(
                    OrderSide.BUY,
                    price(row.price, _client_price_precision(self, str(instrument_id.symbol))),
                    quantity(row.volume),
                    int(row.position),
                )
                for row in bids
            ],
            [
                BookOrder(
                    OrderSide.SELL,
                    price(row.price, _client_price_precision(self, str(instrument_id.symbol))),
                    quantity(row.volume),
                    int(row.position),
                )
                for row in asks
            ],
            [int(row.order_num) for row in bids],
            [int(row.order_num) for row in asks],
            flags,
            0,
            ts_event,
            ts_init,
        )
        raw_deltas = [OrderBookDelta.clear(instrument_id, 0, ts_event, ts_init)]
        for order in [*depth.bids, *depth.asks]:
            raw_deltas.append(
                OrderBookDelta(instrument_id, BookAction.ADD, order, flags, 0, ts_event, ts_init)
            )
        if len(raw_deltas) > 1:
            last = raw_deltas[-1]
            raw_deltas[-1] = OrderBookDelta(
                instrument_id,
                last.action,
                last.order,
                flags | RecordFlag.F_LAST.value,
                last.sequence,
                last.ts_event,
                last.ts_init,
            )
        deltas = OrderBookDeltas(instrument_id, raw_deltas)
        book = OrderBook(instrument_id, BookType.L2_MBP)
        book.apply_deltas(deltas)
        return depth, deltas, book

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
                    price(item.price, _client_price_precision(self, symbol)),
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
        period = getattr(event, "period", None)
        # Longbridge 5.2.2 enums expose str(Period.Min_60), but no .name.
        period_name = str(getattr(period, "name", None) or period).rsplit(".", 1)[-1]
        instrument_id = await self._ensure_instrument(symbol)
        bar_type = _bar_type(instrument_id, period_name)
        ts_event = timestamp_ns(getattr(candle, "timestamp", None), self.clock.timestamp_ns())
        confirmed = bool(getattr(event, "is_confirmed", False))
        fields = (candle.open, candle.high, candle.low, candle.close, candle.volume)
        key = f"bar:{symbol}:{period_name}:{ts_event}:{fields}:{confirmed}"
        if not self._dedup(key):
            return
        bar = Bar(
            bar_type,
            price(candle.open, _client_price_precision(self, symbol)),
            price(candle.high, _client_price_precision(self, symbol)),
            price(candle.low, _client_price_precision(self, symbol)),
            price(candle.close, _client_price_precision(self, symbol)),
            quantity(candle.volume),
            ts_event,
            self.clock.timestamp_ns(),
        )
        if self._lb_config.completed_bars_only:
            if not hasattr(self, "_last_confirmed_bars"):
                self._last_confirmed_bars = {}
            previous = self._last_confirmed_bars.get(str(bar_type))
            if previous is not None and bar.ts_event <= previous.ts_event:
                if bar.ts_event == previous.ts_event and any(
                    getattr(bar, field) != getattr(previous, field)
                    for field in ("open", "high", "low", "close", "volume")
                ):
                    logger.warning("Conflicting confirmed candle ignored for %s at %d", bar_type, ts_event)
                return
            self._last_confirmed_bars[str(bar_type)] = bar
        self._handle_data(bar)

    async def _request_instrument(self, request: Any) -> None:
        provider = self.instrument_provider
        if not isinstance(provider, LongbridgeInstrumentProvider):
            raise ValueError("Longbridge instrument provider required")
        symbol = symbol_from_instrument_id(request.instrument_id)
        if (getattr(request, "params", None) or {}).get("account_reference"):
            if not self._lb_config.reference_account_enabled:
                raise ValueError("account reference requests require explicit connector configuration")
            from .reference import account_reference

            context = getattr(self, "_reference_trade", None)
            if context is None:
                context = self._context_factory.trade(self._lb_config.settings, asyncio.get_running_loop())
                self._reference_trade = context
            reference = await account_reference(context, symbol, received_ns=self.clock.timestamp_ns(),
                                                timeout=self._lb_config.settings.request_timeout_secs)
            reference["received_ns"] = self.clock.timestamp_ns()
            # Rehash with actual completion/availability time, never backdate it.
            import hashlib
            import json

            reference.pop("source_sha256", None)
            reference["source_sha256"] = hashlib.sha256(json.dumps(reference, sort_keys=True,
                separators=(",", ":")).encode()).hexdigest()
            quote = self._quote
            if quote is None:
                raise RuntimeError("quote context is not connected")
            infos = await self._quote_call(lambda: provider._load_static_info([symbol]))
            if len(infos) != 1 or str(provider._field(infos[0], "symbol", "code")).upper() != symbol:
                raise ConfigurationError(
                    f"Longbridge static_info returned no unique instrument for {symbol}"
                )
            referenced = provider._to_instrument(infos[0], reference=reference)
            self._handle_instrument(referenced)
            _dispatch_response(self,
                InstrumentResponse(
                    getattr(self, "client_id", ClientId("LONGBRIDGE-DATA")),
                    referenced.id,
                    referenced,
                    getattr(request, "request_id", UUID4()),
                    self._now_ns(),
                )
            )
            return
        await provider.load_symbols_async([symbol])
        instrument: Any = provider.find(request.instrument_id)
        if instrument is None:
            raise ValueError(f"Longbridge returned no instrument for {symbol}")
        self._handle_instrument(instrument)
        _dispatch_response(self,
            InstrumentResponse(
                getattr(self, "client_id", ClientId("LONGBRIDGE-DATA")),
                instrument.id,
                instrument,
                getattr(request, "request_id", UUID4()),
                self._now_ns(),
            )
        )

    async def _load_instrument_params(self, params: Any) -> list[Any]:
        provider = self.instrument_provider
        if not isinstance(provider, LongbridgeInstrumentProvider):
            raise ConfigurationError("Longbridge instrument provider required")
        params = params or {}
        symbols = params.get("symbols")
        if symbols is None:
            identifiers = params.get("instrument_ids") or params.get("load_ids")
            if identifiers:
                symbols = [symbol_from_instrument_id(identifier) for identifier in identifiers]
        if symbols:
            await provider.load_symbols_async(list(symbols))
        values: list[Any] = list(provider.list_all())
        if not values:
            raise UnsupportedOperation(
                "Longbridge cannot enumerate all securities; configure instrument symbols"
            )
        return values

    async def _request_instruments(self, request: Any) -> None:
        values = await self._load_instrument_params(getattr(request, "params", None))
        for value in values:
            self._handle_instrument(value)
        venue = getattr(request, "venue", None) or getattr(self, "venue", None)
        if venue is None:
            raise ConfigurationError("Longbridge instrument response requires a venue")
        _dispatch_response(self,
            InstrumentsResponse(
                getattr(self, "client_id", ClientId("LONGBRIDGE-DATA")),
                venue,
                values,
                getattr(request, "request_id", UUID4()),
                self._now_ns(),
            )
        )

    async def _subscribe_instrument(self, command: Any) -> None:
        if not isinstance(self.instrument_provider, LongbridgeInstrumentProvider):
            raise ConfigurationError("Longbridge instrument provider required")
        symbol = symbol_from_instrument_id(command.instrument_id)
        await self.instrument_provider.load_symbols_async([symbol])
        instrument = self.instrument_provider.find(command.instrument_id)
        if instrument is None:
            raise ConfigurationError(f"Longbridge returned no instrument for {symbol}")
        self._handle_instrument(instrument)

    async def _subscribe_instruments(self, command: Any) -> None:
        for value in await self._load_instrument_params(getattr(command, "params", None)):
            self._handle_instrument(value)

    async def _unsubscribe_instrument(self, command: Any) -> None:
        return

    async def _unsubscribe_instruments(self, command: Any) -> None:
        return

    async def _ensure_instrument(self, symbol: str) -> InstrumentId:
        instrument_id = instrument_id_from_symbol(symbol)
        provider = self.instrument_provider
        if (
            isinstance(provider, LongbridgeInstrumentProvider)
            and provider.find(instrument_id) is None
        ):
            await provider.load_symbols_async([symbol])
            instrument = provider.find(instrument_id)
            if instrument is None:
                raise ConfigurationError(f"Longbridge returned no instrument for {symbol}")
            self._handle_instrument(instrument)
        elif provider is not None:
            instrument = provider.find(instrument_id)
            if instrument is None:
                raise ConfigurationError(f"Longbridge instrument is not loaded for {symbol}")
        else:
            raise ConfigurationError(f"Longbridge instrument provider is unavailable for {symbol}")
        return instrument_id

    async def _subscribe_quotes(self, command: Any) -> None:
        symbol = symbol_from_instrument_id(command.instrument_id)
        self._desired_quotes.add(symbol)
        self._desired_depth.add(symbol)
        quote = self._quote
        if quote is not None:
            await self._ensure_instrument(symbol)
            from longbridge.openapi import SubType

            await self._quote_call(
                lambda: quote.subscribe([symbol], [SubType.Quote, SubType.Depth])
            )

    async def _unsubscribe_quotes(self, command: Any) -> None:
        symbol = symbol_from_instrument_id(command.instrument_id)
        self._desired_quotes.discard(symbol)
        if (
            symbol not in getattr(self, "_desired_book_depth", {})
            and symbol not in getattr(self, "_desired_book_deltas", set())
        ):
            self._desired_depth.discard(symbol)
        quote = self._quote
        if quote is not None:
            from longbridge.openapi import SubType
            if symbol in getattr(self, "_desired_book_depth", {}) or symbol in getattr(
                self, "_desired_book_deltas", set()
            ):
                await self._quote_call(lambda: quote.unsubscribe([symbol], [SubType.Quote]))
            else:
                await self._quote_call(
                    lambda: quote.unsubscribe([symbol], [SubType.Quote, SubType.Depth])
                )

    async def _subscribe_trades(self, command: Any) -> None:
        symbol = symbol_from_instrument_id(command.instrument_id)
        self._desired_trades.add(symbol)
        quote = self._quote
        if quote is not None:
            await self._ensure_instrument(symbol)
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
        self._desired_bars[(symbol, str(period))] = period
        quote = self._quote
        if quote is not None:
            from longbridge.openapi import TradeSessions

            await self._ensure_instrument(symbol)
            await self._quote_call(
                lambda: quote.subscribe_candlesticks(symbol, period,
                                                     getattr(TradeSessions, self._lb_config.live_trade_sessions))
            )

    async def _unsubscribe_bars(self, command: Any) -> None:
        symbol = symbol_from_instrument_id(command.bar_type.instrument_id)
        period = _period_for_bar_type(command.bar_type)
        subscribed = self._desired_bars.pop((symbol, str(period)), None)
        quote = self._quote
        if quote is not None and subscribed is not None:
            await self._quote_call(lambda: quote.unsubscribe_candlesticks(symbol, period))

    async def _request_quotes(self, request: Any) -> None:
        quote = self._quote
        if quote is None:
            return
        symbol = symbol_from_instrument_id(request.instrument_id)
        await self._ensure_instrument(symbol)
        data: list[Any] = []
        evidence = None
        directory = getattr(self._lb_config, "historical_evidence_dir", None)
        if directory:
            from .evidence import QuoteEvidence

            evidence = QuoteEvidence(directory, str(request.request_id), symbol)
        try:
            values = await self._quote_call(lambda: quote.quote([symbol]))
            if evidence:
                evidence.capture_quotes(values, self.clock.timestamp_ns())
            for value in values:
                self._last_quotes[symbol] = value
                depth = await self._quote_call(lambda: quote.depth(symbol))
                received_ns = self.clock.timestamp_ns()
                if evidence:
                    evidence.capture_depth(depth, received_ns)
                self._last_depth[symbol] = depth
                if not hasattr(self, "_depth_received_ns"):
                    self._depth_received_ns = {}
                self._depth_received_ns[symbol] = received_ns
                tick = self._make_quote_tick(request.instrument_id, symbol, value, depth)
                if tick is not None and (
                    (request.start_ns is None or tick.ts_event >= request.start_ns)
                    and (request.end_ns is None or tick.ts_event <= request.end_ns)
                ):
                    data.append(tick)
        except BaseException as exc:
            if evidence:
                evidence.finish(len(data), exc)
            raise
        else:
            if evidence:
                evidence.finish(len(data))
        _dispatch_response(self,
            QuotesResponse(
                getattr(self, "client_id", ClientId("LONGBRIDGE-DATA")),
                request.instrument_id,
                data,
                getattr(request, "request_id", UUID4()),
                self.clock.timestamp_ns(),
                request.start_ns,
                request.end_ns,
                {"coverage": "current_snapshot_only"},
            )
        )

    async def _request_trades(self, request: Any) -> None:
        quote = self._quote
        if quote is None:
            return
        symbol = symbol_from_instrument_id(request.instrument_id)
        await self._ensure_instrument(symbol)
        limit = min(request.limit or 1000, 1000)
        values = await self._quote_call(lambda: quote.trades(symbol, limit))
        data: list[Any] = []
        for item in values:
            ts_event = timestamp_ns(getattr(item, "timestamp", None), self.clock.timestamp_ns())
            digest = self._trade_digest(symbol, item, ts_event)
            if request.start_ns is not None and ts_event < request.start_ns:
                continue
            if request.end_ns is not None and ts_event > request.end_ns:
                continue
            data.append(
                TradeTick(
                    request.instrument_id,
                    price(item.price, _client_price_precision(self, symbol)),
                    quantity(item.volume),
                    aggressor_side(getattr(item, "direction", None)),
                    TradeId(digest),
                    ts_event,
                    self.clock.timestamp_ns(),
                )
            )
        _dispatch_response(self,
            TradesResponse(
                getattr(self, "client_id", ClientId("LONGBRIDGE-DATA")),
                request.instrument_id,
                data,
                getattr(request, "request_id", UUID4()),
                self.clock.timestamp_ns(),
                request.start_ns,
                request.end_ns,
                {
                    "coverage": "recent_limited_ticks_only",
                    "requested_start_ns": request.start_ns,
                    "requested_end_ns": request.end_ns,
                },
            )
        )

    async def _request_bars(self, request: Any) -> None:
        quote = self._quote
        if quote is None:
            return
        symbol = symbol_from_instrument_id(request.bar_type.instrument_id)
        from longbridge.openapi import AdjustType, TradeSessions

        from .evidence import HistoryEvidence

        period = _period_for_bar_type(request.bar_type)
        adjustment = getattr(self._lb_config, "historical_adjustment", "NoAdjust")
        trade_sessions = getattr(self._lb_config, "historical_trade_sessions", "Intraday")
        evidence = None
        try:
            if request.start_ns is not None or request.end_ns is not None:
                from .history import history_candles

                if request.start_ns is None:
                    raise ValueError("bounded historical bars require a start timestamp")
                directory = getattr(self._lb_config, "historical_evidence_dir", None)
                if directory is not None:
                    evidence = HistoryEvidence(
                        directory, str(request.request_id), symbol, str(period), adjustment,
                        trade_sessions,
                        enable_overnight=self._lb_config.settings.enable_overnight,
                    )
                values = await history_candles(
                    quote,
                    symbol,
                    period,
                    getattr(AdjustType, adjustment),
                    request.start_ns,
                    request.end_ns if request.end_ns is not None else self.clock.timestamp_ns(),
                    call=self._quote_call,
                    evidence=evidence.capture if evidence else None,
                    error_evidence=evidence.capture_error if evidence else None,
                    trade_sessions=trade_sessions,
                )
                if request.limit:
                    values = values[-request.limit :]
            else:
                values = await self._quote_call(
                    lambda: quote.candlesticks(
                        symbol,
                        period,
                        request.limit or 1000,
                        getattr(AdjustType, adjustment),
                        getattr(TradeSessions, trade_sessions),
                    )
                )
            await self._ensure_instrument(symbol)
            data: list[Any] = []
            for candle in values:
                ts_event = timestamp_ns(
                    getattr(candle, "timestamp", None), self.clock.timestamp_ns()
                )
                ts_init = evidence.received_ns[ts_event] if evidence else self.clock.timestamp_ns()
                data.append(
                    Bar(
                        request.bar_type,
                        price(candle.open, _client_price_precision(self, symbol)),
                        price(candle.high, _client_price_precision(self, symbol)),
                        price(candle.low, _client_price_precision(self, symbol)),
                        price(candle.close, _client_price_precision(self, symbol)),
                        quantity(candle.volume),
                        ts_event,
                        ts_init,
                    )
                )
            _dispatch_response(self,
                BarsResponse(
                    getattr(self, "client_id", ClientId("LONGBRIDGE-DATA")),
                    request.bar_type,
                    data,
                    getattr(request, "request_id", UUID4()),
                    self.clock.timestamp_ns(),
                    # The adapter has already filtered by ts_event. rc7's
                    # response bounds trim by ts_init and would drop fetched
                    # history whose real receipt time is later than the range.
                    None,
                    None,
                )
            )
        except BaseException as exc:
            if evidence:
                evidence.finish(exc)
            raise
        else:
            if evidence:
                evidence.finish()

    async def _request_book_snapshot(self, request: Any) -> None:
        quote = self._quote
        if quote is None:
            return
        self._check_book_type(request)
        symbol = symbol_from_instrument_id(request.instrument_id)
        value = await self._quote_call(lambda: quote.depth(symbol))
        instrument_id = await self._ensure_instrument(symbol)
        _, _, book = self._make_book(
            instrument_id,
            value,
            self.clock.timestamp_ns(),
            depth_limit=getattr(request, "depth", None),
        )
        _dispatch_response(self,
            BookResponse(
                getattr(self, "client_id", ClientId("LONGBRIDGE-DATA")),
                request.instrument_id,
                book,
                getattr(request, "request_id", UUID4()),
                self.clock.timestamp_ns(),
            )
        )

    def _check_book_type(self, request: Any) -> None:
        if getattr(request, "book_type", BookType.L2_MBP) != BookType.L2_MBP:
            raise UnsupportedOperation("Longbridge provides L2 price-level depth, not L3 MBO")

    async def _request_book_depth(self, request: Any) -> None:
        quote = self._quote
        if quote is None:
            return
        self._check_book_type(request)
        symbol = symbol_from_instrument_id(request.instrument_id)
        value = await self._quote_call(lambda: quote.depth(symbol))
        instrument_id = await self._ensure_instrument(symbol)
        depth, _, _ = self._make_book(
            instrument_id,
            value,
            self.clock.timestamp_ns(),
            depth_limit=getattr(request, "depth", None) or getattr(request, "limit", None),
        )
        _dispatch_response(self,
            BookDepthResponse(
                getattr(self, "client_id", ClientId("LONGBRIDGE-DATA")),
                request.instrument_id,
                [depth],
                getattr(request, "request_id", UUID4()),
                self.clock.timestamp_ns(),
            )
        )

    async def _request_book_deltas(self, request: Any) -> None:
        quote = self._quote
        if quote is None:
            return
        self._check_book_type(request)
        symbol = symbol_from_instrument_id(request.instrument_id)
        value = await self._quote_call(lambda: quote.depth(symbol))
        instrument_id = await self._ensure_instrument(symbol)
        _, deltas, _ = self._make_book(
            instrument_id,
            value,
            self.clock.timestamp_ns(),
            depth_limit=getattr(request, "limit", None),
        )
        _dispatch_response(self,
            BookDeltasResponse(
                getattr(self, "client_id", ClientId("LONGBRIDGE-DATA")),
                request.instrument_id,
                deltas.deltas,
                getattr(request, "request_id", UUID4()),
                self.clock.timestamp_ns(),
            )
        )

    async def _subscribe_book_depth(self, command: Any) -> None:
        self._check_book_type(command)
        symbol = symbol_from_instrument_id(command.instrument_id)
        if not hasattr(self, "_desired_book_depth"):
            self._desired_book_depth = {}
        if not hasattr(self, "_desired_book_deltas"):
            self._desired_book_deltas = set()
        self._desired_book_depth[symbol] = getattr(command, "depth", None) or 10
        self._desired_depth.add(symbol)
        await self._subscribe_depth_only(symbol)

    async def _unsubscribe_book_depth(self, command: Any) -> None:
        symbol = symbol_from_instrument_id(command.instrument_id)
        getattr(self, "_desired_book_depth", {}).pop(symbol, None)
        await self._maybe_unsubscribe_depth(symbol)

    async def _subscribe_book_deltas(self, command: Any) -> None:
        self._check_book_type(command)
        symbol = symbol_from_instrument_id(command.instrument_id)
        if not hasattr(self, "_desired_book_deltas"):
            self._desired_book_deltas = set()
        self._desired_book_deltas.add(symbol)
        self._desired_depth.add(symbol)
        await self._subscribe_depth_only(symbol)

    async def _unsubscribe_book_deltas(self, command: Any) -> None:
        symbol = symbol_from_instrument_id(command.instrument_id)
        getattr(self, "_desired_book_deltas", set()).discard(symbol)
        await self._maybe_unsubscribe_depth(symbol)

    async def _subscribe_depth_only(self, symbol: str) -> None:
        quote = self._quote
        if quote is None or symbol in self._desired_quotes:
            return
        await self._ensure_instrument(symbol)
        from longbridge.openapi import SubType

        await self._quote_call(lambda: quote.subscribe([symbol], [SubType.Depth]))

    async def _maybe_unsubscribe_depth(self, symbol: str) -> None:
        if symbol in self._desired_quotes or symbol in getattr(self, "_desired_book_depth", {}) \
                or symbol in getattr(self, "_desired_book_deltas", set()):
            return
        self._desired_depth.discard(symbol)
        quote = self._quote
        if quote is not None:
            from longbridge.openapi import SubType

            await self._quote_call(lambda: quote.unsubscribe([symbol], [SubType.Depth]))

    async def _request_data(self, request: Any) -> None:
        raise UnsupportedOperation("Longbridge custom data is not supported")

    async def _subscribe(self, command: Any) -> None:
        raise UnsupportedOperation("Longbridge custom data subscriptions are not supported")

    async def _unsubscribe(self, command: Any) -> None:
        raise UnsupportedOperation("Longbridge custom data subscriptions are not supported")

    async def _request_funding_rates(self, request: Any) -> None:
        raise UnsupportedOperation("Longbridge funding rates are not supported")

    async def _subscribe_funding_rates(self, command: Any) -> None:
        raise UnsupportedOperation("Longbridge funding rates are not supported")

    async def _unsubscribe_funding_rates(self, command: Any) -> None:
        raise UnsupportedOperation("Longbridge funding rates are not supported")

    async def _request_option_chain_reference_price(self, request: Any) -> None:
        raise UnsupportedOperation("Longbridge option reference prices are not supported")

    async def _subscribe_index_prices(self, command: Any) -> None:
        raise UnsupportedOperation("Longbridge index prices are not supported")

    async def _unsubscribe_index_prices(self, command: Any) -> None:
        raise UnsupportedOperation("Longbridge index prices are not supported")

    async def _subscribe_instrument_close(self, command: Any) -> None:
        raise UnsupportedOperation("Longbridge instrument close data is not supported")

    async def _unsubscribe_instrument_close(self, command: Any) -> None:
        raise UnsupportedOperation("Longbridge instrument close data is not supported")

    async def _subscribe_instrument_status(self, command: Any) -> None:
        raise UnsupportedOperation("Longbridge instrument status data is not supported")

    async def _unsubscribe_instrument_status(self, command: Any) -> None:
        raise UnsupportedOperation("Longbridge instrument status data is not supported")

    async def _subscribe_mark_prices(self, command: Any) -> None:
        raise UnsupportedOperation("Longbridge mark prices are not supported")

    async def _unsubscribe_mark_prices(self, command: Any) -> None:
        raise UnsupportedOperation("Longbridge mark prices are not supported")

    async def _subscribe_option_greeks(self, command: Any) -> None:
        raise UnsupportedOperation("Longbridge option greeks are not supported")

    async def _unsubscribe_option_greeks(self, command: Any) -> None:
        raise UnsupportedOperation("Longbridge option greeks are not supported")

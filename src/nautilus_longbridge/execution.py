from __future__ import annotations

import asyncio
import logging
import random
from collections import defaultdict
from collections.abc import Awaitable, Callable
from decimal import Decimal
from functools import partial
from typing import Any

from nautilus_trader.live import (
    GenerateFillReports,
    GenerateOrderStatusReport,
    GenerateOrderStatusReports,
    GeneratePositionStatusReports,
)
from nautilus_trader.live.clients import ExecutionClient
from nautilus_trader.model import (
    AccountBalance,
    AccountId,
    AccountType,
    ClientOrderId,
    Currency,
    FillReport,
    LiquiditySide,
    Money,
    OmsType,
    OrderSide,
    OrderStatus,
    OrderStatusReport,
    OrderType,
    PositionSide,
    PositionStatusReport,
    TimeInForce,
    TradeId,
    Venue,
    VenueOrderId,
)

from .config import LongbridgeExecutionClientConfig, assert_order_enabled
from .constants import TRADE_MARKETS
from .errors import ConfigurationError, UnsupportedOperation
from .instruments import LongbridgeInstrumentProvider, validate_lot_size
from .mapping import as_decimal, enum_name, price, quantity, timestamp_ns
from .rate_limit import SlidingWindowRateLimiter
from .symbols import instrument_id_from_symbol, symbol_from_instrument_id
from .transport import ContextFactory, LongbridgeContextFactory, QuoteContext, TradeContext

logger = logging.getLogger(__name__)

_STATUS_RANK = {
    OrderStatus.SUBMITTED: 0,
    OrderStatus.ACCEPTED: 1,
    OrderStatus.PARTIALLY_FILLED: 2,
    OrderStatus.FILLED: 3,
    OrderStatus.CANCELED: 3,
    OrderStatus.REJECTED: 3,
    OrderStatus.EXPIRED: 3,
}
_TERMINAL_STATUSES = {
    OrderStatus.FILLED,
    OrderStatus.CANCELED,
    OrderStatus.REJECTED,
    OrderStatus.EXPIRED,
}


def _sdk_class(name: str) -> Any:
    from longbridge import openapi

    return getattr(openapi, name)


def client_request_id(client_order_id: ClientOrderId | str) -> str:
    """Return a stable, venue-safe idempotency key."""
    value = str(client_order_id)
    return f"NT-{value}"[:64]


def map_order_type(name: str) -> Any:
    normalized = name.upper()
    if normalized == "MARKET":
        return _sdk_class("OrderType").MO
    if normalized == "LIMIT":
        return _sdk_class("OrderType").LO
    raise UnsupportedOperation(f"Longbridge phase 1 does not support order type {name}")


def map_order_side(side: Any) -> Any:
    name = enum_name(side)
    sdk = _sdk_class("OrderSide")
    if name.endswith("BUY"):
        return sdk.Buy
    if name.endswith("SELL"):
        return sdk.Sell
    raise UnsupportedOperation(f"unsupported order side {name}")


def map_time_in_force(tif: Any, expire_time: int | None = None) -> tuple[Any, Any]:
    name = enum_name(tif)
    sdk = _sdk_class("TimeInForceType")
    if name == "DAY":
        return sdk.Day, None
    if name == "GTC":
        return sdk.GoodTilCanceled, None
    if name == "GTD":
        if expire_time is None:
            raise ConfigurationError("GTD orders require expire_time")
        from datetime import UTC, datetime

        return sdk.GoodTilDate, datetime.fromtimestamp(expire_time / 1_000_000_000, UTC).date()
    raise UnsupportedOperation(f"Longbridge phase 1 does not support TIF {name}")


def map_status(status: Any, filled_qty: Decimal, quantity_value: Decimal) -> OrderStatus:
    name = enum_name(status)
    if "REJECT" in name or "FAIL" in name:
        return OrderStatus.REJECTED
    if "EXPIRE" in name:
        return OrderStatus.EXPIRED
    if "CANCEL" in name or "WITHDRAW" in name:
        return OrderStatus.CANCELED
    if "FILL" in name:
        return OrderStatus.FILLED if filled_qty >= quantity_value else OrderStatus.PARTIALLY_FILLED
    if filled_qty > 0:
        return OrderStatus.PARTIALLY_FILLED
    if "NEW" in name or "ACCEPT" in name or "SUBMIT" in name or "REPORT" in name:
        return OrderStatus.ACCEPTED
    return OrderStatus.SUBMITTED


def map_order_type_report(order_type: Any) -> OrderType:
    return (
        OrderType.MARKET
        if "MO" in enum_name(order_type) or "MARKET" in enum_name(order_type)
        else OrderType.LIMIT
    )


def map_tif_report(value: Any) -> TimeInForce:
    name = enum_name(value)
    if "CANCEL" in name or "GTC" in name:
        return TimeInForce.GTC
    if "DATE" in name or "GTD" in name:
        return TimeInForce.GTD
    return TimeInForce.DAY


class LongbridgeExecutionClient(ExecutionClient):
    """Longbridge order, account and reconciliation client."""

    def __init__(
        self,
        *,
        config: LongbridgeExecutionClientConfig,
        context_factory: ContextFactory | None = None,
        **kwargs: Any,
    ) -> None:
        self.config = config
        self._lb_config = config
        self._context_factory = context_factory or LongbridgeContextFactory()
        self._trade: TradeContext | None = None
        self._quote: QuoteContext | None = None
        self._queue: asyncio.Queue[Any] | None = None
        self._consumer: asyncio.Task[Any] | None = None
        self._health_task: asyncio.Task[Any] | None = None
        self._pending_orders: dict[str, Any] = {}
        self._venue_by_client: dict[str, str] = {}
        self._seen_fills: set[str] = set()
        self._last_filled: defaultdict[str, Decimal] = defaultdict(Decimal)
        self._last_status: dict[str, OrderStatus] = {}
        self._reconciliation_tasks: dict[str, asyncio.Task[Any]] = {}
        self._trade_rate = SlidingWindowRateLimiter(30, 30.0, min_interval_secs=0.02)
        self._reconnect_lock = asyncio.Lock()
        instrument_provider = kwargs.pop("instrument_provider", None)
        if instrument_provider is None:
            instrument_provider = LongbridgeInstrumentProvider()
        super().__init__(
            config=config,
            venue=Venue("LONGBRIDGE"),
            account_id=AccountId(config.account_id),
            account_type=AccountType.CASH,
            oms_type=OmsType.NETTING,
            base_currency=Currency.from_str(config.base_currency),
            instrument_provider=instrument_provider,
            **kwargs,
        )

    async def _connect(self) -> None:
        loop = asyncio.get_running_loop()
        self._queue = asyncio.Queue(maxsize=self._lb_config.settings.queue_size)
        trade = self._context_factory.trade(self._lb_config.settings, loop)
        self._trade = trade
        self._quote = self._context_factory.quote(self._lb_config.settings, loop)
        if isinstance(self.instrument_provider, LongbridgeInstrumentProvider):
            self.instrument_provider.set_quote_context(self._quote)
        trade.set_on_order_changed(self._on_order_changed)
        self._consumer = self.create_task(self._consume(), "trade-event-consumer")
        self._health_task = self.create_task(self._health_loop(), "trade-health")
        try:
            await self._trade_call(lambda: trade.subscribe([_sdk_class("TopicType").Private]))
        except Exception:
            logger.exception("failed to subscribe to Longbridge private order events")
            raise
        await self._emit_account_state()

    async def _trade_call(self, operation: Callable[[], Awaitable[Any]]) -> Any:
        await self._trade_rate.acquire()
        return await asyncio.wait_for(operation(), self._lb_config.settings.request_timeout_secs)

    async def _health_loop(self) -> None:
        while True:
            await asyncio.sleep(max(5.0, self._lb_config.settings.reconnect_initial_secs))
            trade = self._trade
            if trade is None:
                return
            try:
                await self._trade_call(trade.today_orders)
            except asyncio.CancelledError:
                raise
            except Exception:
                await self._reconnect_with_backoff()

    async def _reconnect_with_backoff(self) -> None:
        delay = self._lb_config.settings.reconnect_initial_secs
        while True:
            try:
                async with self._reconnect_lock:
                    loop = asyncio.get_running_loop()
                    trade = self._context_factory.trade(self._lb_config.settings, loop)
                    trade.set_on_order_changed(self._on_order_changed)
                    await self._trade_call(
                        partial(trade.subscribe, [_sdk_class("TopicType").Private])
                    )
                    self._trade = trade
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Longbridge trade reconnect failed (%s); retrying in %.1fs",
                    type(exc).__name__,
                    delay,
                )
                await asyncio.sleep(delay * random.uniform(0.8, 1.2))
                delay = min(self._lb_config.settings.reconnect_max_secs, delay * 2)

    async def _disconnect(self) -> None:
        reconciliation_tasks = list(self._reconciliation_tasks.values())
        for task in reconciliation_tasks:
            task.cancel()
        if reconciliation_tasks:
            await asyncio.gather(*reconciliation_tasks, return_exceptions=True)
        self._reconciliation_tasks.clear()
        trade = self._trade
        if trade is not None:
            try:
                await self._trade_call(
                    lambda: trade.unsubscribe([_sdk_class("TopicType").Private])
                )
            except Exception:
                logger.debug("Longbridge trade unsubscribe during disconnect failed", exc_info=True)
            try:
                trade.set_on_order_changed(lambda *args: None)
            except Exception:
                logger.debug("Longbridge trade callback cleanup failed", exc_info=True)
        if isinstance(self.instrument_provider, LongbridgeInstrumentProvider):
            self.instrument_provider.set_quote_context(None)
        quote = self._quote
        if quote is not None:
            for setter in (
                quote.set_on_quote,
                quote.set_on_depth,
                quote.set_on_trades,
                quote.set_on_candlestick,
            ):
                try:
                    setter(lambda *args: None)
                except Exception:
                    logger.debug("Longbridge execution quote callback cleanup failed", exc_info=True)
        self._quote = None
        tasks = [task for task in (self._consumer, self._health_task) if task is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._consumer = None
        self._health_task = None
        self._queue = None
        self._trade = None

    async def _on_order_changed(self, event: Any) -> None:
        if self._queue is not None:
            try:
                self._queue.put_nowait(event)
            except asyncio.QueueFull:
                logger.error("Longbridge order-event queue is full; reconciliation is required")

    async def _consume(self) -> None:
        assert self._queue is not None
        while True:
            event = await self._queue.get()
            try:
                await self._process_order_event(event)
            finally:
                self._queue.task_done()

    async def _process_order_event(self, event: Any) -> None:
        venue_order_id = str(event.order_id)
        client_order_id = self._client_id_for_venue(venue_order_id)
        if not client_order_id:
            raw_client_id = getattr(event, "client_order_id", None) or getattr(
                event, "client_request_id", None
            )
            if raw_client_id:
                client_order_id = str(raw_client_id).removeprefix("NT-")
        order = self._pending_orders.get(client_order_id)
        if order is None:
            return
        filled = as_decimal(getattr(event, "executed_quantity", 0))
        total = as_decimal(
            getattr(
                event,
                "submitted_quantity",
                getattr(event, "quantity", getattr(order.quantity, "as_decimal", lambda: 0)()),
            )
        )
        status = map_status(getattr(event, "status", None), filled, total)
        venue_id = VenueOrderId(venue_order_id)
        ts_event = timestamp_ns(getattr(event, "updated_at", None), self.clock.timestamp_ns())
        previous = self._last_status.get(client_order_id)
        if previous in _TERMINAL_STATUSES or (
            previous is not None and _STATUS_RANK[status] < _STATUS_RANK[previous]
        ):
            return
        status_changed = previous != status
        self._last_status[client_order_id] = status
        if status == OrderStatus.ACCEPTED and status_changed:
            self.generate_order_accepted(order, venue_id, ts_event)
        elif status == OrderStatus.CANCELED and status_changed:
            self.generate_order_canceled(order, venue_id, ts_event)
        elif status == OrderStatus.REJECTED and status_changed:
            self.generate_order_rejected(order, "Longbridge rejected the order", ts_event, False)
        elif status == OrderStatus.EXPIRED and status_changed:
            self.generate_order_expired(order, venue_id, ts_event)
        if filled > self._last_filled[client_order_id]:
            self._last_filled[client_order_id] = filled
            await self._emit_fills_for_order(venue_order_id, order)

    async def _submit_order(self, command: Any) -> None:
        order = command.order
        try:
            assert_order_enabled(self._lb_config.settings)
            symbol = symbol_from_instrument_id(order.instrument_id)
            market = symbol.rsplit(".", 1)[1]
            if market not in TRADE_MARKETS:
                raise UnsupportedOperation("A-share trading is not enabled in phase 1")
            order_type = map_order_type(enum_name(order.order_type))
            side = map_order_side(order.side)
            order_quantity = order.quantity.as_decimal()
            if order_quantity <= 0:
                raise ConfigurationError("order quantity must be positive")
            instrument = await self._load_instrument(order.instrument_id, symbol)
            validate_lot_size(order_quantity, instrument.lot_size.as_decimal(), symbol)
            if order_type is _sdk_class("OrderType").LO and (
                getattr(order, "price", None) is None or order.price.as_decimal() <= 0
            ):
                raise ConfigurationError("limit orders require a positive price")
            tif, expire_date = map_time_in_force(
                order.time_in_force, getattr(order, "expire_time", None)
            )
            if getattr(order, "is_post_only", False) or getattr(order, "is_reduce_only", False):
                raise UnsupportedOperation("post-only and reduce-only are not supported")
        except Exception as exc:
            self.generate_order_denied(order, str(exc))
            return
        self._pending_orders[str(order.client_order_id)] = order
        self.generate_order_submitted(order)
        try:
            trade = self._trade
            assert trade is not None
            response = await self._trade_call(
                lambda: trade.submit_order(
                    symbol=symbol,
                    order_type=order_type,
                    side=side,
                    submitted_quantity=order_quantity,
                    time_in_force=tif,
                    submitted_price=order.price.as_decimal()
                    if getattr(order, "price", None) is not None
                    else None,
                    expire_date=expire_date,
                    outside_rth=None,
                    client_request_id=client_request_id(order.client_order_id),
                    remark=client_request_id(order.client_order_id),
                )
            )
        except TimeoutError:
            logger.error(
                "Longbridge submit timed out for %s; keeping order submitted and reconciling",
                order.client_order_id,
            )
            self._schedule_reconciliation(str(order.client_order_id))
            return
        except Exception as exc:
            self._pending_orders.pop(str(order.client_order_id), None)
            self.generate_order_rejected(order, str(exc), self.clock.timestamp_ns(), False)
            return
        venue_id = str(getattr(response, "order_id", ""))
        if not venue_id:
            logger.error(
                "Longbridge submit returned no venue order ID for %s; reconciling",
                order.client_order_id,
            )
            self._schedule_reconciliation(str(order.client_order_id))
            return
        self._venue_by_client[str(order.client_order_id)] = venue_id
        self._last_status[str(order.client_order_id)] = OrderStatus.ACCEPTED
        self.generate_order_accepted(order, VenueOrderId(venue_id), self.clock.timestamp_ns())

    async def _load_instrument(self, instrument_id: Any, symbol: str) -> Any:
        provider = self.instrument_provider
        instrument = provider.find(instrument_id) if provider is not None else None
        if instrument is not None:
            return instrument
        if not isinstance(provider, LongbridgeInstrumentProvider):
            raise ConfigurationError(f"instrument metadata unavailable for {symbol}")
        if self._quote is None:
            raise ConfigurationError(
                f"instrument metadata unavailable for {symbol}; execution client is not connected"
            )
        await provider.load_symbols_async([symbol])
        instrument = provider.find(instrument_id)
        if instrument is None:
            raise ConfigurationError(f"instrument metadata not found for {symbol}")
        return instrument

    def _schedule_reconciliation(self, client_order_id: str) -> None:
        existing = self._reconciliation_tasks.get(client_order_id)
        if existing is not None and not existing.done():
            return
        coroutine = self._reconcile_uncertain_order(client_order_id)
        task_factory = getattr(self, "create_task", None)
        if callable(task_factory) and getattr(self, "_runtime", None) is not None:
            task = task_factory(coroutine, f"reconcile-{client_order_id}")
        else:
            task = asyncio.create_task(coroutine, name=f"reconcile-{client_order_id}")
        self._reconciliation_tasks[client_order_id] = task
        task.add_done_callback(
            lambda _: self._reconciliation_tasks.pop(client_order_id, None)
        )

    async def _reconcile_uncertain_order(
        self, client_order_id: str, *, max_attempts: int = 5
    ) -> bool:
        """Resolve a timed-out submission without ever submitting it again."""
        order = self._pending_orders.get(client_order_id)
        if order is None or self._trade is None:
            return False
        request_id = client_request_id(client_order_id)
        for attempt in range(max_attempts):
            try:
                values = await self._trade_call(self._trade.today_orders)
                matches = [
                    value
                    for value in values
                    if self._matches_pending_order(value, order, request_id)
                ]
                if len(matches) == 1:
                    venue_id = str(matches[0].order_id)
                    self._venue_by_client[client_order_id] = venue_id
                    await self._process_order_event(matches[0])
                    logger.info(
                        "Reconciled uncertain Longbridge order %s as venue order %s",
                        client_order_id,
                        venue_id,
                    )
                    return True
                if len(matches) > 1:
                    logger.error(
                        "Multiple Longbridge orders matched uncertain client order %s",
                        client_order_id,
                    )
                    return False
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Longbridge order reconciliation attempt %d/%d failed for %s: %s",
                    attempt + 1,
                    max_attempts,
                    client_order_id,
                    type(exc).__name__,
                )
            if attempt + 1 < max_attempts:
                await asyncio.sleep(min(2**attempt, 16))
        logger.error(
            "Longbridge order %s remains in unknown state after reconciliation",
            client_order_id,
        )
        return False

    @staticmethod
    def _matches_pending_order(value: Any, order: Any, request_id: str) -> bool:
        for field in ("client_order_id", "client_request_id", "remark"):
            raw_value = getattr(value, field, None)
            if raw_value is not None and str(raw_value) in {
                str(order.client_order_id),
                request_id,
            }:
                return True
        return False

    async def _modify_order(self, command: Any) -> None:
        order = command.order
        venue_id = self._venue_by_client.get(str(order.client_order_id))
        if not venue_id or self._trade is None:
            self.generate_order_modify_rejected(
                order,
                VenueOrderId(venue_id) if venue_id else None,
                "unknown venue order",
                self.clock.timestamp_ns(),
            )
            return
        trade = self._trade
        try:
            await self._trade_call(
                lambda: trade.replace_order(
                    order_id=venue_id,
                    quantity=command.quantity.as_decimal(),
                    price=command.price.as_decimal() if command.price is not None else None,
                )
            )
            self.generate_order_updated(
                order,
                VenueOrderId(venue_id),
                command.quantity,
                command.price,
                None,
                None,
                self.clock.timestamp_ns(),
            )
        except Exception as exc:
            self.generate_order_modify_rejected(
                order, VenueOrderId(venue_id), str(exc), self.clock.timestamp_ns()
            )

    async def _cancel_order(self, command: Any) -> None:
        order = command.order
        venue_id = self._venue_by_client.get(str(order.client_order_id))
        if not venue_id or self._trade is None:
            self.generate_order_cancel_rejected(
                order,
                VenueOrderId(venue_id) if venue_id else None,
                "unknown venue order",
                self.clock.timestamp_ns(),
            )
            return
        trade = self._trade
        try:
            await self._trade_call(lambda: trade.cancel_order(venue_id))
            for attempt in range(10):
                orders = await self._trade_call(lambda: trade.today_orders(order_id=venue_id))
                if orders:
                    value = orders[0]
                    status = map_status(
                        getattr(value, "status", None),
                        as_decimal(getattr(value, "executed_quantity", 0)),
                        as_decimal(
                            getattr(
                                value,
                                "submitted_quantity",
                                getattr(value, "quantity", order.quantity.as_decimal()),
                            )
                        ),
                    )
                    if status in _TERMINAL_STATUSES:
                        await self._process_order_event(value)
                        return
                if attempt < 9:
                    await asyncio.sleep(0.3)
            logger.warning("Longbridge cancel confirmation timed out for %s", venue_id)
        except Exception as exc:
            self.generate_order_cancel_rejected(
                order, VenueOrderId(venue_id), str(exc), self.clock.timestamp_ns()
            )

    async def _emit_account_state(self) -> None:
        if self._trade is None:
            return
        try:
            values = await self._trade_call(self._trade.account_balance)
        except Exception as exc:
            logger.warning("Longbridge account balance unavailable: %s", type(exc).__name__)
            return
        balances: list[AccountBalance] = []
        for value in values:
            currency = Currency.from_str(
                str(getattr(value, "currency", self._lb_config.base_currency)).upper()
            )
            total = as_decimal(getattr(value, "net_assets", getattr(value, "total_cash", 0)))
            free = as_decimal(
                getattr(value, "available_cash", getattr(value, "cash_available", total))
            )
            balances.append(
                AccountBalance(
                    Money.from_decimal(total, currency),
                    Money.from_decimal(total - free, currency),
                    Money.from_decimal(free, currency),
                )
            )
        if balances:
            self.generate_account_state(balances, [], True, self.clock.timestamp_ns())

    async def _emit_fills_for_order(self, venue_order_id: str, order: Any) -> None:
        if self._trade is None:
            return
        trade = self._trade
        for value in await self._trade_call(
            lambda: trade.today_executions(order_id=venue_order_id)
        ):
            execution_id = str(getattr(value, "execution_id", getattr(value, "trade_id", "")))
            if not execution_id or execution_id in self._seen_fills:
                continue
            self._seen_fills.add(execution_id)
            fill = self._fill_report(value, ClientOrderId(str(order.client_order_id)))
            self._handle_report(fill)
            self.generate_order_filled(
                order,
                VenueOrderId(venue_order_id),
                None,
                fill.trade_id,
                fill.last_qty,
                fill.last_px,
                fill.commission.currency,
                fill.commission,
                fill.liquidity_side,
                fill.ts_event,
            )

    def _client_id_for_venue(self, venue_order_id: str) -> str:
        for client, venue in self._venue_by_client.items():
            if venue == venue_order_id:
                return client
        return ""

    async def _generate_order_status_report(
        self, command: GenerateOrderStatusReport
    ) -> OrderStatusReport | None:
        if self._trade is None:
            return None
        trade = self._trade
        venue_order_id = getattr(command, "venue_order_id", None)
        values = await self._trade_call(
            lambda: trade.today_orders(
                order_id=str(venue_order_id) if venue_order_id else None,
            )
        )
        return self._order_report(values[0]) if values else None

    async def _generate_order_status_reports(
        self, command: GenerateOrderStatusReports
    ) -> list[OrderStatusReport]:
        if self._trade is None:
            return []
        trade = self._trade
        values = await self._trade_call(trade.today_orders)
        return [self._order_report(value) for value in values]

    async def _generate_fill_reports(self, command: GenerateFillReports) -> list[FillReport]:
        if self._trade is None:
            return []
        trade = self._trade
        venue_order_id = getattr(command, "venue_order_id", None)
        values = await self._trade_call(
            lambda: trade.today_executions(
                order_id=str(venue_order_id) if venue_order_id else None
            )
        )
        return [self._fill_report(value) for value in values]

    async def _generate_position_status_reports(
        self, command: GeneratePositionStatusReports
    ) -> list[PositionStatusReport]:
        if self._trade is None:
            return []
        result = await self._trade_call(self._trade.stock_positions)
        channels = getattr(result, "channels", None)
        if channels is not None:
            values = [position for channel in channels for position in channel.positions]
        else:
            values = getattr(result, "positions", getattr(result, "list", result))
        reports = []
        for value in values:
            symbol = str(getattr(value, "symbol", ""))
            if not symbol:
                continue
            instrument_id = instrument_id_from_symbol(symbol)
            qty = as_decimal(getattr(value, "quantity", getattr(value, "available_quantity", 0)))
            reports.append(
                PositionStatusReport(
                    AccountId(self._lb_config.account_id),
                    instrument_id,
                    PositionSide.LONG if qty >= 0 else PositionSide.SHORT,
                    quantity(abs(qty)),
                    self.clock.timestamp_ns(),
                    self.clock.timestamp_ns(),
                    avg_px_open=as_decimal(getattr(value, "cost_price", 0)),
                )
            )
        return reports

    def _order_report(self, value: Any) -> OrderStatusReport:
        symbol = str(value.symbol)
        instrument_id = instrument_id_from_symbol(symbol)
        precision = self._price_precision(instrument_id)
        qty = as_decimal(getattr(value, "submitted_quantity", getattr(value, "quantity", 0)))
        filled = as_decimal(
            getattr(value, "executed_quantity", getattr(value, "filled_quantity", 0))
        )
        client_id = getattr(value, "client_order_id", None)
        if not client_id:
            client_id = self._client_id_for_venue(str(value.order_id))
        submitted_price = getattr(value, "submitted_price", getattr(value, "price", None))
        return OrderStatusReport(
            AccountId(self._lb_config.account_id),
            instrument_id,
            VenueOrderId(str(value.order_id)),
            OrderSide.BUY if "BUY" in enum_name(value.side) else OrderSide.SELL,
            map_order_type_report(value.order_type),
            map_tif_report(getattr(value, "time_in_force", None)),
            map_status(getattr(value, "status", None), filled, qty),
            quantity(qty),
            quantity(filled),
            timestamp_ns(getattr(value, "submitted_at", None), self.clock.timestamp_ns()),
            timestamp_ns(getattr(value, "updated_at", None), self.clock.timestamp_ns()),
            self.clock.timestamp_ns(),
            client_order_id=ClientOrderId(str(client_id))
            if client_id
            else None,
            price=price(submitted_price, precision) if submitted_price is not None else None,
        )

    def _fill_report(
        self, value: Any, client_order_id: ClientOrderId | None = None
    ) -> FillReport:
        symbol = str(getattr(value, "symbol", ""))
        instrument_id = instrument_id_from_symbol(symbol)
        precision = self._price_precision(instrument_id)
        currency = Currency.from_str(
            str(getattr(value, "currency", self._lb_config.base_currency)).upper()
        )
        return FillReport(
            AccountId(self._lb_config.account_id),
            instrument_id,
            VenueOrderId(str(value.order_id)),
            TradeId(str(getattr(value, "execution_id", getattr(value, "trade_id", "")))),
            OrderSide.BUY if "BUY" in enum_name(value.side) else OrderSide.SELL,
            quantity(getattr(value, "quantity", getattr(value, "executed_quantity", 0))),
            price(getattr(value, "price", 0), precision),
            Money.from_decimal(as_decimal(getattr(value, "commission", 0)), currency),
            LiquiditySide.TAKER,
            timestamp_ns(
                getattr(
                    value,
                    "executed_at",
                    getattr(value, "trade_time", getattr(value, "trade_done_at", None)),
                ),
                self.clock.timestamp_ns(),
            ),
            self.clock.timestamp_ns(),
            client_order_id=client_order_id
            or (
                ClientOrderId(self._client_id_for_venue(str(value.order_id)))
                if self._client_id_for_venue(str(value.order_id))
                else None
            ),
        )

    def _price_precision(self, instrument_id: Any) -> int:
        provider = getattr(self, "instrument_provider", None)
        instrument = provider.find(instrument_id) if provider is not None else None
        return int(getattr(instrument, "price_precision", 4))

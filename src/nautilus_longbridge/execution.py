from __future__ import annotations

import asyncio
import hashlib
import logging
import random
from collections import defaultdict
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from functools import partial
from types import SimpleNamespace
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
    MarginBalance,
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

from .accounts import paper_margin_balance
from .config import LongbridgeExecutionClientConfig, assert_order_enabled
from .constants import TRADE_MARKETS
from .errors import ConfigurationError, UnsupportedOperation
from .instruments import LongbridgeInstrumentProvider, validate_lot_size
from .lifecycle import ConnectionState
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
    if status is None:
        return OrderStatus.SUBMITTED
    name = enum_name(status).replace("_", "")
    if name in {"REJECTED", "FAILED", "SUBMITFAILED"}:
        return OrderStatus.REJECTED
    if name == "EXPIRED":
        return OrderStatus.EXPIRED
    if name in {"CANCELED", "CANCELLED"}:
        return OrderStatus.CANCELED
    if name in {"FILLED", "PARTIALFILLED"}:
        return OrderStatus.FILLED if filled_qty >= quantity_value else OrderStatus.PARTIALLY_FILLED
    if name == "PARTIALWITHDRAWAL":
        return OrderStatus.CANCELED
    if filled_qty > 0:
        return OrderStatus.PARTIALLY_FILLED
    if name in {
        "NEW",
        "WAITTONEW",
        "PENDINGCANCEL",
        "WAITTOCANCEL",
        "PENDINGREPLACE",
        "WAITTOREPLACE",
        "REPLACED",
    }:
        return OrderStatus.ACCEPTED
    if name in {
        "NOTREPORTED",
        "DELAYEDNOTREPORTED",
        "PROTECTEDNOTREPORTED",
        "VARIETIESNOTREPORTED",
        "REPLACEDNOTREPORTED",
    }:
        return OrderStatus.SUBMITTED
    raise UnsupportedOperation(f"unsupported Longbridge order status {name}")


def map_order_type_report(order_type: Any) -> OrderType:
    name = enum_name(order_type).replace("_", "")
    if name in {"MO", "MARKET"}:
        return OrderType.MARKET
    if name in {"LO", "LIMIT"}:
        return OrderType.LIMIT
    raise UnsupportedOperation(f"unsupported Longbridge report order type {name}")


def map_tif_report(value: Any) -> TimeInForce:
    name = enum_name(value).replace("_", "")
    if name in {"GTC", "GOODTILCANCELED", "GOODTILCANCELLED"}:
        return TimeInForce.GTC
    if name in {"GTD", "GOODTILDATE"}:
        return TimeInForce.GTD
    if name in {"DAY", "NONE"}:
        return TimeInForce.DAY
    raise UnsupportedOperation(f"unsupported Longbridge report time in force {name}")


def _command_ns(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return timestamp_ns(value, 0)
    return int(value)


def _command_datetime(value: Any) -> datetime | None:
    stamp = _command_ns(value)
    return datetime.fromtimestamp(stamp / 1_000_000_000, UTC) if stamp is not None else None


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
        self._reconnect_task: asyncio.Task[Any] | None = None
        self._connection_state = ConnectionState.DISCONNECTED
        self._margin_submit_lock = asyncio.Lock()
        self._margin_exit_lock = asyncio.Lock()
        self._margin_preparing: set[str] = set()
        self._margin_cancelled: set[str] = set()
        instrument_provider = kwargs.pop("instrument_provider", None)
        if instrument_provider is None:
            instrument_provider = LongbridgeInstrumentProvider()
        super().__init__(
            config=config,
            venue=Venue("LONGBRIDGE"),
            account_id=AccountId(config.account_id),
            account_type=AccountType.MARGIN if config.account_mode == "MARGIN" else AccountType.CASH,
            oms_type=OmsType.NETTING,
            base_currency=Currency.from_str(config.base_currency),
            instrument_provider=instrument_provider,
            **kwargs,
        )

    @property
    def connection_state(self) -> ConnectionState:
        return self._connection_state

    async def _connect(self) -> None:
        self._connection_state = ConnectionState.CONNECTING
        try:
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
            await self._trade_call(lambda: trade.subscribe([_sdk_class("TopicType").Private]))
            self._connection_state = ConnectionState.CONNECTED
        except BaseException:
            self._connection_state = ConnectionState.FAILED
            logger.exception("failed to subscribe to Longbridge private order events")
            raise
        await self._reconcile_state()

    async def _trade_call(self, operation: Callable[[], Awaitable[Any]]) -> Any:
        await self._trade_rate.acquire()
        return await asyncio.wait_for(operation(), self._lb_config.settings.request_timeout_secs)

    async def _health_loop(self) -> None:
        while True:
            await asyncio.sleep(max(5.0, self._lb_config.settings.reconnect_initial_secs))
            trade = self._trade
            if trade is None or self._connection_state is ConnectionState.DISCONNECTED:
                return
            if self._connection_state is not ConnectionState.CONNECTED:
                continue
            try:
                await self._trade_call(trade.today_orders)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._start_reconnect()

    def _start_reconnect(self) -> asyncio.Task[Any] | None:
        existing = self._reconnect_task
        if existing is not None and not existing.done():
            return existing
        self._connection_state = ConnectionState.RECONNECTING
        task = asyncio.create_task(self._reconnect_loop(), name="longbridge-trade-reconnect")
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
        max_attempts = self._lb_config.settings.reconnect_max_attempts
        for attempt in range(max_attempts):
            try:
                async with self._reconnect_lock:
                    loop = asyncio.get_running_loop()
                    trade = self._context_factory.trade(self._lb_config.settings, loop)
                    trade.set_on_order_changed(self._on_order_changed)
                    await self._trade_call(
                        partial(trade.subscribe, [_sdk_class("TopicType").Private])
                    )
                    self._trade = trade
                    await self._reconcile_state()
                self._connection_state = ConnectionState.CONNECTED
                logger.info("Longbridge trade connection restored")
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Longbridge trade reconnect attempt %d/%d failed (%s); retrying in %.1fs",
                    attempt + 1,
                    max_attempts,
                    type(exc).__name__,
                    delay,
                )
                if attempt + 1 < max_attempts:
                    await asyncio.sleep(delay * random.uniform(0.8, 1.2))
                    delay = min(self._lb_config.settings.reconnect_max_secs, delay * 2)
        self._connection_state = ConnectionState.FAILED
        logger.error("Longbridge trade reconnection failed after %d attempts", max_attempts)

    async def _reconcile_state(self) -> None:
        """Refresh broker state after connect/reconnect before accepting new work."""
        await self._emit_account_state()
        command = SimpleNamespace(
            start=None,
            end=None,
            instrument_id=None,
            open_only=False,
            venue_order_id=None,
            client_order_id=None,
        )
        try:
            if self._trade is not None:
                for value in await self._trade_call(self._trade.today_orders):
                    await self._process_order_event(value)
        except Exception:
            logger.warning("Longbridge order push reconciliation failed", exc_info=True)
        generators: tuple[Any, ...] = (
            self._generate_order_status_reports,
            self._generate_fill_reports,
            self._generate_position_status_reports,
        )
        for generator in generators:
            try:
                for report in await generator(command):
                    if isinstance(report, FillReport):
                        self._seen_fills.add(str(report.trade_id))
                    elif isinstance(report, OrderStatusReport):
                        client = str(report.client_order_id) if report.client_order_id else ""
                        if client:
                            self._venue_by_client[client] = str(report.venue_order_id)
                    self._handle_report(report)
            except Exception:
                logger.warning(
                    "Longbridge typed reconciliation failed for %s",
                    generator.__name__,
                    exc_info=True,
                )

    async def _disconnect(self) -> None:
        reconnect_task = getattr(self, "_reconnect_task", None)
        self._reconnect_task = None
        if reconnect_task is not None and not reconnect_task.done():
            reconnect_task.cancel()
            await asyncio.gather(reconnect_task, return_exceptions=True)
        self._connection_state = ConnectionState.DISCONNECTED
        reconciliation_tasks = list(self._reconciliation_tasks.values())
        for task in reconciliation_tasks:
            task.cancel()
        if reconciliation_tasks:
            await asyncio.gather(*reconciliation_tasks, return_exceptions=True)
        self._reconciliation_tasks.clear()
        trade = self._trade
        if trade is not None:
            try:
                await self._trade_call(lambda: trade.unsubscribe([_sdk_class("TopicType").Private]))
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
                    logger.debug(
                        "Longbridge execution quote callback cleanup failed", exc_info=True
                    )
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
        try:
            status = map_status(getattr(event, "status", None), filled, total)
        except UnsupportedOperation as exc:
            logger.error("Ignoring Longbridge order event with unsupported status: %s", exc)
            return
        venue_id = VenueOrderId(venue_order_id)
        ts_event = timestamp_ns(getattr(event, "updated_at", None), self.clock.timestamp_ns())
        previous = self._last_status.get(client_order_id)
        if previous in _TERMINAL_STATUSES or (
            previous is not None and _STATUS_RANK[status] < _STATUS_RANK[previous]
        ):
            return
        status_changed = previous != status
        # Report executions before a terminal cancellation/rejection, so a
        # partially filled order cannot disappear before its fills arrive.
        if filled > self._last_filled[client_order_id]:
            await self._emit_fills_for_order(venue_order_id, order)
            if self._last_filled[client_order_id] < filled:
                logger.warning("Longbridge executions lag order status; deferring terminal event")
                return
        self._last_status[client_order_id] = status
        if status == OrderStatus.ACCEPTED and status_changed:
            self.generate_order_accepted(order, venue_id, ts_event)
        elif status == OrderStatus.CANCELED and status_changed:
            self.generate_order_canceled(order, venue_id, ts_event)
        elif status == OrderStatus.REJECTED and status_changed:
            self.generate_order_rejected(order, "Longbridge rejected the order", ts_event, False)
        elif status == OrderStatus.EXPIRED and status_changed:
            self.generate_order_expired(order, venue_id, ts_event)

    async def paper_snapshot(self) -> dict[str, Any]:
        """Read collateral, cash and positions without enabling short selling.

        Returns only non-secret risk inputs. This is intentionally unavailable
        for real-account clients; it supports paper-account supervision.
        """
        if not self._lb_config.settings.paper_trading or self._trade is None:
            raise ConfigurationError("paper_snapshot requires a connected paper client")
        started_ns = self.clock.timestamp_ns()
        trade = self._trade
        orders = await self._trade_call(trade.today_orders)
        for value in orders:
            await self._process_order_event(value)
        balances = await self._trade_call(lambda: trade.account_balance(currency="USD"))
        usd = [value for value in balances if str(getattr(value, "currency", "")).upper() == "USD"]
        if len(usd) != 1:
            raise ConfigurationError("paper risk requires one USD account balance")
        balance = usd[0]
        margin = (
            paper_margin_balance(balance) if self._lb_config.account_mode == "MARGIN" else None
        )
        # Do not treat net_assets as spendable cash or silently invent cash
        # availability when a field is not supplied by the broker.
        cash_infos = getattr(balance, "cash_infos", [])
        cash_info = next(
            (v for v in cash_infos if str(getattr(v, "currency", "")).upper() == "USD"), None
        )
        available = getattr(balance, "available_cash", None)
        if available is None and cash_info is not None:
            available = getattr(cash_info, "available_cash", None)
        if available is None:
            available = getattr(balance, "cash_available", None)
        if available is None and margin is None:
            raise ConfigurationError("broker did not supply available USD cash")
        response = await self._trade_call(trade.stock_positions)
        channels = getattr(response, "channels", None)
        values = (
            [p for channel in channels for p in channel.positions]
            if channels is not None
            else getattr(response, "positions", response)
        )
        positions: dict[str, float] = {}
        for value in values:
            positions[str(value.symbol)] = positions.get(str(value.symbol), 0.0) + float(
                value.quantity
            )
        open_orders = []
        open_order_details = []
        unmanaged = []
        fees = getattr(self, "_paper_order_fees", {})
        self._paper_order_fees = fees
        for value in orders:
            qty = as_decimal(getattr(value, "submitted_quantity", getattr(value, "quantity", 0)))
            filled = as_decimal(getattr(value, "executed_quantity", 0))
            if map_status(value.status, filled, qty) not in _TERMINAL_STATUSES:
                open_orders.append(str(value.order_id))
                open_order_details.append({
                    "order_id": str(value.order_id), "symbol": str(value.symbol),
                    "side": enum_name(value.side), "remaining": str(qty - filled),
                    "price": str(getattr(value, "price", "0") or "0"),
                })
                if not self._client_id_for_venue(str(value.order_id)):
                    unmanaged.append(str(value.order_id))
            elif filled > 0 and (client_id := self._client_id_for_venue(str(value.order_id))):
                if client_id not in fees and hasattr(trade, "order_detail"):
                    detail = await self._trade_call(partial(trade.order_detail, str(value.order_id)))
                    charges = getattr(detail, "charge_detail", None)
                    if charges is not None and str(charges.currency).upper() == "USD":
                        amount = as_decimal(charges.total_amount)
                        if amount.is_finite() and amount >= 0:
                            fees[client_id] = float(amount)
        equity = as_decimal(balance.net_assets)
        cash = as_decimal(available) if available is not None else None
        if not equity.is_finite() or equity <= 0 or (
            margin is None and (cash is None or not cash.is_finite() or cash < 0)
        ):
            raise ConfigurationError("invalid paper account balance")
        if margin is not None:
            self._publish_margin_state(balance, margin)
        return {
            **(margin or {"account_mode": "CASH", "account_healthy": True}),
            "equity": float(balance.net_assets),
            "available_cash": float(available) if available is not None else None,
            "positions": positions,
            "open_orders": open_orders,
            "open_order_details": open_order_details,
            "can_short": False,
            "unmanaged_open_orders": unmanaged,
            "order_fees": dict(fees),
            "timestamp_ns": started_ns,
        }

    async def paper_buy_capacity(
        self, symbol: str, order_type: Any, limit_price: Decimal | None = None
    ) -> dict[str, Any]:
        """Read the broker's symbol/order-specific ceiling, never place an order."""
        if not self._lb_config.settings.paper_trading or self._trade is None:
            raise ConfigurationError("buy capacity requires a connected paper client")
        if not symbol.endswith(".US"):
            raise UnsupportedOperation("paper margin currently supports US equities only")
        trade = self._trade
        response = await self._trade_call(lambda: trade.estimate_max_purchase_quantity(
            symbol=symbol, order_type=order_type, side=_sdk_class("OrderSide").Buy,
            price=limit_price, currency="USD", fractional_shares=False,
        ))
        result: dict[str, Any] = {"symbol": symbol, "timestamp_ns": self.clock.timestamp_ns()}
        for name in ("cash_max_qty", "margin_max_qty"):
            raw = getattr(response, name, None)
            if raw is None or str(raw) == "":
                raise ConfigurationError("broker did not supply purchase capacity")
            value = as_decimal(raw)
            if not value.is_finite() or value < 0 or value != value.to_integral_value():
                raise ConfigurationError("invalid broker purchase capacity")
            result[name] = int(value)
        return result

    async def _margin_preflight(self, order: Any, symbol: str, order_type: Any) -> None:
        if not symbol.endswith(".US"):
            raise UnsupportedOperation("paper margin currently supports US equities only")
        snapshot = await self.paper_snapshot()
        if enum_name(order.side).endswith("SELL"):
            reserved = Decimal(0)
            seen = set()
            for pending in snapshot["open_order_details"]:
                seen.add(pending["order_id"])
                if pending["symbol"] == symbol and pending["side"].endswith("SELL"):
                    reserved += as_decimal(pending["remaining"])
            for key, pending in self._pending_orders.items():
                if (
                    symbol_from_instrument_id(pending.instrument_id) == symbol
                    and enum_name(pending.side).endswith("SELL")
                    and self._venue_by_client.get(key) not in seen
                    and self._last_status.get(key) not in _TERMINAL_STATUSES
                ):
                    reserved += pending.quantity.as_decimal() - self._last_filled.get(key, Decimal(0))
            if order.quantity.as_decimal() > as_decimal(snapshot["positions"].get(symbol, 0)) - reserved:
                raise ConfigurationError("paper margin sell may only reduce an existing long")
            return
        if not snapshot["account_healthy"] or snapshot["buying_power"] <= 0:
            raise ConfigurationError("broker margin risk or buying power blocks new entries")
        capacity = await self.paper_buy_capacity(
            symbol, order_type,
            order.price.as_decimal() if getattr(order, "price", None) is not None else None,
        )
        if order.quantity.as_decimal() > capacity["margin_max_qty"]:
            raise ConfigurationError("order exceeds broker margin purchase capacity")
        if not 0 <= self.clock.timestamp_ns() - snapshot["timestamp_ns"] <= 20_000_000_000:
            raise ConfigurationError("margin account snapshot expired during preflight")
        tick = self._fresh_margin_quote(symbol)
        # Recheck the 1x cap after the asynchronous broker queries. Include
        # locally observed fills and pending entries while broker views lag.
        owned = dict(snapshot["positions"])
        local: dict[str, Decimal] = defaultdict(Decimal)
        for key, pending in self._pending_orders.items():
            name = symbol_from_instrument_id(pending.instrument_id)
            sign = 1 if enum_name(pending.side).endswith("BUY") else -1
            local[name] += sign * self._last_filled.get(key, Decimal(0))
        for name, qty in local.items():
            owned[name] = max(as_decimal(owned.get(name, 0)), qty)
        if any(as_decimal(qty) < 0 for qty in owned.values()):
            raise ConfigurationError("unexpected short position requires reconciliation")
        gross = Decimal(0)
        slots = set()
        for name, qty in owned.items():
            if qty:
                if not name.endswith(".US"):
                    raise ConfigurationError("non-US collateral positions require reconciliation")
                slots.add(name)
                gross += as_decimal(qty) * self._fresh_margin_quote(name).ask_price.as_decimal()
        seen = set()
        for pending in snapshot["open_order_details"]:
            seen.add(pending["order_id"])
            if pending["side"].endswith("BUY"):
                name = pending["symbol"]
                slots.add(name)
                ask = self._fresh_margin_quote(name).ask_price.as_decimal()
                gross += as_decimal(pending["remaining"]) * max(ask, as_decimal(pending["price"]))
        for key, pending in self._pending_orders.items():
            if (
                self._venue_by_client.get(key) in seen
                or self._last_status.get(key) in _TERMINAL_STATUSES
                or not enum_name(pending.side).endswith("BUY")
            ):
                continue
            name = symbol_from_instrument_id(pending.instrument_id)
            remaining = pending.quantity.as_decimal() - self._last_filled.get(key, Decimal(0))
            if remaining > 0:
                slots.add(name)
                gross += remaining * self._fresh_margin_quote(name).ask_price.as_decimal()
        slots.add(symbol)
        needed = order.quantity.as_decimal() * tick.ask_price.as_decimal() * Decimal("1.001") + 1
        if len(slots) > 3 or gross + needed > as_decimal(snapshot["equity"]):
            raise ConfigurationError("paper margin entry exceeds 3 positions or 1x equity exposure")
        if needed > as_decimal(snapshot["buying_power"]):
            raise ConfigurationError("paper margin entry exceeds current broker buying power")
        self._last_margin_preflight = {
            "symbol": symbol, "quantity": str(order.quantity),
            "capacity": capacity, "gross_before": str(gross), "entry_budget": str(needed),
            "equity": snapshot["equity"], "timestamp_ns": self.clock.timestamp_ns(),
        }

    def _fresh_margin_quote(self, symbol: str) -> Any:
        tick = self.cache.quote(instrument_id_from_symbol(symbol))
        if tick is None:
            raise ConfigurationError("margin entry requires a complete fresh quote")
        now = self.clock.timestamp_ns()
        ages = [(now - stamp) / 1_000_000_000 for stamp in (tick.ts_event, tick.ts_init)]
        if (
            any(age < 0 or age > self._lb_config.max_quote_age_secs for age in ages)
            or tick.bid_price.as_decimal() <= 0
            or tick.ask_price.as_decimal() < tick.bid_price.as_decimal()
            or tick.bid_size.as_decimal() <= 0 or tick.ask_size.as_decimal() <= 0
        ):
            raise ConfigurationError("margin entry quote is stale or incomplete after preflight")
        return tick

    async def _submit_order(self, command: Any) -> None:
        if self._lb_config.account_mode != "MARGIN":
            await self._submit_order_inner(command)
            return
        key = str(command.order.client_order_id)
        self._margin_preparing.add(key)
        try:
            # Serialize entries; protective SELL orders need not wait for this lock.
            if enum_name(command.order.side).endswith("BUY"):
                async with self._margin_submit_lock:
                    await self._submit_order_inner(command)
            else:
                async with self._margin_exit_lock:
                    await self._submit_order_inner(command)
        finally:
            self._margin_preparing.discard(key)
            if key in self._venue_by_client or key not in self._pending_orders:
                self._margin_cancelled.discard(key)

    async def _submit_order_inner(self, command: Any) -> None:
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
            if self._lb_config.account_mode == "MARGIN":
                await self._margin_preflight(order, symbol, order_type)
                if str(order.client_order_id) in self._margin_cancelled:
                    raise ConfigurationError("entry canceled during margin preflight")
        except Exception as exc:
            self.generate_order_denied(order, str(exc))
            return
        self._pending_orders[str(order.client_order_id)] = order
        self.generate_order_submitted(order)
        try:
            trade = self._trade
            assert trade is not None
            def submit() -> Awaitable[Any]:
                # The rate limiter can wait after preflight. Recheck at the
                # actual transport boundary, before any SDK submission starts.
                if self._lb_config.account_mode == "MARGIN":
                    if str(order.client_order_id) in self._margin_cancelled:
                        raise ConfigurationError("order canceled before transport submission")
                    if enum_name(order.side).endswith("BUY"):
                        self._fresh_margin_quote(symbol)
                        checked = self._last_margin_preflight["timestamp_ns"]
                        if self.clock.timestamp_ns() - checked > 20_000_000_000:
                            raise ConfigurationError("margin preflight expired before submission")
                return trade.submit_order(
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
            response = await self._trade_call(submit)
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
        if str(order.client_order_id) in getattr(self, "_margin_cancelled", set()):
            await self._cancel_order(command)

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
        task.add_done_callback(lambda _: self._reconciliation_tasks.pop(client_order_id, None))

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
                    if client_order_id in getattr(self, "_margin_cancelled", set()):
                        if self._last_status.get(client_order_id) not in _TERMINAL_STATUSES:
                            from types import SimpleNamespace

                            await self._cancel_order(SimpleNamespace(order=order))
                        self._margin_cancelled.discard(client_order_id)
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
        if self._lb_config.account_mode == "MARGIN":
            self.generate_order_modify_rejected(
                order, VenueOrderId(venue_id) if venue_id else None,
                "paper margin amendments require cancel and a newly checked order",
                self.clock.timestamp_ns(),
            )
            return
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

    async def _cancel_all_orders(self, command: Any) -> None:
        instrument_id = getattr(command, "instrument_id", None)
        order_side = getattr(command, "order_side", None)
        strategy_id = getattr(command, "strategy_id", None)
        for order in list(self._pending_orders.values()):
            if instrument_id is not None and order.instrument_id != instrument_id:
                continue
            if order_side is not None and order.side != order_side:
                continue
            if strategy_id is not None and str(order.strategy_id) != str(strategy_id):
                continue
            client_id = str(order.client_order_id)
            if self._last_status.get(client_id) in _TERMINAL_STATUSES:
                continue
            await self._cancel_order(SimpleNamespace(order=order))

    async def _query_account(self, command: Any) -> None:
        await self._emit_account_state()

    async def _query_order(self, command: Any) -> None:
        report = await self._generate_order_status_report(command)
        if report is not None:
            self._handle_report(report)

    async def _submit_order_list(self, command: Any) -> None:
        order_list = getattr(command, "order_list", None)
        orders = list(getattr(order_list, "orders", []) or [])
        if not orders:
            orders = list(getattr(command, "order_inits", []) or [])
        for order in orders:
            self.generate_order_denied(
                order,
                "Longbridge order lists and contingent orders are unsupported",
            )

    async def _cancel_order(self, command: Any) -> None:
        order = command.order
        key = str(order.client_order_id)
        if (
            self._lb_config.account_mode == "MARGIN"
            and key not in self._venue_by_client
            and (key in self._margin_preparing or key in self._pending_orders)
        ):
            self._margin_cancelled.add(key)
            if key not in self._margin_preparing:
                self._schedule_reconciliation(key)
            return
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
        trade = self._trade
        try:
            values = await self._trade_call(
                (lambda: trade.account_balance(currency="USD"))
                if self._lb_config.account_mode == "MARGIN" else trade.account_balance
            )
        except Exception as exc:
            logger.warning("Longbridge account balance unavailable: %s", type(exc).__name__)
            return
        if self._lb_config.account_mode == "MARGIN":
            if len(values) != 1:
                raise ConfigurationError("margin account requires one USD valuation")
            self._publish_margin_state(values[0], paper_margin_balance(values[0]))
            return
        balances: list[AccountBalance] = []
        for value in values:
            currency = Currency.from_str(
                str(getattr(value, "currency", self._lb_config.base_currency)).upper()
            )
            infos = getattr(value, "cash_infos", [])
            if infos:
                for info in infos:
                    cash_currency = Currency.from_str(str(info.currency).upper())
                    free = as_decimal(info.available_cash)
                    locked = as_decimal(getattr(info, "frozen_cash", 0)) + as_decimal(
                        getattr(info, "settling_cash", 0)
                    )
                    if free.is_finite() and locked.is_finite() and free >= 0 and locked >= 0:
                        balances.append(
                            AccountBalance(
                                Money.from_decimal(free + locked, cash_currency),
                                Money.from_decimal(locked, cash_currency),
                                Money.from_decimal(free, cash_currency),
                            )
                        )
            else:
                total = as_decimal(getattr(value, "total_cash", 0))
                free = as_decimal(
                    getattr(value, "available_cash", getattr(value, "cash_available", 0))
                )
                if total.is_finite() and free.is_finite() and 0 <= free <= total:
                    balances.append(
                        AccountBalance(
                            Money.from_decimal(total, currency),
                            Money.from_decimal(total - free, currency),
                            Money.from_decimal(free, currency),
                        )
                    )
        if balances:
            self.generate_account_state(balances, [], True, self.clock.timestamp_ns())

    def _publish_margin_state(self, balance: Any, info: dict[str, Any]) -> None:
        # USD is broker-valued collateral equity, not a fabricated USD cash balance.
        usd = Currency.from_str("USD")
        equity = as_decimal(balance.net_assets)
        initial = as_decimal(balance.init_margin)
        maintenance = as_decimal(balance.maintenance_margin)
        self.generate_account_state(
            [AccountBalance(
                Money.from_decimal(equity, usd), Money.from_decimal(initial, usd),
                Money.from_decimal(equity - initial, usd),
            )],
            [MarginBalance(Money.from_decimal(initial, usd), Money.from_decimal(maintenance, usd))],
            True, self.clock.timestamp_ns(),
            {**info, "balance_semantics": "BROKER_VALUED_COLLATERAL_EQUITY"},
        )

    async def _emit_fills_for_order(self, venue_order_id: str, order: Any) -> None:
        if self._trade is None:
            return
        trade = self._trade
        for value in await self._trade_call(
            lambda: trade.today_executions(order_id=venue_order_id)
        ):
            execution_id = self._execution_id(value)
            if not execution_id or execution_id in self._seen_fills:
                continue
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
            self._seen_fills.add(execution_id)
            if not hasattr(self, "_last_filled"):
                self._last_filled = defaultdict(Decimal)
            key = str(order.client_order_id)
            self._last_filled[key] = (
                self._last_filled.get(key, Decimal(0)) + fill.last_qty.as_decimal()
            )

    def _execution_id(self, value: Any) -> str:
        for field in ("execution_id", "trade_id"):
            raw_value = getattr(value, field, None)
            if raw_value is not None and str(raw_value).strip():
                return str(raw_value)
        raw_key = "|".join(
            (
                str(getattr(value, "order_id", "")),
                str(getattr(value, "symbol", "")),
                str(getattr(value, "price", "")),
                str(getattr(value, "quantity", getattr(value, "executed_quantity", ""))),
                str(getattr(value, "side", "")),
                str(
                    getattr(
                        value,
                        "executed_at",
                        getattr(value, "trade_time", getattr(value, "trade_done_at", "")),
                    )
                ),
            )
        )
        synthetic = f"SYN-{hashlib.blake2b(raw_key.encode(), digest_size=16).hexdigest()}"
        logger.warning(
            "Longbridge execution %s has no execution_id; using synthetic fill ID %s",
            getattr(value, "order_id", "unknown"),
            synthetic,
        )
        return synthetic

    def _client_id_for_venue(self, venue_order_id: str) -> str:
        for client, venue in self._venue_by_client.items():
            if venue == venue_order_id:
                return client
        return ""

    async def _generate_order_status_report(
        self, command: GenerateOrderStatusReport
    ) -> OrderStatusReport | None:
        venue_order_id = getattr(command, "venue_order_id", None)
        client_order_id = getattr(command, "client_order_id", None)
        values = await self._order_values_for_single_report(command)
        for value in values:
            try:
                report = self._order_report(value)
            except UnsupportedOperation:
                continue
            if venue_order_id is not None and str(report.venue_order_id) != str(venue_order_id):
                continue
            if client_order_id is not None and str(report.client_order_id) != str(client_order_id):
                continue
            if venue_order_id is None and client_order_id is None:
                continue
            return report
        return None

    async def _order_values_for_single_report(self, command: Any) -> list[Any]:
        trade = self._trade
        if trade is None:
            return []
        venue_order_id = getattr(command, "venue_order_id", None)
        client_order_id = getattr(command, "client_order_id", None)
        if venue_order_id is None and client_order_id is not None:
            venue_order_id = self._venue_by_client.get(str(client_order_id))
        symbol = None
        if getattr(command, "instrument_id", None) is not None:
            symbol = symbol_from_instrument_id(command.instrument_id)
        if venue_order_id is not None:
            values = await self._trade_call(
                lambda: trade.today_orders(order_id=str(venue_order_id), symbol=symbol)
            )
            if values:
                return values
            return await self._trade_call(lambda: trade.history_orders(symbol=symbol))
        if client_order_id is not None:
            values = await self._trade_call(lambda: trade.today_orders(symbol=symbol))
            values += await self._trade_call(lambda: trade.history_orders(symbol=symbol))
            return values
        return await self._order_values_for_report(
            SimpleNamespace(start=None, end=None, instrument_id=command.instrument_id)
        )

    async def _order_values_for_report(self, command: Any) -> list[Any]:
        if self._trade is None:
            return []
        trade = self._trade
        instrument_id = getattr(command, "instrument_id", None)
        symbol = symbol_from_instrument_id(instrument_id) if instrument_id is not None else None
        start = _command_datetime(getattr(command, "start", None))
        end = _command_datetime(getattr(command, "end", None))
        values = await self._trade_call(lambda: trade.today_orders(symbol=symbol))
        if start is not None or end is not None:
            values += await self._trade_call(
                lambda: trade.history_orders(symbol=symbol, start_at=start, end_at=end)
            )
        return values

    @staticmethod
    def _report_in_range(report: Any, command: Any) -> bool:
        start = _command_ns(getattr(command, "start", None))
        end = _command_ns(getattr(command, "end", None))
        return (
            (start is None or report.ts_last >= start)
            and (end is None or report.ts_last <= end)
        )

    async def _all_execution_values(
        self,
        trade: TradeContext,
        *,
        symbol: str | None,
        order_id: str | None,
    ) -> list[Any]:
        """Read the paginated Longbridge execution history used by NT reports."""
        values: list[Any] = []
        page = 1
        while True:
            async def fetch_page(page_number: int = page) -> Any:
                return await trade.all_executions(
                    symbol=symbol,
                    order_id=order_id,
                    page=page_number,
                )

            response = await self._trade_call(fetch_page)
            page_values = getattr(response, "trades", response)
            values.extend(list(page_values or []))
            if not bool(getattr(response, "has_more", False)):
                return values
            page += 1
            if page > 1000:
                raise RuntimeError("Longbridge execution pagination exceeded 1000 pages")

    async def _generate_order_status_reports(
        self, command: GenerateOrderStatusReports
    ) -> list[OrderStatusReport]:
        result: dict[str, OrderStatusReport] = {}
        for value in await self._order_values_for_report(command):
            try:
                report = self._order_report(value)
            except UnsupportedOperation:
                logger.warning(
                    "Skipping unsupported Longbridge order report %s", getattr(value, "order_id", "")
                )
                continue
            if not self._report_in_range(report, command):
                continue
            if getattr(command, "instrument_id", None) is not None \
                    and report.instrument_id != command.instrument_id:
                continue
            if getattr(command, "open_only", False) and report.order_status in _TERMINAL_STATUSES:
                continue
            key = str(report.venue_order_id)
            previous = result.get(key)
            if previous is None or report.ts_last >= previous.ts_last:
                result[key] = report
        return list(result.values())

    async def _generate_fill_reports(self, command: GenerateFillReports) -> list[FillReport]:
        if self._trade is None:
            return []
        trade = self._trade
        venue_order_id = getattr(command, "venue_order_id", None)
        command_instrument_id = getattr(command, "instrument_id", None)
        symbol = symbol_from_instrument_id(command_instrument_id) if command_instrument_id else None
        start = _command_datetime(getattr(command, "start", None))
        end = _command_datetime(getattr(command, "end", None))
        order_id = str(venue_order_id) if venue_order_id else None
        if start is None and end is None:
            values = await self._all_execution_values(
                trade,
                symbol=symbol,
                order_id=order_id,
            )
        else:
            values = await self._trade_call(
                lambda: trade.today_executions(symbol=symbol, order_id=order_id)
            )
            values += await self._trade_call(
                lambda: trade.history_executions(symbol=symbol, start_at=start, end_at=end)
            )
        result: dict[str, FillReport] = {}
        for value in values:
            if venue_order_id is not None and str(value.order_id) != str(venue_order_id):
                continue
            report = self._fill_report(value)
            if command_instrument_id is not None \
                    and report.instrument_id != command.instrument_id:
                continue
            start_ns = _command_ns(getattr(command, "start", None))
            end_ns = _command_ns(getattr(command, "end", None))
            if not (
                (start_ns is None or report.ts_event >= start_ns)
                and (end_ns is None or report.ts_event <= end_ns)
            ):
                continue
            result[str(report.trade_id)] = report
        return list(result.values())

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
            if (
                getattr(command, "instrument_id", None) is not None
                and instrument_id != command.instrument_id
            ):
                continue
            qty = as_decimal(getattr(value, "quantity", getattr(value, "available_quantity", 0)))
            side = (
                PositionSide.LONG
                if qty > 0
                else PositionSide.SHORT
                if qty < 0
                else PositionSide.FLAT
            )
            reports.append(
                PositionStatusReport(
                    AccountId(self._lb_config.account_id),
                    instrument_id,
                    side,
                    quantity(abs(qty)),
                    self.clock.timestamp_ns(),
                    self.clock.timestamp_ns(),
                    avg_px_open=as_decimal(getattr(value, "cost_price", 0))
                    if getattr(value, "cost_price", None) is not None
                    else None,
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
        if not client_id:
            remark = str(getattr(value, "remark", "") or "")
            if remark.startswith("NT-"):
                client_id = remark.removeprefix("NT-")
        order_type = map_order_type_report(value.order_type)
        submitted_price = getattr(value, "submitted_price", getattr(value, "price", None))
        executed_price = getattr(value, "executed_price", None)
        return OrderStatusReport(
            AccountId(self._lb_config.account_id),
            instrument_id,
            VenueOrderId(str(value.order_id)),
            OrderSide.BUY if "BUY" in enum_name(value.side) else OrderSide.SELL,
            order_type,
            map_tif_report(getattr(value, "time_in_force", None)),
            map_status(getattr(value, "status", None), filled, qty),
            quantity(qty),
            quantity(filled),
            timestamp_ns(getattr(value, "submitted_at", None), self.clock.timestamp_ns()),
            timestamp_ns(getattr(value, "updated_at", None), self.clock.timestamp_ns()),
            self.clock.timestamp_ns(),
            client_order_id=ClientOrderId(str(client_id)) if client_id else None,
            price=(price(submitted_price, precision)
                   if order_type == OrderType.LIMIT and submitted_price is not None else None),
            avg_px=as_decimal(executed_price) if executed_price is not None and filled > 0 else None,
        )

    def _fill_report(self, value: Any, client_order_id: ClientOrderId | None = None) -> FillReport:
        symbol = str(getattr(value, "symbol", ""))
        instrument_id = instrument_id_from_symbol(symbol)
        precision = self._price_precision(instrument_id)
        provider: Any = getattr(self, "instrument_provider", None)
        instrument = provider.find(instrument_id) if provider is not None else None
        currency = Currency.from_str(
            str(
                getattr(
                    value,
                    "currency",
                    getattr(instrument, "quote_currency", self._lb_config.base_currency),
                )
            ).upper()
        )
        logger.warning(
            "Longbridge execution %s has no commission or liquidity-side fields; "
            "emitting structural zero fee and NO_LIQUIDITY_SIDE",
            self._execution_id(value),
        )
        return FillReport(
            AccountId(self._lb_config.account_id),
            instrument_id,
            VenueOrderId(str(value.order_id)),
            TradeId(self._execution_id(value)),
            OrderSide.BUY if "BUY" in enum_name(value.side) else OrderSide.SELL,
            quantity(getattr(value, "quantity", getattr(value, "executed_quantity", 0))),
            price(getattr(value, "price", 0), precision),
            Money.from_decimal(as_decimal(getattr(value, "commission", 0)), currency),
            LiquiditySide.NO_LIQUIDITY_SIDE,
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
        provider: Any = getattr(self, "instrument_provider", None)
        instrument = provider.find(instrument_id) if provider is not None else None
        return int(getattr(instrument, "price_precision", 4))

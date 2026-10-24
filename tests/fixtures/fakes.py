from __future__ import annotations

from collections.abc import Callable
from typing import Any


class FakeQuoteContext:
    def __init__(self) -> None:
        self.quote_callback: Callable[..., Any] | None = None
        self.depth_callback: Callable[..., Any] | None = None
        self.trade_callback: Callable[..., Any] | None = None
        self.bar_callback: Callable[..., Any] | None = None
        self.subscription_calls: list[tuple[list[str], list[Any]]] = []
        self.bar_subscriptions: list[tuple[str, Any, Any]] = []
        self.unsubscriptions: list[tuple[list[str], list[Any]]] = []
        self.trade_values: dict[str, list[Any]] = {}
        self.subscriptions_exception: BaseException | None = None

    def set_on_quote(self, callback: Callable[..., Any]) -> None:
        self.quote_callback = callback

    def set_on_depth(self, callback: Callable[..., Any]) -> None:
        self.depth_callback = callback

    def set_on_trades(self, callback: Callable[..., Any]) -> None:
        self.trade_callback = callback

    def set_on_candlestick(self, callback: Callable[..., Any]) -> None:
        self.bar_callback = callback

    async def subscribe(self, symbols: list[str], sub_types: list[Any]) -> None:
        self.subscription_calls.append((symbols, sub_types))

    async def unsubscribe(self, symbols: list[str], sub_types: list[Any]) -> None:
        self.unsubscriptions.append((symbols, sub_types))

    async def subscribe_candlesticks(
        self, symbol: str, period: Any, trade_sessions: Any = None
    ) -> None:
        self.bar_subscriptions.append((symbol, period, trade_sessions))

    async def unsubscribe_candlesticks(self, symbol: str, period: Any) -> None:
        self.bar_subscriptions = [
            item for item in self.bar_subscriptions if item[:2] != (symbol, period)
        ]

    async def subscriptions(self) -> list[Any]:
        if self.subscriptions_exception is not None:
            raise self.subscriptions_exception
        return self.subscription_calls

    async def static_info(self, symbols: list[str]) -> list[Any]:
        return []

    async def quote(self, symbols: list[str]) -> list[Any]:
        return []

    async def trades(self, symbol: str, count: int) -> list[Any]:
        return self.trade_values.get(symbol, [])[:count]

    async def candlesticks(
        self, symbol: str, period: Any, count: int, adjust_type: Any, trade_sessions: Any = None
    ) -> list[Any]:
        return []

    async def depth(self, symbol: str) -> Any:
        return None


class FakeTradeContext:
    def __init__(self) -> None:
        self.order_callback: Callable[..., Any] | None = None
        self.subscribed_topics: list[Any] = []
        self.unsubscribed_topics: list[Any] = []
        self.submitted: list[dict[str, Any]] = []
        self.replaced: list[dict[str, Any]] = []
        self.canceled: list[str] = []
        self.orders: list[Any] = []
        self.executions: list[Any] = []
        self.balances: list[Any] = []
        self.positions: Any = []
        self.submit_exception: BaseException | None = None
        self.subscribe_exception: BaseException | None = None

    def set_on_order_changed(self, callback: Callable[..., Any]) -> None:
        self.order_callback = callback

    async def subscribe(self, topics: list[Any]) -> None:
        if self.subscribe_exception is not None:
            raise self.subscribe_exception
        self.subscribed_topics.extend(topics)

    async def unsubscribe(self, topics: list[Any]) -> None:
        self.unsubscribed_topics.extend(topics)

    async def today_orders(self, **kwargs: Any) -> list[Any]:
        order_id = kwargs.get("order_id")
        if order_id is None:
            return self.orders
        return [order for order in self.orders if str(getattr(order, "order_id", "")) == order_id]

    async def today_executions(self, **kwargs: Any) -> list[Any]:
        order_id = kwargs.get("order_id")
        if order_id is None:
            return self.executions
        return [
            execution
            for execution in self.executions
            if str(getattr(execution, "order_id", "")) == order_id
        ]

    async def submit_order(self, **kwargs: Any) -> Any:
        self.submitted.append(kwargs)
        if self.submit_exception is not None:
            raise self.submit_exception
        return type("SubmitResponse", (), {"order_id": "123"})()

    async def replace_order(self, **kwargs: Any) -> None:
        self.replaced.append(kwargs)

    async def cancel_order(self, order_id: str, is_attached: bool = False) -> None:
        self.canceled.append(order_id)

    async def account_balance(self, currency: str | None = None) -> list[Any]:
        return self.balances

    async def stock_positions(self, symbols: list[str] | None = None) -> Any:
        return self.positions


class FakeContextFactory:
    def __init__(self) -> None:
        self.quote_context = FakeQuoteContext()
        self.trade_context = FakeTradeContext()
        self.quote_sequence: list[FakeQuoteContext] = [self.quote_context]
        self.trade_sequence: list[FakeTradeContext] = [self.trade_context]

    def quote(self, settings: Any, loop: Any) -> FakeQuoteContext:
        if self.quote_sequence:
            self.quote_context = self.quote_sequence.pop(0)
        return self.quote_context

    def trade(self, settings: Any, loop: Any) -> FakeTradeContext:
        if self.trade_sequence:
            self.trade_context = self.trade_sequence.pop(0)
        return self.trade_context

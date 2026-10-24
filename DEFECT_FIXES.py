"""
长桥-NautilusTrader 连接器缺陷修复示例代码

本文件包含审计报告中发现的所有缺陷的修复代码。
注意: 这些是修复建议，不是完整的可运行代码，需要集成到实际模块中。
"""

# ============================================================================
# 缺陷 #1: 改进的去重机制
# ============================================================================

import time
from collections import deque
from typing import Dict


class TimedDeduplicator:
    """基于时间戳的去重器，避免固定大小 deque 的问题"""

    def __init__(self, ttl_secs: float = 300.0, cleanup_interval: float = 60.0):
        """
        Args:
            ttl_secs: 记录的生存时间（秒）
            cleanup_interval: 清理过期记录的间隔（秒）
        """
        self._seen: Dict[str, float] = {}
        self._ttl = ttl_secs
        self._cleanup_interval = cleanup_interval
        self._last_cleanup = time.monotonic()

    def is_duplicate(self, key: str) -> bool:
        """
        检查是否重复，并记录新的 key

        Returns:
            True 如果是重复，False 如果是新 key
        """
        now = time.monotonic()

        # 定期清理过期记录
        if now - self._last_cleanup > self._cleanup_interval:
            self._cleanup(now)

        # 检查重复
        if key in self._seen:
            return True

        # 记录新 key
        self._seen[key] = now
        return False

    def _cleanup(self, now: float) -> None:
        """清理过期记录"""
        expired_keys = [k for k, timestamp in self._seen.items() if now - timestamp >= self._ttl]
        for key in expired_keys:
            del self._seen[key]
        self._last_cleanup = now

    def size(self) -> int:
        """返回当前记录数"""
        return len(self._seen)


# 使用方式 (在 data.py 中替换现有的 _dedup 方法):
"""
class LongbridgeDataClient:
    def __init__(self, ...):
        # 替换
        # self._seen: deque[str] = deque(maxlen=20_000)
        # self._seen_set: set[str] = set()

        # 使用新的去重器
        self._deduplicator = TimedDeduplicator(ttl_secs=300.0)

    def _dedup(self, key: str) -> bool:
        # 新逻辑：返回 True 表示"不是重复"（与原来相反，需要调整调用处）
        return not self._deduplicator.is_duplicate(key)
"""


# ============================================================================
# 缺陷 #2: 订单提交超时后的 Reconciliation
# ============================================================================

import asyncio
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class OrderReconciliation:
    """订单提交超时后的对账逻辑"""

    @staticmethod
    async def reconcile_uncertain_order(
        client: Any,  # LongbridgeExecutionClient
        client_order_id: str,
        max_attempts: int = 5,
    ) -> bool:
        """
        在订单提交超时后，尝试查询订单状态

        Args:
            client: 执行客户端实例
            client_order_id: 客户端订单 ID
            max_attempts: 最大重试次数

        Returns:
            True 如果成功对账，False 如果失败
        """
        for attempt in range(max_attempts):
            await asyncio.sleep(2 ** attempt)  # 指数退避: 1s, 2s, 4s, 8s, 16s

            try:
                orders = await client._trade_call(lambda: client._trade.today_orders())

                for order in orders:
                    # 尝试通过 client_request_id 匹配
                    raw_client_id = getattr(order, "client_order_id", None) or getattr(
                        order, "client_request_id", None
                    )

                    if raw_client_id:
                        normalized_id = str(raw_client_id).removeprefix("NT-")
                        if normalized_id == client_order_id:
                            # 找到订单！更新状态
                            venue_order_id = str(order.order_id)
                            client._venue_by_client[client_order_id] = venue_order_id

                            # 根据订单状态发送事件
                            pending_order = client._pending_orders.get(client_order_id)
                            if pending_order:
                                from nautilus_trader.model import VenueOrderId
                                client.generate_order_accepted(
                                    pending_order,
                                    VenueOrderId(venue_order_id),
                                    client.clock.timestamp_ns(),
                                )
                                logger.info(
                                    "Reconciliation success: order %s → venue %s",
                                    client_order_id,
                                    venue_order_id,
                                )
                            return True

            except Exception as exc:
                logger.debug(
                    "Reconciliation attempt %d/%d failed: %s",
                    attempt + 1,
                    max_attempts,
                    type(exc).__name__,
                )
                continue

        # 所有尝试失败
        logger.error("Reconciliation failed for order %s after %d attempts", client_order_id, max_attempts)
        return False


# 使用方式 (在 execution.py 的 _submit_order 中):
"""
async def _submit_order(self, command: Any) -> None:
    order = command.order
    # ... 前置验证 ...

    self._pending_orders[str(order.client_order_id)] = order
    self.generate_order_submitted(order)

    try:
        trade = self._trade
        assert trade is not None
        response = await self._trade_call(
            lambda: trade.submit_order(...)
        )
    except TimeoutError as exc:
        # 【修复点】不再直接抛出 SubmissionUncertain
        logger.error(
            "Order submission timeout for %s; starting reconciliation",
            order.client_order_id,
        )
        # 启动异步对账任务
        self.create_task(
            OrderReconciliation.reconcile_uncertain_order(
                self, str(order.client_order_id)
            ),
            f"reconcile-{order.client_order_id}",
        )
        # 保持订单在 SUBMITTED 状态，等待对账结果
        return

    except Exception as exc:
        self._pending_orders.pop(str(order.client_order_id), None)
        self.generate_order_rejected(order, str(exc), self.clock.timestamp_ns(), False)
        return

    # ... 正常流程 ...
"""


# ============================================================================
# 缺陷 #3: 重连期间数据丢失的缓解
# ============================================================================

from datetime import UTC, datetime, timedelta
from typing import Set, Dict, Any, List


class ReconnectionManager:
    """管理重连期间的数据完整性"""

    @staticmethod
    async def backfill_gap(
        client: Any,  # LongbridgeDataClient
        disconnect_time_ns: int,
        reconnect_time_ns: int,
    ) -> None:
        """
        在重连后查询断连期间的历史数据，填补空窗

        Args:
            client: 数据客户端实例
            disconnect_time_ns: 断连时间戳（纳秒）
            reconnect_time_ns: 重连时间戳（纳秒）
        """
        gap_seconds = (reconnect_time_ns - disconnect_time_ns) / 1_000_000_000

        if gap_seconds < 5.0:
            # 短暂断连，SDK 推送应该能覆盖
            return

        logger.warning(
            "Detected %.1fs connection gap; backfilling historical data",
            gap_seconds,
        )

        # 为所有活跃订阅查询历史数据
        for symbol in client._desired_trades:
            try:
                # 查询历史成交（最多 1000 条）
                trades = await client._quote_call(lambda: client._quote.trades(symbol, 1000))

                for item in trades:
                    ts_event_ns = timestamp_ns(getattr(item, "timestamp", None), reconnect_time_ns)
                    # 只处理断连期间的数据
                    if disconnect_time_ns <= ts_event_ns <= reconnect_time_ns:
                        await client._publish_trades(symbol, type("Event", (), {"trades": [item]})())

            except Exception as exc:
                logger.error("Backfill failed for %s: %s", symbol, type(exc).__name__)


# 使用方式 (在 data.py 的 _reconnect_with_backoff 中):
"""
async def _reconnect_with_backoff(self) -> None:
    disconnect_time = self.clock.timestamp_ns()  # 记录断连时间
    delay = self._lb_config.settings.reconnect_initial_secs

    while True:
        try:
            async with self._reconnect_lock:
                loop = asyncio.get_running_loop()
                quote = self._context_factory.quote(self._lb_config.settings, loop)
                self._configure_quote_context(quote)
                self._quote = quote
                await self._restore_subscriptions()

                reconnect_time = self.clock.timestamp_ns()  # 记录重连时间

                # 【修复点】填补数据空窗
                await ReconnectionManager.backfill_gap(
                    self, disconnect_time, reconnect_time
                )
            return

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(...)
            await asyncio.sleep(delay * random.uniform(0.8, 1.2))
            delay = min(self._lb_config.settings.reconnect_max_secs, delay * 2)
"""


# ============================================================================
# 缺陷 #10: Lot Size 验证
# ============================================================================

from decimal import Decimal
from nautilus_trader.model import Quantity


class OrderValidator:
    """订单参数验证器"""

    @staticmethod
    def validate_lot_size(
        order_quantity: Decimal,
        lot_size: Decimal,
        symbol: str,
    ) -> None:
        """
        验证订单数量是否为 lot size 的整数倍

        Args:
            order_quantity: 订单数量
            lot_size: 最小交易单位
            symbol: 证券代码

        Raises:
            ConfigurationError: 如果验证失败
        """
        if order_quantity <= 0:
            raise ConfigurationError("order quantity must be positive")

        if lot_size <= 0:
            raise ConfigurationError(f"invalid lot size {lot_size} for {symbol}")

        # 检查是否为整数倍
        if order_quantity % lot_size != 0:
            raise ConfigurationError(
                f"Order quantity {order_quantity} for {symbol} must be a multiple of lot size {lot_size}. "
                f"Valid quantities: {lot_size}, {lot_size * 2}, {lot_size * 3}, ..."
            )

        # 检查是否至少一手
        if order_quantity < lot_size:
            raise ConfigurationError(
                f"Order quantity {order_quantity} is less than minimum lot size {lot_size} for {symbol}"
            )


# 使用方式 (在 execution.py 的 _submit_order 中):
"""
async def _submit_order(self, command: Any) -> None:
    order = command.order

    try:
        assert_order_enabled(self._lb_config.settings)
        symbol = symbol_from_instrument_id(order.instrument_id)

        # 【修复点】加载 instrument 并验证 lot size
        instrument = self.instrument_provider.find(order.instrument_id)
        if instrument is None:
            # 尝试加载
            if isinstance(self.instrument_provider, LongbridgeInstrumentProvider):
                await self.instrument_provider.load_symbols_async([symbol])
                instrument = self.instrument_provider.find(order.instrument_id)

        if instrument is None:
            raise ConfigurationError(f"Instrument {order.instrument_id} not found")

        lot_size = instrument.lot_size.as_decimal()
        order_quantity = order.quantity.as_decimal()

        # 验证 lot size
        OrderValidator.validate_lot_size(order_quantity, lot_size, symbol)

        # ... 继续原有逻辑 ...

    except Exception as exc:
        self.generate_order_denied(order, str(exc))
        return
"""


# ============================================================================
# 缺陷 #8: 改进的撤单确认
# ============================================================================


class CancelOrderHelper:
    """撤单操作的辅助类"""

    @staticmethod
    async def cancel_and_confirm(
        client: Any,  # LongbridgeExecutionClient
        order: Any,
        venue_id: str,
        max_attempts: int = 10,
        poll_interval: float = 0.3,
    ) -> bool:
        """
        撤单并轮询确认状态

        Args:
            client: 执行客户端实例
            order: 订单对象
            venue_id: 交易所订单 ID
            max_attempts: 最大轮询次数
            poll_interval: 轮询间隔（秒）

        Returns:
            True 如果撤单成功，False 如果失败或订单已完成
        """
        trade = client._trade
        from nautilus_trader.model import VenueOrderId

        # 发送撤单请求
        try:
            await client._trade_call(lambda: trade.cancel_order(venue_id))
        except Exception as exc:
            logger.error("Cancel order failed: %s", exc)
            client.generate_order_cancel_rejected(
                order, VenueOrderId(venue_id), str(exc), client.clock.timestamp_ns()
            )
            return False

        # 轮询确认
        for attempt in range(max_attempts):
            await asyncio.sleep(poll_interval)

            try:
                orders = await client._trade_call(lambda: trade.today_orders(order_id=venue_id))

                if not orders:
                    logger.warning("Order %s not found during cancel confirmation", venue_id)
                    return False

                status = enum_name(getattr(orders[0], "status", None))

                if "CANCEL" in status:
                    # 撤单成功
                    client.generate_order_canceled(
                        order, VenueOrderId(venue_id), client.clock.timestamp_ns()
                    )
                    logger.info("Order %s canceled successfully", venue_id)
                    return True

                elif status in ("FILLED", "FILLED_QTY", "COMPLETE"):
                    # 订单已成交，撤单失败
                    logger.warning("Cancel failed for %s: order already filled", venue_id)
                    # 不发送 cancel_rejected，因为订单已成交（推送事件会处理）
                    return False

                elif status in ("EXPIRED", "REJECTED"):
                    # 订单已终止
                    logger.info("Cancel skipped for %s: order already %s", venue_id, status)
                    return False

            except Exception as exc:
                logger.debug("Cancel confirmation attempt %d failed: %s", attempt, exc)
                continue

        # 超时：依赖推送事件
        logger.warning(
            "Cancel confirmation timeout for %s after %.1fs; relying on push events",
            venue_id,
            max_attempts * poll_interval,
        )
        return False


# 使用方式 (在 execution.py 中替换 _cancel_order):
"""
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

    # 【修复点】使用改进的撤单逻辑
    await CancelOrderHelper.cancel_and_confirm(self, order, venue_id)
"""


# ============================================================================
# 缺陷 #9: 动态价格精度
# ============================================================================

from decimal import Decimal
from typing import Dict


class PrecisionManager:
    """按市场管理价格精度"""

    # 市场精度配置
    MARKET_PRECISION: Dict[str, tuple[int, Decimal]] = {
        "HK": (3, Decimal("0.001")),  # 港股: 3 位小数
        "US": (4, Decimal("0.0001")), # 美股: 4 位小数
        "SH": (2, Decimal("0.01")),   # 上海: 2 位小数
        "SZ": (2, Decimal("0.01")),   # 深圳: 2 位小数
    }

    # 特殊证券的精度覆盖（例如高价股）
    SYMBOL_OVERRIDES: Dict[str, tuple[int, Decimal]] = {
        # "BABA.US": (2, Decimal("0.01")),  # 示例
    }

    @classmethod
    def get_precision(cls, symbol: str) -> tuple[int, Decimal]:
        """
        获取证券的价格精度

        Args:
            symbol: 证券代码（如 "700.HK"）

        Returns:
            (precision, increment) 元组
        """
        # 1. 检查特殊覆盖
        if symbol in cls.SYMBOL_OVERRIDES:
            return cls.SYMBOL_OVERRIDES[symbol]

        # 2. 按市场后缀
        try:
            market = symbol.rsplit(".", 1)[1].upper()
            if market in cls.MARKET_PRECISION:
                return cls.MARKET_PRECISION[market]
        except IndexError:
            pass

        # 3. 默认值
        return (4, Decimal("0.0001"))


# 使用方式 (在 instruments.py 中):
"""
class LongbridgeInstrumentProvider(InstrumentProvider):
    def _to_equity(self, info: Any) -> Equity:
        raw_symbol = str(info.symbol).upper()

        # 【修复点】动态获取精度
        price_precision, price_increment = PrecisionManager.get_precision(raw_symbol)

        instrument = Equity(
            instrument_id=instrument_id,
            raw_symbol=Symbol(raw_symbol),
            currency=currency,
            price_precision=price_precision,
            price_increment=Price.from_decimal_dp(price_increment, price_precision),
            # ...
        )
        return instrument
"""


# ============================================================================
# 辅助函数
# ============================================================================

from nautilus_longbridge.mapping import timestamp_ns
from nautilus_longbridge.mapping import enum_name
from nautilus_longbridge.errors import ConfigurationError


# ============================================================================
# 使用说明
# ============================================================================

"""
集成步骤：

1. 缺陷 #1 (去重)：
   - 将 TimedDeduplicator 类添加到 data.py
   - 在 __init__ 中初始化
   - 替换所有 _dedup() 调用

2. 缺陷 #2 (超时对账)：
   - 将 OrderReconciliation 类添加到 execution.py
   - 在 _submit_order 的 TimeoutError 处理中调用
   - 添加 _reconcile_uncertain_order 任务

3. 缺陷 #3 (重连空窗)：
   - 将 ReconnectionManager 添加到 data.py
   - 在 _reconnect_with_backoff 中调用 backfill_gap
   - 记录断连和重连时间戳

4. 缺陷 #10 (Lot Size)：
   - 将 OrderValidator 添加到 execution.py
   - 在 _submit_order 开头添加验证
   - 确保 instrument_provider 已加载证券信息

5. 缺陷 #8 (撤单确认)：
   - 将 CancelOrderHelper 添加到 execution.py
   - 替换 _cancel_order 方法实现

6. 缺陷 #9 (动态精度)：
   - 将 PrecisionManager 添加到 mapping.py 或新建 precision.py
   - 在 instruments.py 的 _to_equity 中使用
   - 在 data.py 中需要精度的地方查询 instrument

测试建议：

1. 单元测试每个修复的类/函数
2. 集成测试验证端到端流程
3. 纸面交易验证实际效果
4. 压力测试验证性能影响

注意事项：

- 这些修复会增加一定的复杂度和性能开销
- 建议逐个修复并测试，而非一次性全部应用
- 某些修复（如 #2 和 #3）需要在生产环境观察效果
- 所有修复都应该有对应的日志，便于调试
"""

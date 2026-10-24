# 长桥连接器剩余问题清单

**更新日期**: 2026-10-24
**当前版本**: `0.1.2`（本次修复待提交）

---

## 概览

- ✅ **此前已修复**: 4 个（P0: 2, P1: 2）
- ✅ **本次已处理**: 6 个（P1: 2, P2: 4）
- 📊 **总体进度**: 10/10 已处理

本文件原先记录的六项剩余问题已经逐项核对并实现。下面保留原始问题、影响和测试要求，
并在每项标题标记当前状态；实现范围以代码和 README 为准。

---

## P1 原问题（均已处理）

### ✅ 缺陷 #1: 去重机制的边缘情况（已处理）

**位置**: `data.py:267-274`

**问题描述**:
- 使用 `deque(maxlen=20000)` + `set` 做去重
- 满时丢弃最旧的 key，但 hash 碰撞可能导致误判
- 高频行情下可能导致重复或遗漏

**影响范围**: 低概率的重复 QuoteTick/TradeTick

**修复建议**:
```python
# 方案 1: 时间窗口去重（推荐）
class TimedDeduplicator:
    def __init__(self, ttl_secs: float = 300.0):
        self._seen: Dict[str, float] = {}  # key -> timestamp
        self._ttl = ttl_secs

    def is_duplicate(self, key: str) -> bool:
        now = time.monotonic()
        # 清理过期记录
        expired = [k for k, t in self._seen.items() if now - t >= self._ttl]
        for k in expired:
            del self._seen[k]
        # 检查重复
        if key in self._seen:
            return True
        self._seen[key] = now
        return False

# 方案 2: Bloom Filter（更高效，允许误判）
from pybloom_live import BloomFilter
self._bloom = BloomFilter(capacity=100000, error_rate=0.001)
```

**修复工作量**: 4 小时

**测试要求**:
- 高频行情压力测试（1000 symbols × 10 updates/sec）
- 验证内存占用稳定
- 验证无重复事件

---

### ✅ 缺陷 #3: 重连期间的数据丢失（已处理）

**位置**: `data.py:177-197`

**问题描述**:
1. 重连时创建新 context，旧 context 推送的事件丢失
2. 重连期间有数据空窗（几秒到几分钟）
3. 队列中可能有旧 context 的事件

**影响范围**: 短暂重连影响小（< 5 秒），长时间断连会丢数据

**修复建议**:
```python
async def _reconnect_with_backoff(self) -> None:
    disconnect_time_ns = self.clock.timestamp_ns()

    # ... 重连逻辑 ...

    reconnect_time_ns = self.clock.timestamp_ns()
    gap_seconds = (reconnect_time_ns - disconnect_time_ns) / 1e9

    if gap_seconds > 5.0:
        logger.warning("Connection gap %.1fs, backfilling historical data", gap_seconds)
        await self._backfill_gap(disconnect_time_ns, reconnect_time_ns)

async def _backfill_gap(self, start_ns: int, end_ns: int) -> None:
    """查询历史数据填补空窗"""
    for symbol in self._desired_trades:
        try:
            # 查询历史成交
            trades = await self._quote_call(lambda: self._quote.trades(symbol, 1000))
            for item in trades:
                ts = timestamp_ns(getattr(item, "timestamp", None), end_ns)
                if start_ns <= ts <= end_ns:
                    await self._publish_trades(symbol, SimpleNamespace(trades=[item]))
        except Exception as exc:
            logger.error("Backfill failed for %s: %s", symbol, exc)

    # 同样处理 quotes, bars
```

**修复工作量**: 8 小时

**测试要求**:
- 模拟网络中断（5 秒、30 秒、5 分钟）
- 验证历史数据正确回填
- 验证无重复和遗漏

---

## P2 原问题（均已处理）

### ✅ 缺陷 #4: 证券类型检测不完整（已处理）

**位置**: `instruments.py:54-60`

**问题描述**:
- 使用字符串匹配黑名单判断衍生品
- 可能误判或遗漏新类型

**影响范围**: 低（第一阶段限制股票/ETF）

**修复建议**:
```python
# 改用白名单
SUPPORTED_SECURITY_TYPES = frozenset({"STOCK", "ETF", "ETP"})

security_type = str(getattr(info, "security_type", "")).upper()
if security_type not in SUPPORTED_SECURITY_TYPES:
    raise UnsupportedOperation(
        f"Only {SUPPORTED_SECURITY_TYPES} supported, got {security_type!r}"
    )
```

**修复工作量**: 1 小时

---

### ✅ 缺陷 #5: 成交去重依赖 `execution_id`（已处理）

**位置**: `execution.py:468-470`

**问题描述**:
- 如果 API 返回的 `execution_id` 为空，去重失败
- 可能导致重复计算成交

**影响范围**: 中等（依赖 API 质量）

**修复建议**:
```python
execution_id = str(getattr(value, "execution_id", getattr(value, "trade_id", "")))

if not execution_id:
    # 生成合成 ID
    import hashlib
    raw = f"{value.order_id}:{value.price}:{value.quantity}:{getattr(value, 'trade_done_at', '')}"
    execution_id = hashlib.sha256(raw.encode()).hexdigest()[:32]
    logger.warning("Missing execution_id for order %s, using synthetic %s",
                   value.order_id, execution_id)

if execution_id in self._seen_fills:
    logger.debug("Duplicate fill %s, skipping", execution_id)
    continue

self._seen_fills.add(execution_id)
```

**修复工作量**: 2 小时

**测试要求**:
- 模拟 API 返回空 execution_id
- 验证合成 ID 稳定且唯一

---

### ✅ 缺陷 #6: 速率限制器的公平性问题（已处理）

**位置**: `rate_limit.py:23-40`

**问题描述**:
- `asyncio.Lock` 不保证 FIFO
- 高并发时某些请求可能"饥饿"

**影响范围**: 低（实际并发不高）

**修复建议**:
```python
class FairSlidingWindowRateLimiter:
    def __init__(self, limit: int, period_secs: float, *, min_interval_secs: float = 0.0):
        self.limit = limit
        self.period_secs = period_secs
        self.min_interval_secs = min_interval_secs
        self._timestamps: deque[float] = deque()
        self._last = 0.0
        self._lock = asyncio.Lock()
        self._waiters: deque[asyncio.Event] = deque()  # FIFO 队列
        self._processor: asyncio.Task | None = None

    async def acquire(self) -> None:
        event = asyncio.Event()
        self._waiters.append(event)

        # 确保处理器在运行
        if self._processor is None or self._processor.done():
            self._processor = asyncio.create_task(self._process_waiters())

        await event.wait()

    async def _process_waiters(self) -> None:
        while self._waiters:
            event = self._waiters.popleft()

            # 等待窗口可用
            while True:
                async with self._lock:
                    now = time.monotonic()
                    # 清理过期时间戳
                    while self._timestamps and now - self._timestamps[0] >= self.period_secs:
                        self._timestamps.popleft()

                    wait_window = ...
                    wait_interval = ...
                    wait_for = max(wait_window, wait_interval)

                    if wait_for <= 0:
                        self._timestamps.append(now)
                        self._last = now
                        event.set()
                        break

                await asyncio.sleep(wait_for)
```

**修复工作量**: 4 小时

**测试要求**:
- 100 并发请求验证 FIFO
- 压力测试验证无饥饿

---

### ✅ 缺陷 #7: 健康检查失败后的级联重连（已处理）

**位置**: `data.py:155-167`, `execution.py:202-213`

**问题描述**:
1. 健康检查失败立即触发重连
2. 可能同时有多个重连任务
3. 没有连接状态机

**影响范围**: 中等（可能资源泄漏）

**修复建议**:
```python
from enum import Enum

class ConnectionState(Enum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    RECONNECTING = "RECONNECTING"
    FAILED = "FAILED"

class LongbridgeDataClient:
    def __init__(self, ...):
        self._connection_state = ConnectionState.DISCONNECTED
        self._reconnect_lock = asyncio.Lock()

    async def _health_loop(self) -> None:
        while True:
            await asyncio.sleep(self._lb_config.settings.health_check_secs)

            # 只在 CONNECTED 状态做健康检查
            if self._connection_state != ConnectionState.CONNECTED:
                continue

            try:
                await self._quote_call(lambda: self._quote.subscriptions())
            except Exception as exc:
                logger.warning("Health check failed: %s", exc)

                # 防止重复重连
                if self._connection_state == ConnectionState.CONNECTED:
                    self._connection_state = ConnectionState.RECONNECTING
                    self.create_task(self._reconnect_with_backoff(), "reconnect")

    async def _reconnect_with_backoff(self) -> None:
        async with self._reconnect_lock:  # 确保只有一个重连任务
            delay = self._lb_config.settings.reconnect_initial_secs
            max_attempts = 5

            for attempt in range(max_attempts):
                try:
                    # ... 重连逻辑 ...
                    self._connection_state = ConnectionState.CONNECTED
                    logger.info("Reconnection successful")
                    return

                except asyncio.CancelledError:
                    raise

                except Exception as exc:
                    logger.warning("Reconnect attempt %d/%d failed: %s",
                                  attempt + 1, max_attempts, exc)

                    if attempt == max_attempts - 1:
                        self._connection_state = ConnectionState.FAILED
                        logger.error("Reconnection failed after %d attempts", max_attempts)
                        raise

                    await asyncio.sleep(delay * random.uniform(0.8, 1.2))
                    delay = min(self._lb_config.settings.reconnect_max_secs, delay * 2)
```

**修复工作量**: 6 小时

**测试要求**:
- 模拟健康检查失败
- 验证不会创建多个重连任务
- 验证状态转换正确

---

## 📊 修复优先级矩阵

| 缺陷 | 优先级 | 影响 | 概率 | 修复成本 | 推荐顺序 |
|------|--------|------|------|----------|----------|
| #1 去重边缘 | P1 | 中 | 低 | 4h | 3 |
| #3 重连丢失 | P1 | 高 | 中 | 8h | 1 |
| #4 类型检测 | P2 | 低 | 低 | 1h | 5 |
| #5 成交 ID | P2 | 中 | 低 | 2h | 4 |
| #6 速率公平 | P2 | 低 | 低 | 4h | 6 |
| #7 级联重连 | P2 | 中 | 中 | 6h | 2 |

**推荐修复顺序**:
1. 🥇 **#3 重连丢失** - 影响最大，用户体验关键
2. 🥈 **#7 级联重连** - 稳定性基础，影响 #3
3. 🥉 **#1 去重边缘** - P1 要求，相对简单
4. **#5 成交 ID** - 数据准确性
5. **#4 类型检测** - 快速胜利
6. **#6 速率公平** - 可选优化

---

## 🚀 修复路线图

### 第 1 周（P1 核心）
- Day 1-2: 修复 #3 重连丢失（含测试）
- Day 3-4: 修复 #7 级联重连（含状态机）
- Day 5: 集成测试和验证

### 第 2 周（P1 完成）
- Day 1-2: 修复 #1 去重机制
- Day 3: 压力测试（1000 symbols）
- Day 4-5: 文档更新和 Code Review

### 第 3-4 周（P2 优化）
- Week 3: 修复 #5（成交 ID）和 #4（类型检测）
- Week 4: 修复 #6（速率公平），性能调优

---

## 🧪 测试需求

### 新增测试场景

**重连测试**:
```python
async def test_reconnect_backfills_historical_data():
    """验证重连后回填历史数据"""
    # 1. 订阅 700.HK trades
    # 2. 断开连接 30 秒
    # 3. 期间生成 100 笔成交
    # 4. 重连后验证所有成交都收到
```

**去重测试**:
```python
async def test_dedup_handles_high_frequency():
    """验证去重在高频下稳定"""
    # 1. 发送 100k 个 quote 事件
    # 2. 其中 10k 个重复
    # 3. 验证只收到 90k 个唯一事件
```

**级联重连测试**:
```python
async def test_health_check_prevents_duplicate_reconnects():
    """验证健康检查不会触发多个重连"""
    # 1. 模拟健康检查失败
    # 2. 同时触发多次健康检查
    # 3. 验证只有一个重连任务
```

---

## 📈 性能基准

修复后需要验证的性能指标：

| 指标 | 当前 | 目标 | 测试方法 |
|------|------|------|----------|
| 去重内存 | ? | < 10MB | 100k symbols × 1h |
| 去重延迟 | ? | < 1ms | 单次 dedup 调用 |
| 重连时间 | ? | < 5s | 断网后恢复 |
| 回填延迟 | N/A | < 10s | 30s 断连回填 |
| 级联防护 | N/A | 1 task | 并发健康检查 |

---

## 📚 相关文档

- **完整审计**: `AUDIT_REPORT.md`
- **修复验证**: `REVIEW_FIXES.md`
- **修复代码**: `DEFECT_FIXES.py`

---

## ✅ 完成标准

每个缺陷修复必须包含：
1. ✅ 代码实现（通过 ruff/mypy）
2. ✅ 单元测试（覆盖核心逻辑）
3. ✅ 集成测试（覆盖端到端场景）
4. ✅ 文档更新（README/CHANGELOG）
5. ✅ Code Review 通过

## 本次复核验证记录

- `TimedDeduplicator` 使用完整事件 key、300 秒 TTL 和有界容量；测试覆盖 TTL 过期、容量淘汰、
  高频 100 个并发限频等待者以及取消等待者。
- 行情重连记录断连/恢复时间，恢复后通过 `AsyncQuoteContext.trades(symbol, 1000)` 回填活跃
  Trade 订阅，并复用实时发布去重；Quote/Depth 和未完成 Candlestick 无法按时间范围安全回填时
  输出明确 warning。
- 证券类型优先读取显式 `security_type`/`asset_type`，同时识别 SDK 5.2.2 的 `board`；字段缺失
  仍兼容官方 `SecurityStaticInfo`，明确不支持类型会拒绝。
- 成交缺少 execution/trade ID 时生成长度符合 NautilusTrader `TradeId` 限制的稳定合成 ID，
  并通过 `_seen_fills` 去重。
- 行情和交易限频器均按 FIFO 发放，取消 waiter 会从队列移除；两类客户端均使用统一连接状态和
  单一 reconnect task，失败达到 `LONGBRIDGE_RECONNECT_MAX_ATTEMPTS` 后进入 `FAILED`。
- 离线验证：Python 3.13/3.14 均为 41 个测试通过，Ruff、mypy 和 `uv build` 通过；未使用真实 Longbridge 凭证或真实订单。

---

**文档维护者**: Claude Opus 5.5
**最后更新**: 2026-10-24

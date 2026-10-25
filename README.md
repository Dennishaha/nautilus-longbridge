# nautilus-longbridge

独立的 NautilusTrader v2 Longbridge OpenAPI 连接器。当前版本面向测试和纸面交易验证，固定绑定：

- Python 3.13/3.14
- `nautilus_trader==2.0.0rc7`
- `longbridge==5.2.2`

当前连接器版本：`0.1.4`（测试版，不声明生产级稳定性）。

## 安装

```bash
uv venv --python 3.14 .venv
uv pip install --python .venv/bin/python -e '.[dev]'
```

原 NautilusTrader 安装目录不会被修改。适配器依赖固定在项目的独立环境中。

## 配置

复制 `.env.example` 并通过环境变量提供凭证：

```text
LONGBRIDGE_APP_KEY=
LONGBRIDGE_APP_SECRET=
LONGBRIDGE_ACCESS_TOKEN=
LONGBRIDGE_ACCOUNT_ID=
LONGBRIDGE_PAPERTRADING=true
LONGBRIDGE_DRY_RUN=true
LONGBRIDGE_REGION=hk
```

`LONGBRIDGE_ACCOUNT_ID` 可填写长桥原始账户编号。编号不含 `-` 时，连接器会自动添加
`LONGBRIDGE-` 前缀以满足 NautilusTrader 内部账户 ID 的格式要求；已带分隔符的 ID 保持原样。

连接参数还包括 `LONGBRIDGE_HTTP_URL`、`LONGBRIDGE_QUOTE_WS_URL`、
`LONGBRIDGE_TRADE_WS_URL`、`LONGBRIDGE_REQUEST_TIMEOUT_SECS`、
`LONGBRIDGE_RECONNECT_INITIAL_SECS`、`LONGBRIDGE_RECONNECT_MAX_SECS`、
`LONGBRIDGE_RECONNECT_MAX_ATTEMPTS` 和
`LONGBRIDGE_QUEUE_SIZE`。SDK 5.2.2 对 `hk` 和 `cn` 使用官方 global endpoint；显式 URL
会覆盖默认值。`region` 用于校验和市场路由标签。

连接器不会把凭证放入配置对象、`repr`、JSON、异常或日志。默认 `dry_run=True`，提交订单还需要：

```text
LONGBRIDGE_ENABLE_LIVE_SMOKE=1
LONGBRIDGE_ENABLE_ORDER_SMOKE=1
```

真实账户额外需要 `LONGBRIDGE_ENABLE_REAL_TRADING=1`。CI 永远不会连接真实长桥服务。

## LiveNode 注册

```python
from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import TraderId

from nautilus_longbridge import (
    LongbridgeDataClientConfig,
    LongbridgeDataClientFactory,
    LongbridgeExecutionClientConfig,
    LongbridgeExecutionClientFactory,
)

builder = LiveNode.builder("longbridge", TraderId("TRADER-001"), Environment.LIVE)
builder = builder.add_data_client(
    "LONGBRIDGE_DATA",
    LongbridgeDataClientFactory,
    LongbridgeDataClientConfig(),
)
builder = builder.add_exec_client(
    "LONGBRIDGE_EXEC",
    LongbridgeExecutionClientFactory,
    LongbridgeExecutionClientConfig(),  # reads LONGBRIDGE_ACCOUNT_ID
)
node = builder.build()
```

策略只使用 NautilusTrader 的标准订阅、历史数据请求和订单 API。策略层不会接触 Longbridge SDK context。

## 支持矩阵

| 能力 | 港股 | 美股 | A 股 |
| --- | --- | --- | --- |
| 股票/ETF instrument | 支持 | 支持 | 支持指定 symbol |
| Quote/Trade | 支持 | 支持 | 支持 |
| Candlestick | 支持 | 支持 | 支持 |
| Depth | 视行情权限 | 视行情权限 | 视行情权限 |
| 市价/限价下单 | 纸面验证 | 纸面验证 | 暂不开放 |
| Day/GTC/GTD | 取决于市场与账户 | 取决于市场与账户 | 暂不开放 |
| 修改/撤单 | 纸面验证 | 纸面验证 | 暂不开放 |

普通 Quote 推送不含 bid/ask。连接器使用 Quote 与 Depth 组合生成 `QuoteTick`；没有有效顶层买卖盘时不会伪造 QuoteTick。证券的真实 tick size 可能需要通过配置覆盖，默认值只用于第一阶段测试。

第一阶段不支持期权、窝轮、CBBC、组合订单、网格订单、DCA 和其他衍生品能力。

## 设计

- 行情由 `AsyncQuoteContext` 提供，订单由 `AsyncTradeContext` 提供。
- SDK 回调只入有界队列，转换和去重在客户端所属 event loop 执行。
- 行情事件使用完整稳定 key 和 300 秒 TTL 去重，容量有界；TradeId 使用完整输入生成的
  128-bit digest，避免截断摘要导致的碰撞风险。
- Quote 调用限制为每秒 10 次、并发 5；Trade 调用限制为 30 秒 30 次且间隔不少于 20ms。
- 限频等待者按 FIFO 发放，取消等待者会从队列移除，不会阻塞后续请求。
- 订阅状态保存在适配器本地，连接恢复后重放订阅。
- 数据和执行客户端均有 `DISCONNECTED`、`CONNECTING`、`CONNECTED`、`RECONNECTING`、
  `FAILED` 状态；健康检查只触发一个重连任务，重连次数受配置限制。
- 重连后按断连时间和恢复时间回填活跃 Trade 订阅；Quote/Depth 和未完成 Candlestick
  的时间范围不能由 SDK 5.2.2 安全回填时会明确记录日志。
- 下单使用 `client_request_id`，超时不会盲目重复提交。
- 下单数量按 instrument 的 Longbridge board lot 校验；港股非法手数在本地拒绝。
- 提交超时或响应缺少 venue order ID 时保持 `SUBMITTED`，通过稳定 `remark` 查询对账，禁止自动重复下单。
- 撤单后轮询终态确认；订单和成交报告优先使用 instrument 的价格精度。
- 启动 reconciliation 查询订单、成交、持仓和账户资金，并通过 rc7 的 typed reports 送入核心。
- 成交没有 `execution_id`/`trade_id` 时使用订单、标的、价格、数量、方向和成交时间生成
  稳定合成 ID，并继续经过同一去重集合。
- 适配器只通过 NautilusTrader client output API 发送事件，不直接修改 cache 或 engine。

## 开发与验证

```bash
uv run --python .venv/bin/python ruff check .
uv run --python .venv/bin/python mypy src
uv run --python .venv/bin/python pytest
```

纸面 smoke test 默认不会运行。手动运行前必须显式设置 `.env` 中的 opt-in 变量：

```bash
LONGBRIDGE_PAPERTRADING=true \
LONGBRIDGE_ENABLE_LIVE_SMOKE=1 \
LONGBRIDGE_ENABLE_ORDER_SMOKE=1 \
.venv/bin/python examples/paper_execution_smoke.py
```

## 安全

不要提交 `.env`、token、secret、账户信息、行情缓存或日志。提交前检查：

```bash
git status --short
git diff --cached
rg -n --hidden -g '!*.pyc' -g '!.git/**' '(LONGBRIDGE_APP_SECRET|LONGBRIDGE_ACCESS_TOKEN|sk-[A-Za-z0-9])' .
```

## 兼容策略

适配器以本机验证的 NautilusTrader `2.0.0rc7` 行为为准。升级到正式 v2 或其他 rc 版本时，先重新运行 Factory 注册 smoke、配置构造、typed response 和 report 构造测试，再调整依赖范围。

## ORB execution support (0.1.3)

`LongbridgeDataClientConfig(completed_bars_only=True, max_depth_age_secs=2)` can
forward only confirmed candles and reject stale BBO. Depth callbacks now produce
Quote ticks themselves. BBO timestamps use local depth receipt time (including
queue delay), not the timestamp of an unrelated last-trade update. PushDepth does
not provide certified exchange/NBBO timestamps. Unsupported aggregate bar periods
are rejected instead of silently subscribing to 1-minute bars.

Both clients expose `connection_state`. A connected paper execution client offers
`await client.paper_snapshot()` with USD equity, actual available cash, aggregated
positions, open/unmanaged orders and verified terminal-order fee details when
available. It never infers short permission from the CASH account model. Account
cash balances use `cash_infos`, rather than treating net assets as spendable cash.
Missing available cash fails closed in the default CASH mode.

### Paper cross-currency margin (0.1.4)

`LongbridgeExecutionClientConfig(account_mode="MARGIN", paper_trading=True,
base_currency="USD")` explicitly enables the paper margin account model. The
default connector mode remains `CASH`. Real-account margin and non-USD valuation
are rejected by this release.

The broker's USD-valued net equity is published as margin collateral, with its
reported initial/maintenance margins. `AccountState.info` retains separate
per-currency cash details (including negative USD available cash), buying power,
financing limits and risk level. A missing USD cash row stays `None`; it is never
replaced with buying power. These cash deficits are not a separately verified
loan principal or accrued-interest statement.

Before each new US equity buy, the adapter requests the broker's order-specific
`estimate_max_purchase_quantity` (including the market-order type), checks
`margin_max_qty`, current account risk, complete fresh quotes, pending orders,
local fills and a maximum of 3 positions / 1x net-equity exposure. Broker queries
that fail or quotes that expire block entry. Market gaps can still move actual
filled exposure beyond its pretrade estimate.

Sales may only reduce an existing long, with outstanding exits reserved to avoid
opening shorts. Protective sales have a separate submission lock and remain
available when account risk blocks new buys. Cancellation during preflight or
an uncertain submission is retained through acknowledgement/reconciliation;
submission is never blindly retried. Margin amendments require cancel and a new
checked order. This mode does not enable short selling or currency exchange.

`await client.paper_buy_capacity("AAPL.US", OrderType.MO)` is a read-only capacity
query. `paper_snapshot()` in margin mode additionally returns `account_mode`,
`buying_power`, `currency_cash`, margin fields and `account_healthy`.

Execution reports are emitted before partial-cancel terminal events. If the
execution endpoint lags the order status, terminal delivery waits for a later
reconciliation rather than discarding residual fills. SDK Execution has no normal
commission field; consumers must reconcile order `charge_detail` and cannot treat
missing fees as zero. These changes support the separate strategy-lab ORB paper
supervisor; this connector still has no native stop-order implementation.

SDK naive output datetimes are OS-local (`datetime.fromtimestamp(..., None)`),
not UTC wall-clock values. Conversion now preserves that instant before converting
to UTC nanoseconds; timezone-aware timestamps retain their own offset. See the
[upstream conversion](https://github.com/longbridge/openapi/blob/main/python/src/time.rs).

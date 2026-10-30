# nautilus-longbridge

独立的 NautilusTrader v2 Longbridge OpenAPI 连接器。当前版本面向测试和纸面交易验证，固定绑定：

- Python 3.13/3.14
- `nautilus_trader==2.0.0rc8`
- `longbridge==5.2.2`

当前连接器版本：`0.1.9`（测试版，不声明生产级稳定性）。

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

## 有界历史行情请求

策略通过 NT `request_bars` 提供起止时间时，连接器按供应商日期接口分页、限频，检查重复与 OHLCV，
并按事件时间筛选；没有时间范围时保留近期行情接口。复用既有 context 工厂、端点配置和超时控制。
`LongbridgeDataClientConfig` 可配置 `historical_adjustment="NoAdjust"`（或 `"ForwardAdjust"`）与
`historical_evidence_dir`。启用证据目录后，每个请求按关联 ID 留存原始响应和 SHA-256，先记录再校验；
失败保留证据与异常类别。`ts_init` 保留实际获取/标准化时间；不重写为历史收盘时间。

连接器负责长桥协议适配及 NT 标准对象，不依赖研究工具包或策略。
研究审计导入、候选区间选择、预热交易日规划、研究合约占位定义和快照发布由独立的
`nt-data-tools` 项目负责，其 `nt-data fetch` 通过 NT 原生历史请求使用本连接器。
原 `nt-longbridge-catalog` 入口已移除。连接器无需 `exchange-calendars` 或 ML 验收规则。

## K 线落盘：Parquet Data Catalog

实时采集使用 NT rc8 的原生链路：`LongbridgeDataClient -> DataEngine -> StreamingConfig ->
Feather 暂存 -> ParquetDataCatalog`。配置入口：

```python
from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNodeBuilder, LiveNodeConfig
from nautilus_trader.model import TraderId
from nautilus_longbridge import (
    LongbridgeDataClientConfig,
    LongbridgeDataClientFactory,
    bar_catalog_streaming,
)

config = LiveNodeConfig(
    environment=Environment.LIVE,
    trader_id=TraderId("RECORDER-001"),
    streaming=bar_catalog_streaming("./data/catalog"),
)
node = (
    LiveNodeBuilder.from_config("longbridge-recorder", config)
    .add_data_client(
        "LONGBRIDGE_DATA",
        LongbridgeDataClientFactory,
        LongbridgeDataClientConfig(completed_bars_only=True),
    )
    .build()
)
# 添加订阅 EXTERNAL LAST Bar 的策略，然后 node.run()；退出时在 finally 中 node.dispose()。
```

可直接运行只采集行情的示例，凭证沿用 `.env`：

```bash
uv run --python .venv/bin/python --env-file .env examples/record_bars.py \
  --catalog ./data/catalog --period minute AAPL.US 700.HK
```

`--period` 支持 `minute`、`hour`、`day`；已有策略可同时订阅同一标的的多个周期。
采集入口启用 `completed_bars_only=True`，忽略未确认 K 线。同一客户端运行期间按周期保留
最新确认时间，过滤重复推送、重连重放及较旧的确认帧；同一时间的冲突确认值记录警告并保留首值。
默认配置仍允许盘中更新；盘中更新与最终确认分别去重，去重键包括完整 OHLCV。

默认每秒 flush 暂存数据、每 60 秒提交到 Catalog，`promotion_interval_ms` 可调整。
仅 flush 不表示 Parquet 已可查询；周期提交成功后或 `node.dispose()` 关闭写入器后才可查询。
适配器断开连接时先排空已接收的行情队列，再释放消费者。Catalog 同时保留 Equity 定义，
可以直接用于 NT 回测；`ts_event` 保留长桥周期开始时间，`ts_init` 保留本地接收/转换时间。

```python
from nautilus_trader.persistence import ParquetDataCatalog

catalog = ParquetDataCatalog("./data/catalog")
instruments = catalog.instruments()
bars = catalog.query_bars(identifiers=["AAPL.US.LONGBRIDGE-1-MINUTE-LAST-EXTERNAL"])
```

Parquet 位于 `data/catalog/data/bars/<BarType>/`，原始暂存文件保留在
`data/catalog/live/<instance_id>/`。对尚未提交的 Bar 暂存文件，可使用 NT 的
`catalog.convert_stream_to_data(instance_id, NautilusDataType.Bar, environment=Environment.LIVE)`
恢复该次运行（`NautilusDataType` 从 `nautilus_trader.model` 导入）。本项目绑定的 rc8 对
手动重复转换已提交的暂存文件会报告区间重叠，应先核对现有 Catalog，保留原始暂存文件。
不同进程重新收到的相同业务 K 线不会
自动跨运行去重，历史下载及重启期间的缺口也不会由这个实时采集入口自动回补。
Catalog 路径应由一个采集进程写入。项目默认忽略 `data/` 下的本地行情文件。
参见 [NT 原生流式 Catalog 文档](https://nautilustrader.io/docs/latest/how_to/stream_parquet_catalog/)。

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

第一阶段不支持期权、窝轮、CBBC、组合订单、网格订单、DCA 和其他衍生品交易能力。

### 全球期货元数据边界

连接器已经具备把供应商返回的完整期货静态资料转换为 NT `FuturesContract` 的路径，
并会使用合约自己的乘数、价格最小变动、币种、交易所、根合约和到期时间。缺少任一关键
字段时会拒绝加载，绝不把股票、指数或 ETF 当作期货。

当前固定依赖的 `longbridge==5.2.2` Python SDK 没有期货静态资料接口；它只公开证券
静态资料和股票衍生品类型（期权、窝轮）。因此对 `ESZ6.US`、`ESmain.US` 等期货代码，
适配器会明确返回“installed SDK does not expose futures static metadata”，不会生成研究
合约或假数据。若未来官方 quote context 提供 `future_static_info(symbols)`，连接器会自动
优先使用该接口；`nt-data-tools fetch --provider longbridge --instrument-kind futures` 的
NT 入口已经保留，待官方 SDK 和账户权限同时具备后即可接入。

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
- 启动 reconciliation 查询订单、成交、持仓和账户资金，并通过 rc8 的 typed reports 送入核心。
- 成交没有 `execution_id`/`trade_id` 时使用订单、标的、价格、数量、方向和成交时间生成
  稳定合成 ID，并继续经过同一去重集合。
- 适配器只通过 NautilusTrader client output API 发送事件，不直接修改 cache 或 engine。

## NT rc8 标准接口覆盖（0.1.9）

行情客户端已覆盖 rc8 的标准 Instrument、Instruments、Quote、Trade、Bar、L2 book
snapshot、book depth 和 book deltas 请求，以及对应的订阅/取消订阅路径。Longbridge
只提供 L2 price-level depth；L3 MBO、custom data、funding、index、mark、期权和
instrument status/close 请求会明确返回不支持错误。Book deltas 由一次 L2 快照转换为
带 `CLEAR`/`ADD`/`LAST` 标志的 NT `OrderBookDeltas`，不能解释为供应商原生增量流。

执行客户端已覆盖下单、改单、撤单、批量撤单、账户查询、订单查询、订单状态/成交/持仓
报告生成，以及连接和重连后的订单、成交、持仓、账户 reconciliation。Longbridge order
list/contingent orders 仍显式拒绝；成交接口没有佣金和流动性方向字段，因此报告保留结构性
零费用与 `NO_LIQUIDITY_SIDE`，不能据此认定真实费用为零。Instrument 的价格 increment
默认来自连接器配置并在 metadata 标明未由供应商静态资料确认，实际研究和执行前应按市场
规格覆盖配置。

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

### 逐笔与报价语义修正（2026-10-25）

Longbridge 的 `TradeDirection.Up/Down` 是相对上一笔成交价的涨跌，并不认证主动买卖方，
现映射为 NT `NO_AGGRESSOR`，不能拿它计算主动买卖量差/CVD。此前已保存逐笔对象保留原值，需视作旧语义证据。
依据：[官方 trades 字段说明](https://open.longbridge.com/docs/cli/market-data/trades)。
单独请求或订阅逐笔/报价时也会确保真实 Instrument 经连接器送入 NT，避免依赖先请求K线才能填充 Cache。
原生 `request_instrument` 也支持从静态资料发布真实合约；供应商未返回该合约时明确失败，不构造占位合约。

配置 `historical_evidence_dir` 后，`request_quotes` 在转换前记录SDK返回的报价 / 深度选定字段，
保存在 `quotes-<request-id>/manifest.json`，包括空价格、零量、空响应、请求失败类型及实际NT报价行数。
这不是原始网络报文或实时流录制；目录不包含凭证或供应商异常消息。合法性转换规则不因留档而放宽。

现有 TradeId 来自时间/价格/数量/方向/类型的内容摘要，不是交易所唯一成交号；同秒同价同量可能是不同成交。
实时去重未利用供应商 sequence，不能声明完整逐笔流。NT Tick 未携带供应商成交条件及 sequence，盘口本地接收时间也不是交易所/NBBO时间。
这些限制须由采集审计保留；不能因 Tick 能写入 Catalog 就认证逐笔数据的完整性。

适配器以本机验证的 NautilusTrader `2.0.0rc8` 行为为准。rc8 的批量订阅命令使用 `instrument_ids` / `bar_types`，适配器逐项处理并保留每个供应商订阅的独立生命周期；Catalog 重放促销按 rc8 的幂等语义验证。升级到其他版本时，先重新运行 Factory 注册 smoke、配置构造、typed response 和 report 构造测试，再调整依赖范围。

## Market data and paper execution support (0.1.3)

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
missing fees as zero. This connector still has no native stop-order implementation.

SDK naive output datetimes are OS-local (`datetime.fromtimestamp(..., None)`),
not UTC wall-clock values. Conversion now preserves that instant before converting
to UTC nanoseconds; timezone-aware timestamps retain their own offset. See the
[upstream conversion](https://github.com/longbridge/openapi/blob/main/python/src/time.rs).

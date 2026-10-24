# nautilus-longbridge

独立的 NautilusTrader v2 Longbridge OpenAPI 连接器。当前版本面向测试和纸面交易验证，固定绑定：

- Python 3.13/3.14
- `nautilus_trader==2.0.0rc7`
- `longbridge==5.2.2`

当前连接器版本：`0.1.0`（测试版，不声明生产级稳定性）。

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
LONGBRIDGE_REGION=hk
```

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
- Quote 调用限制为每秒 10 次、并发 5；Trade 调用限制为 30 秒 30 次且间隔不少于 20ms。
- 订阅状态保存在适配器本地，连接恢复后重放订阅。
- 下单使用 `client_request_id`，超时不会盲目重复提交。
- 启动 reconciliation 查询订单、成交、持仓和账户资金，并通过 rc7 的 typed reports 送入核心。
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

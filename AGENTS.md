# 连接器职责边界

用户要求：**lab 只从 NT 获取数据；连接器保持通用职责，研究数据任务移出连接器。**

- 本包只负责供应商 SDK、凭证/端点、请求分页与限频、连接生命周期、原始响应记录及 NT 标准对象转换；不反向依赖工具层或 lab。
- 通用历史请求在 `data.py` / `history.py`，可选供应商响应留档在 `evidence.py`；使用现有 context 工厂、配置和请求控制。
- 不加入 lab 报告解析、候选研究区间、ML/总回报验收、RVOL/预热策略、研究 Instrument 占位定义或研究快照发布命令。
- 这些上层职责属于独立的 `nt-data-tools`。连接器不能依赖该包、`etf_rotation`、`orb` 或 `exchange_calendars`。
- 实时录制沿用现有 DataEngine/StreamingConfig；通用录制辅助可以保留，研究快照契约不放进连接器。
- 保留事件时间和实际获取时间，不改价、不混复权、不静默覆盖冲突。
- 此数据工作不涉及下单。修改后运行 pytest、Ruff、mypy；职责回归在 `tests/unit/test_architecture.py`。

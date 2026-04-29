# QAgent Agent 使用说明与最佳实践

本文面向调用 QAgent 的 coding / research agent。QAgent 的定位是本地优先、单用户、低频量化研究执行引擎：agent 通过 REST API 或 MCP 工具完成研究链路，human 通过 React UI 对结果做可视化和量化验收。V2.0 支持 `US` 和 A 股 `CN` 两个 market；旧调用不传 `market` 时必须按 `US` 处理。

## 1. 工作边界

QAgent 负责：

- 行情数据管理、股票池管理、因子研究、特征工程、标签定义、模型训练、策略回测、信号生成、模拟交易。
- 保存可复现的研究资产，包括源码、配置、依赖、指标、诊断和任务结果。
- 提供 REST API、MCP 工具和 React UI，三者必须共享同一套后端服务层。

QAgent 不负责：

- 实盘券商下单、撮合所级别仿真、高频交易、多人协作权限系统。
- 用界面绕过服务层写入数据。
- 用随机 K-Fold 等不适合时序金融数据的验证方式制造虚假指标。

## 2. Market Scope 规则

所有 agent 调用都应显式带 `market`，除非正在验证旧系统兼容性。

- `market="US"`：美股，默认数据源为 yfinance，常用 benchmark 为 `SPY`。
- `market="CN"`：A 股，当前数据源为 BaoStock，ticker 使用 BaoStock 原生代码，如 `sh.600000`、`sz.000001`，默认 benchmark 为 `sh.000300`。
- 不允许把 US group / factor / label / feature set / model / strategy / benchmark 用在 CN 回测、信号或 paper trading 中。
- 所有长任务返回 `task_id` 后，通过 `/api/tasks/{task_id}` 轮询；MCP 工具会尽量返回 `market`、`asset_scope`、`poll_url`，agent 应记录这些字段。
- 发现 REST、MCP、UI 入口 market 行为不一致时，优先记录到 `docs/backlog.md`，再修服务层。

## 3. 启动与健康检查

优先使用项目脚本：

```bash
scripts/start.sh
scripts/stop.sh
```

分别启动时：

```bash
uv run uvicorn backend.app:app --host 127.0.0.1 --port 8000 --reload
cd frontend && pnpm dev
```

常用入口：

- API: `http://127.0.0.1:8000/api`
- UI: `http://localhost:5173`
- MCP: `http://127.0.0.1:8000/mcp/`
- 任务轮询: `GET /api/tasks/{task_id}`

长任务必须走 `TaskExecutor`，返回 `task_id` 后轮询 `/api/tasks/{task_id}`。不要在 API handler、MCP tool 或前端请求里同步阻塞训练、回测、因子计算、信号诊断、paper trading 推进。

## 4. Agent 标准研究链路

### 4.1 数据就绪

先确认本地数据覆盖目标股票池和日期窗口。优先小范围补数，不要默认全量刷新。

验收要点：

- 数据截止日期覆盖回测 / 信号目标日期。
- 股票池成分不为空。
- 缺失、停牌、异常价格有明确处理方式。

### 4.2 因子与特征

自定义因子必须继承 `FactorBase`，实现 `compute(data: pd.DataFrame) -> pd.Series`。因子计算和评估应通过服务层和任务系统执行。

验收要点：

- 因子无未来函数，窗口只使用决策日可见数据。
- 因子评估保留 IC、IR、分组收益、覆盖率等结果。
- 特征集记录因子引用、预处理配置和依赖快照。

### 4.3 模型训练

训练前确认标签定义、时间切分、股票池和特征集。市场时序任务不得使用随机 K-Fold。

验收要点：

- 使用 time split、rolling、expanding、purge-gap 或 calendar-aware 方案。
- 模型资产保留训练配置、特征集、标签、数据窗口和评估指标。
- 模型预测失败、缺失或依赖不完整时要显式报错或在诊断里暴露，不能静默 no-op。
- 同日候选竞争模型使用 `objective_type="ranking"`、`"pairwise"` 或 `"listwise"`。V2.0 中 `pairwise` 是 LambdaRank 支撑的用户目标别名，模型 metadata 应记录 `pairwise_mode="lambdarank"`；不要声称已经实现 true pair-sampling learner，除非另有明确实现和验证。

### 4.4 策略与回测

自定义策略必须继承 `StrategyBase`，输出以 ticker 为 index 的 DataFrame，列包含 `signal`、`weight`、`strength`。

验收要点：

- 策略声明 `required_factors()` 和 `required_models()`；源码中直接引用的 model id 也应被系统解析、注入和校验。
- 回测遵循 T+1 / open-price 执行语义。
- 回测保存 trades、NAV、drawdown、summary、rebalance diagnostics。
- 对 stateful 策略，重点检查当前持仓、holding days、unrealized PnL 是否按同一语义传给 strategy context。

### 4.5 信号诊断

信号生成用于生产候选结果，`diagnose_signals` 用于解释为什么某票进入或没有进入组合。

验收要点：

- `model_diagnostics` 能看到 required / injected / missing 模型状态。
- `strategy_diagnostics` 能看到候选池、选中项、当前持仓占位、转换候选、阻塞原因。
- focus ticker 的模型分数、因子快照、候选池成员关系可直接定位问题。

### 4.6 模拟交易与对账

paper trading 是 live-like forward test，不是另一个策略实现。它必须复用同一服务层、同一策略执行语义和同一 T+1/open-price 规则。

验收要点：

- 新 session 第一天只记录 baseline，首笔交易在下一个可执行交易日。
- `GET /api/paper-trading/sessions/{id}/daily` 返回 NAV、cash、position_count、trade_count。
- `GET /api/paper-trading/sessions/{id}/positions?date=YYYY-MM-DD` 能回看历史持仓快照。
- `GET /api/paper-trading/sessions/{id}/compare-backtest/{backtest_id}` 能逐日对齐 paper 和 backtest 的交易、目标仓位、NAV 差异。

## 5. Human 验收方式

Human 不直接相信 agent 的文字结论，优先看 UI 和可量化证据。Agent 完成一次研究或修复后，应给出以下验收材料：

- 资产 ID：factor_id、feature_set_id、label_id、model_id、strategy_id、backtest_id、paper_session_id、task_id。
- 配置快照：股票池、日期窗口、资金、成本、rebalance frequency、max_positions、模型/标签/特征配置。
- 核心指标：IC/IR、AUC、Sharpe、max drawdown、annual turnover、total return、trade count、final NAV、paper/backtest delta。
- 诊断入口：相关 UI 页面、API endpoint、对账接口、focus ticker 诊断结果。
- 验证命令：至少包含后端单元测试、前端构建或对应的 live API 复验。

UI 变更需要浏览器验收；后端逻辑变更需要 API 或服务层测试；数据敏感变更需要用真实本地数据做窄范围复验。

## 6. API/MCP 调用规范

优先调用高层服务入口，不要直接写 DuckDB 绕过服务层。

通用异步模式：

1. 调用创建/执行 endpoint 或 MCP tool。
2. 获取 `task_id`。
3. 轮询 `/api/tasks/{task_id}`。
4. 状态为 `completed` 后读取持久化资产详情。
5. 把资产 ID、配置和指标写入结论。

错误处理：

- `failed` 任务必须读取 `error`，不要重试到覆盖原始错误。
- MCP 输入校验错误应包含可修复字段信息；例如非法 `market` 会返回 `Invalid MCP request` 并提示允许值 `US, CN`。
- 数据锁或 DuckDB 写锁问题优先确认是否已有服务进程在运行。
- 缺数据时先缩小股票池和日期窗口复现，再决定是否补数。

### 6.1 MCP 常用示例

CN 数据更新：

```python
update_data(mode="incremental", market="CN")
```

CN 因子评估：

```python
evaluate_factor(
    factor_id="<cn_factor_id>",
    label_id="<cn_label_id>",
    universe_group_id="cn_all_a",
    start_date="2024-01-02",
    end_date="2024-03-29",
    market="CN",
)
```

CN ranking / listwise 模型训练：

```python
train_model(
    name="cn_ranker_v1",
    feature_set_id="<cn_feature_set_id>",
    label_id="<cn_rank_label_id>",
    model_type="lightgbm",
    model_params={},
    train_config={
        "train_start": "2024-01-02",
        "train_end": "2024-06-28",
        "valid_start": "2024-07-01",
        "valid_end": "2024-08-30",
        "test_start": "2024-09-02",
        "test_end": "2024-10-31",
    },
    universe_group_id="cn_all_a",
    market="CN",
    objective_type="listwise",
    ranking_config={"query_group": "date", "eval_at": [5, 10], "min_group_size": 5},
)
```

CN 回测：

```python
run_backtest(
    strategy_id="<cn_strategy_id>",
    universe_group_id="cn_all_a",
    market="CN",
    config_json='{"start_date":"2024-01-02","end_date":"2024-12-31","benchmark":"sh.000300"}',
)
```

## 7. 开发最佳实践

### 7.1 服务层优先

涉及 REST、MCP、UI 多入口的行为，先改 `backend/services/`，再让 API、MCP、前端共享同一服务方法。不要在路由层复制业务逻辑。

### 7.2 时间序列正确性优先

- 不引入 look-ahead bias。
- 决策日只能使用当时可见数据。
- 回测、信号、paper trading 的执行日和价格语义必须一致。
- 对 stateful 策略要明确区分策略目标仓位、实际成交仓位和估值权重。

### 7.3 可复现优先

每个研究产物都应能回答：

- 用了哪些数据窗口和股票池？
- 用了哪些因子、特征、标签、模型和策略源码？
- 当时的配置是什么？
- 哪个任务生成了它？
- human 可以在哪个 UI 页面复验？

### 7.4 小范围复验优先

先用少量 ticker、短日期窗口、单个策略或单个 focus ticker 定位问题。只有明确需要时才做全量数据更新、全股票池训练或长窗口回测。

### 7.5 前端类型同步

新增或修改 API response 字段时，同步更新 `frontend/src/api/index.ts` 和相关组件。前端 TypeScript 开启 strict、noUnusedLocals、noUnusedParameters，必须跑 `cd frontend && pnpm build`。

## 8. 问题和需求记录位置

所有未闭环问题、需求、验收缺口统一记录到：

```text
docs/backlog.md
```

记录规则：

- 发现问题后先复现，再记录；不能只写猜测。
- 每条记录必须包含日期、类型、优先级、影响范围、复现步骤、当前证据、期望行为、验收标准。
- 如果问题来自 human 反馈，保留原始表达和具体页面 / API / 资产 ID。
- 如果临时不修，明确写入原因和重新评估条件。
- 修复后不要直接删除记录，先移动到 Done，并补充 commit、验证命令和复验证据。

## 9. 交付前检查清单

后端或服务层变更：

```bash
uv run python -m unittest discover -s tests -v
uv run python -m compileall backend tests
```

前端变更：

```bash
cd frontend && pnpm build
```

通用检查：

```bash
git diff --check
git status --short
```

需要 live 复验时，启动本地服务后用 API 或 UI 验证原始问题。最终回复必须说明实际跑过的命令、结果、残余风险和相关资产 ID。

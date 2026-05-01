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
- `market="CN"`：A 股，当前数据源为 BaoStock，ticker 使用 BaoStock 原生代码，如 `sh.600000`、`sz.000001`，默认 benchmark 为 `sh.000300`，默认股票池为 `cn_a_core_indices_union`。
- CN 默认股票池由上证50、沪深300、中证500、创业板指成分股去重并集构成。`update_data(mode="incremental", market="CN")` 和 `refresh_stock_list(market="CN")` 都以这个并集为市场级 universe，不使用 BaoStock 全 A 列表作为默认下载范围。
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
- 模型 metadata 保留 `feature_lineage`，用训练列名追溯到 `factor_id` / `factor_name`。声明但因覆盖率不足未进入矩阵的因子会记录为 missing；训练列未在 feature set 声明时必须阻断保存。
- 模型预测失败、缺失或依赖不完整时要显式报错或在诊断里暴露，不能静默 no-op。
- 同日候选竞争模型使用 `objective_type="ranking"`、`"pairwise"` 或 `"listwise"`。V2.0 中 `pairwise` 是 LambdaRank 支撑的用户目标别名，模型 metadata 应记录 `pairwise_mode="lambdarank"`；不要声称已经实现 true pair-sampling learner，除非另有明确实现和验证。ranking/listwise 的默认 `label_gain` 为 `ordinal`，会把同日 rank 标签映射为 dense non-negative relevance label；只有已经是 dense relevance label 时才使用 `label_gain="identity"`。

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

## 5. A 股研究全流程

A 股链路使用 `market="CN"`，默认 universe 是 `cn_a_core_indices_union`。这组股票池由上证50、沪深300、中证500、创业板指去重得到，当前种子并集为 `806` 支。Agent 做 A 股研究时不要改用 `cn_all_a`，除非任务明确要求全市场扫描。

### 5.1 数据和股票池

先刷新股票池，再做数据更新：

```python
refresh_index_groups(market="CN")
refresh_stock_list(market="CN")
update_data(mode="incremental", market="CN")
```

验收点：

- `GET /api/groups/cn_a_core_indices_union?market=CN` 的 `member_count` 大于 `700`，当前正常值为 `806`。
- 股票代码保持 BaoStock 格式，例如 `sh.600519`、`sz.300750`。
- `update_data(mode="incremental", market="CN")` 对新股票回补 10 年数据，对已有股票从本地最新交易日后增量补齐。

如果 BaoStock 或创业板页面临时不可用，`GroupService` 会用内置真实种子填充核心指数分组。不要因为外部源短时失败就把 CN 股票池改成全 A。

### 5.2 A 股因子

A 股因子和美股因子共用 `FactorBase` 协议，但资产必须按 market 隔离。创建或评估因子时都传 `market="CN"`，股票池用 `cn_a_core_indices_union`。

推荐顺序：

1. 用短窗口和少量 ticker 验证因子无未来函数。
2. 创建 CN factor asset。
3. 用 CN label 做因子评估。
4. 再把稳定因子加入 CN feature set。

REST / MCP 评估示例：

```python
evaluate_factor(
    factor_id="<cn_factor_id>",
    label_id="<cn_label_id>",
    universe_group_id="cn_a_core_indices_union",
    start_date="2024-01-02",
    end_date="2024-06-28",
    market="CN",
)
```

验收点：

- factor、label、feature set 都返回 `market="CN"`。
- 评估结果写入 CN 范围，不混用 US label 或 US group。
- 因子覆盖率和 IC 序列可在 UI 或 API 中复验。

### 5.3 A 股模型

模型训练必须显式传 `market="CN"`、`universe_group_id="cn_a_core_indices_union"`。时间切分使用交易日顺序，保留 purge gap；不要用随机 K-Fold。

回归 / 分类模型示例：

```python
train_model(
    name="cn_baseline_lgbm_v1",
    feature_set_id="<cn_feature_set_id>",
    label_id="<cn_return_or_binary_label_id>",
    model_type="lightgbm",
    model_params={},
    train_config={
        "train_start": "2020-01-02",
        "train_end": "2023-12-29",
        "valid_start": "2024-01-02",
        "valid_end": "2024-06-28",
        "test_start": "2024-07-01",
        "test_end": "2024-12-31",
        "purge_gap": 20,
    },
    universe_group_id="cn_a_core_indices_union",
    market="CN",
)
```

同日候选竞争模型使用 ranking 目标：

```python
train_model(
    name="cn_ranker_v1",
    feature_set_id="<cn_feature_set_id>",
    label_id="<cn_rank_or_return_label_id>",
    model_type="lightgbm",
    model_params={},
    train_config={
        "train_start": "2020-01-02",
        "train_end": "2023-12-29",
        "valid_start": "2024-01-02",
        "valid_end": "2024-06-28",
        "test_start": "2024-07-01",
        "test_end": "2024-12-31",
        "purge_gap": 20,
    },
    universe_group_id="cn_a_core_indices_union",
    market="CN",
    objective_type="listwise",
    ranking_config={"query_group": "date", "eval_at": [5, 10, 20], "min_group_size": 20},
)
```

ranking/listwise 默认使用 `label_gain="ordinal"`，rank 类标签会先按同日候选池转成 dense relevance label，再交给 LightGBM。只有当标签本身已经是 `0..N` 的 dense 非负整数 relevance label 时，才显式设置 `ranking_config={"label_gain":"identity"}`。

验收点：

- `/api/tasks/{task_id}` 返回 `completed`，且 `GET /api/models/{model_id}?market=CN` 可读取模型。
- metadata 记录 feature set、label、股票池、训练窗口、ranking_config、`feature_lineage` 和评估指标。
- 模型预测缺失时策略、信号、回测必须显式暴露 missing model，不允许静默退化。

### 5.4 A 股策略

策略源码仍继承 `StrategyBase`。CN 策略创建必须传 `market="CN"`，策略依赖的因子名和模型 id 也必须属于 CN。

极简 no-op 策略可作为创建链路 smoke test：

```python
class NoopCnStrategy(StrategyBase):
    def generate_signals(self, context):
        return pd.DataFrame(columns=["signal", "weight", "strength"])
```

创建后应能通过：

```text
GET /api/strategies?market=CN
GET /api/strategies/{strategy_id}?market=CN
```

CN 策略创建已经走正式 `strategies` 表 market 隔离约束，策略名版本唯一性按 `(market, name, version)` 判定。正式交付必须有 `strategy_id` 和后续 `backtest_id`；临时脚本只能作为诊断材料。

### 5.5 A 股回测

正式 CN 回测必须使用服务层保存到 `backtest_results`，不接受只输出本地 JSON 的临时研究结果作为最终产物。

回测示例：

```python
run_backtest(
    strategy_id="<cn_strategy_id>",
    universe_group_id="cn_a_core_indices_union",
    market="CN",
    config_json='{"start_date":"2024-01-02","end_date":"2024-12-31","benchmark":"sh.000300","rebalance_freq":"weekly"}',
)
```

验收点：

- 任务完成后有 `backtest_id`。
- `GET /api/strategies/backtests?market=CN` 能看到记录。
- `GET /api/strategies/backtests/{backtest_id}?market=CN` 返回 summary、NAV、drawdown、trades、config。
- config 中能追溯 `universe_group_id="cn_a_core_indices_union"`、benchmark、日期窗口、交易成本和调仓频率。
- summary 中包含 `reproducibility_fingerprint`，列表接口暴露轻量 `reproducibility_hash`，用于比较同配置复跑是否可比。

组合底仓 + 增强卫星使用 `portfolio_overlay` 配置。当前支持 `base_leg="equal_weight"` 和 `base_leg="core_union_equal_weight"`，系统会用同一股票池构建等权底仓，再与策略 overlay NAV 按权重合成，并在回测详情中保存 base leg、overlay leg、权重、总 NAV 和分腿贡献。

```python
run_backtest(
    strategy_id="<cn_strategy_id>",
    universe_group_id="cn_a_core_indices_union",
    market="CN",
    config_json='{"start_date":"2024-01-02","end_date":"2024-12-31","benchmark":"sh.000300","rebalance_freq":"weekly","portfolio_overlay":{"base_leg":"core_union_equal_weight","base_weight":0.65,"overlay_weight":0.35}}',
)
```

### 5.6 A 股交付材料

A 股研究交付至少包含：

- `group_id`: 固定为 `cn_a_core_indices_union`，并记录成员数。
- `factor_id` / `feature_set_id` / `label_id` / `model_id` / `strategy_id` / `backtest_id`。
- 数据窗口、训练窗口、回测窗口、benchmark、成本、调仓频率。
- 因子 IC/IR、模型 ranking 或回归指标、回测 Sharpe、最大回撤、总收益、交易数。
- UI 或 API 复验入口。
- 如果某一步被 backlog 阻断，明确写出 backlog 条目标题和可复现命令。

## 6. Human 验收方式

Human 不直接相信 agent 的文字结论，优先看 UI 和可量化证据。Agent 完成一次研究或修复后，应给出以下验收材料：

- 资产 ID：factor_id、feature_set_id、label_id、model_id、strategy_id、backtest_id、paper_session_id、task_id。
- 配置快照：股票池、日期窗口、资金、成本、rebalance frequency、max_positions、模型/标签/特征配置。
- 核心指标：IC/IR、AUC、Sharpe、max drawdown、annual turnover、total return、trade count、final NAV、paper/backtest delta。
- 诊断入口：相关 UI 页面、API endpoint、对账接口、focus ticker 诊断结果。
- 验证命令：至少包含后端单元测试、前端构建或对应的 live API 复验。

UI 变更需要浏览器验收；后端逻辑变更需要 API 或服务层测试；数据敏感变更需要用真实本地数据做窄范围复验。

## 7. API/MCP 调用规范

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

### 7.1 MCP 常用示例

CN 数据更新：

```python
update_data(mode="incremental", market="CN")
```

CN 核心股票池刷新：

```python
refresh_stock_list(market="CN")
refresh_index_groups(market="CN")
```

CN 因子评估：

```python
evaluate_factor(
    factor_id="<cn_factor_id>",
    label_id="<cn_label_id>",
    universe_group_id="cn_a_core_indices_union",
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
    universe_group_id="cn_a_core_indices_union",
    market="CN",
    objective_type="listwise",
    ranking_config={
        "query_group": "date",
        "eval_at": [5, 10],
        "min_group_size": 5,
        "label_gain": "ordinal",
    },
)
```

CN 回测：

```python
run_backtest(
    strategy_id="<cn_strategy_id>",
    universe_group_id="cn_a_core_indices_union",
    market="CN",
    config_json='{"start_date":"2024-01-02","end_date":"2024-12-31","benchmark":"sh.000300"}',
)
```

CN 组合层回测：

```python
run_backtest(
    strategy_id="<cn_strategy_id>",
    universe_group_id="cn_a_core_indices_union",
    market="CN",
    config_json='{"start_date":"2024-01-02","end_date":"2024-12-31","benchmark":"sh.000300","portfolio_overlay":{"base_leg":"core_union_equal_weight","base_weight":0.65,"overlay_weight":0.35}}',
)
```

## 8. 开发最佳实践

### 8.1 服务层优先

涉及 REST、MCP、UI 多入口的行为，先改 `backend/services/`，再让 API、MCP、前端共享同一服务方法。不要在路由层复制业务逻辑。

### 8.2 时间序列正确性优先

- 不引入 look-ahead bias。
- 决策日只能使用当时可见数据。
- 回测、信号、paper trading 的执行日和价格语义必须一致。
- 对 stateful 策略要明确区分策略目标仓位、实际成交仓位和估值权重。

### 8.3 可复现优先

每个研究产物都应能回答：

- 用了哪些数据窗口和股票池？
- 用了哪些因子、特征、标签、模型和策略源码？
- 当时的配置是什么？
- 哪个任务生成了它？
- human 可以在哪个 UI 页面复验？

### 8.4 小范围复验优先

先用少量 ticker、短日期窗口、单个策略或单个 focus ticker 定位问题。只有明确需要时才做全量数据更新、全股票池训练或长窗口回测。

### 8.5 前端类型同步

新增或修改 API response 字段时，同步更新 `frontend/src/api/index.ts` 和相关组件。前端 TypeScript 开启 strict、noUnusedLocals、noUnusedParameters，必须跑 `cd frontend && pnpm build`。

## 9. 问题和需求记录位置

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

## 10. 交付前检查清单

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

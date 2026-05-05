# QAgent Agent 使用手册

本文面向在 QAgent 中执行开发、研究和验收的 agent。QAgent 是本地优先、单用户、低频量化研究系统；human 主要通过 React UI 看可视化和指标，agent 主要通过 REST API、MCP 工具和服务层测试完成研究链路。所有入口必须共享后端服务层，不能让 REST、MCP、UI 形成三套行为。

当前系统处于 V2.0 分支语义：默认兼容 US，美股仍是 legacy/default；新增 `market` 隔离后支持 A 股 `CN`。所有新调用都应显式传 `market`，只有验证旧兼容性时才依赖缺省 `US`。

## 1. 系统定位与边界

QAgent 负责：

- 行情数据、股票池、因子、特征集、标签、模型、策略、回测、信号生成、模拟交易。
- 保存可复现研究资产，包括源码、参数、依赖快照、训练/回测窗口、指标、诊断和任务结果。
- 让 agent 通过 REST/MCP 执行，让 human 通过 UI 验收；二者看到的资产、状态和指标必须一致。

QAgent 不负责：

- 券商实盘下单、交易所级撮合仿真、高频或日内策略、多用户权限。
- 让策略代码绕过服务层直接写库、读外部文件、调网络接口。
- 用随机 K-Fold 之类不适合市场时间序列的验证方式制造指标。

系统优先级：

1. 时间序列正确性：不引入 look-ahead bias。
2. 可复现：每个资产都能追溯配置和依赖。
3. 市场隔离：US/CN 资产不能混用。
4. agent 友好：长任务可轮询，诊断可读，错误可复现。
5. human 可验收：UI 能看到关键指标、图表、诊断和历史记录。

## 2. Market Scope

所有 V2 REST 和 MCP 调用都应携带 `market`。

- `US`：美股，默认数据源 yfinance，常用 benchmark 为 `SPY`。
- `CN`：A 股，数据源 BaoStock，ticker 使用 BaoStock 格式，如 `sh.600000`、`sz.000001`，默认 benchmark 为 `sh.000300`。
- 缺省 `market` 按 `US` 处理，仅用于向后兼容。

隔离规则：

- group、factor、label、feature set、model、strategy、backtest、signal run、paper session 都按 market 解析。
- CN 策略不能依赖 US factor/model，US 回测不能使用 CN benchmark 或 CN 股票池。
- benchmark 会做市场校验；CN 应使用 `sh.000300` 这类 BaoStock 指数代码。
- 发现 REST、MCP、UI 对 market 的行为不一致，先记录到 `docs/backlog.md`，再修服务层。

CN 默认股票池：

- 当前 A 股默认研究 universe 是 `cn_a_core_indices_union`。
- 它由上证50、沪深300、中证500、创业板指成分股去重并集构成。
- 这不是 BaoStock 全 A 列表；除非任务明确要求全市场扫描，否则不要改用全 A。
- 正常情况下该 group 成员数应大于 700，当前种子并集约 806 支。

## 3. 启动、停止与健康检查

开发环境：

```bash
scripts/start.sh
scripts/stop.sh
```

分别启动：

```bash
uv run uvicorn backend.app:app --host 127.0.0.1 --port 8000 --reload
cd frontend && pnpm dev
```

agent 需要后台服务时：

```bash
scripts/start_detached.sh
scripts/status.sh
scripts/stop.sh
```

常用入口：

- API: `http://127.0.0.1:8000/api`
- UI: `http://localhost:5173`
- MCP: `http://127.0.0.1:8000/mcp/`
- 任务详情: `GET /api/tasks/{task_id}`
- 任务列表: `GET /api/tasks?source=agent&market=CN&task_type=strategy_backtest`

健康检查建议：

- backend reload 或数据任务刚启动时，health check 短时超时先退避重试。
- 连续失败再看 `scripts/status.sh`、`logs/backend-detached.log`、`logs/qagent.log`。
- 不要直接打开运行中的 `data/qagent.duckdb` 做诊断；优先使用官方只读 API，避免 DuckDB 文件锁。

## 4. 任务系统

长任务必须走 `TaskExecutor`，返回 `task_id` 后轮询 `/api/tasks/{task_id}`。不要在 API handler、MCP tool 或前端请求里同步阻塞数据更新、因子评估、模型训练、回测、信号诊断或 paper trading 推进。

标准异步模式：

1. 调用 REST endpoint 或 MCP tool。
2. 保存返回的 `task_id`、`task_type`、`market`、`poll_url`。
3. 轮询 `GET /api/tasks/{task_id}`。
4. `completed` 后读取持久化资产详情。
5. 在结论中记录资产 ID、配置、指标和复验入口。

任务控制：

- `GET /api/tasks`
- `GET /api/tasks/{task_id}`
- `POST /api/tasks/{task_id}/cancel`
- `POST /api/tasks/bulk-cancel`
- `GET /api/tasks/pause-rules`
- `POST /api/tasks/pause-rules`
- `DELETE /api/tasks/pause-rules/{rule_id}`

使用原则：

- 批量取消前先用 `source`、`market`、`task_type` 缩小范围。
- pause rule 只阻止 qagent 后续任务入队，不负责杀死外部脚本或旧进程。
- `failed` 任务要读取 `error`，不要盲目重试覆盖原始现场。
- 被取消或中断的任务如果生成了局部结果，要看任务 result/error 和对应服务是否明确标记。

## 5. 能自由实现什么，不能自由实现什么

### 5.1 因子

因子是受控代码插件。可以自由写计算逻辑，但必须继承 `FactorBase` 并实现：

```python
def compute(self, data: pd.DataFrame) -> pd.Series:
    ...
```

输入是单只股票的 `open/high/low/close/volume` 时间序列，输出是同 index 的因子值序列。loader 只允许 `pandas`、`numpy`、`math`、`backend.indicators` 等白名单模块；禁止相对导入、文件访问、网络访问、数据库访问和任意第三方库。

影响：

- 适合技术因子、滚动统计、单标的价量特征。
- 不适合直接在因子源码内实现横截面分位数、行业排名、全市场同日排序。
- 需要横截面特征时，应在 feature/model 服务层扩展，而不是放宽因子沙箱。

### 5.2 模型

模型层当前不是自由代码插件。`model_type` 注册表目前只有 `lightgbm`。可以配置 `model_params`、标签、特征集、时间切分和目标类型，但不能通过 UI/MCP 上传任意 PyTorch、XGBoost、sklearn 自定义 estimator。

支持的 `objective_type`：

- `regression`
- `classification`
- `ranking`
- `pairwise`
- `listwise`

V2.0 中 `pairwise` 是基于 LightGBM LambdaRank 的用户目标别名，不是独立 pair-sampling learner。ranking/listwise 当前强制 `ranking_config.query_group="date"`，用于同日候选竞争。

影响：

- 想新增模型类型，需要在后端实现 `ModelBase` 并注册到模型 registry。
- 训练强制使用时间切分、purge gap、同 market feature/label/group。
- 保存模型时会记录 `feature_lineage`；训练列未在 feature set 声明会阻断保存，声明但覆盖不足的 feature 会进入 missing 记录。

### 5.3 策略

策略是受控代码插件。必须继承 `StrategyBase`，实现：

```python
def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
    ...
```

返回 DataFrame 以 ticker 为 index，列包含：

- `signal`: `1` 买入、`-1` 卖出、`0` 持有。
- `weight`: 策略建议目标权重。
- `strength`: 排序和 sizing 用信号强度。

策略 loader 只允许 `pandas`、`numpy`、`math`、`backend.strategies.base`。策略依赖通过 `required_factors()` 和 `required_models()` 声明；源码中直接访问 `context.model_predictions["model_id"]` 也会被静态解析并合并校验。

影响：

- 策略可以写候选池、打分、模型融合、换手控制、持仓状态逻辑。
- 不能直接查库、联网、读取本地文件或跨 market 使用资产。
- 策略输出不是最终订单；回测、信号和 paper trading 会再执行 position sizing 与约束。

## 6. 策略权重、约束和执行语义

策略输出经过 `position_sizing` 后才进入回测/信号/paper trading。

可选 sizing：

- `equal_weight`：只看买入候选，等权；会忽略策略自定义 `weight`。
- `signal_weight`：按 `strength` 归一化。
- `max_position`：按 strength 分配后做单票上限。
- `raw_weight`：读取策略输出的 `weight`；如未显式设置 `normalize_target_weights`，系统默认不再归一化，允许留现金。

通用执行约束：

- T+1 open 执行：T 日生成目标，T+1 开盘成交。
- 交易成本包括 commission 和 slippage。
- `max_positions` 会截断持仓数量。
- `normalize_target_weights=true` 时非空目标会归一到满仓。
- `max_single_name_weight` 会裁剪单票权重。
- `rebalance_drift_buffer`、`holding_period.min_days`、`holding_period.max_days`、`reentry_cooldown_days` 会改变实际交易。

`constraint_config` 可保存在策略上，也可在单次回测、信号、paper trading 中覆盖：

```json
{
  "max_single_name_weight": 0.15,
  "weekly_turnover_floor": 0.30,
  "rebalance_drift_buffer": 0.05,
  "holding_period": {"min_days": 1, "max_days": 21}
}
```

验收口径：

- 要验证策略自定义权重，使用 `position_sizing="raw_weight"`。
- 要验证是否留现金，确认 `normalize_target_weights=false`。
- 不要只看策略源码判断最终仓位，必须看回测 summary、trades、rebalance diagnostics 和 `constraint_report`。

## 7. 标准研究链路

### 7.1 数据和股票池

先检查数据覆盖，再补数。优先小股票池、短窗口验证，不默认全量刷新。

CN 常用 MCP：

```python
refresh_index_groups(market="CN")
refresh_stock_list(market="CN")
update_data(mode="incremental", market="CN")
```

验收点：

- group 成员不为空，CN 核心并集大于 700。
- 行情覆盖研究起止日期。
- ticker 规范正确：US 如 `AAPL`，CN 如 `sh.600519`。
- 缺数据、停牌、异常价格有可解释诊断。

### 7.2 因子和特征集

因子创建后应先小范围计算/评估，再进入正式 feature set。

因子评估示例：

```python
evaluate_factor(
    factor_id="<factor_id>",
    label_id="<label_id>",
    universe_group_id="cn_a_core_indices_union",
    start_date="2024-01-02",
    end_date="2024-06-28",
    market="CN",
)
```

中文或非 ASCII `factor_id` 推荐使用 body 版 REST：

```http
POST /api/factors/evaluate
```

验收点：

- factor、label、feature set 的 `market` 一致。
- 因子值无未来函数，warm-up 区间处理明确。
- 评估结果包含覆盖率、IC/IR、分组收益等。
- feature set 记录 factor 引用和预处理配置。

### 7.3 标签

标签必须按 market 创建或读取 preset。ranking/listwise 任务优先使用能表达同日候选优劣的标签，例如 forward return rank、top quantile、路径质量或其他同日可排序标签。

验收点：

- 标签 horizon 不越过训练/测试窗口边界。
- 标签生成使用未来收益时，只作为训练标签，不得进入决策日特征。
- ranking 标签进入模型时，默认用 `label_gain="ordinal"` 做 dense relevance 映射。

### 7.4 模型训练

训练示例：

```python
train_model(
    name="cn_ranker_v1",
    feature_set_id="<cn_feature_set_id>",
    label_id="<cn_label_id>",
    model_type="lightgbm",
    model_params={},
    train_config={
        "train_start": "2020-01-02",
        "train_end": "2023-12-29",
        "valid_start": "2024-01-02",
        "valid_end": "2024-06-28",
        "test_start": "2024-07-01",
        "test_end": "2024-12-31",
        "purge_gap": 20
    },
    universe_group_id="cn_a_core_indices_union",
    market="CN",
    objective_type="listwise",
    ranking_config={
        "query_group": "date",
        "eval_at": [5, 10, 20],
        "min_group_size": 20,
        "label_gain": "ordinal"
    },
)
```

验收点：

- 任务完成后能读取 `model_id`。
- metadata 包含 feature set、label、universe、train_config、ranking_config、feature_lineage。
- valid/test 指标存在；ranking 任务至少看 NDCG、hit/rank 相关指标和样本组数量。
- 缺模型预测时，策略/信号/回测必须显式报错或诊断显示 missing，不接受静默 0 trades。

### 7.5 策略创建

创建策略时明确 market、position sizing 和默认约束。

```python
create_strategy(
    name="cn_rank_weekly_v1",
    source_code="<strategy source>",
    position_sizing="raw_weight",
    market="CN",
    constraint_config={
        "max_single_name_weight": 0.15,
        "rebalance_drift_buffer": 0.05,
        "holding_period": {"min_days": 1, "max_days": 21}
    },
)
```

策略源码建议使用 `StageTracer` 暴露决策过程：

```python
tracer = StageTracer(context)
tracer.log("candidate_pool", sorted(candidates))
tracer.log("score_map", scores)
tracer.log("selected_set", sorted(selected))
```

验收点：

- `GET /api/strategies?market=CN` 能看到策略。
- `required_factors()` 和 `required_models()` 完整。
- 如果源码自定义权重，避免使用 `equal_weight`。
- 策略 diagnostics 能解释候选池、打分、入选、剔除和持仓状态。

### 7.6 回测

正式回测必须通过服务层保存到 `backtest_results`，不能只用临时脚本 JSON 作为交付。

基础回测：

```python
run_backtest(
    strategy_id="<cn_strategy_id>",
    universe_group_id="cn_a_core_indices_union",
    market="CN",
    config_json='{"start_date":"2024-01-02","end_date":"2024-12-31","benchmark":"sh.000300","rebalance_freq":"weekly"}',
)
```

warm-up / evaluation split：

```python
run_backtest(
    strategy_id="<cn_strategy_id>",
    universe_group_id="cn_a_core_indices_union",
    market="CN",
    config_json='{"warmup_start_date":"2025-12-15","start_date":"2026-01-05","evaluation_start_date":"2026-01-05","end_date":"2026-04-01","benchmark":"sh.000300","rebalance_freq":"weekly","initial_entry_policy":"require_warmup_state"}',
)
```

`initial_entry_policy` 支持：

- `wait_for_anchor`
- `open_immediately`
- `bootstrap_from_history`
- `require_warmup_state`

组合层回测：

```python
run_backtest(
    strategy_id="<cn_strategy_id>",
    universe_group_id="cn_a_core_indices_union",
    market="CN",
    config_json='{"start_date":"2024-01-02","end_date":"2024-12-31","benchmark":"sh.000300","rebalance_freq":"weekly","portfolio_overlay":{"base_leg":"core_union_equal_weight","base_weight":0.65,"overlay_weight":0.35}}',
)
```

验收点：

- 任务完成后有 `backtest_id`。
- `GET /api/strategies/backtests/{backtest_id}?market=CN` 返回 summary、NAV、drawdown、trades、config。
- `GET /api/strategies/backtests/{backtest_id}/rebalance-diagnostics?market=CN` 能读取调仓诊断。
- summary 包含 `portfolio_compliance`、`constraint_report`、`reproducibility_fingerprint`。
- 列表接口应暴露轻量 `reproducibility_hash`。
- warm-up split 时主窗口指标只统计 `evaluation_start_date` 后，诊断中 `phase` 标记 `warmup/evaluation`。

`portfolio_compliance` 是 human 验收持仓分散度和约束执行的优先口径。关键字段包括：

- `min_position_count`
- `avg_position_count`
- `max_target_weight`
- `max_trade_holding_days`
- `max_target_sum`
- `compliance_pass`
- `violations`

高收益但 `compliance_pass=false` 的结果不能直接作为交付最优。

### 7.7 信号生成和诊断

信号生成：

```python
generate_signals(
    strategy_id="<strategy_id>",
    target_date="2026-04-30",
    universe_group_id="cn_a_core_indices_union",
    market="CN",
)
```

REST：

- `POST /api/signals/generate`
- `POST /api/signals/diagnose`
- `GET /api/signals?market=CN`
- `GET /api/signals/{run_id}?market=CN`
- `GET /api/signals/{run_id}/export?market=CN`

验收点：

- `dependency_snapshot` 记录 factor、model、strategy、constraint。
- `model_diagnostics` 能看到 required / injected / missing 模型状态。
- `strategy_diagnostics` 能看到候选池、选中项、gate、stage trace。
- `auto_stage_trace` 可用于 focus ticker 定位进入/退出原因。
- constraint_config 会应用到最终 signal weights，并保存 `constraint_report`。

### 7.8 Paper Trading

paper trading 是 live-like forward test，必须复用策略、回测和信号语义。

MCP：

```python
create_paper_session(
    name="cn_rank_forward_v1",
    strategy_id="<strategy_id>",
    start_date="2026-01-05",
    initial_capital=1000000,
    config={"universe_group_id": "cn_a_core_indices_union", "rebalance_freq": "weekly"},
    market="CN",
)

advance_paper_session(session_id="<session_id>", to_date="2026-04-30", market="CN")
```

验收点：

- 新 session 第一天只记录 baseline，首笔交易在下一个可执行交易日。
- 与回测一致使用 T+1/open-price、position sizing、constraint_config。
- `GET /api/paper-trading/sessions/{id}/daily` 返回 NAV、cash、position_count、trade_count。
- `GET /api/paper-trading/sessions/{id}/positions?date=YYYY-MM-DD` 可回看历史持仓。
- `GET /api/paper-trading/sessions/{id}/compare-backtest/{backtest_id}` 可对齐 paper/backtest 差异。

## 8. 只读诊断 API

这些接口用于 agent 在 backend 运行时安全读取小样本数据，不直接碰 DuckDB 文件。

```http
GET /api/diagnostics/daily-bars?market=CN&date=2026-04-30&tickers=sh.600000&tickers=sz.000001
GET /api/diagnostics/factor-values?market=CN&date=2026-04-30&factor_id=<factor_id>&tickers=sh.600000
```

其他常用诊断：

```http
GET /api/data/index-bars/{symbol}?market=CN&start=YYYY-MM-DD&end=YYYY-MM-DD
GET /api/data/groups/{group_id}/daily-snapshot?market=CN&date=YYYY-MM-DD
GET /api/strategies/backtests/{backtest_id}/rebalance-diagnostics?market=CN
GET /api/strategies/backtests/{backtest_id}/stock/{ticker}?market=CN
```

使用原则：

- 单次诊断 ticker 数量保持小样本；`diagnostics` 接口最多支持 200 个 ticker。
- 先用诊断 API 证明缺数据或错配，再决定是否补数或修代码。
- 不把诊断接口当批量导出工具。

## 9. REST 和 MCP 调用规范

MCP 常用工具：

- `get_stock_data`
- `search_stocks`
- `get_data_status`
- `update_data`
- `refresh_stock_list`
- `refresh_index_groups`
- `list_groups`
- `create_group`
- `list_factors`
- `create_factor`
- `evaluate_factor`
- `list_labels`
- `create_label`
- `list_feature_sets`
- `create_feature_set`
- `list_models`
- `train_model`
- `list_strategies`
- `create_strategy`
- `run_backtest`
- `generate_signals`
- `get_task_status`
- `cancel_task`
- `list_paper_sessions`
- `create_paper_session`
- `advance_paper_session`

规范：

- 新调用显式传 `market`。
- 长任务保存 `task_id`，不要只保存即时返回。
- 资产创建后用 list/get 接口复验。
- API response 新增字段时，同步 `frontend/src/api/index.ts` 和相关 UI。
- MCP 输入错误应返回可修复字段信息，例如非法 market 应提示允许值 `US, CN`。

## 10. Human 验收材料

agent 交付研究或修复时，不只写结论。至少给出：

- 资产 ID：`factor_id`、`feature_set_id`、`label_id`、`model_id`、`strategy_id`、`backtest_id`、`signal_run_id`、`paper_session_id`、`task_id`。
- 配置快照：market、股票池、日期窗口、benchmark、资金、成本、调仓频率、max_positions、position_sizing、constraint_config。
- 核心指标：IC/IR、AUC 或 ranking 指标、Sharpe、max drawdown、total return、turnover、trade count、final NAV、paper/backtest delta。
- 诊断入口：UI 页面、API endpoint、rebalance diagnostics、signal diagnose、focus ticker 结果。
- 验证命令：后端测试、前端构建、live API 复验。

human UI 验收优先看：

- 数据覆盖和股票池成员数。
- 因子评估图和覆盖率。
- 模型训练指标和 feature importance。
- 回测 NAV/drawdown/trades/rebalance diagnostics。
- `portfolio_compliance` 和 `constraint_report`。
- signal run 的 dependency snapshot 与候选明细。
- paper trading 的 daily NAV、持仓和 backtest compare。

## 11. 开发最佳实践

服务层优先：

- 共享行为先改 `backend/services/`。
- REST 只做请求/响应整形。
- MCP 调同一服务方法。
- UI 只消费 API，不复制业务逻辑。

时间序列正确性：

- 决策日只使用当时可见数据。
- 标签可以用未来收益，但只能作为训练监督信号。
- 回测、信号、paper trading 的 T+1/open-price 语义必须一致。
- 对 stateful 策略区分目标仓位、实际成交仓位、估值权重、holding days。

数据库升级：

- schema 变更放在 `backend/db.py` 或 `backend/services/schema_migrations.py` 的无损迁移里。
- 新字段要兼容旧数据库，缺省值不破坏 US legacy。
- 不删除用户已有研究资产，除非 human 明确要求。

性能：

- 使用批量 SQL、DataFrame/DuckDB 操作，避免 ticker/date 双层小查询。
- 数据更新和全量训练昂贵，先窄范围复验。
- 性能问题如果由本机资源限制导致，记录证据后可归档，不做无意义重构。

前端：

- TypeScript strict，更新 API 字段必须同步类型。
- UI 保持 Ant Design dark layout 和现有路由风格。
- human 验收界面重视表格、图表、筛选、诊断详情，不做营销页。

## 12. Backlog 和归档

统一看板：

```text
docs/backlog.md
```

使用规则：

- 未闭环问题、需求和验收缺口都记录到 backlog。
- 当前已修复并通过验证的问题不长期留在 backlog。
- 每个已完成问题单独归档到 `docs/archive/backlog/`，保留复现、修复、验证和 commit。
- 当前 backlog 文件只保存未修复或待验证的问题；如果没有未修复问题，明确写“暂无”。

记录要求：

- 写清楚 market、入口、资产 ID、请求参数、页面路径。
- 写实际结果、错误、日志或指标，不写纯猜测。
- 给出期望行为和可量化验收标准。
- human 反馈保留原始问题表达。
- 暂不处理的问题放 Deferred，并写重新评估条件。

## 13. 交付前验证

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

需要 live 复验时：

1. 启动 backend/frontend。
2. 用窄范围 API 或 UI 复现原问题。
3. 跑对应任务并轮询完成。
4. 读取持久化资产详情。
5. 记录命令、结果、资产 ID 和残余风险。

最终回复不要只说“已修复”或“已验证”。必须说明实际修改的文件、跑过的验证、结果和还没覆盖的风险。

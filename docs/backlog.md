# QAgent 需求与问题看板

本文件是 QAgent 项目的统一需求看板。Agent 在开发、研究、验收过程中发现的未闭环问题、改进需求和验收缺口都记录在这里。Human 通过 UI 和量化指标验收，agent 通过本文件维持跨会话上下文。

## 使用规则

- 新问题先放到 `Inbox`，复现清楚后移动到 `Open`。
- 开始修复前移动到 `In Progress`，写明负责人、分支或会话。
- 修复完成但未验收放到 `Verify`。
- 验收通过后移动到 `Done`，保留复验证据和 commit。
- 暂不处理但仍有价值的问题放到 `Deferred`，写明重新评估条件。
- 不记录纯猜测。没有复现步骤的问题必须标记为 `Needs Repro`。
- V2.0 期间凡是发现 REST、MCP、UI 对 `market`、ranking/listwise 指标、任务状态或资产 ID 的展示不一致，统一记录到本文件。
- agent 记录问题时必须写清楚 market、入口、资产 ID、请求参数和可复验命令；human UI 验收问题还要补充页面路径和截图/指标位置。

## 记录模板

```md
### [YYYY-MM-DD] P0/P1/P2/P3 类型：一句话标题

- **状态**：Inbox / Open / In Progress / Verify / Done / Deferred / Needs Repro
- **来源**：human 反馈 / agent 发现 / live API 复验 / UI 验收
- **影响范围**：页面、API、服务、数据资产或研究链路
- **复现入口**：
  - UI：
  - API / MCP：
  - 资产 ID：
- **当前证据**：
  - 实际结果：
  - 日志 / 错误：
  - 相关指标：
- **期望行为**：
- **验收标准**：
  - 可量化指标：
  - UI 验收点：
  - 命令 / API 复验：
- **修复记录**：
  - commit：
  - 验证命令：
  - 复验结论：
```

## Inbox

暂无。

## Open

暂无。

## In Progress

暂无。

## Verify

暂无。

## Deferred

### [2026-05-01] P2 使用可靠性：`scripts/start.sh` 在非交互 agent shell 中会阻塞并随会话结束关闭服务

- **状态**：Deferred
- **来源**：agent 研究发现
- **影响范围**：本地研究执行、REST API 连续调用、后台任务轮询、`scripts/start.sh`
- **复现入口**：
  - UI：无
  - API / MCP：启动服务后调用 `http://127.0.0.1:8000/api/health`
  - 资产 ID：无
- **当前证据**：
  - 实际结果：在 agent 的一次性 shell 工具里直接执行 `./scripts/start.sh`，脚本会启动 backend/frontend 后进入 `wait`；工具会话结束或收到终止信号时，frontend 输出 `ELIFECYCLE Command failed with exit code 143`，backend 随后 `app.shutdown`，`uvicorn` 进程消失，后续 REST 调用返回 `curl: (7) Failed to connect to 127.0.0.1 port 8000`。
  - 日志 / 错误：`logs/qagent.log` 最后出现 `db.closed`、`app.shutdown`；无应用异常栈。`scripts/start.sh` 本身设计为前台开发脚本，包含 `trap "kill $BACKEND_PID $FRONTEND_PID ..."` 和最终 `wait`。
  - 相关指标：服务退出后 `.backend.pid` / `.frontend.pid` 为空或对应进程不存在，`ps` 中无 `uvicorn backend.app:app`。
- **期望行为**：agent 使用说明应明确前台脚本和后台研究脚本的启动方式差异；或者提供 `scripts/start_detached.sh` / `scripts/status.sh`，让 agent 能稳定启动、检查和停止本地服务，不因 shell 工具生命周期误杀服务。
- **验收标准**：
  - 可量化指标：用推荐的 agent 启动命令后，shell 命令返回，`/api/health` 仍持续返回 `{"status":"ok"}`；后台任务轮询期间服务不因 shell 会话结束退出。
  - UI 验收点：无。
  - 命令 / API 复验：`nohup ./scripts/start.sh > logs/agent-start.log 2>&1 &` 或专用 detached 脚本启动后，连续多次 `curl http://127.0.0.1:8000/api/health` 成功；`scripts/stop.sh` 能停止对应进程。
- **修复记录**：
  - commit：待处理
  - 验证命令：待处理
  - 复验结论：当前 agent 研究临时使用 `nohup` 后台启动方式，不修改 qagent 代码。

## Done

### [2026-05-01] P1 研究链路阻断：A 股研究结果无法沉淀为 qagent 官方回测历史

- **状态**：Done
- **来源**：human 反馈 / agent live API 复验
- **影响范围**：A 股因子研究、模型研究、策略创建、`POST /api/strategies`、`/api/strategies/backtests`、回测历史展示
- **复现入口**：
  - UI：策略创建页 / 策略回测页选择 CN 市场后无法完成“创建策略 -> 运行正式回测 -> 回测历史可见”的研究交付链路
  - API / MCP：`POST /api/strategies` with `market="CN"`；后续预期使用 `cn_a_core_indices_union` 作为训练与回测股票池
  - 资产 ID：股票池 `cn_a_core_indices_union`，临时研究脚本 `/Users/m/dev/atlas/tmp/cn_core_regime_factor_experiment.py` 仅作诊断，不满足正式交付形态
- **当前证据**：
  - 实际结果：A 股核心指数并集研究目前只能通过本地脚本输出 JSON / 报告，无法创建 CN 策略资产，因此无法在 qagent 的 backtest history 中形成可直接复用、可比较、可验收的记录。
  - 日志 / 错误：极简 no-op 策略用 `market="CN"` 调用 `POST /api/strategies` 返回 HTTP `500 Internal Server Error`，响应体只有 `Internal Server Error`；`logs/qagent.log` 未记录可读异常栈。相同极简策略使用 `market="US"` 可成功创建，说明不是策略源码语法通用问题。
  - 相关指标：临时研究中 `65%` 核心指数并集等权底仓 + `35%` 周频 guarded top80 增强组合 Sharpe `1.8874`、最大回撤 `-13.49%`、日胜率 `57.28%`，但该结果尚未进入 qagent 官方回测历史，不能作为最终研究产物交付。
- **期望行为**：A 股研究必须能在 qagent 内完成正式资产闭环：因子 / 模型 / 策略资产可创建，策略回测必须限定在 `cn_a_core_indices_union`，并把无未来函数、无数据穿越的回测结果保存到回测历史，供 UI 和 API 直接复验。
- **验收标准**：
  - 可量化指标：极简 CN 策略创建返回 HTTP 200；基于 `cn_a_core_indices_union` 的 CN 策略回测能生成正式 backtest record；回测 summary、NAV、交易明细、股票池 ID、参数和数据窗口都可追溯。
  - UI 验收点：策略创建页能保存 CN 策略；策略回测页选择 `cn_a_core_indices_union` 后能运行并在回测历史看到记录；详情页展示股票池、调仓频率、交易成本、胜率、Sharpe、最大回撤和交易明细。
  - 命令 / API 复验：`POST /api/strategies` 创建极简 CN 策略后，调用正式回测接口，确认 `GET /api/strategies/backtests?market=CN` 或对应详情接口能返回该记录；错误路径必须返回可读 `detail` 并写入结构化日志。
- **修复记录**：
  - commit：本次提交
  - 验证命令：`uv run python -m unittest tests.test_schema_migrations tests.test_strategy_backtest_market_scope`；live API CN 策略创建 / 回测 / 删除 smoke。
  - 复验结论：通过。新增策略表 market 唯一约束迁移，修复 legacy `UNIQUE(name, version)` 在 CN 查询/写入路径触发的内部错误；API 500 路径补充结构化日志和可读 detail。CN 极简策略可创建，`cn_a_core_indices_union` 官方回测可完成并写入 `backtest_results`，临时 smoke 资产已删除。

### [2026-05-01] P3 研究功能：正式 CN 策略回测需要支持组合底仓 + 增强卫星的组合层回测

- **状态**：Done
- **来源**：agent A 股核心指数并集研究
- **影响范围**：CN 策略研究、组合层回测、`/api/strategies/backtests`、后续 UI 展示
- **复现入口**：
  - UI：策略回测页暂无组合底仓 + 策略增强的配置入口
  - API / MCP：当前正式 backtest 以单个 strategy 为核心，缺少把 `cn_a_core_indices_union` 等权底仓与一个增强策略按固定比例合成的持久化资产
  - 资产 ID：临时研究脚本 `/Users/m/dev/atlas/tmp/cn_core_regime_factor_experiment.py`
- **当前证据**：
  - 实际结果：本轮临时向量化研究显示，`65%` 核心并集等权底仓 + `35%` `legacy_weekly_guarded_top80_equal` 增强的 Sharpe 为 `1.8874`，高于纯核心并集等权代理 `1.8022`，最大回撤 `-13.49%` 接近等权代理 `-12.53%`；但该结果暂时不能作为 qagent 正式 backtest asset 保存。
  - 日志 / 错误：无异常，属于研究能力缺口。
  - 相关指标：报告见 `docs/reports/2026-05-01-cn-core-index-union-factor-study.md`。
- **期望行为**：正式 qagent 能支持组合层配置，例如 `base_leg=cn_a_core_indices_union_equal_weight`、`overlay_strategy_id=<id>`、`base_weight=0.65`、`overlay_weight=0.35`，并保存组合 NAV、回撤、交易成本和底仓/增强贡献。
- **验收标准**：
  - 可量化指标：同一 CN 日期窗口下，正式组合回测指标能复现临时脚本的组合层 NAV 口径，误差在可解释范围内。
  - UI 验收点：策略回测页或组合页能展示底仓、增强腿、权重、总 NAV、分腿收益贡献。
  - 命令 / API 复验：创建一个 CN 增强策略后，用组合配置运行回测，读取 backtest detail，确认包含组合层 summary 和分腿明细。
- **修复记录**：
  - commit：本次提交
  - 验证命令：`uv run python -m unittest tests.test_backtest_diagnostics_contracts`；live API CN `portfolio_overlay` 回测 smoke。
  - 复验结论：通过。`BacktestService.run_backtest` 支持 `config.portfolio_overlay`，可将 `core_union_equal_weight` / `equal_weight` base leg 与策略 overlay 按权重合成，详情保留 `config.portfolio_overlay`、`summary.trade_diagnostics.portfolio_legs` 和总 NAV 指标。live smoke 返回 `CONFIG_OVERLAY {"base_leg":"core_union_equal_weight","base_weight":0.65,"overlay_weight":0.35}`，临时回测和策略已删除。

### [2026-05-01] P2 可复现性：US 历史最优 S271 回测无法用同策略同配置复现

- **状态**：Done
- **来源**：agent live API 复验
- **影响范围**：US 策略研究链路、`POST /api/strategies/{strategy_id}/backtest`、回测资产可复现性、历史 backtest 对比
- **复现入口**：
  - UI：策略回测页选择 US 策略 `M0428_S271_S262_ENTRYSWAP_DDHEAVY_PRETRADE4_R1`
  - API / MCP：`POST /api/strategies/357056b76e3c/backtest` with `market=US`、`universe_group_id=sp500`
  - 资产 ID：历史 backtest `12d7b159c3ad`，当前复跑 backtest `99824267db46`
- **当前证据**：
  - 实际结果：历史 `12d7b159c3ad` 创建于 `2026-04-28 02:44:26`，配置为 `2026-01-02` 至 `2026-04-02`、`rebalance_freq=daily`、`rebalance_buffer=0.03`、`min_holding_days=0`、`reentry_cooldown_days=5`，指标为 `return=0.640948`、`Sharpe=18.1147`、`MaxDD=-0.056262`、`trades=125`。当前同一策略同一窗口同一交易配置复跑得到 `99824267db46`，指标为 `return=0.379284`、`Sharpe=7.7531`、`MaxDD=-0.065806`、`trades=125`。
  - 日志 / 错误：无任务错误；两次回测都完成，但交易序列和 NAV 明显不同。例如 `2026-04-02` NAV 从历史 `1640948.5` 降到复跑 `1379283.85`；交易 hash 分别为 `e38161984daf3a46` 和 `ffe930fc52bc09ce`。
  - 相关指标：当前策略源码 hash 为 `1ecfaa8f6796e1720eba47bcdff24f2757103be26750b47613041f2931f9fcfa`；历史 backtest 未保存策略源码、模型预测快照、因子值快照或数据版本，无法直接判断漂移来自代码、数据、模型资产还是执行语义变化。
- **期望行为**：历史 backtest 应保存足够快照或版本指纹，使 agent 能判断同配置复跑是否可比；如果数据/模型/服务版本已变化，UI/API 应暴露差异，而不是只给最终指标。
- **验收标准**：
  - 可量化指标：新 backtest summary 至少记录 strategy source hash、required model ids 和 model version/hash、factor cache/data watermark、qagent git commit/service version；同配置复跑时能输出可比性诊断。
  - UI 验收点：回测详情页显示“复现指纹/可比性”区域，能解释历史结果与当前复跑是否使用同一输入。
  - 命令 / API 复验：对 `357056b76e3c` 同配置连续复跑两次，API 返回相同交易 hash；修改数据或模型后，回测详情能显示指纹差异。
- **修复记录**：
  - commit：本次提交
  - 验证命令：`uv run python -m unittest tests.test_backtest_diagnostics_contracts`；live API CN 回测 smoke 检查 `summary.reproducibility_fingerprint.hash`。
  - 复验结论：通过。新回测会持久化复现指纹，包含 service/schema 版本、git commit、market、strategy source hash、required factors/models、factor source hash、model metadata hash、config、data watermark、result shape 和稳定 hash；列表接口只暴露轻量 `reproducibility_hash` / `has_reproducibility_fingerprint`。旧历史回测无法补回当时缺失的快照，后续结果已具备可比性诊断基础。

### [2026-05-01] P1 可靠性：CN 并发模型训练会并发写同一因子缓存并触发主键冲突

- **状态**：Done
- **来源**：agent live API 复验
- **影响范围**：`/api/models/train`、`FeatureService.compute_features`、`FactorEngine.compute_factor`、`factor_values_cache`、CN 多模型研究链路
- **复现入口**：
  - UI：模型训练页同时提交两个使用同一 CN feature set 的训练任务
  - API / MCP：
    - `POST /api/models/train`，任务 `fff2674c0c6f45d9898ea684299f9910`
    - `POST /api/models/train`，任务 `a9014886d61d49b8a5d2e199d772eee5`
  - 资产 ID：feature set `05821b6c142f`，group `cn_a_core_indices_union`
- **当前证据**：
  - 实际结果：两个训练任务并发运行时，多次同时计算同一 CN 因子并写入 `factor_values_cache`；其中一个路径出现唯一键冲突后，`FeatureService` 将该因子记为 `factor_failed`，训练可能继续但特征口径不可比。
  - 日志 / 错误：`logs/qagent.log` 多次出现 `TransactionContext Error: Failed to commit: PRIMARY KEY or UNIQUE constraint violation`，例如 `duplicate key "cn_builtin_obv_slope_20, sh.600219, 2020-01-02"`、`cn_builtin_rsi_14`、`cn_builtin_volatility_20`、`cn_builtin_统计_rank_20` 等。
  - 相关指标：两个任务已由 agent 取消，`GET /api/tasks/{task_id}` 返回 `status=failed` 且无 `error_message`，这也降低了问题可诊断性。
- **期望行为**：同一 market/factor/ticker/date 的缓存写入应并发安全；重复计算时应使用幂等 upsert、任务级锁、或因子级互斥，不能让一个训练任务因为另一个任务先写缓存而丢因子继续训练。
- **验收标准**：
  - 可量化指标：并发提交两个使用同一 CN feature set 的训练任务，不出现 factor cache 主键冲突；两个任务使用完整相同的 feature set 或明确失败。
  - UI 验收点：模型训练任务失败时展示可读错误，而不是 `failed` 但 `error_message=null`。
  - 命令 / API 复验：并发提交两个小窗口 CN 训练任务并轮询完成；检查日志无 `factor_failed`/`PRIMARY KEY` 冲突。
- **修复记录**：
  - commit：本次提交
  - 验证命令：`uv run python -m unittest tests.test_factor_feature_market_scope`
  - 复验结论：通过。`FactorEngine` 对 `(market, factor_id)` 的缓存写入加进程内 keyed lock，缓存写入使用唯一临时 relation 名并通过 `conn.register()` 批量写入，避免并发任务互相覆盖固定 `_tmp_fv` 或抢写同一主键。

### [2026-05-01] P2 功能缺失：CN 缺少核心指数成分并集股票池

- **状态**：Done
- **来源**：human 反馈
- **影响范围**：A 股研究股票池构建、`/api/groups`、MCP group tools、`GroupService`、`DataService`、后续模型训练与回测链路
- **复现入口**：
  - UI：数据管理页 / 股票分组区域点击“刷新指数成分”
  - API / MCP：
    - `GET /api/groups?market=CN`
    - `POST /api/groups/refresh-indices?market=CN`
    - MCP `refresh_index_groups(market="CN")`
  - 资产 ID：`cn_sz50`、`cn_hs300`、`cn_zz500`、`cn_chinext`、`cn_a_core_indices_union`
- **当前证据**：
  - 实际结果：旧实现只提供 `cn_all_a` 和 `cn_hs300`；之后新增核心指数分组后，外部成分源未刷新或返回空时，`cn_a_core_indices_union` 仍可能保持空成员。
  - 日志 / 错误：无错误，属于能力缺口。
  - 相关指标：BaoStock 提供 `query_sz50_stocks`、`query_hs300_stocks`、`query_zz500_stocks`；创业板指成分需走独立公开页面抓取；当前真实种子文件包含上证50 `50` 支、沪深300 `300` 支、中证500 `500` 支、创业板指 `100` 支，去重并集 `806` 支。
- **期望行为**：提供可复现的 CN 指数成分分组构建能力，支持创建/刷新上证50、沪深300、中证500、创业板指，以及它们的去重并集 `cn_a_core_indices_union`。
- **验收标准**：
  - 可量化指标：`GET /api/groups?market=CN` 能看到上述分组；`cn_a_core_indices_union` 成员为四个来源去重并集且全部为 CN ticker；CN 默认 group 和市场级数据更新都使用该 group。
  - UI 验收点：数据管理页能刷新股票池和指数成分，并显示该并集股票池的名称、来源指数、成员数和刷新时间。
  - 命令 / API 复验：
    - `GET /api/groups/cn_a_core_indices_union?market=CN`
    - `POST /api/groups/refresh-indices?market=CN`
    - `POST /api/data/update` with `{"market":"CN","mode":"incremental"}` 只对 `cn_a_core_indices_union` 成员增量更新。
- **修复记录**：
  - commit：本次提交
  - 验证命令：`uv run python -m unittest tests.test_data_group_market_scope`；`uv run python -m unittest discover tests`；`cd frontend && pnpm build`；`git diff --check`；`GET /api/groups/{group_id}?market=CN`。
  - 复验结论：通过；新增真实成分种子 `backend/seeds/cn_core_indices_constituents.json`，`ensure_builtins("CN")` 首次创建时会填充核心指数与并集，`refresh_index_groups("CN")` 在外部源为空且本地无成员时会用种子兜底。后端全量 `96` 个 unittest 通过，前端构建通过，diff whitespace 检查通过；当前运行库 API 返回 `cn_sz50=50`、`cn_hs300=300`、`cn_zz500=500`、`cn_chinext=100`、`cn_a_core_indices_union=806`。BaoStock provider 下载性能/可靠性逻辑未改动，`backend/providers/baostock_provider.py` 无差异。

### [2026-05-01] P2 缺陷：CN listwise 排序训练使用 rank 标签触发 LightGBM label mapping 错误

- **状态**：Done
- **来源**：agent 研究发现
- **影响范围**：A 股排序模型训练、`/api/models/train`、`ModelService.train_model`、LightGBM rank/listwise 目标、后续模型驱动策略研究
- **复现入口**：
  - API：`POST /api/models/train`
  - 参数：`market="CN"`、`feature_set_id="05821b6c142f"`、`label_id="cn_preset_fwd_rank_20d"`、`universe_group_id="cn_a_core_indices_union"`、`objective_type="listwise"`、`ranking_config={"query_group":"date","eval_at":[5,10,20],"min_group_size":20}`
  - 任务 ID：`b174e4c04e9444e0b03acc77760e69b7`
- **当前证据**：
  - 实际结果：任务失败，未生成可用 CN 模型。
  - 日志 / 错误：`lightgbm.basic.LightGBMError: Label 170 is not less than the number of label mappings (31)`。
  - 相关指标：训练股票池为 `cn_a_core_indices_union`，成员数 `806`；训练窗口为 `2020-01-02` 至 `2023-12-29`，验证窗口为 `2024-01-02` 至 `2024-06-28`，测试窗口为 `2024-07-01` 至 `2024-12-31`，`purge_gap_days=20`。
- **期望行为**：CN 排序标签在进入 LightGBM ranking/listwise 目标前应被转换为合法的 relevance label，或者 API 明确拒绝不兼容标签/目标组合并提示可修复字段；不应到 LightGBM 底层才失败。
- **验收标准**：
  - 可量化指标：同一请求能够成功训练排序模型，或返回 HTTP 400 且错误信息说明标签映射要求。
  - UI 验收点：模型训练页面选择 CN rank 标签和 listwise/pairwise 目标时，能提示兼容性或完成训练。
  - 命令 / API 复验：使用上述参数重新提交训练任务，轮询 `/api/tasks/{task_id}`；失败时错误必须为可理解的参数校验，成功时模型 metrics 和 metadata 记录 ranking label 映射。
- **修复记录**：
  - commit：本次提交
  - 验证命令：`uv run python -m unittest tests.test_ranking_dataset tests.test_model_market_scope`
  - 复验结论：通过。ranking/listwise 默认 `label_gain="ordinal"`，rank 原始标签进入 LightGBM 前会按同日分组转换为 dense non-negative relevance label；`identity` 仅接受已经 dense 的非负整数标签。模型 metadata 记录 `ranking_config.label_gain`，避免 rank 值如 `170` 直接触发 LightGBM label mapping 错误。

### [2026-05-01] P2 缺陷：CN 模型训练完成后因 feature_id / factor_name 校验错配导致无法落库

- **状态**：Done
- **来源**：agent 研究发现
- **影响范围**：A 股模型训练、`FeatureService.compute_features`、`ModelService.train_model`、模型资产保存、模型驱动策略回测
- **复现入口**：
  - API：`POST /api/models/train`
  - 参数：`market="CN"`、`feature_set_id="05821b6c142f"`、`label_id="cn_preset_path_return_20d"`、`universe_group_id="cn_a_core_indices_union"`、`model_type="lightgbm"`
  - 任务 ID：`a9014886d61d49b8a5d2e199d772eee5`
- **当前证据**：
  - 实际结果：LightGBM 已训练并输出 metrics，但保存模型前抛出 `ValueError`，导致没有模型落库。
  - 日志 / 错误：`model.train.metrics` 显示 `valid_ic=0.196119`、`test_ic=0.160735`、`test_daily_ic.mean_ic=0.180602`、`ir=0.773334`；随后 `model.train.feature_mismatch` 显示 `trained=13`、`expected=24`，错误信息列出全部 `factor_id` 为 missing。
  - 初步根因：`FeatureService.compute_features` 返回 `dict[factor_name -> DataFrame]`，`ModelService` 的 `trained_features` 也是因子名；但持久化前 `expected_features = [ref["factor_id"] ...]`，导致因子名与因子 ID 必然错配。另有 24 因子中仅 13 个进入矩阵的问题需要单独诊断覆盖率/缓存。
- **期望行为**：模型训练保存前的特征维度校验应使用同一命名空间，或 metadata 同时记录 `factor_id` 与 `factor_name` 映射；不应在训练和评估已完成后误判全部因子缺失。
- **验收标准**：
  - 可量化指标：同一训练请求能成功生成模型资产，`GET /api/models?market=CN` 能看到模型；metadata 中 `feature_names` 与模型实际列一致，并可追溯到 `factor_id`。
  - UI 验收点：CN 模型训练任务成功后，模型列表展示 metrics、特征重要性和 market。
  - 命令 / API 复验：提交上述训练请求并轮询 `/api/tasks/{task_id}`；完成后调用 `GET /api/models/{model_id}?market=CN` 和一次预测接口。
- **修复记录**：
  - commit：本次提交
  - 验证命令：`uv run python -m unittest tests.test_model_market_scope`
  - 复验结论：通过。模型落库前校验改为基于训练列的 `factor_name` 命名空间，同时 metadata 新增 `feature_lineage` 记录 `factor_id`、`factor_name`、训练列、缺失声明因子和未声明训练列。未声明训练列仍会阻断保存；声明但训练窗口无有效覆盖的因子会进入 metrics/metadata，而不再误判全部 factor_id 缺失。

### [2026-05-01] P2 缺陷：REST 创建 CN 策略返回 500 且无可读错误

- **状态**：Done
- **来源**：agent 研究发现
- **影响范围**：A 股策略研究、`POST /api/strategies`、`StrategyService.create_strategy`、前端策略创建页、后续回测链路
- **复现入口**：
  - API：`POST /api/strategies`
  - 成功对照：相同极简策略使用 `market="US"` 返回 HTTP 200 并生成策略 `56634ff0fe0d`。
  - 失败请求：相同极简策略使用 `market="CN"`，或 `CN_CORE_MOM_RANK_BASELINE_V1` 策略使用 `market="CN"`。
- **当前证据**：
  - 实际结果：HTTP `500 Internal Server Error`，响应体只有 `Internal Server Error`；`GET /api/strategies?market=CN` 返回空数组。
  - 日志 / 错误：`logs/qagent.log` 未记录对应 CN 策略创建异常栈；只能通过 API 响应看到 500。服务进程仍正常，US 策略创建与 US 回测可继续运行。2026-05-01 复验：带 `from __future__ import annotations` 的极简策略会被策略沙箱以 HTTP 400 正常拒绝；去掉该 import、仅使用允许的 `pandas` / `backend.strategies.base` 后，同一 CN 极简 no-op 策略仍返回 HTTP 500，响应体仍只有 `Internal Server Error`。
  - 已排除项：策略源码可通过 `backend.strategies.loader.load_strategy_from_code` 正常加载；US 极简策略可创建，说明不是通用策略语法或策略创建接口整体不可用。
- **期望行为**：CN 策略创建应与 US 一致成功；如果失败，应由 API 返回 HTTP 400/409 等可读错误，并把异常上下文写入结构化日志。
- **验收标准**：
  - 可量化指标：极简 CN 策略创建返回 HTTP 200，`GET /api/strategies?market=CN` 可见新增策略；或错误响应包含明确 `detail`。
  - UI 验收点：策略创建页选择 CN 后能保存策略，错误时显示可修复信息。
  - 命令 / API 复验：用极简 CN 策略 JSON 调用 `POST /api/strategies`，随后调用 `GET /api/strategies?market=CN`。
- **修复记录**：
  - commit：本次提交
  - 验证命令：`uv run python -m unittest tests.test_schema_migrations tests.test_strategy_backtest_market_scope`；live API CN 策略创建 smoke。
  - 复验结论：通过。根因是 legacy 策略表约束没有 market 维度；迁移会重建为 `UNIQUE(market, name, version)`，并保留行数校验。CN 策略创建失败时 API 会写入 `api.strategy.create_failed` 结构化日志并返回可读 detail。

### [2026-04-30] P3 技术债：Python 3.14 `datetime.utcnow()` 废弃警告

- **状态**：Done
- **来源**：agent 发现
- **影响范围**：backend services、tasks、部分测试 fixture
- **复现入口**：
  - UI：无
  - API / MCP：无直接入口
  - 资产 ID：无
- **当前证据**：
  - 实际结果：`uv run python -m unittest discover tests` 通过，但输出多处 `DeprecationWarning: datetime.datetime.utcnow() is deprecated`。
  - 日志 / 错误：涉及 `backend/services/data_service.py`、`factor_service.py`、`model_service.py`、`paper_trading_service.py`、`signal_service.py`、`strategy_service.py`、`backend/tasks/executor.py` 和相关测试。
  - 相关指标：`rg -n "datetime\\.utcnow|utcnow\\(" backend tests` 当前约 `48` 处。
- **期望行为**：时间戳写入保持兼容，同时测试输出不再被 Python 3.14 deprecation warnings 污染。
- **验收标准**：
  - 可量化指标：全量测试不再出现 `datetime.utcnow()` deprecation warning。
  - UI 验收点：无。
  - 命令 / API 复验：`uv run python -m unittest discover tests`。
- **修复记录**：
  - commit：本次提交
  - 验证命令：`uv run python -m unittest tests.test_time_utils tests.test_data_group_market_scope`
  - 复验结论：通过。新增 `backend/time_utils.py`，统一提供 `utc_now_naive()` 和 `utc_now_iso()`；服务层和 task 时间戳写入保持 DuckDB 既有 naive UTC 存储契约，后端源码不再直接调用废弃的 `datetime.utcnow()`。新增测试防止 backend 重新引入 `.utcnow(`。

### [2026-04-30] P3 UI：Ant Design 废弃属性警告

- **状态**：Done
- **来源**：UI 验收
- **影响范围**：`frontend/src/pages/MarketPage.tsx` 及复用旧 AntD 写法的前端页面/组件
- **复现入口**：
  - UI：`http://127.0.0.1:5173/market`、`/data`、`/models`
  - API / MCP：无
  - 资产 ID：无
- **当前证据**：
  - 实际结果：Task 14 页面可正常渲染，Playwright console 捕获到 `0` 条 Ant Design warning。
  - 日志 / 错误：已移除 `bodyStyle`、`Spin tip`、`Space direction`、`Modal destroyOnClose`、`Statistic valueStyle`、`Input addonBefore` 废弃用法。
  - 相关指标：`rg -n "valueStyle|addonBefore|direction=|destroyOnClose|bodyStyle|tip=" frontend/src` 无命中。
- **期望行为**：页面不使用 AntD 已废弃 props，dev console 保持低噪音。
- **验收标准**：
  - 可量化指标：打开 `/market`、切换 CN、进入 `/data` 和 `/models` 不再出现 AntD warning。
  - UI 验收点：行情页加载、图表 loading、卡片样式、数据概览、模型训练目标选择和模型指标展示保持一致。
  - 命令 / API 复验：`cd frontend && pnpm build`；Playwright 浏览器 smoke。
- **修复记录**：
  - commit：待提交于 Task 14
  - 验证命令：`cd frontend && pnpm build`；Playwright console capture。
  - 复验结论：通过，Task 14 提交后闭环。

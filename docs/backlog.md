# QAgent 需求与问题看板

本文件是 QAgent 项目的统一需求看板。Agent 在开发、研究、验收过程中发现的未闭环问题、改进需求和验收缺口都记录在这里。Human 通过 UI 和量化指标验收，agent 通过本文件维持跨会话上下文。

已完成并通过验收的问题不在本文件长期堆积，按单问题归档到 `docs/archive/backlog/`。未完全修复、待 live 复验、待 UI 验收、暂缓处理的问题继续保留在本文件。

## 使用规则

- 新问题先放到 `Inbox`，复现清楚后移动到 `Open`。
- 开始修复前移动到 `In Progress`，写明负责人、分支或会话。
- 修复完成但未验收放到 `Verify`。
- 验收通过后移动到 `Done`，并新建单问题归档文档保存复验证据和 commit。
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

### [2026-05-02] P2 性能：CN 9 模型 200+ 特征策略回测耗时高且会阻塞 API 响应

- **状态**：Verify
- **来源**：agent A 股 200+ 特征策略回测发现
- **影响范围**：CN 多模型策略、`strategy_backtest`、模型预测缓存、API 可用性
- **复现入口**：
  - 策略：`8571bb7cc7d9`、`1a5803b186d9`、`5b04a756d0a0`、`bc87585be0e2`
  - 股票池：`cn_a_core_indices_union`
  - 模型：9 个 CN 模型，特征集分别为 430F、273F、288F
  - 回测窗口：`2026-01-02~2026-04-02` 或 `2026-04-06~2026-04-24`
- **当前证据**：
  - 原始 live 任务中，`8571bb7cc7d9` 主窗任务 `91cee58db6b347538e3756b8f28b1c06` 运行约 10 分钟；短窗任务 `c61b4f3213274b248e674f0edb0d15ce` 运行约 5.5 分钟。
  - 本轮代码审查发现真实低效实现：同一回测中多个模型共享同一 feature set 时，`_batch_predict_all_dates()` 会重复计算/加载整段 feature matrix。
- **期望行为**：多模型预测应复用同一 feature set 的全窗口特征矩阵；长任务运行时 API 查询不应被阻塞，任务进度应包含当前日期/预测阶段。
- **验收标准**：
  - 可量化指标：同一 9 模型 CN 主窗回测耗时显著下降，重复窗口能复用预测缓存；API 任务查询 P95 保持在 2 秒以内。
  - UI 验收点：回测任务显示进度、当前阶段、已处理日期数。
  - 命令 / API 复验：重复运行 `8571bb7cc7d9` 主窗回测，对比首次与二次耗时，并在运行中持续调用 `/api/tasks/{task_id}`。
- **修复记录**：
  - commit：本次提交
  - 验证命令：`uv run python -m unittest tests.test_backtest_diagnostics_contracts`；`uv run python -m unittest tests.test_backtest_diagnostics_contracts tests.test_factor_feature_market_scope tests.test_task_executor_contracts tests.test_data_group_market_scope`；`cd frontend && pnpm build`
  - 复验结论：代码级修复通过。`BacktestService._batch_predict_all_dates()` 已按 `feature_set_id` 缓存全窗口 feature matrix，多个模型共享同一 feature set 时特征计算从每模型一次降为每 feature set 一次。该问题不是纯机器限制，仍需 live 9 模型 CN 主窗 benchmark 验收耗时和 API P95。

### [2026-05-02] P1 回测可靠性：持仓当日缺失行情时 NAV 将该持仓按 0 估值，导致窗口末端异常暴跌

- **状态**：Verify
- **来源**：agent US 稳健性研究 live API 复验
- **影响范围**：US 策略回测、`BacktestEngine` 持仓估值、数据质量校验、回测指标可信度
- **复现入口**：
  - UI：策略回测页运行 US 策略并查看详情
  - API / MCP：`POST /api/strategies/47425ba22837/backtest`，`market="US"`，`universe_group_id="sp500"`，窗口 `2026-04-06` 到 `2026-04-27`，配置 `rebalance_buffer=0.08`、`rebalance_buffer_mode="hold_overlap_only"`
  - 资产 ID：策略 `47425ba22837`，异常回测 `5ada1f38a2f1`；对照正常窗口回测 `f4c90c5e3948`（`2026-04-06` 到 `2026-04-24`）
- **当前证据**：
  - 实际结果：`5ada1f38a2f1` 的 NAV 从 `2026-04-24` 的 `1,239,521.70` 跳到 `2026-04-27` 的 `485,821.83`，summary 显示 `total_return=-0.514178`、`max_drawdown=-0.608057`。但同一策略同一配置到 `2026-04-24` 的正常回测 `f4c90c5e3948` 为 `total_return=0.239522`、`max_drawdown=-0.018292`。
  - 日志 / 错误：无任务错误。复验持仓发现 `STX`、`INTC`、`MCHP` 等在 `2026-04-24` 有价格但 `2026-04-27` 缺 daily bar；`BacktestEngine` 估值循环只在当日 close 存在时计入持仓价值，缺价持仓被跳过，相当于按 0 估值。
  - 相关指标：`GET /api/data/status?market=US` 同时返回 `date_range.max=2026-04-27`、`latest_trading_day=2026-04-30`、`stale_tickers=5378`，说明数据覆盖存在大量局部陈旧，回测入口未阻断或告警。
- **期望行为**：回测不应在持仓缺少当日价格时静默把价值归零；应使用可解释的估值策略，例如按该 ticker 最近可用 close 在有限天数内 carry-forward，或直接阻断并返回缺失持仓/日期清单。数据状态也应区分“市场日历最新日”和“可用于回测的全池覆盖日期”。
- **验收标准**：
  - 可量化指标：复跑上述 `2026-04-06` 到 `2026-04-27` 回测时，不再出现由缺价持仓导致的 `40%+` 单日 NAV 断崖；若选择阻断，API 返回可读错误并列出缺价持仓。
  - UI 验收点：回测详情展示数据覆盖告警或缺失估值处理方式；异常窗口不能只显示成功指标。
  - 命令 / API 复验：对 `STX` / `INTC` / `MCHP` 在 `2026-04-27` 缺价的持仓场景运行最小回测，确认 NAV 口径稳定或任务明确失败。
- **修复记录**：
  - commit：`4e383b5 fix: improve backtest task reliability`
  - 验证命令：`uv run python -m unittest tests.test_backtest_engine_contracts`；`uv run python -m unittest discover tests`；`cd frontend && pnpm build`；`git diff --check`
  - 复验结论：代码级修复通过并已提交。`BacktestEngine` 对单只持仓缺少当日 close 的场景改为使用该 ticker 最近可用 close 估值，并在 `trade_diagnostics.missing_price_valuations` 记录日期、ticker、持仓数、carry-forward 价格和估值方式；不再把部分缺价持仓静默按 0 估值。待 live API 复跑 `47425ba22837` 的 `2026-04-06~2026-04-27` 窗口确认异常 NAV 断崖消失。

### [2026-05-02] P2 任务可靠性：并发回测任务被标记为 Cancelled by user，但后台仍保存 late result

- **状态**：Verify
- **来源**：agent US 稳健性研究 live API 复验
- **影响范围**：`TaskExecutor`、`POST /api/strategies/{strategy_id}/backtest`、批量研究任务、任务状态与 backtest 历史一致性
- **复现入口**：
  - UI：任务页 / 回测历史页
  - API / MCP：短时间内并发提交多个 `strategy_backtest` 任务，策略 `47425ba22837`，`market="US"`，`universe_group_id="sp500"`，窗口包括 `2026-01-02~2026-01-30`、`2026-02-02~2026-02-27`、`2026-03-02~2026-03-31`，配置包含 `rebalance_buffer_reference="actual_open"` 与可选 `rebalance_buffer_add/reduce`
  - 资产 ID：任务 `9511a9c90785429380cc6a56491b746c`、`7d21b45384e14b0a9bc333168c852eba`、`dcb09be316f44511a5f905cfd9435c19`；late result backtest `49a6a8e65127`、`d69e53c8526e`、`b02e477f2f4d`、`72215fc31c0d`
- **当前证据**：
  - 实际结果：多个任务 API 状态为 `failed`，`error="Cancelled by user"`，`result=null`；但 `logs/qagent.log` 同时出现 `backtest_service.saved` 和 `task.cancelled_late_result_ignored`，回测历史中可以查询到对应配置的 backtest 记录。
  - 日志 / 错误：例如任务 `9511a9c90785429380cc6a56491b746c` 显示 failed/cancelled，但日志记录 `backtest_service.saved backtest_id=49a6a8e65127`，随后 `task.cancelled_late_result_ignored task_id=9511...`。
  - 相关指标：这些 late result 的 summary 可通过 `/api/strategies/backtests/{backtest_id}` 读取，但原任务状态不会返回 backtest_id，agent 需要反查回测历史才能恢复结果。
- **期望行为**：任务取消、失败、完成和资产落库应保持一致；如果任务已经保存 backtest，应将任务状态更新为 completed 或至少在 task result/error 中暴露 `backtest_id` 和 late-result 状态，避免研究链路误判为无结果。
- **验收标准**：
  - 可量化指标：并发提交同类回测任务后，任务状态与回测历史一致；不存在 `failed + result=null` 但已落库 backtest 的不可追踪状态。
  - UI 验收点：任务页能提示 late result 或直接链接到已生成回测；回测历史不出现来源任务不可追踪的孤儿记录。
  - 命令 / API 复验：并发提交 4 个小窗口回测，轮询 `/api/tasks/{task_id}`，确认每个已保存 backtest 的任务能返回对应 `backtest_id`。
- **修复记录**：
  - commit：`4e383b5 fix: improve backtest task reliability`
  - 验证命令：`uv run python -m unittest tests.test_task_executor_contracts tests.test_strategy_backtest_market_scope tests.test_mcp_market_contracts`；`uv run python -m unittest discover tests`；`cd frontend && pnpm build`；`git diff --check`
  - 复验结论：代码级修复通过并已提交。取消后或超时后如果内部任务已经返回结果，`TaskExecutor` 不再静默忽略 late result；取消任务保持 `failed`，超时任务保持 `timeout`，并在 `result.late_result`、REST/MCP `late_result_id`、错误信息中暴露已保存资产。任务页会显示 late result 提示。待 live API 并发提交 4 个小窗口回测复验任务状态与回测历史可追踪。

### [2026-05-02] P1 缺陷：CN benchmark 缺失导致 excess-return 标签无法训练

- **状态**：Verify
- **来源**：agent A 股 200+ 特征模型研究发现
- **影响范围**：A 股模型训练、`cn_preset_fwd_excess_10d` 等超额收益标签、`/api/models/train`、后续相对沪深300的选股模型研究
- **复现入口**：
  - API：`POST /api/models/train`
  - 参数：`market="CN"`、`universe_group_id="cn_a_core_indices_union"`、`benchmark="sh.000300"` 或使用默认 CN benchmark 的 excess-return 预设标签
  - 相关日志：`label.no_benchmark_data benchmark=sh.000300 market=CN`
- **当前证据**：
  - 实际结果：训练任务失败，错误为 `ValueError: No aligned (date, ticker) pairs after joining features and labels`。
  - 回测指纹也显示 `data_watermark.benchmark.symbol="sh.000300"` 但 `rows=0`、`min_date=null`、`max_date=null`，说明当前库里没有可用于标签/benchmark 的沪深300指数行情。
  - 本轮只能改用 rank/return/path-return 标签完成 CN 200+ 模型训练，不能训练与 `sh.000300` 对齐的超额收益模型。
- **期望行为**：CN benchmark 数据应随 CN 数据更新或内置指数同步可用；如果 benchmark 缺失，标签训练应在任务开始前返回可读错误，并提示需要更新的指数符号和日期范围。
- **验收标准**：
  - 可量化指标：`sh.000300` benchmark 在 CN 数据状态中有完整覆盖，excess-return 标签能产出非空样本。
  - UI 验收点：选择 CN excess 标签时能看到 benchmark 覆盖状态；缺失时给出明确提示。
  - 命令 / API 复验：用 `cn_a_core_indices_union` 和 `cn_preset_fwd_excess_10d` 重新提交模型训练，任务完成并生成模型，或失败为明确的 benchmark 数据缺失 4xx 校验错误。
- **修复记录**：
  - commit：`4e383b5 fix: improve backtest task reliability`
  - 验证命令：`uv run python -m unittest tests.test_label_market_scope tests.test_data_group_market_scope`；`uv run python -m unittest discover tests`；`cd frontend && pnpm build`；`git diff --check`
  - 复验结论：代码级修复通过并已提交。CN/US 数据更新时 benchmark/index 增量起点不再固定为近 7 天；若 `index_bars` 没有 `sh.000300` 历史，会从 10 年窗口开始补齐。excess-return/excess-binary 标签在 benchmark 缺失时直接抛出可读 `Benchmark data missing for sh.000300 in market CN`，避免训练阶段退化为 “No aligned pairs”。待 live 执行 CN 数据增量更新并重训 `cn_preset_fwd_excess_10d` 做最终验收。

### [2026-05-02] P2 缺陷：任务 cancel 接口对 running 任务不稳定，返回 not found or not cancellable

- **状态**：Verify
- **来源**：agent A 股回测执行发现
- **影响范围**：`POST /api/tasks/{task_id}/cancel`、长任务管理、队列清理、跨市场研究隔离
- **复现入口**：
  - API：`POST /api/tasks/{task_id}/cancel`
  - 示例任务：`9e898c69791442e09b9e7ec4d489902c`、`38ec25ab47c74446968849388e3a1bab`、`5a08134df84f488cae6195c83c1120ab`
  - 任务类型：US `strategy_backtest`，状态在 `GET /api/tasks` 中显示为 `running`
- **当前证据**：
  - 取消接口多次返回 `{"detail":"Task not found or not cancellable"}`，但同一时间任务列表仍显示对应任务为 running，且后续会自然完成并生成 US 回测结果。
  - 另一个 US 训练任务 `0333c556dc2f4abc9cbf9563eef521dd` 可以正常取消，说明不是接口整体不可用，而是 running backtest 状态或 executor 映射不一致。
- **期望行为**：任务列表中显示为 running/queued 的任务应能一致取消；若底层不可中断，API 也应返回明确状态，例如 `cancel_requested`，并在任务结束时标记取消原因。
- **验收标准**：
  - 可量化指标：对 running backtest 调用 cancel 后，任务最终进入 `failed/cancelled`，不再落库新回测。
  - UI 验收点：任务列表取消按钮状态与后端实际 cancellable 状态一致。
  - 命令 / API 复验：提交一个长 backtest，立即调用 cancel，轮询 `/api/tasks/{task_id}` 验证状态和结果表。
- **修复记录**：
  - commit：`4e383b5 fix: improve backtest task reliability`
  - 验证命令：`uv run python -m unittest tests.test_task_executor_contracts`；`uv run python -m unittest discover tests`
  - 复验结论：代码级修复通过并已提交。当前 Python 线程无法强杀正在执行的内部任务，因此语义调整为：取消请求会将任务置为 `failed` / `Cancelled by user`；如果内部任务之后返回结果，任务保留失败语义但补充 `late_result` 与 `late_result_id`，避免不可追踪资产。待 live 长回测取消场景复验。

### [2026-05-02] P3 体验：CN 回测起始日被自动调整为下一交易日，但提交响应不提示

- **状态**：Verify
- **来源**：agent A 股对齐窗口回测发现
- **影响范围**：`POST /api/strategies/{strategy_id}/backtest`、回测配置可解释性、防穿越审计
- **复现入口**：
  - 请求配置：`start_date="2026-01-02"` 或 `start_date="2026-04-06"`，`market="CN"`，`universe_group_id="cn_a_core_indices_union"`
  - 回测结果：主窗实际保存为 `2026-01-05` 起，近端实际保存为 `2026-04-07` 起
- **当前证据**：
  - 提交请求返回只包含 `task_id/status/strategy_id/market`，没有说明起始日会被交易日历调整。
  - 回测详情和 leakage warning 中显示实际 `backtest_start` 分别为 `2026-01-05` / `2026-04-07`，需要事后查询才知道。
- **期望行为**：提交响应或任务结果应明确记录 `requested_start_date` 与 `effective_start_date`，并说明调整原因是非交易日/无数据日。
- **验收标准**：
  - 可量化指标：所有回测结果配置同时保留 requested/effective 日期。
  - UI 验收点：回测详情页显示“请求日期 -> 实际交易日期”的映射。
  - 命令 / API 复验：用非交易日提交 CN 回测，任务结果中可直接看到日期调整说明。
- **修复记录**：
  - commit：`4e383b5 fix: improve backtest task reliability`
  - 验证命令：`uv run python -m unittest tests.test_strategy_backtest_market_scope`；`cd frontend && pnpm build`
  - 复验结论：代码级修复通过并已提交。回测保存配置和任务摘要现在包含 `requested_start_date`、`effective_start_date`、`requested_end_date`、`effective_end_date` 和 `date_adjustment`；任务列表会展示请求起始日到实际起始日的映射。待 UI 详情页进一步做专门展示优化。

### [2026-05-02] P2 研究限制：非空目标权重会被回测引擎归一化为满仓，无法正式验证动态现金/风险预算

- **状态**：Verify
- **来源**：agent 美股 S294 风险预算与做 T 控制研究发现
- **影响范围**：`BacktestEngine.run`、动态仓位缩放、风险预算、现金权重、vol targeting / loss cooldown 类策略研究
- **复现入口**：
  - 策略输出任意非空 `target_weights`，例如所有股票权重和为 `0.5`。
  - 回测配置：`position_sizing="equal_weight"` 或策略权重经过引擎执行。
- **当前证据**：
  - `/Users/m/dev/qagent/backend/services/backtest_engine.py` 中 `target_weights` 在执行前只要 `weight_sum > 0` 就会归一化到 `sum=1`。
  - 因此离线风险覆盖实验只能近似复盘，不能在正式 qagent 回测中验证“降低总仓位到 50%/70%”这类动态现金方案；除非策略返回空信号直接全空仓。
  - 0502 美股研究中，vol target / loss cooldown 离线复盘 Sharpe 低于基线，但该类结论仍缺少正式 backtest engine 支持。
- **期望行为**：回测引擎应支持可选的 `allow_cash_weight` / `normalize_target_weights=false`，保留策略输出的总风险预算，同时继续支持旧策略默认满仓归一化。
- **验收标准**：
  - 可量化指标：同一策略输出权重和 `0.5` 时，开启现金权重配置后实际持仓约 50%，NAV 中剩余现金不被强制再分配。
  - UI 验收点：策略回测页可显示目标权重和、现金权重、是否归一化。
  - 命令 / API 复验：构造固定 2 票各 25% 权重策略，对比默认归一化与 `normalize_target_weights=false` 的成交额和 NAV。
- **修复记录**：
  - commit：`4e383b5 fix: improve backtest task reliability`
  - 验证命令：`uv run python -m unittest tests.test_backtest_engine_contracts`；`uv run python -m unittest discover tests`
  - 复验结论：代码级修复通过并已提交。新增 `BacktestConfig.normalize_target_weights`，默认 `true` 保持旧策略满仓归一化；配置为 `false` 时保留策略输出的总权重，未分配部分作为现金，并在 `trade_diagnostics.target_weight_policy` 记录是否归一化和现金权重。待 UI 回测面板增加显式开关。

### [2026-05-02] P2 稳定性：被取消或本地等待中断的长任务可能已经落库结果，研究脚本必须手动从历史回填

- **状态**：Verify
- **来源**：agent 美股 path-quality model probe 研究发现
- **影响范围**：`/api/tasks`、`strategy_backtest`、`model_train`、长任务取消/超时语义、agent 自动化研究脚本
- **复现入口**：
  - 模型训练任务 `0333c556dc2f4abc9cbf9563eef521dd` 显示 `failed` 且错误为 `Cancelled by user`，但模型 `7c6a99802c81` 已生成并可用于策略。
  - 回测任务 `99c8379a70d24c3d83ee7e43a804cebb` 显示 `failed` 且错误为 `Cancelled by user`，相同窗口已有完成回测 `43610a7cb9dc` / `b9dde45bb74c`。
  - 本地研究命令超时或 SIGTERM 后，后端任务可能已 completed 并保存 backtest，例如 S302 主窗 `08c978fbc6f3`。
- **当前证据**：
  - `/api/tasks` 状态与资产表/回测历史有时需要二次核对；只依赖任务状态会重复提交同一长窗口。
  - 已在研究脚本 `/Users/m/dev/atlas/tmp/research_0502_pathq_model_probe.py` 和 `/Users/m/dev/atlas/tmp/research_0502_residual_crowding_guard.py` 中加入按 `strategy_id + window + config` 从 `/api/strategies/backtests` 回填的兜底逻辑。
- **期望行为**：任务取消、超时、late result 的语义应统一；如果结果已经落库，任务状态或错误信息应指向对应 asset/backtest id，避免 agent 和 UI 重复运行。
- **验收标准**：
  - 可量化指标：取消/超时后若 late result 被保存，`/api/tasks/{task_id}` 返回可见 `late_result_id` 或状态转为可解释的 late_completed；若 late result 被忽略，则资产表不出现孤儿结果。
  - UI 验收点：任务列表可区分“真正失败”、“用户取消但结果已保存”、“后台超时后晚完成”。
  - 命令 / API 复验：提交长回测并在不同阶段取消/中断客户端，核对任务状态、backtest history、日志三者一致。
- **修复记录**：
  - commit：`4e383b5 fix: improve backtest task reliability`
  - 验证命令：`uv run python -m unittest tests.test_task_executor_contracts tests.test_mcp_market_contracts`；`uv run python -m unittest discover tests`；`cd frontend && pnpm build`
  - 复验结论：代码级修复通过并已提交。REST/MCP 任务状态会在 late result 存在时暴露 `late_result` 和 `late_result_id`，前端任务页会显示 late result 提示。旧研究脚本仍可保留历史回填兜底，但新任务不应再出现完全不可追踪的 late result。

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

已验收通过的问题已按单问题归档到 `docs/archive/backlog/`，本文件不再保留完整正文。

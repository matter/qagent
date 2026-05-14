# QAgent Agent 完整使用手册

本文档面向自动化 agent、研究脚本和 MCP 调用者。目标是让 agent 阅读后可以理解 QAgent 的系统边界、所有主要功能、REST API、MCP 工具、任务语义、研究流程和使用禁区。

QAgent 是本地优先、单用户、低频量化研究系统。当前主线是 3.0 重构：优先服务 US equities，把市场数据、因子、数据集、模型、组合、风控、执行、策略图、QA、正式信号和 paper trading 解耦为可复用资产。2.0 legacy 模块仍可用，用于继续运行旧因子、旧模型、旧策略、旧回测和旧信号。

## 0. 最新能力地图

| 能力 | 当前 agent 用法 | 验收重点 |
| --- | --- | --- |
| 市场隔离 | legacy REST/MCP 缺省 `market=US`；3.0 使用 `US_EQ` / `CN_A` market profile。任何 group、factor、label、feature set、model、strategy、backtest、signal、paper 依赖都必须在同一市场内解析。 | 返回结果里的 `market` 或 `market_profile_id` 一致；不要混用 US ticker 和 CN ticker。 |
| A股数据 | CN legacy 默认 provider 是 BaoStock，默认股票池是 `cn_a_core_indices_union`，来源为上证50、沪深300、中证500、创业板指成分股去重并集。 | 先查 group 成员和数据覆盖；CN 免费源仍是 exploratory，不满足 PIT / survivorship-safe。 |
| 长任务 | 数据更新、因子计算、模型训练、回测、信号、paper 推进等走 `TaskExecutor`，用 `/api/tasks` 轮询。 | 看 `authoritative_terminal`、`late_result_quarantined`、`serial_wait`、`stage_domain_write`，不要消费晚到 quarantined 结果。 |
| 研究缓存 | feature matrix 与 label values 有 48 小时 hot cache；factor values 仍走 market-aware cache。 | cache key 必须带稳定 data version；通过 `/api/research-cache/*` 查库存、预热、清理，不手删文件。 |
| 模型目标 | legacy model 支持 `regression`、`classification`、`ranking`、`pairwise`、`listwise`；ranking 类目标按同日横截面分组。 | 检查 `objective_type`、`ranking_groups`、`ndcg@k`、`rank_ic`、`pairwise_accuracy_sampled` 和 split/purge gap。 |
| 策略与执行 | legacy strategy 继续 subclass `StrategyBase`；3.0 推荐 StrategyGraph。V3.1 支持 `planned_price`，并可设置 `planned_price_fallback` / `fill_fallback`。 | 对 planned execution 检查 fill diagnostics、blocked reason、fallback close rate 和策略输出的 `planned_price` 质量。 |
| 研究治理 | 3.0 agent research plan 记录 hypothesis、trial、budget、trial matrix、QA 和 promotion。 | trial 不能只留在聊天记录；候选结果必须有 evidence package 才能 promotion。 |
| 人类验收 | human 主要通过 React UI 看任务、图表、诊断、paper 和研究工作台；agent 使用 REST/MCP 操作同一 service layer。 | UI/API/MCP 行为一致；新增字段同步更新 `frontend/src/api/index.ts`。 |

## 1. Agent 必须遵守的系统原则

- 人类 UI、REST API、MCP server 必须共享同一 backend service layer。新增能力时先改 `backend/services/`，再暴露 REST、MCP、UI。
- 不要直接写 DuckDB 业务表绕过 service。只读诊断也优先用 API、MCP 或受控 service。
- 长任务必须通过 `TaskExecutor` 提交，返回 `task_id`，由 `/api/tasks/{task_id}` 轮询。
- 时间序列正确性优先于功能数量。不得引入未来函数，不得用随机 K-Fold 验证市场时间序列。
- 信号日期和执行日期要分离，遵守现有 T+1、next open、calendar-aware 语义。
- 研究资产必须可复现。因子、数据集、模型、策略、回测、信号、paper session 要保留 source/config/dependency snapshot、artifact、lineage。
- 正式结果和探索结果要隔离。scratch/trial/experiment 可以清理，validated/published 结果必须保护。
- 除非用户明确要求，不要启动、取消、重启或中断数据更新任务。

## 2. 服务、地址和基础调试

| 项目 | 用法 |
| --- | --- |
| 前端 UI | `http://127.0.0.1:5173` |
| 后端 API | `http://127.0.0.1:8000` |
| 健康检查 | `GET /api/health` |
| 系统信息 | `GET /api/system/info` |
| OpenAPI schema | `GET /openapi.json` |
| Swagger UI | `GET /docs` |
| MCP endpoint | mounted at `/mcp` |
| 本地状态 | `scripts/status.sh` |
| 后台启动 | `scripts/start_detached.sh` |
| 停止服务 | `scripts/stop.sh` |

Agent 需要最新精确参数时，优先读取 `/openapi.json`，本文档提供使用语义、接口索引和推荐流程。

## 3. 市场、项目和版本边界

| 概念 | 当前约定 |
| --- | --- |
| 当前优先市场 | US equities |
| Legacy 市场枚举 | `US`, `CN` |
| 3.0 market profile | `US_EQ`, `CN_A` |
| 默认 project | bootstrap project，通常为 `bootstrap_us` 或 `/api/research/projects/bootstrap` 返回值 |
| 3.0 推荐入口 | `/api/research/*`, `/api/market-data/*`, `/api/research-assets/*`, `/api/research/agent/*` |
| 2.0 fallback | `/api/data/*`, `/api/factors/*`, `/api/feature-sets/*`, `/api/models/*`, `/api/strategies/*`, `/api/signals/*`, `/api/paper-trading/*` |
| CN 默认 group | `cn_a_core_indices_union` |

用法选择：

- 新研究优先走 3.0：project、run、artifact、lineage、universe、dataset、factor spec、model experiment、portfolio assets、StrategyGraph、QA、production signal。
- 旧资产继续走 2.0 legacy：已有 factor、feature set、model、strategy、backtest、signal、paper session。
- 跨版本迁移使用 migration 和 legacy adapter，不要直接复制表数据。
- A股研究可以走 legacy `market=CN` 或 3.0 `market_profile_id=CN_A`，但 maturity 低于 US。正式结论必须额外说明 BaoStock/free data 的非 PIT、非 survivorship-safe 限制。

## 4. 任务系统

长任务会返回类似：

```json
{
  "task_id": "...",
  "run_id": "...",
  "status": "queued",
  "task_type": "...",
  "poll_url": "/api/tasks/..."
}
```

轮询：

```bash
curl -fsS http://127.0.0.1:8000/api/tasks/<task_id>
```

任务接口：

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/api/tasks` | 按 `task_type/status/source/market/limit` 查询任务 |
| `GET` | `/api/tasks/{task_id}` | 查询单个任务状态、结果、错误、隔离诊断 |
| `POST` | `/api/tasks/{task_id}/cancel` | 请求取消单个任务 |
| `POST` | `/api/tasks/bulk-cancel` | 按条件批量取消任务 |
| `GET` | `/api/tasks/pause-rules` | 查询任务暂停规则 |
| `POST` | `/api/tasks/pause-rules` | 创建暂停规则 |
| `DELETE` | `/api/tasks/pause-rules/{rule_id}` | 删除暂停规则 |

常见长任务：

- `data_update`, `data_update_markets`, `stock_list_refresh`, `ticker_update`, `group_update`
- `factor_compute`, `factor_evaluate`, `factor_materialize_3_0`
- `dataset_materialize`
- `model_train`, `model_train_experiment_3_0`
- `strategy_backtest`
- `signal_generate`, `signal_diagnose`
- `paper_trading_advance`, `paper_trading_signals`

运行中数据任务处理规则：

- 先查 `/api/tasks` 和 `/api/data/update/progress`。
- 用户没有明确要求时，不调用 `/api/data/update`、`/api/data/update/markets`、MCP `update_data`、MCP `update_data_markets`。
- 不要为了 smoke test 启动 full backfill。使用小 ticker、小日期区间、只读查询验证。
- `cancel_requested` 或 `timeout` 后，任务终态以 `authoritative_terminal=true` 为准。晚到结果只会出现在 `late_result_diagnostics`，并标记 `late_result_quarantined=true`；不要把其中的 backtest/model/run id 当成已验收资产继续使用。
- 串行任务会在 `/api/tasks/{task_id}.result.progress` 中暴露 `serial_wait` 和 `serial_acquired`，并带 `serial_key`。`strategy_backtest` 使用类似 `US:legacy-backtest` 的串行 lane；看到 `serial_wait` 时说明任务是在排队，不等于计算卡死。
- 支持 `stage_domain_write` 的长任务会先暂存最终业务写入，只在任务仍处于 accepted completion boundary 时提交。`timeout`、`cancel` 或晚到结果不会发布 staged domain rows。

## 5. 3.0 REST API 完整索引

### 5.1 Research Kernel

Research Kernel 管 project、run、artifact、lineage、promotion，是 3.0 的审计骨架。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/api/research/projects/bootstrap` | 获取默认 project |
| `POST` | `/api/research/runs` | 创建 research run |
| `GET` | `/api/research/runs` | 查询 runs |
| `GET` | `/api/research/runs/{run_id}` | 查询 run 详情 |
| `POST` | `/api/research/artifacts/json` | 写入 JSON artifact |
| `GET` | `/api/research/artifacts` | 查询 artifacts |
| `GET` | `/api/research/artifacts/{artifact_id}` | 查询 artifact 详情 |
| `POST` | `/api/research/artifacts/cleanup-preview` | 预览可清理 artifacts |
| `POST` | `/api/research/artifacts/{artifact_id}/archive` | 归档 artifact |
| `GET` | `/api/research/lineage/{run_id}` | 查询 run lineage |
| `GET` | `/api/research/promotions` | 查询 promotion records |
| `GET` | `/api/research/promotions/{promotion_id}` | 查询 promotion 详情 |

关键请求字段：

- `CreateRunRequest`: `run_type`, `project_id`, `market_profile_id`, `lifecycle_stage`, `retention_class`, `created_by`, `params`
- `CreateJsonArtifactRequest`: `run_id`, `artifact_type`, `payload`, `lifecycle_stage`, `retention_class`, `metadata`, `rebuildable`
- `CleanupPreviewRequest`: `project_id`, `run_id`, `artifact_ids`, `lifecycle_stage`, `retention_class`, `artifact_type`, `include_published`, `limit`
- `ArchiveArtifactRequest`: `retention_class`, `archive_reason`

### 5.2 Market/Data Foundation

3.0 市场能力使用 market profile，而不是在业务逻辑里硬编码 market 分支。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/api/market-data/profiles` | 查询 market profiles |
| `GET` | `/api/market-data/profiles/{profile_id}` | 查询单个 market profile |
| `GET` | `/api/market-data/projects/{project_id}/context` | 查询 project market context |
| `GET` | `/api/market-data/projects/{project_id}/status` | 查询 project 数据状态 |
| `GET` | `/api/market-data/provider-capabilities` | 查询 provider/dataset 能力和质量等级 |
| `GET` | `/api/market-data/quality-contract` | 查询数据质量策略摘要 |
| `GET` | `/api/market-data/assets/search` | 按 symbol/name 搜索 asset |
| `POST` | `/api/market-data/bars/query` | 使用 `asset_id` 查询行情 |

`QueryBarsRequest`:

- `project_id`
- `market_profile_id`
- `asset_ids`
- `start`
- `end`
- `limit`

Provider capability 是系统级约束，不是展示字段。当前免费源标注如下：

| Provider | Dataset | 质量 | PIT | 约束 |
| --- | --- | --- | --- | --- |
| `yfinance` | US stock list / daily bars / index bars | exploratory | 否 | 免费网页源，无 SLA，不能当验证级行情 |
| `baostock` | CN stock list / daily bars / trade status | exploratory | 否 | 免费网页源，交易可行性仍需显式检查 |
| `fred` | macro observations | research_grade | 否 | 需要 API key，当前不是历史 realtime window 回放 |

### 5.3 Macro Data / FRED

宏观数据是跨市场辅助数据源，不属于 `US/CN` 股票 market scope。当前只接入 FRED，提交版 `config.yaml` 只能放空占位；运行时用环境变量 `FRED_API_KEY`，或本地忽略文件 `config.local.yaml` 的 `external_data.fred.api_key`。不要把 FRED 数据默认当成严格历史发布回放；FRED API 默认返回当前实时窗口，`realtime_start/realtime_end/available_at` 只是当前返回版本的可见性字段。需要严格 PIT 宏观研究时，必须先实现按历史 realtime window 回放和质量标注。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/macro-data/fred/update` | 拉取 FRED series，长任务，返回 `task_id` |
| `GET` | `/api/macro-data/series` | 查询已入库 series 元数据 |
| `GET` | `/api/macro-data/observations` | 查询已入库 observations |

`POST /api/macro-data/fred/update` 请求字段：

- `series_ids`: FRED series id 列表，例如 `["DGS10", "FEDFUNDS"]`
- `start_date`: 可选，`YYYY-MM-DD`
- `end_date`: 可选，`YYYY-MM-DD`

`GET /api/macro-data/observations` 查询参数：

- `series_ids`: 逗号分隔，例如 `DGS10,FEDFUNDS`
- `start_date`, `end_date`: 可选，按 observation date 过滤
- `as_of`: 可选，按 `available_at <= as_of` 过滤
- `limit`: 默认 `10000`

使用规则：

- 更新宏观数据会写 DB，必须走 REST/MCP 的任务接口，不要在运行中的后端外直接连接主 DuckDB 写入。
- 查询宏观数据是只读，可以用于因子研究前的诊断和样本检查。
- 对外报告中要标注 FRED 当前实现为 research-grade，不是完整 PIT macro feed。

### 5.4 Migration

Migration 是 side-by-side 迁移入口，先 report，再 apply。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/migration/report` | 生成迁移报告，不写正式 3.0 资产 |
| `POST` | `/api/migration/apply` | 执行迁移 |

请求字段：`db_path` 可选。常规本地运行不要传。

### 5.5 Research Cache

Research Cache 用于减少重复特征矩阵和 label values 计算。它是性能优化层，不是正式研究资产；正式资产仍应通过 dataset、model、backtest、artifact 和 lineage 持久化。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/api/research-cache/inventory` | 查询 cache summary 和 entries |
| `POST` | `/api/research-cache/feature-matrix/warmup` | 异步预热 feature matrix cache |
| `GET` | `/api/research-cache/factor-cache/cleanup-preview` | 预览可清理 factor cache |
| `POST` | `/api/research-cache/factor-cache/cleanup-apply` | 应用 factor cache 清理 |
| `POST` | `/api/research-cache/expired/cleanup-apply` | 清理过期 research cache |

`WarmupFeatureMatrixRequest`: `market`, `feature_set_id`, `universe_group_id`, `start_date`, `end_date`, `timeout`。

使用规则：

- feature matrix 和 label values hot cache 默认 TTL 为 2 天，即 48 小时。
- cache key 包含 market、tickers、日期、factor refs、preprocessing、label definition 和 data version。禁止使用不可复现的 unversioned latest cache。
- cache 文件位于 `data/research_cache`，不要手工删除；用 cleanup API 让 DB metadata 和文件状态一致。
- model training、batch prediction、dataset materialize 已优先走 `compute_features_from_cache` / `compute_label_values_cached`。agent 重复训练前可先 warmup 同一 feature set、universe 和日期范围。

### 5.6 Universe 和 Dataset

Universe 定义研究股票池。Dataset 固化 universe、feature、label、时间范围、split policy，是模型训练唯一推荐输入。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/research-assets/universes/static` | 创建静态 universe |
| `POST` | `/api/research-assets/universes/legacy-group` | 从 legacy group 创建 3.0 universe |
| `GET` | `/api/research-assets/universes` | 查询 universes |
| `GET` | `/api/research-assets/universes/{universe_id}` | 查询 universe |
| `POST` | `/api/research-assets/universes/{universe_id}/materialize` | 固化 universe membership |
| `GET` | `/api/research-assets/universes/{universe_id}/profile` | 查询 universe profile |
| `POST` | `/api/research-assets/datasets` | 创建 dataset 定义 |
| `GET` | `/api/research-assets/datasets` | 查询 datasets |
| `GET` | `/api/research-assets/datasets/{dataset_id}` | 查询 dataset |
| `POST` | `/api/research-assets/datasets/{dataset_id}/materialize` | 异步固化 dataset |
| `GET` | `/api/research-assets/datasets/{dataset_id}/profile` | 查询 dataset profile |
| `GET` | `/api/research-assets/datasets/{dataset_id}/sample` | 查询 dataset 样本 |
| `POST` | `/api/research-assets/datasets/{dataset_id}/query` | 查询 dataset panel |

关键请求字段：

- `CreateStaticUniverseRequest`: `project_id`, `market_profile_id`, `name`, `description`, `tickers`, `lifecycle_stage`, `metadata`
- `CreateLegacyUniverseRequest`: `project_id`, `market`, `legacy_group_id`, `name`, `description`, `lifecycle_stage`
- `MaterializeUniverseRequest`: `start_date`, `end_date`, `lifecycle_stage`
- `CreateDatasetRequest`: `project_id`, `market_profile_id`, `name`, `description`, `universe_id`, `feature_pipeline_id`, `feature_set_id`, `label_spec_id`, `label_id`, `start_date`, `end_date`, `split_policy`, `lifecycle_stage`, `retention_class`, `metadata`
- `QueryDatasetRequest`: `start_date`, `end_date`, `asset_ids`, `columns`, `limit`

Split policy 必须避免泄漏，`purge_gap` 应不小于 label horizon。composite label 使用 `effective_horizon`，应按最大子 label horizon 设 purge gap。

### 5.7 Factor Engine 3.0

Factor Engine 3.0 把因子定义为 `FactorSpec`，把计算结果定义为 `FactorRun`。preview 用于探索，不写正式 factor values；materialize 才写正式资产。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/research-assets/factor-specs/legacy` | 从 legacy factor 导入 FactorSpec |
| `POST` | `/api/research-assets/factor-specs` | 创建 Python FactorSpec |
| `GET` | `/api/research-assets/factor-specs` | 查询 FactorSpecs |
| `GET` | `/api/research-assets/factor-specs/{factor_spec_id}` | 查询 FactorSpec |
| `POST` | `/api/research-assets/factor-specs/{factor_spec_id}/preview` | 小范围预览因子 |
| `POST` | `/api/research-assets/factor-specs/{factor_spec_id}/materialize` | 异步正式计算因子 |
| `POST` | `/api/research-assets/factor-runs/{factor_run_id}/evaluate` | 评估 FactorRun |
| `GET` | `/api/research-assets/factor-runs` | 查询 FactorRuns |
| `GET` | `/api/research-assets/factor-runs/{factor_run_id}` | 查询 FactorRun |
| `GET` | `/api/research-assets/factor-runs/{factor_run_id}/sample` | 查询 FactorRun 样本 |

关键请求字段：

- `CreateFactorSpecRequest`: `project_id`, `market_profile_id`, `name`, `description`, `source_code`, `params_schema`, `default_params`, `required_inputs`, `compute_mode`, `expected_warmup`, `applicable_profiles`, `semantic_tags`, `lifecycle_stage`, `status`, `metadata`
- `FactorComputeRequest`: `universe_id`, `start_date`, `end_date`, `params`
- `FactorMaterializeRequest`: `FactorComputeRequest` + `lifecycle_stage`
- `FactorEvaluateRequest`: `label_id`, `start_date`, `end_date`

自定义因子代码应遵守 `FactorBase.compute(data: pd.DataFrame) -> pd.Series` 语义。当前 `compute_mode="time_series"` 最成熟；`cross_sectional` 不要假设完整可用。

### 5.8 Model Experiment 3.0

模型训练应从 dataset artifact 读取 X/y，不在模型服务里临时拼特征和标签。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/research-assets/model-experiments/train` | 异步训练 model experiment |
| `GET` | `/api/research-assets/model-experiments` | 查询 experiments，可按 `dataset_id` 过滤 |
| `GET` | `/api/research-assets/model-experiments/{experiment_id}` | 查询 experiment |
| `POST` | `/api/research-assets/model-experiments/{experiment_id}/promote` | 生成 model package |
| `GET` | `/api/research-assets/model-packages/{package_id}` | 查询 model package |
| `POST` | `/api/research-assets/model-packages/{package_id}/predict-panel` | 对 dataset panel 预测 |

关键请求字段：

- `TrainExperimentRequest`: `name`, `dataset_id`, `model_type`, `objective`, `model_params`, `random_seed`, `lifecycle_stage`
- `PromoteExperimentRequest`: `package_name`, `approved_by`, `rationale`, `lifecycle_stage`
- `PredictPanelRequest`: `dataset_id`

### 5.9 Portfolio、Risk、Rebalance、Execution、State

3.0 中仓位、风控、再平衡、执行和状态策略是独立资产，不能继续堆进单个 strategy config。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/research-assets/portfolio-construction-specs` | 创建组合构建规格 |
| `GET` | `/api/research-assets/portfolio-construction-specs/{spec_id}` | 查询组合构建规格 |
| `POST` | `/api/research-assets/risk-control-specs` | 创建风控规格 |
| `GET` | `/api/research-assets/risk-control-specs/{spec_id}` | 查询风控规格 |
| `POST` | `/api/research-assets/rebalance-policy-specs` | 创建再平衡策略 |
| `GET` | `/api/research-assets/rebalance-policy-specs/{spec_id}` | 查询再平衡策略 |
| `POST` | `/api/research-assets/execution-policy-specs` | 创建执行策略 |
| `GET` | `/api/research-assets/execution-policy-specs/{spec_id}` | 查询执行策略 |
| `POST` | `/api/research-assets/state-policy-specs` | 创建状态策略 |
| `POST` | `/api/research-assets/portfolio-runs/construct` | 使用 alpha frame 构建组合 |
| `POST` | `/api/research-assets/portfolio-runs/compare-builders` | 对比多个组合构建器 |
| `GET` | `/api/research-assets/portfolio-runs` | 查询组合构建 runs |
| `GET` | `/api/research-assets/portfolio-runs/{portfolio_run_id}` | 查询组合 run |

关键请求字段：

- `PortfolioConstructionSpecRequest`: `name`, `method`, `params`, `project_id`, `market_profile_id`, `description`, `lifecycle_stage`, `status`, `metadata`
- `RiskControlSpecRequest`: `name`, `rules`, `params`, `project_id`, `market_profile_id`, `description`, `lifecycle_stage`, `status`, `metadata`
- `PolicySpecRequest`: `name`, `policy_type`, `params`, `project_id`, `market_profile_id`, `description`, `lifecycle_stage`, `status`, `metadata`
- `ConstructPortfolioRequest`: `decision_date`, `alpha_frame`, `portfolio_spec_id`, `risk_control_spec_id`, `rebalance_policy_spec_id`, `execution_policy_spec_id`, `state_policy_spec_id`, `current_weights`, `portfolio_value`, `lifecycle_stage`
- `ComparePortfolioBuildersRequest`: `decision_date`, `alpha_frame`, `portfolio_spec_ids`, `risk_control_spec_id`, `current_weights`

`alpha_frame` 常用字段：`asset_id` 或 `ticker`，`score`，可附带 `signal`、`strength`、`planned_price`、`metadata`。

执行策略支持两种模式：

- `next_open`: 默认模式，T 日决策，T+1 开盘价成交。
- `planned_price`: 计划价模式，T 日决策生成计划价，T+1 用 high/low 判断是否成交。

创建计划价执行策略示例：

```json
{
  "name": "Planned Price 50bps",
  "policy_type": "planned_price",
  "params": {
    "planned_price_buffer_bps": 50,
    "fallback": "decision_close",
    "order_ttl": "same_day"
  }
}
```

计划价模式的成交条件为 `low * (1 + buffer) <= planned_price <= high * (1 - buffer)`。默认 `buffer=50bps`，即 `low * 1.005 <= planned_price <= high * 0.995`。成交成功按 `planned_price` 记账；失败订单当日取消，不改变现金和持仓。策略或 alpha frame 没有输出有效 `planned_price` 时，系统使用决策日 close 作为 fallback，并在 diagnostics 中记录来源。

### 5.10 StrategyGraph Runtime

StrategyGraph 把 alpha、selection、portfolio、risk、rebalance、execution、state 串成显式 DAG。production signal 和 3.0 paper 都应复用这个 runtime。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/research-assets/strategy-graphs/builtin-alpha` | 创建内置 alpha StrategyGraph |
| `POST` | `/api/research-assets/strategy-graphs/legacy-adapter` | 从 legacy strategy 创建 adapter graph |
| `GET` | `/api/research-assets/strategy-graphs` | 查询 graphs |
| `GET` | `/api/research-assets/strategy-graphs/{strategy_graph_id}` | 查询 graph |
| `POST` | `/api/research-assets/strategy-graphs/{strategy_graph_id}/simulate-day` | 单日模拟，输出 trace、target、orders |
| `POST` | `/api/research-assets/strategy-graphs/{strategy_graph_id}/backtest` | 异步历史回测，返回 `task_id` |
| `GET` | `/api/research-assets/strategy-graphs/{strategy_graph_id}/backtests` | 查询 graph 回测 |
| `GET` | `/api/research-assets/backtests/{backtest_run_id}` | 查询单个 3.0 回测 |
| `GET` | `/api/research-assets/strategy-signals/{strategy_signal_id}/explain` | 查询单日信号解释 |

关键请求字段：

- `BuiltinAlphaGraphRequest`: `name`, `selection_policy`, `portfolio_construction_spec_id`, `risk_control_spec_id`, `rebalance_policy_spec_id`, `execution_policy_spec_id`, `state_policy_spec_id`, `project_id`, `market_profile_id`, `description`, `lifecycle_stage`, `status`, `metadata`
- `LegacyAdapterGraphRequest`: `name`, `legacy_strategy_id`, plus same policy refs
- `SimulateDayRequest`: `decision_date`, `alpha_frame`, `legacy_signal_frame`, `current_weights`, `portfolio_value`, `lifecycle_stage`
- `BacktestGraphRequest`: `start_date`, `end_date`, `alpha_frames_by_date`, `legacy_signal_frames_by_date`, `initial_capital`, `lifecycle_stage`, `price_field`

3.0 StrategyGraph 回测复用 `simulate_day`，并用已入库 `daily_bars` 做 close-to-close 权重组合估值。缺少任一持仓端点价格时，系统不外推收益，NAV 保持不变，并在 `backtest_daily.diagnostics.valuation.status = "missing_prices"` 中记录原因。

如果 graph 绑定 `planned_price` execution policy，`alpha_frame` 或 legacy strategy 输出可以带 `planned_price`。回测执行会读取 T+1 high/low 和 T 日 close，诊断写入 `summary.fill_diagnostics`、`backtest_daily.diagnostics.execution` 和 `backtest_trades.metadata`。agent 验收时至少检查：

- `fill_diagnostics.execution_model == "planned_price"`
- `filled_order_count`、`blocked_order_count`、`fill_rate`
- blocked 里的 `planned_price_outside_buffered_range`、`missing_execution_price`、`suspended`、`limit_up_buy_blocked`、`st_buy_blocked` 等原因
- `planned_price_source` 是否大量落到 `decision_close`

### 5.11 Agent Research、QA、Promotion

Agent 自治研究必须有计划、预算、trial 记录、QA 和 promotion gate。不要只把结果写成散乱 artifact。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/research/agent/playbooks/ensure-builtins` | 确保内置 playbooks |
| `GET` | `/api/research/agent/playbooks` | 查询 playbooks |
| `GET` | `/api/research/agent/playbooks/{playbook_id}` | 查询 playbook |
| `POST` | `/api/research/agent/plans` | 创建研究计划 |
| `GET` | `/api/research/agent/plans` | 查询研究计划 |
| `GET` | `/api/research/agent/plans/{plan_id}` | 查询计划详情 |
| `GET` | `/api/research/agent/plans/{plan_id}/performance` | 查询 trial 排名和指标分布 |
| `GET` | `/api/research/agent/plans/{plan_id}/trial-matrix` | 查询结构化 trial 矩阵、hypothesis 分组和 stop/promote 决策 |
| `GET` | `/api/research/agent/plans/{plan_id}/budget` | 查询剩余额度 |
| `POST` | `/api/research/agent/plans/{plan_id}/trials` | 记录单个 trial |
| `POST` | `/api/research/agent/plans/{plan_id}/trials/batch` | 批量记录 trials |
| `GET` | `/api/research/agent/plans/{plan_id}/trials` | 查询 trials |
| `POST` | `/api/research/agent/qa` | 评估 QA gate |
| `GET` | `/api/research/agent/qa` | 查询 QA reports |
| `GET` | `/api/research/agent/qa/{qa_report_id}` | 查询 QA report |
| `POST` | `/api/research/agent/promotion` | 评估是否可 promotion |
| `POST` | `/api/research/agent/promotion-policies/default` | 创建或查询默认 promotion policy |

关键请求字段：

- `CreateResearchPlanRequest`: `hypothesis`, `playbook_id`, `search_space`, `budget`, `stop_conditions`, `project_id`, `market_profile_id`, `created_by`, `metadata`
- `RecordTrialRequest`: `trial_type`, `params`, `result_refs`, `metrics`, `qa_report_id`, `status`
- `RecordTrialsRequest`: `trials`, `dedupe_by_params`
- `EvaluateQaRequest`: `source_type`, `source_id`, `metrics`, `artifact_refs`, `project_id`, `market_profile_id`
- `EvaluatePromotionRequest`: `source_type`, `source_id`, `qa_report_id`, `metrics`, `policy_id`, `approved_by`, `rationale`

legacy 策略研究也应使用 3.0 agent research plan 记录 trial。建议在 plan `metadata` 中写 `baseline_strategy_id` 和 `baseline_backtest_id`，每个 trial 的 `params` 至少包含 `changed_module`、`changed_variable`、`hypothesis`、`config_hash`、`baseline_strategy_id`、`baseline_backtest_id`；停止方向写 `conclusion="stop"` 和 `stop_reason`。`trial-matrix` 会按主指标排序，并按 changed module 聚合，便于下一个 agent 避免重复失败方向。

QA gate 对 promotion-like source 会校验 artifact refs。不存在的 artifact 会阻断；scratch artifact 不能支撑 StrategyGraph、backtest、model package、production signal 等进入推广链路。

Promotion-like source 包括 `strategy_graph`、`backtest_run`、`model_package`、`model_experiment`、`production_signal_run`。这些 QA 必须在 `metrics.evidence` 提供完整证据包：`data_quality_contract`、`pit_status`、`split_policy`、`dependency_snapshot`、`valuation_diagnostics`、`artifact_hashes`、`reviewer_decision`。缺少证据包时，即使 headline metrics 达标也应视为阻断。

### 5.12 Production Signal、Paper、Reproducibility

正式信号和 paper 必须尽量从 validated/published StrategyGraph 生成。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/research-assets/production-signals/generate` | 生成正式信号 |
| `GET` | `/api/research-assets/production-signals` | 查询正式信号 runs |
| `GET` | `/api/research-assets/production-signals/{signal_run_id}` | 查询正式信号 |
| `POST` | `/api/research-assets/paper-sessions` | 创建 3.0 paper session |
| `GET` | `/api/research-assets/paper-sessions` | 查询 paper sessions |
| `GET` | `/api/research-assets/paper-sessions/{session_id}` | 查询 paper session |
| `POST` | `/api/research-assets/paper-sessions/{session_id}/advance` | 推进 paper session |
| `POST` | `/api/research-assets/reproducibility-bundles` | 导出复现包 |
| `GET` | `/api/research-assets/reproducibility-bundles/{bundle_id}` | 查询复现包 |

关键请求字段：

- `GenerateProductionSignalRequest`: `strategy_graph_id`, `decision_date`, `alpha_frame`, `legacy_signal_frame`, `current_weights`, `portfolio_value`, `qa_report_id`, `approved_by`
- `CreatePaperSessionRequest`: `strategy_graph_id`, `start_date`, `name`, `initial_capital`, `config`
- `AdvancePaperSessionRequest`: `decision_date`, `alpha_frame`, `legacy_signal_frame`

3.0 paper 每次 `advance` 会先按上次 `current_date` 到本次 `decision_date` 的持仓价格相对值重估 NAV，再用重估后的 NAV 和漂移权重生成新 target。默认估值字段为 `close`，可在 session `config.valuation_price_field` 覆盖。缺价时不估收益，只写 diagnostics。
- `ExportBundleRequest`: `source_type`, `source_id`, `name`

## 6. Legacy REST API 完整索引

Legacy 模块是 2.0 可用路径。Agent 可以使用，但新研究应优先向 3.0 资产沉淀。

### 6.1 Data、Stock、Index、Diagnostics

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/api/data/status` | 查询数据状态 |
| `POST` | `/api/data/update` | 单市场数据更新，谨慎使用 |
| `POST` | `/api/data/update/markets` | 多市场顺序更新，谨慎使用 |
| `POST` | `/api/data/refresh-stock-list` | 刷新股票列表 |
| `POST` | `/api/data/update/tickers` | 更新指定 tickers |
| `POST` | `/api/data/update/group` | 更新指定 group |
| `GET` | `/api/data/update/progress` | 查询最近数据更新进度 |
| `GET` | `/api/data/quality` | 查询数据质量 |
| `DELETE` | `/api/data/bars` | 删除指定日期日线，破坏性操作 |
| `GET` | `/api/stocks/search` | 搜索股票 |
| `GET` | `/api/stocks/{ticker}/daily` | 查询 ticker 日线 |
| `GET` | `/api/data/index-bars/{symbol}` | 查询指数日线 |
| `GET` | `/api/data/groups/{group_id}/daily-snapshot` | 查询分组某日行情快照 |
| `GET` | `/api/diagnostics/daily-bars` | 只读诊断日线小样本 |
| `GET` | `/api/diagnostics/factor-values` | 只读诊断因子值小样本 |
| `GET` | `/api/diagnostics/db-preflight` | 主 DuckDB 维护预检，检查锁和可读性 |

数据更新请求字段：

- `UpdateRequest`: `mode`, `market`, `history_years`, `start_date`
- `MultiMarketUpdateRequest`: `mode`, `markets`, `history_years`, `start_date`
- `UpdateTickersRequest`: `tickers`, `market`
- `UpdateGroupRequest`: `group_id`, `market`

### 6.2 Groups

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/groups/refresh-indices` | 刷新内置指数分组 |
| `POST` | `/api/groups` | 创建股票组 |
| `GET` | `/api/groups` | 查询股票组 |
| `GET` | `/api/groups/{group_id}` | 查询股票组详情 |
| `PUT` | `/api/groups/{group_id}` | 更新股票组 |
| `DELETE` | `/api/groups/{group_id}` | 删除股票组 |
| `POST` | `/api/groups/{group_id}/refresh` | 刷新 filter group |

请求字段：

- `CreateGroupRequest`: `market`, `name`, `description`, `group_type`, `tickers`, `filter_expr`
- `UpdateGroupRequest`: `market`, `name`, `description`, `tickers`, `filter_expr`

### 6.3 Labels

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/labels` | 创建 label |
| `GET` | `/api/labels` | 查询 labels |
| `GET` | `/api/labels/{label_id}` | 查询 label |
| `PUT` | `/api/labels/{label_id}` | 更新 label |
| `DELETE` | `/api/labels/{label_id}` | 删除 label |

请求字段：

- `CreateLabelRequest`: `market`, `name`, `description`, `target_type`, `horizon`, `benchmark`, `config`
- `UpdateLabelRequest`: same fields plus `status`

### 6.4 Legacy Factors

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/factors` | 创建 legacy factor |
| `GET` | `/api/factors` | 查询 factors |
| `GET` | `/api/factors/templates` | 查询内置模板 |
| `GET` | `/api/factors/templates/{template_name}` | 查询模板源码 |
| `GET` | `/api/factors/evaluations` | 查询所有评估 |
| `GET` | `/api/factors/evaluations/{eval_id}` | 查询评估详情 |
| `GET` | `/api/factors/{factor_id}` | 查询 factor |
| `PUT` | `/api/factors/{factor_id}` | 更新 factor |
| `DELETE` | `/api/factors/{factor_id}` | 删除 factor |
| `POST` | `/api/factors/{factor_id}/compute` | 异步计算 factor |
| `POST` | `/api/factors/{factor_id}/evaluate` | 异步评估 factor |
| `POST` | `/api/factors/evaluate` | body 中指定 factor_id 评估 |
| `GET` | `/api/factors/{factor_id}/evaluations` | 查询单 factor 评估 |

请求字段：

- `CreateFactorRequest`: `market`, `name`, `source_code`, `description`, `category`, `params`
- `UpdateFactorRequest`: `market`, `source_code`, `description`, `category`, `params`, `status`
- `ComputeFactorRequest`: `market`, `universe_group_id`, `start_date`, `end_date`
- `EvaluateFactorRequest`: `market`, `label_id`, `universe_group_id`, `start_date`, `end_date`

### 6.5 Feature Sets

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/feature-sets` | 创建 feature set |
| `GET` | `/api/feature-sets` | 查询 feature sets |
| `GET` | `/api/feature-sets/{fs_id}` | 查询 feature set |
| `PUT` | `/api/feature-sets/{fs_id}` | 更新 feature set |
| `DELETE` | `/api/feature-sets/{fs_id}` | 删除 feature set |
| `POST` | `/api/feature-sets/{fs_id}/correlation` | 计算相关性矩阵 |

请求字段：

- `CreateFeatureSetRequest`: `market`, `name`, `description`, `factor_refs`, `preprocessing`
- `CorrelationRequest`: `market`, `universe_group_id`, `start_date`, `end_date`

### 6.6 Legacy Models

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/models/train` | 异步训练 legacy model |
| `POST` | `/api/models/train-distillation` | 用 teacher 模型预测生成 soft label 并异步训练 student model |
| `GET` | `/api/models` | 查询 models |
| `GET` | `/api/models/{model_id}` | 查询 model |
| `DELETE` | `/api/models/{model_id}` | 删除 model |
| `POST` | `/api/models/{model_id}/predict` | 单日预测 |
| `POST` | `/api/models/{model_id}/predict-batch` | 多日批量预测 |

请求字段：

- `TrainModelRequest`: `market`, `name`, `feature_set_id`, `label_id`, `model_type`, `model_params`, `train_config`, `universe_group_id`, `sample_weight_config`, `objective_type`, `ranking_config`
- `TrainDistillationRequest`: `market`, `name`, `teacher_model_id`, `student_feature_set_id`, `universe_group_id`, `start_date`, `end_date`, `model_type`, `model_params`, `train_config`, `sample_weight_config`, `objective_type`, `ranking_config`, `prediction_feature_set_id`, `label_name`
- `PredictRequest`: `market`, `tickers`, `date`, `feature_set_id`
- `PredictBatchRequest`: `market`, `tickers`, `dates`, `feature_set_id`

`objective_type` 可传：

- `regression`: 连续收益、rank 值或 composite 分数回归。
- `classification`: 二分类、top/bottom quantile、large move 等分类目标。
- `ranking`: 同日候选横截面排序目标，内部 task 为 `ranking`。
- `pairwise`: 同日候选 pairwise 竞争目标，当前使用 LightGBM lambdarank 风格训练并写 `pairwise_mode="lambdarank"`。
- `listwise`: 同日候选 listwise 排序目标，当前也落到 ranking task，并保留 `objective_type="listwise"` 供审计。

`ranking_config` 当前支持 `query_group="date"`、`min_group_size`、`label_gain`、`eval_at`。连续 label 会按每个交易日转成 ordinal relevance；`label_gain="identity"` 只适合已经是 dense non-negative integer 的 relevance label。验收 ranking 模型时检查 `eval_metrics.ranking_groups`，确认没有大量日期因 `min_group_size` 被丢弃，并使用 `valid/test_ndcg@k`、`rank_ic_mean`、`pairwise_accuracy_sampled` 评估同日候选竞争表现。

蒸馏训练用于把已冻结 teacher 模型的预测变成 `target_type="prediction"` 的 soft label，再用 student feature set 训练新模型。`start_date` 和 `end_date` 是 teacher 预测标签生成区间，必须早于受保护策略回测窗口；生成的 label config 会记录 `teacher_model_id`、teacher feature/label、`cutoff_end_date`、`universe_group_id`、`row_count` 和 `prediction_label_values` 存储信息。agent 验收时必须检查新模型 metadata 中的 `distillation_label_id`，并用后续 backtest 的 `leakage_warnings.time_overlap=false` 证明 cutoff 没穿越。

`GET /api/models/{model_id}` 会把审计信息展开到顶层，agent 不需要只读 `metadata.json`。关键字段包括 `train_start`、`train_end`、`valid_start`、`valid_end`、`test_start`、`test_end`、`purge_gap`、`metrics`、`label_horizon`、`effective_label_horizon` 和 `metadata.label_data_end`。比较模型和回测时使用 `metadata.audit.cutoff_rule = "label_data_end < backtest_start"`，不要只比较 `test_end`。

### 6.7 Legacy Strategies 和 Backtests

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/strategies` | 创建 legacy strategy |
| `GET` | `/api/strategies` | 查询 strategies |
| `GET` | `/api/strategies/templates` | 查询内置策略模板 |
| `GET` | `/api/strategies/templates/{template_name}` | 查询策略模板源码 |
| `GET` | `/api/strategies/backtests` | 查询 backtests |
| `GET` | `/api/strategies/backtests/{backtest_id}` | 查询 backtest |
| `GET` | `/api/strategies/backtests/{backtest_id}/rebalance-diagnostics` | 查询再平衡诊断 |
| `GET` | `/api/strategies/backtests/{backtest_id}/debug-replay` | 查询可选 debug replay 临时包 |
| `DELETE` | `/api/strategies/backtests/debug-replay/expired` | 清理过期 debug replay 临时包 |
| `DELETE` | `/api/strategies/backtests/{backtest_id}` | 删除 backtest |
| `GET` | `/api/strategies/backtests/{backtest_id}/stock/{ticker}` | 查询个股回测图数据 |
| `POST` | `/api/strategies/backtests/compare` | 比较多个 backtests |
| `POST` | `/api/strategies/backtests/research-summary` | 生成紧凑研究对比摘要 |
| `GET` | `/api/strategies/{strategy_id}` | 查询 strategy |
| `PUT` | `/api/strategies/{strategy_id}` | 更新 strategy |
| `DELETE` | `/api/strategies/{strategy_id}` | 删除 strategy |
| `POST` | `/api/strategies/{strategy_id}/backtest` | 异步运行 backtest |

请求字段：

- `CreateStrategyRequest`: `market`, `name`, `source_code`, `description`, `position_sizing`, `constraint_config`
- `UpdateStrategyRequest`: same fields plus `status`
- `RunBacktestRequest`: `market`, `config`, `universe_group_id`
- `CompareBacktestsRequest`: `market`, `backtest_ids`
- `ResearchSummaryRequest`: `market`, `baseline_backtest_id`, `trial_backtest_id`, `changed_variable`, `conclusion`, `reason`, `max_rebalance_items`

自定义 strategy 应 subclass `StrategyBase`，返回按 ticker indexed 的 DataFrame，包含 `signal`, `weight`, `strength`。V3.1 planned execution 可额外返回 `planned_price`。

策略代码运行边界：

- strategy source 会先做静态安全检查，再在隔离子进程里执行；单次 metadata 和 signal 执行都有超时保护。
- 允许导入范围很窄：`pandas/pd`、`numpy/np`、`math`、`backend.strategies.base`。相对导入、文件/网络/系统调用、危险 dunder 访问和明显无界循环会被拒绝。
- `required_factors()` 返回 factor name，`required_models()` 返回 model id。依赖缺失会进入诊断，不要在 strategy 内自行读 DB 或 model artifact。
- 需要排查决策链时使用 `StageTracer(context)` 或写入 `context.diagnostics`，诊断结果会进入 signal diagnose、backtest rebalance diagnostics 和 debug replay。
- 这不是强安全沙箱。不要运行不可信来源策略；它是本地单用户研究系统的防误操作边界。

legacy backtest 从 V3.1 开始支持计划价执行。`config` 可传：

```json
{
  "execution_model": "planned_price",
  "planned_price_buffer_bps": 50,
  "planned_price_fallback": "cancel"
}
```

策略输出 DataFrame 可以额外提供 `planned_price` 列：

```python
return pd.DataFrame(
    {
        "signal": [1],
        "weight": [1.0],
        "strength": [0.8],
        "planned_price": [101.5],
    },
    index=["AAPL"],
)
```

没有 `planned_price` 或值无效时，系统使用决策日 close。回测结果会在 `trade_diagnostics.planned_price_execution` 记录成交、失败和成交率，在 `trade_diagnostics.planned_price_inputs` 记录 fallback 计数和样本。失败订单不生成真实成交，不改变持仓和现金。

`planned_price_fallback` 支持：

- `cancel`：默认值。计划价没有落入 T+1 high/low 缓冲区间时，当日取消订单。
- `next_close`：仅当计划价有效但未落入缓冲区间时，按 T+1 close 兜底成交。无效计划价、缺 high/low 或缺 close 仍会阻断，不会静默成交。

启用 `next_close` 后，agent 验收必须检查 `trade_diagnostics.planned_price_execution` 中的 `planned_fill_count`、`fallback_close_count`、`blocked_order_count`、`planned_fill_rate` 和 `fallback_close_rate`。如果 `fallback_close_rate` 很高，说明策略计划价质量不足，不应把结果解释为高质量挂单成交。

回测保存时会写 `summary.reproducibility_fingerprint`。除 `git_commit`、strategy source hash、模型 metadata hash、因子 source hash、配置和数据水位外，还会写 `runtime.dirty`、`runtime.patch_hash` 和 `comparability.warnings`。若存在 `dirty_worktree`，不要把不同运行的结果标成严格可比；先提交或归档代码状态，再跑正式对比。

研究迭代中不要拉完整 backtest JSON 做判断。优先调用 `/api/strategies/backtests/research-summary`，它返回 baseline/trial 核心指标、delta、有限条 rebalance digest、交易数量和 `decision.conclusion`。大 payload 仍通过 `/rebalance-diagnostics` 分页或 debug replay 定点查看。

排查模型分数、信号、仓位或约束时，可在 backtest `config` 中显式开启临时 replay：

```json
{
  "debug_mode": true,
  "debug_level": "signals",
  "debug_tickers": ["AAPL"],
  "debug_dates": ["2026-04-06"]
}
```

默认关闭。开启后任务摘要会返回 `debug_artifact_id`，系统在 `data/backtest_debug/{backtest_id}` 写入短期 JSONL bundle。通过 `GET /api/strategies/backtests/{backtest_id}/debug-replay?date=YYYY-MM-DD&ticker=AAPL` 可读取该日的模型预测、因子快照、raw signals、目标权重、约束后权重、持仓状态和 strategy diagnostics，不需要重复调用 predict。该包不是正式研究资产，默认按 24 小时 TTL 清理；清理接口只删除 debug bundle，不删除正式 backtest、model、strategy、factor 或 promotion artifact。

### 6.8 Legacy Signals

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/signals/generate` | 异步生成正式信号 |
| `POST` | `/api/signals/diagnose` | 异步诊断信号，不持久化正式结果 |
| `GET` | `/api/signals` | 查询 signal runs |
| `GET` | `/api/signals/{run_id}` | 查询 signal run |
| `GET` | `/api/signals/{run_id}/export` | 导出 signal，支持 csv/json |

请求字段：

- `GenerateSignalsRequest`: `market`, `strategy_id`, `target_date`, `universe_group_id`, `constraint_config`
- `DiagnoseSignalsRequest`: `market`, `strategy_id`, `target_date`, `universe_group_id`, `date_role`, `max_tickers`, `focus_tickers`, `timeout`, `current_weights`, `holding_days`, `avg_entry_price`, `unrealized_pnl`, `backtest_id`

`diagnose` 是 agent 排查策略行为的关键接口，可用当前仓位注入或 backtest replay 还原状态。

### 6.9 Legacy Paper Trading

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/api/paper-trading/sessions` | 查询 sessions |
| `POST` | `/api/paper-trading/sessions` | 创建 session |
| `GET` | `/api/paper-trading/sessions/{session_id}` | 查询 session |
| `DELETE` | `/api/paper-trading/sessions/{session_id}` | 删除 session |
| `POST` | `/api/paper-trading/sessions/{session_id}/pause` | 暂停 session |
| `POST` | `/api/paper-trading/sessions/{session_id}/resume` | 恢复 session |
| `POST` | `/api/paper-trading/sessions/{session_id}/advance` | 异步推进 session |
| `GET` | `/api/paper-trading/sessions/{session_id}/daily` | 查询 daily equity |
| `GET` | `/api/paper-trading/sessions/{session_id}/positions` | 查询持仓 |
| `GET` | `/api/paper-trading/sessions/{session_id}/compare-backtest/{backtest_id}` | 与 backtest 对比 |
| `GET` | `/api/paper-trading/sessions/{session_id}/trades` | 查询交易 |
| `GET` | `/api/paper-trading/sessions/{session_id}/summary` | 查询 summary |
| `GET` | `/api/paper-trading/sessions/{session_id}/signals` | 查询或异步生成最新信号 |
| `GET` | `/api/paper-trading/sessions/{session_id}/stock/{ticker}` | 查询个股图数据 |

请求字段：

- `CreateSessionRequest`: `market`, `strategy_id`, `universe_group_id`, `start_date`, `name`, `config`
- `AdvanceRequest`: `market`, `target_date`, `steps`

legacy paper trading 也支持同一套计划价配置，写在 session `config` 中：

```json
{
  "execution_model": "planned_price",
  "planned_price_buffer_bps": 50,
  "planned_price_fallback": "next_close"
}
```

paper 推进时会用策略信号中的 `planned_price`，缺失时用决策日 close。T+1 high/low 满足缓冲区间时按计划价成交；若 `planned_price_fallback="next_close"` 且计划价有效但未达缓冲区间，则按 T+1 close 兜底成交；否则该计划单当日取消，并把失败或 fallback 原因写入 daily/trade diagnostics。

`paper_trading_advance` 通过 `TaskExecutor` 运行时会使用 staged domain write：daily snapshot 和 session current state 先登记为 `paper_trading_advance` staged write，只有任务完成并被验收时才提交。取消、超时和晚到结果不会更新正式 paper session 状态。同步调用 service 的内部测试路径仍可直接写入。

## 7. MCP 工具索引

MCP 工具调用同一 service layer。3.0 工具通常以 `_3_0` 结尾；legacy 工具保留原名。精确参数以 MCP schema 为准。

### 7.1 3.0 Research Kernel MCP

- `get_bootstrap_project_3_0`
- `list_research_runs_3_0`
- `get_research_run_3_0`
- `list_research_artifacts_3_0`
- `get_research_artifact_3_0`
- `get_research_lineage_3_0`
- `preview_artifact_cleanup_3_0`
- `archive_research_artifact_3_0`
- `list_promotion_records_3_0`

### 7.2 3.0 Market/Migration MCP

- `list_market_profiles`
- `get_market_profile`
- `get_project_data_status`
- `list_provider_capabilities_3_0`
- `get_data_quality_contract_3_0`
- `search_assets_3_0`
- `query_bars_3_0`
- `build_migration_report`
- `apply_migration`

### 7.3 Macro Data MCP

- `update_fred_series`
- `query_macro_series`

`update_fred_series` 会提交 `macro_data_update` 任务，必须轮询 `/api/tasks/{task_id}`。`query_macro_series` 只读查询已入库 observation。

### 7.4 3.0 Legacy Adapter MCP

- `preview_legacy_factor_3_0`
- `materialize_legacy_universe_3_0`
- `backtest_legacy_strategy_3_0`

### 7.5 3.0 Universe/Dataset MCP

- `create_static_universe_3_0`
- `list_universes_3_0`
- `create_universe_from_legacy_group_3_0`
- `materialize_universe_3_0`
- `profile_universe_3_0`
- `create_dataset_3_0`
- `list_datasets_3_0`
- `materialize_dataset_3_0`
- `profile_dataset_3_0`
- `sample_dataset_3_0`
- `query_dataset_3_0`

### 7.6 3.0 Factor/Model MCP

- `import_legacy_factor_spec_3_0`
- `create_factor_spec_3_0`
- `preview_factor_3_0`
- `materialize_factor_3_0`
- `evaluate_factor_run_3_0`
- `sample_factor_run_3_0`
- `train_model_experiment_3_0`
- `promote_model_experiment_3_0`
- `predict_model_package_panel_3_0`

### 7.7 3.0 Portfolio/StrategyGraph MCP

- `create_portfolio_construction_spec_3_0`
- `create_risk_control_spec_3_0`
- `create_rebalance_policy_spec_3_0`
- `create_execution_policy_spec_3_0`
- `construct_portfolio_3_0`
- `compare_portfolio_builders_3_0`
- `create_builtin_alpha_strategy_graph_3_0`
- `create_legacy_strategy_adapter_graph_3_0`
- `simulate_strategy_graph_day_3_0`
- `backtest_strategy_graph_3_0`
- `list_strategy_graph_backtests_3_0`
- `get_strategy_graph_backtest_3_0`
- `explain_strategy_signal_3_0`

`create_execution_policy_spec_3_0` 可创建 `policy_type="planned_price"`。`params.fallback="decision_close"` 表示策略未输出有效计划价时用决策日 close 作为计划价来源；`params.fill_fallback="next_close"` 表示计划价有效但未达 T+1 缓冲区间时按 T+1 close 兜底成交。`construct_portfolio_3_0`、`simulate_strategy_graph_day_3_0` 和 `backtest_strategy_graph_3_0` 会沿用该执行策略；alpha row 可传 `planned_price`。

### 7.8 3.0 Agent Research/Production MCP

- `list_research_playbooks_3_0`
- `create_agent_research_plan_3_0`
- `record_agent_research_trial_3_0`
- `record_agent_research_trials_batch_3_0`
- `check_agent_research_budget_3_0`
- `get_agent_research_plan_performance_3_0`
- `evaluate_qa_gate_3_0`
- `evaluate_research_promotion_3_0`
- `generate_production_signal_3_0`
- `create_paper_session_3_0`
- `advance_paper_session_3_0`
- `export_reproducibility_bundle_3_0`

### 7.9 Legacy MCP

- Data: `get_stock_data`, `search_stocks`, `get_data_status`, `update_data`, `update_data_markets`, `refresh_stock_list`
- Factor: `list_factors`, `evaluate_factor`, `create_factor`
- Model: `list_models`, `train_model`
- Strategy/backtest: `list_strategies`, `create_strategy`, `run_backtest`
- Signal: `generate_signals`
- Task: `get_task_status`, `cancel_task`
- Group: `list_groups`, `create_group`, `refresh_index_groups`
- Label: `list_labels`, `create_label`
- Feature set: `list_feature_sets`, `create_feature_set`
- Paper: `list_paper_sessions`, `create_paper_session`, `advance_paper_session`

legacy MCP 的 `run_backtest.config_json` 和 `create_paper_session.config` 均可传 `execution_model="planned_price"`、`planned_price_buffer_bps=50`、`planned_price_fallback="cancel" | "next_close"`。缺省仍为 `next_open`，保证旧 agent 脚本兼容。

## 8. 推荐端到端研究流程

### 流程 A：数据可用性与 universe 准备

1. `GET /api/research/projects/bootstrap` 获取 project。
2. `GET /api/market-data/profiles` 确认 `US_EQ`。
3. `GET /api/market-data/projects/{project_id}/status` 检查数据范围和 coverage。
4. `GET /api/market-data/assets/search?q=AAPL&project_id=...` 验证 asset 映射。
5. `POST /api/research-assets/universes/static` 或 `/universes/legacy-group` 创建 universe。
6. `POST /api/research-assets/universes/{universe_id}/materialize` 固化 membership。
7. `GET /api/research-assets/universes/{universe_id}/profile` 检查覆盖率。

### 流程 B：因子研究

1. `POST /api/research-assets/factor-specs` 创建 FactorSpec，或 `/factor-specs/legacy` 导入旧因子。
2. `POST /api/research-assets/factor-specs/{id}/preview` 做小范围 preview。
3. `POST /api/research-assets/factor-specs/{id}/materialize` 正式计算，拿到 `task_id/run_id`。
4. 轮询 `/api/tasks/{task_id}`。
5. `GET /api/research-assets/factor-runs/{factor_run_id}/sample` 检查样本。
6. `POST /api/research-assets/factor-runs/{factor_run_id}/evaluate` 评估 IC、coverage 等。
7. 写 trial 或 QA，决定是否进入 dataset。

### 流程 C：Dataset 到模型

1. 准备 legacy `feature_set_id/label_id` 或未来 3.0 feature pipeline/label spec。
2. 对重复实验可先 `POST /api/research-cache/feature-matrix/warmup`，预热 48 小时 hot cache。
3. `POST /api/research-assets/datasets` 创建 dataset。
4. `POST /api/research-assets/datasets/{dataset_id}/materialize` 固化 panel。
5. 轮询 task，查看 `/profile` 和 `/sample`。
6. `POST /api/research-assets/model-experiments/train` 训练，或 legacy `/api/models/train` 训练 `ranking/pairwise/listwise`。
7. 轮询 task，查询 experiment 或 model detail，检查 split、purge、ranking metrics 和 metadata audit。
8. `POST /api/research-assets/model-experiments/{experiment_id}/promote` 生成 package。
9. `POST /api/research-assets/model-packages/{package_id}/predict-panel` 生成 prediction panel。

### 流程 D：组合、风控和 StrategyGraph

1. `POST /api/research-assets/portfolio-construction-specs` 创建组合构建器。
2. `POST /api/research-assets/risk-control-specs` 创建风控规则。
3. `POST /api/research-assets/rebalance-policy-specs` 和 `POST /api/research-assets/execution-policy-specs` 创建再平衡和执行策略；需要计划价复现时使用 `policy_type="planned_price"`。
4. `POST /api/research-assets/portfolio-runs/compare-builders` 对比多个 builder。
5. `POST /api/research-assets/strategy-graphs/builtin-alpha` 或 `POST /api/research-assets/strategy-graphs/legacy-adapter` 创建 StrategyGraph。
6. `POST /api/research-assets/strategy-graphs/{id}/simulate-day` 检查单日 trace。
7. `POST /api/research-assets/strategy-graphs/{id}/backtest` 跑历史回测。
8. `GET /api/research-assets/strategy-signals/{signal_id}/explain` 查看解释。

### 流程 E：Agent 自治优化

1. `GET /api/research/agent/playbooks` 选择 playbook。
2. `POST /api/research/agent/plans` 创建 bounded plan。
3. 每个实验结果用 `/plans/{plan_id}/trials` 或 `/trials/batch` 记录。
4. `GET /api/research/agent/plans/{plan_id}/budget` 检查预算。
5. `GET /api/research/agent/plans/{plan_id}/performance` 排序 trial。
6. 对候选结果 `POST /api/research/agent/qa`。
7. QA 通过后 `POST /api/research/agent/promotion`。

### 流程 F：正式信号、paper 和复现

1. 只对 validated/published StrategyGraph 生成正式结果。
2. `POST /api/research-assets/production-signals/generate` 生成信号。
3. `POST /api/research-assets/paper-sessions` 创建 3.0 paper session。
4. `POST /api/research-assets/paper-sessions/{id}/advance` 推进。
5. `POST /api/research-assets/reproducibility-bundles` 导出复现包。

### 流程 G：Legacy 资产继续使用

1. `/api/groups` 创建或选择 universe group。
2. `/api/factors` 创建因子，`/compute` 和 `/evaluate` 验证。
3. `/api/feature-sets` 创建 feature set。
4. `/api/models/train` 训练模型。
5. `/api/strategies` 创建 legacy strategy。
6. `/api/strategies/{strategy_id}/backtest` 回测。
7. `/api/signals/diagnose` 排查，`/api/signals/generate` 生成信号。
8. `/api/paper-trading/sessions` 做 legacy paper。
9. 需要迁移时用 3.0 adapter 或 migration，不要手工搬表。

## 9. 常用请求示例

获取默认 project：

```bash
curl -fsS http://127.0.0.1:8000/api/research/projects/bootstrap
```

查询任务：

```bash
curl -fsS "http://127.0.0.1:8000/api/tasks?limit=20"
```

查询 3.0 数据状态：

```bash
curl -fsS "http://127.0.0.1:8000/api/market-data/projects/bootstrap_us/status"
```

按 asset 查询行情：

```bash
curl -fsS -X POST http://127.0.0.1:8000/api/market-data/bars/query \
  -H "Content-Type: application/json" \
  -d '{"project_id":"bootstrap_us","asset_ids":["US:AAPL"],"start":"2024-01-01","end":"2024-01-31","limit":1000}'
```

更新 FRED 宏观序列：

```bash
curl -fsS -X POST http://127.0.0.1:8000/api/macro-data/fred/update \
  -H "Content-Type: application/json" \
  -d '{"series_ids":["DGS10","FEDFUNDS"],"start_date":"2024-01-01","end_date":"2024-12-31"}'
```

查询已入库宏观 observations：

```bash
curl -fsS "http://127.0.0.1:8000/api/macro-data/observations?series_ids=DGS10,FEDFUNDS&start_date=2024-01-01&end_date=2024-12-31&limit=1000"
```

查询免费数据源能力契约：

```bash
curl -fsS "http://127.0.0.1:8000/api/market-data/provider-capabilities"
```

查询研究缓存库存：

```bash
curl -fsS "http://127.0.0.1:8000/api/research-cache/inventory?market=US&limit=50"
```

预热 feature matrix cache：

```bash
curl -fsS -X POST http://127.0.0.1:8000/api/research-cache/feature-matrix/warmup \
  -H "Content-Type: application/json" \
  -d '{"market":"US","feature_set_id":"<feature_set_id>","universe_group_id":"sp500","start_date":"2021-01-01","end_date":"2023-12-31"}'
```

触发 StrategyGraph 历史回测：

```bash
curl -fsS -X POST http://127.0.0.1:8000/api/research-assets/strategy-graphs/<graph_id>/backtest \
  -H "Content-Type: application/json" \
  -d '{"start_date":"2024-01-02","end_date":"2024-01-31","alpha_frames_by_date":{"2024-01-02":[{"asset_id":"US_EQ:AAPL","score":1.0}]},"initial_capital":1000000}'
```

预览 artifact cleanup：

```bash
curl -fsS -X POST http://127.0.0.1:8000/api/research/artifacts/cleanup-preview \
  -H "Content-Type: application/json" \
  -d '{"project_id":"bootstrap_us","lifecycle_stage":"scratch","include_published":false,"limit":100}'
```

创建 agent research plan：

```bash
curl -fsS -X POST http://127.0.0.1:8000/api/research/agent/plans \
  -H "Content-Type: application/json" \
  -d '{"hypothesis":"Quality plus momentum improves US large-cap risk-adjusted return","project_id":"bootstrap_us","market_profile_id":"US_EQ","budget":{"max_trials":50},"stop_conditions":{"min_sharpe":1.2}}'
```

## 10. 存储、清理和生命周期

推荐 lifecycle：

- `scratch`: 临时预览、失败实验、中间诊断，可清理。
- `experiment`: 正常实验结果，可回看但不代表可发布。
- `candidate`: 准备进入 QA/promotion。
- `validated`: 通过 QA，可用于更严肃评估。
- `published`: 正式可用结果。
- `archived`: 已归档，不再参与默认研究。

推荐 retention：

- `rebuildable`: 可从 source/config 重新生成。
- `standard`: 默认保留。
- `protected`: 重要结果。
- `archived`: 已归档。

清理流程：

1. `POST /api/research/artifacts/cleanup-preview`
2. 检查 candidates 和 protected。
3. 对不再需要的 artifact 调用 archive。
4. 不物理删除 still referenced artifact。

## 11. 问题看板和文档维护

| 文件 | 用途 |
| --- | --- |
| `docs/agent-guide.md` | 小写兼容入口，指向本文档 |
| `docs/AGENT_GUIDE.md` | agent 使用、REST/MCP、任务语义、研究流程的主手册 |
| `docs/USER_GUIDE.md` | human UI 验收和日常使用手册 |
| `docs/backlog.md` | 只保存未修复、未完全缓解或明确延期的问题 |
| `docs/v2.0/archive/backlog/` | 已修复、已缓解、已判定不修的问题归档 |
| `docs/v3.1/` | 计划交易和策略参数/执行语义升级设计文档 |

写 backlog 时使用当前 `docs/backlog.md` 的结构，至少包含 market、entry、current mitigation、remaining issue、expected behavior、validation standard、fix necessity、estimated workload。修复后不要把已完成内容留在 backlog；新建归档文档记录问题、修复范围、验证命令和残余风险。

更新系统能力时同步维护：

- REST 或 service 行为变化：更新本文档对应 API、流程、禁区。
- UI 行为变化：更新 `docs/USER_GUIDE.md`。
- V3.1 planned execution 或策略参数语义变化：更新 `docs/v3.1/` 对应设计/计划文档。
- backlog 评估或修复：未完成留 `docs/backlog.md`，完成后移到 archive。

## 12. 验证和 smoke

代码变更后按影响范围选择验证。不要为了文档或只读检查打断数据任务。

| 范围 | 命令 |
| --- | --- |
| 3.0 research kernel | `uv run python scripts/smoke_3_0_research_api.py` |
| 3.0 market data | `uv run python scripts/smoke_3_0_market_data_api.py` |
| Macro data / FRED | `uv run python -m unittest tests.test_macro_data_config tests.test_macro_data_service tests.test_macro_data_api tests.test_macro_data_mcp -v` |
| Data quality/provider capability | `uv run python -m unittest tests.test_provider_contracts tests.test_data_quality_service -v` |
| Research cache | `uv run python -m unittest tests.test_research_cache_service tests.test_research_cache_api -v` |
| 3.0 universe/dataset | `uv run python scripts/smoke_3_0_universe_dataset_api.py` |
| 3.0 factor | `uv run python scripts/smoke_3_0_factor_engine_api.py` |
| 3.0 model | `uv run python scripts/smoke_3_0_model_experiment_api.py` |
| 3.0 portfolio | `uv run python scripts/smoke_3_0_portfolio_assets_api.py` |
| 3.0 StrategyGraph | `uv run python scripts/smoke_3_0_strategy_graph_api.py` |
| 3.0 agent research | `uv run python scripts/smoke_3_0_agent_research_api.py` |
| 3.0 production signal | `uv run python scripts/smoke_3_0_production_signal_api.py` |
| Legacy e2e | `uv run python scripts/e2e_demo.py` |
| Frontend | `cd frontend && pnpm build` |

DuckDB 有单写限制。不要把会写 DB 的 smoke、后台任务、数据更新并行执行。

维护或备份前先调用 `/api/diagnostics/db-preflight`，或运行 `bash scripts/backup_data.sh` 让脚本执行预检。若返回 `status=locked`，不要直接用外部 Python 进程连主库；改用运行中的 API 做只读诊断，或先 `bash scripts/stop.sh` 进入维护窗口。

## 13. Agent 禁区

- 不要直接修改 `data/` 下 DuckDB 文件或 model artifacts。
- 不要在运行中的 full backfill 上做取消、重启、覆盖性更新，除非用户明确要求。
- 不要对 market 做横向硬编码。新增市场从 market profile、calendar、trading rules、cost model、benchmark policy 接入。
- 不要把组合、风控、执行继续塞回 strategy 单体配置。3.0 用独立资产和 StrategyGraph。
- 不要把 scratch trial 当正式结果。
- 不要在没有 QA/promotion 的情况下发布 production signal。
- 不要忽略 task 的 `late_result_quarantined`、`late_result_diagnostics`、`authoritative_terminal`、`interrupted`、`retryable`、`cancel_requested` 字段。
- 不要因为某个 ticker 无数据就判定任务失败。退市、权证、无成交、provider 限流都需要分开诊断。
- 不要把 FRED 当前实时窗口数据包装成严格 PIT 宏观数据。没有历史 realtime window 回放前，只能作为 research-grade 外部数据。
- 不要把用户自定义 factor/strategy 的静态安全检查当成完整沙箱。明显危险代码会被拒绝，但强隔离仍需要独立进程和资源限制。

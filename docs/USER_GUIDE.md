# QAgent Human 使用手册

本文档面向通过 React UI 验收系统的人类用户。Agent 自动化、REST API 和 MCP 细节见 [AGENT_GUIDE.md](./AGENT_GUIDE.md)。

QAgent 是本地优先、单用户、低频量化研究系统。当前主线是 3.0 重构，同时保留 2.0 legacy 因子、模型、策略、回测、信号和模拟交易路径。US equities 是成熟度最高的默认市场；A股能力通过 `CN` / `CN_A` 隔离，数据源优先使用 BaoStock，默认股票池为上证50、沪深300、中证500、创业板指成分股去重并集。

## 1. 启停和入口

| 项目 | 地址或命令 |
| --- | --- |
| 前端 UI | `http://127.0.0.1:5173` |
| 后端 API | `http://127.0.0.1:8000` |
| 健康检查 | `http://127.0.0.1:8000/api/health` |
| 查看运行状态 | `scripts/status.sh` |
| 后台启动服务 | `scripts/start_detached.sh` |
| 停止服务 | `scripts/stop.sh` |

如果系统已经运行，先用 `scripts/status.sh` 确认状态，不要重复启动。停止服务或恢复备份前，先在任务页确认没有数据更新、模型训练、回测、信号生成或 paper 推进任务仍在运行。

## 2. 页面导航

| 页面 | 路径 | 用途 |
| --- | --- | --- |
| 研究工作台 | `/research` | 3.0 project、runs、artifacts、playbooks、QA、promotion、production signals、paper sessions |
| 行情浏览 | `/market` | 查看股票、指数、行情样本、market profile 和数据覆盖 |
| 数据管理 | `/data` | 查看数据状态，执行补数、个股更新、分组维护和数据质量检查 |
| 因子研究 | `/factors` | legacy 因子创建、模板、计算、评估 |
| 特征工程 | `/features` | legacy feature set 管理、相关性分析 |
| 模型训练 | `/models` | legacy 模型训练、ranking/pairwise/listwise 目标、模型列表、预测 |
| 策略回测 | `/backtest` | legacy 策略创建、回测、回测历史、计划价成交配置、诊断 |
| 信号生成 | `/signals` | legacy 信号生成和导出 |
| 模拟交易 | `/paper-trading` | legacy paper session 创建、推进、持仓、交易、计划价成交配置 |
| 任务管理 | `/tasks` | 查看任务进度、串行排队、取消状态、错误和晚到结果隔离 |
| 系统设置 | `/settings` | 本地配置和系统信息 |

3.0 推荐从研究工作台进入。旧页面仍可用于继续使用已有 legacy 资产。

## 3. 推荐验收流程

1. 进入 `/research`，确认 bootstrap project、近期 runs、artifacts、QA 和 promotion 状态。
2. 进入 `/market` 或 `/data`，确认目标市场的数据覆盖、最新交易日和股票池成员。CN 默认 group 是 `cn_a_core_indices_union`。
3. 创建或选择因子，先做小范围 preview / compute，再看 coverage、IC、样本和失败原因。
4. 创建 feature set、label 和 dataset。训练前检查 split 时间段、purge gap、label horizon，避免未来函数。
5. 在 `/models` 训练模型。排序类研究选择 `ranking`、`pairwise` 或 `listwise`，并检查 `ndcg@k`、`rank_ic`、pairwise accuracy、训练/验证/测试时间段。
6. 在 `/backtest` 跑策略回测。需要复现更现实的挂单成交时选择 `planned_price`，并检查 planned fill、fallback close、blocked order 和交易诊断。
7. 通过 `/signals` 或 3.0 production signal 生成信号；只把通过 QA/promotion 的结果当成正式候选。
8. 在 `/paper-trading` 创建模拟交易。计划价模式下，检查每日交易是否按计划价、T+1 close fallback 或取消处理。
9. 在 `/tasks` 查看长任务。看到 `serial_wait` 说明排队，不等于卡死；看到 late/quarantined 结果时不要作为正式结果验收。

## 4. 数据和市场注意事项

- US 是当前默认成熟路径，legacy 缺省 `market=US`。
- CN/A股路径必须显式使用 `market=CN` 或 `market_profile_id=CN_A`。
- BaoStock、yfinance 都是免费探索源，不提供严格 PIT、survivorship-safe、完整 corporate actions 保证。发布级研究不能只依赖这些数据。
- 全市场补数耗时较长，且受数据源限流影响。日常验收优先用小 ticker、小 group、短日期区间。
- DuckDB 是本地单文件数据库。并发写入容易冲突，维护、备份、恢复前先停止后端或使用系统预检。

## 5. 模型和策略要点

- 普通预测使用 `regression` 或 `classification`。
- 同日候选竞争使用 `ranking`、`pairwise`、`listwise`，系统会按交易日分组训练和评估。
- ranking 训练需要足够大的同日股票池；如果大量日期小于 `min_group_size`，结果没有研究价值。
- legacy strategy 必须通过系统策略编辑和回测入口运行。策略可以输出 `signal`、`weight`、`strength`，计划价模式可额外输出 `planned_price`。
- 策略参数、执行模式、回测配置会进入结果摘要和可复现指纹。正式对比前应先提交代码或确认 dirty worktree 状态。

## 6. 计划价成交模式

默认 legacy 回测和 paper 仍兼容 `next_open`。V3.1 新增可选 `planned_price`：

- T 日收盘后策略产生 T+1 计划。
- 策略输出目标仓位和可选 `planned_price`。
- T+1 若满足 `low * 1.005 <= planned_price <= high * 0.995`，按计划价成交。
- `planned_price_fallback="cancel"` 时，未触达计划价就取消。
- `planned_price_fallback="next_close"` 时，计划价有效但未触达缓冲区间，可按 T+1 close 兜底成交；无效计划价、缺 high/low、缺 close 仍会阻断。

验收时不要只看收益率。需要查看 planned fill rate、fallback close rate、blocked rate 和 blocked reason。如果 fallback close rate 很高，说明策略计划价质量不足。

## 7. 问题和文档

- 面向 agent 的完整操作说明维护在 [AGENT_GUIDE.md](./AGENT_GUIDE.md)。
- 未修复问题只记录在 [backlog.md](./backlog.md)。
- 已修复、已缓解、判定暂不修的问题放到 `docs/v2.0/archive/backlog/` 归档，不继续留在 backlog。
- 新增系统行为时，应同时更新 human 手册、agent 手册和相关版本设计文档。

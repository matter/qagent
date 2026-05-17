# QAgent Next 系统开发任务目录

> 文档状态：开发过程跟踪目录  
> 创建日期：2026-05-17  
> 适用范围：QAgent Next 从零开发  
> 主需求文档：`docs/v4.0/next-system-requirements.md`

## 1. 使用规则

本文件用于记录后续开发过程中的任务拆分、当前状态、验收证据和遗留问题。任何阶段性开发完成后，必须更新本文件，而不是只在聊天记录中说明。

状态枚举：

- `TODO`：未开始。
- `DOING`：正在开发。
- `BLOCKED`：被外部决策、依赖或缺陷阻塞。
- `REVIEW`：实现完成，等待审查或验收。
- `DONE`：验收完成，有明确证据。
- `DEFERRED`：有意延期，必须写明原因。

更新要求：

1. 每个任务必须有唯一 ID。
2. 每个任务必须关联需求文档章节。
3. 每个任务必须有可验证验收项。
4. 完成任务必须填写验收证据，例如测试命令、截图、日志、demo 路径或人工验收记录。
5. 发现设计偏差时，先记录到“风险与决策”，再修改需求或实现。

## 2. 阶段总览

| 阶段 | 名称 | 状态 | 目标里程碑 | 负责人 | 最近更新 |
| --- | --- | --- | --- | --- | --- |
| P0 | 项目骨架、领域合约和任务底座 | TODO | REST/CLI/MCP 提交同一个 noop task，artifact writer fence 生效 | TBD | 2026-05-17 |
| P1 | Market Foundation、Study 和基础数据闭环 | TODO | US_EQ 小范围数据闭环，CN_A exploratory smoke | TBD | 2026-05-17 |
| P2 | Artifact Query、Universe、Factor 和 Feature/Dataset | TODO | 150+ Feature Set、Dataset artifact、Factor Evaluation | TBD | 2026-05-17 |
| P3 | 模型研究和 Prediction Run | TODO | LightGBM 训练、Prediction Run、并发训练资源控制 | TBD | 2026-05-17 |
| P4 | Strategy Graph、SDK 和快速筛选回测 | TODO | 策略图六种接入模式、fast screening、Strategy Studio MVP | TBD | 2026-05-17 |
| P5 | Ledger-Accurate Backtest、QA、Promotion 和复现 | TODO | 订单级 ledger、QA Gate、Candidate Ranking、Bundle | TBD | 2026-05-17 |
| P6 | T+1 Signal、Paper Session 和人工跟盘 | TODO | T+1 Trade Plan、Paper Session、Manual Adjustment | TBD | 2026-05-17 |
| P7 | Agent/CLI/MCP 高级研究工作流和并发优化 | TODO | Agent 研究闭环、批量 trial、并发压测 | TBD | 2026-05-17 |
| P8 | MVP Hardening、文档和发布候选 | TODO | 空库 seed demo、backup/restore、性能基线 | TBD | 2026-05-17 |

## 3. 当前任务目录

### P0：项目骨架、领域合约和任务底座

| ID | 任务 | 状态 | 需求章节 | 验收项 | 证据 |
| --- | --- | --- | --- | --- | --- |
| P0-T01 | 初始化新项目目录和包结构 | TODO | 28 / 31.11 | 创建 domain/storage/task/api/cli/mcp/ui 边界 | 待补 |
| P0-T02 | 定义核心 domain schema 和状态枚举 | TODO | 6 / 19 / 28.1 | domain 层无 storage/API 依赖 | 待补 |
| P0-T03 | 实现 metadata store 和 schema migration MVP | TODO | 20 / 31.3 | schema 可初始化、可升级、可查询版本 | 待补 |
| P0-T04 | 实现 artifact store 原语 | TODO | 20.2 / 31.12 | temp path + atomic publish + hash 校验 | 待补 |
| P0-T05 | 实现 Task Service MVP | TODO | 19 / 19.1 | submit/poll/cancel/heartbeat/timeout/idempotency | 待补 |
| P0-T06 | 实现 resource lease 和 artifact writer fence | TODO | 19 / 31.12 | 同一 write scope 只有一个 writer 发布 | 待补 |
| P0-T07 | 搭建 REST、CLI、MCP 骨架 | TODO | 22 / 28.1 | 三种入口提交同一个 noop task | 待补 |
| P0-T08 | 搭建 UI 工作台骨架和 Task 页面 | TODO | 23 / 28 | 展示 task status、blocked_by、progress | 待补 |

### P1：Market Foundation、Study 和基础数据闭环

| ID | 任务 | 状态 | 需求章节 | 验收项 | 证据 |
| --- | --- | --- | --- | --- | --- |
| P1-T01 | 实现 Market Profile schema | TODO | 7 / 28 | US_EQ、CN_A profile 可创建和校验 | 待补 |
| P1-T02 | 实现 calendar、trading rule、cost model | TODO | 7.1-7.3 | market rule 可被 data/backtest 引用 | 待补 |
| P1-T03 | 实现 Study 和 Research Objective | TODO | 17 / 28 | Study 聚合 task、trial、decision | 待补 |
| P1-T04 | 实现 Asset 和 Daily Bar ingestion | TODO | 8 / 28 | 小范围 US_EQ bars artifact 生成 | 待补 |
| P1-T05 | 实现 Data Quality summary | TODO | 8.3 / 18 | 数据质量声明可进入 QA evidence | 待补 |
| P1-T06 | 实现 Study Workbench MVP | TODO | 23.2 / 28 | UI 可查看 Study、任务和数据状态 | 待补 |

### P2：Artifact Query、Universe、Factor 和 Feature/Dataset

| ID | 任务 | 状态 | 需求章节 | 验收项 | 证据 |
| --- | --- | --- | --- | --- | --- |
| P2-T01 | 实现 DuckDB/Polars query layer | TODO | 20 / 31.3 | 支持 projection/predicate pushdown | 待补 |
| P2-T02 | 实现 Universe materialization | TODO | 9 / 28 | Universe artifact 可复用 | 待补 |
| P2-T03 | 实现 Research Insight 和 Factor Idea | TODO | 10 / 13 / 17 | 亏损/盈利归因可生成 Factor Idea | 待补 |
| P2-T04 | 实现 Factor Spec、Run、Evaluation | TODO | 10 | cheap factor evaluation 可运行 | 待补 |
| P2-T05 | 实现 Feature Pipeline 和 Feature Set | TODO | 11.1 | 150+ 有效特征并输出 profile | 待补 |
| P2-T06 | 实现 Label Spec 和 Dataset materialization | TODO | 11.2-11.3 | Dataset artifact + leakage check | 待补 |
| P2-T07 | 实现 Factor/Dataset Tear Sheet | TODO | 10.5 / 12.3 / 31.7 | 报告数据保存为 artifact | 待补 |

### P3：模型研究和 Prediction Run

| ID | 任务 | 状态 | 需求章节 | 验收项 | 证据 |
| --- | --- | --- | --- | --- | --- |
| P3-T01 | 实现 Model Experiment 和 training task | TODO | 12 / 19 | LightGBM 训练任务可提交和取消 | 待补 |
| P3-T02 | 实现 time split、rolling、purge-gap validation | TODO | 4 / 12 | 随机 K-Fold 被拒绝 | 待补 |
| P3-T03 | 实现 Model Package | TODO | 12.2 | 保存模型文件、schema、metrics、依赖 | 待补 |
| P3-T04 | 实现 Prediction Run artifact | TODO | 12 / 31.5 | 策略读取预测 artifact，不实时推理 | 待补 |
| P3-T05 | 实现 model diversity profile | TODO | 12.2 / 27.2 | 高同质模型标记 duplicate | 待补 |
| P3-T06 | 实现并发训练资源控制 | TODO | 19.1 / 31.12 | 3 个训练并发不阻塞 API | 待补 |

### P4：Strategy Graph、SDK 和快速筛选回测

| ID | 任务 | 状态 | 需求章节 | 验收项 | 证据 |
| --- | --- | --- | --- | --- | --- |
| P4-T01 | 实现 Strategy Node Spec 和 contract test | TODO | 14.4 / 21 | schema、权限、超时可被检测 | 待补 |
| P4-T02 | 实现 Strategy Graph 六种接入模式 | TODO | 14.2-14.3 | alpha/target/order/state/signal/position 可组合 | 待补 |
| P4-T03 | 实现 Strategy Variant 和参数 diff | TODO | 14.6 | clone/fork、节点替换、参数快照 | 待补 |
| P4-T04 | 实现 Strategy SDK | TODO | 14.5 | Context、OrderIntent、State、Diagnostics | 待补 |
| P4-T05 | 实现 simulate day 和 explain | TODO | 14.10 / 23.5 | 小窗口解释按需生成 | 待补 |
| P4-T06 | 实现 fast screening engine | TODO | 15 / 31.6 | 结果标记 screening，不能 promotion | 待补 |
| P4-T07 | 实现 Strategy Studio MVP | TODO | 23.4 | contract test、simulate day、submit backtest | 待补 |

### P5：Ledger-Accurate Backtest、QA、Promotion 和复现

| ID | 任务 | 状态 | 需求章节 | 验收项 | 证据 |
| --- | --- | --- | --- | --- | --- |
| P5-T01 | 实现 ledger-accurate execution engine | TODO | 15 / 31.6 | 订单、成交、成本、现金、持仓完整 | 待补 |
| P5-T02 | 实现 execution diagnostics | TODO | 15.3 | 每笔订单有 fill/block/fallback 诊断 | 待补 |
| P5-T03 | 实现 Strategy Tear Sheet | TODO | 15.4 / 31.7 | 输出收益、Sharpe、回撤、换手等 | 待补 |
| P5-T04 | 实现 Candidate Ranking | TODO | 17.4 / 23.2 | summary-first、分页、过滤 | 待补 |
| P5-T05 | 实现 QA Gate 和 Promotion Record | TODO | 18 | 缺 evidence refs 不能 promotion | 待补 |
| P5-T06 | 实现 Reproducibility Bundle | TODO | 18.3 | 可重放 summary 指标 | 待补 |

### P6：T+1 Signal、Paper Session 和人工跟盘

| ID | 任务 | 状态 | 需求章节 | 验收项 | 证据 |
| --- | --- | --- | --- | --- | --- |
| P6-T01 | 实现 Production Signal Run | TODO | 16.2 | 从 promoted strategy 生成正式信号 | 待补 |
| P6-T02 | 实现 T+1 Trade Plan | TODO | 16.1 | 明确执行日、计划价、fallback、原因 | 待补 |
| P6-T03 | 实现 Paper Session | TODO | 16.3 | NAV、positions、trades、cash、cost | 待补 |
| P6-T04 | 实现 Manual Position Adjustment | TODO | 16.3 / 24 | 记录调整前后、原因、NAV 影响 | 待补 |
| P6-T05 | 实现三组合对比 | TODO | 16.3 / 23 | suggested、actual、shadow 可解释 | 待补 |
| P6-T06 | 实现 Paper Dashboard | TODO | 23 | 展示信号、持仓偏离、绩效、诊断 | 待补 |

### P7：Agent/CLI/MCP 高级研究工作流和并发优化

| ID | 任务 | 状态 | 需求章节 | 验收项 | 证据 |
| --- | --- | --- | --- | --- | --- |
| P7-T01 | 实现 CLI batch commands | TODO | 22.2 / 31.1 | submit trials、inspect artifacts、compare candidates | 待补 |
| P7-T02 | 实现 MCP resources/tools | TODO | 22.2 / 24 | MCP 只调用服务层，高风险操作需确认 | 待补 |
| P7-T03 | 实现 Agent workflow templates | TODO | 13 / 17 / 26.3 | agent 完成 factor/model/strategy 研究闭环 | 待补 |
| P7-T04 | 接入 Optuna 参数搜索 | TODO | 13.3 / 31.4 | trial 结果回写系统 Trial Result | 待补 |
| P7-T05 | 实现 batch trial dashboard | TODO | 13.3 / 27.6 | 展示 trial、失败率、资源、ETA | 待补 |
| P7-T06 | 实现并发压测脚本 | TODO | 19.1 / 27.6 | 3 train + 5 screening + 2 ledger + 1 paper | 待补 |
| P7-T07 | 实现 cache diagnostics 和 cleanup | TODO | 20.3 / 31.12 | cache hit/miss/rebuild reason 可解释 | 待补 |

### P8：MVP Hardening、文档和发布候选

| ID | 任务 | 状态 | 需求章节 | 验收项 | 证据 |
| --- | --- | --- | --- | --- | --- |
| P8-T01 | 实现空库 seed demo | TODO | 27 / 28 | 完成 Study 到 Paper 全链路 | 待补 |
| P8-T02 | 实现 CN_A exploratory smoke | TODO | 5.1 / 27.1 | 展示 CN_A 限制和探索级结果 | 待补 |
| P8-T03 | 实现 backup/restore | TODO | 18.3 / 28 | restore 后可查询和重放 summary | 待补 |
| P8-T04 | 建立 performance baseline | TODO | 27.6 / 31.12 | 各阶段性能指标有基线 | 待补 |
| P8-T05 | 编写用户和开发文档 | TODO | 28 | Human、Agent、SDK、运维文档齐全 | 待补 |
| P8-T06 | MVP 验收总表 | TODO | 27 | 每条 MVP 验收标准有证据 | 待补 |

## 4. 进度日志

| 日期 | 更新人 | 内容 | 关联任务 | 证据 |
| --- | --- | --- | --- | --- |
| 2026-05-17 | Codex | 创建开发任务目录。 | 全局 | 本文件 |

## 5. 风险与决策

| ID | 状态 | 问题 | 决策 / 下一步 | 关联章节 | 更新日期 |
| --- | --- | --- | --- | --- | --- |
| R-001 | OPEN | 新项目已清理旧代码，后续需要从空仓库重建脚手架。 | Phase 0 首个任务必须初始化新项目结构和开发工具链。 | 28 / 31 | 2026-05-17 |

## 6. 验收证据模板

每个任务完成时，在对应任务行的“证据”列填写简短证据，并在必要时追加到本节。

```text
任务 ID：
完成日期：
变更摘要：
验证命令：
验证结果：
人工验收：
遗留问题：
```

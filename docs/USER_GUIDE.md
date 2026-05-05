# QAgent 当前系统使用手册

QAgent 是本地优先、单用户、低频量化研究系统。当前主线是 3.0 重构：默认回归 US equities，把多市场能力按市场画像、数据策略、交易规则、成本模型和 benchmark policy 设计好；CN/A 股能力保留并逐步迁移，但成熟度低于 US。

## 1. 服务入口

| 项目 | 地址或命令 |
| --- | --- |
| 前端 UI | http://127.0.0.1:5173 |
| 后端 API | http://127.0.0.1:8000 |
| 健康检查 | http://127.0.0.1:8000/api/health |
| 查看运行状态 | `scripts/status.sh` |
| 后台启动服务 | `scripts/start_detached.sh` |
| 停止服务 | `scripts/stop.sh` |

如果系统已经在运行，优先使用 `scripts/status.sh` 确认状态，不要重复启动。停止服务前先打开任务页确认是否有正在运行的数据更新、回测、模型训练或 signal/paper 任务。

## 2. 页面导航

| 页面 | 路径 | 当前用途 |
| --- | --- | --- |
| 研究工作台 | `/research` | 3.0 默认入口，查看 project、runs、artifacts、QA、promotion、playbooks、signals、paper sessions |
| 行情浏览 | `/market` | 查看股票、指数、行情样本和市场范围 |
| 数据管理 | `/data` | 查看数据状态，执行补数、个股更新、指数/分组维护 |
| 因子研究 | `/factors` | 2.0 legacy 因子创建、计算、评估入口 |
| 特征工程 | `/features` | 2.0 legacy feature set 管理和相关性分析 |
| 模型训练 | `/models` | 2.0 legacy 模型训练与预测入口 |
| 策略回测 | `/backtest` | 2.0 legacy 策略创建、回测和诊断入口 |
| 信号生成 | `/signals` | 2.0 legacy 信号生成入口 |
| 模拟交易 | `/paper-trading` | 2.0 legacy paper trading 入口 |
| 任务管理 | `/tasks` | 查看、取消和诊断长任务 |
| 系统设置 | `/settings` | 本地配置和系统信息 |

3.0 的推荐入口是研究工作台。旧模块页仍可用，适合继续使用既有因子、模型、策略和回测资产。

## 3. 推荐研究流程

1. 进入 `/research`，确认 bootstrap project、近期 runs、artifacts、QA 和 promotion 状态。
2. 进入 `/market` 或 `/data`，确认 US 数据覆盖、最新交易日和目标股票池是否可用。
3. 在 3.0 research asset API 或旧 `/factors` 页面中创建因子，先 preview，再 materialize 和 evaluate。
4. 创建 universe 和 dataset，固定样本范围、特征、标签、purge gap、训练验证时间切分，避免每次实验临时拼数据。
5. 基于 dataset 训练模型，保存 experiment、prediction run、model package 和 lineage。
6. 把 alpha、selection、portfolio construction、risk control、rebalance、execution 组成 StrategyGraph，使用 `simulate-day` 或回测验证。
7. 通过 QA gate 和 promotion policy 后，再生成 production signal 或推进 paper session。
8. 定期在研究工作台查看 artifacts，归档 scratch 和失败实验，正式结果保留可复现 bundle。

## 4. 数据更新注意事项

- 全市场补数可能耗时很长，并受网络、数据源限流、退市 ticker、无成交 ticker 影响。
- 补数前先查看 `/tasks` 和 `/data/update/progress`，确认没有同类任务正在运行。
- 正在运行的补数任务不要中断，除非明确知道影响范围。
- 小范围验证优先使用个股、分组或短日期区间更新，不要把 full backfill 当作常规 smoke test。

## 5. 量化研究约束

- 不允许引入未来函数。信号只能使用决策日期当时可获得的数据。
- 回测、信号和 paper trading 要遵守现有 T+1、open price 和 calendar-aware 语义。
- 不使用随机 K-Fold 做时间序列验证，优先使用 time split、rolling、expanding、purge gap。
- 因子、数据集、模型、策略、回测、信号和 paper session 都应保留 source/config/dependency snapshot，保证可复现。

## 6. 当前成熟度

US equities 是当前生产优先市场。CN/A 股已经有 2.0 legacy 支持，3.0 也有 market profile 和规则设计，但还在分阶段迁移。做正式研究时优先使用 US；做 CN 实验时要额外检查交易日历、停牌、涨跌停、复权、指数成分和数据覆盖。

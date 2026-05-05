# QAgent Agent 回测优化研究路径

本文档面向 agent。它不是论文列表，也不是固定步骤清单，而是一套用于改进回测表现的研究方法论。agent 应根据当前因子、模型、策略、回测和数据质量的实际表现，沿着这些路径形成假设、设计实验、记录证据，并把有效结果沉淀为 QAgent 资产。

核心目标：

- 提升回测的可解释收益，而不是只提升某一次曲线。
- 让每次优化都能复现、比较、回滚。
- 防止优化路径滑向过拟合、数据挖掘和不可交易收益。
- 把因子、特征、标签、模型、策略图、组合、风控、执行和数据质量放在同一个研究闭环中。

## 1. Agent 工作原则

### 1.1 先诊断，再优化

不要看到回测不好就立刻加因子或调参数。先回答：

- 回测差，是因为 alpha 没有预测力，还是预测力没有转成仓位收益？
- 收益不稳定，是因为市场状态变化，还是股票池质量问题？
- 成本后变差，是因为换手、执行、流动性，还是策略信号本身太短命？
- 模型分数有效但策略无效，是因为阈值、选股、仓位、风控或再平衡不匹配？

### 1.2 每轮研究只改变一个主要假设

可以同时记录多个观察，但一次实验只验证一个主变量：

- 只改因子，不改模型和仓位。
- 只改特征集合，不改标签和策略。
- 只改标签，不改 universe。
- 只改组合映射，不改 alpha。
- 只改执行和再平衡，不改模型。

如果一次改动过多，结果无法归因，不能 promotion。

### 1.3 用树状探索代替线性调参

研究不是“不断调到回测更好”。正确结构是：

```text
观察问题
├─ 形成假设
├─ 选择一个研究路径
├─ 做最小可验证实验
├─ 比较 baseline
├─ 判断原因
│  ├─ 有效 -> 扩大验证范围
│  ├─ 无效 -> 回到上级诊断
│  └─ 不确定 -> 缩小问题或增加只读诊断
└─ 记录 trial / QA / promotion decision
```

### 1.4 不把最佳 trial 当成真实能力

agent 必须记录失败实验。一次研究中尝试越多，越要提高通过门槛。不能只保留胜出的 trial，也不能把“刚好最好的一次”直接变成正式策略。

## 2. 系统落地方式

| 研究对象 | 推荐系统能力 |
| --- | --- |
| 研究计划和 trial | `/api/research/agent/plans`, `/api/research/agent/plans/{plan_id}/trials/batch` |
| 审计和产物 | `/api/research/runs`, `/api/research/artifacts`, `/api/research/lineage/{run_id}` |
| 数据与资产 | `/api/market-data/*`, `/api/data/status`, `/api/diagnostics/*` |
| 股票池 | `/api/research-assets/universes*`, legacy `/api/groups*` |
| 因子 | `/api/research-assets/factor-specs*`, `/api/research-assets/factor-runs*`, legacy `/api/factors*` |
| 数据集 | `/api/research-assets/datasets*`, legacy `/api/feature-sets*` |
| 模型 | `/api/research-assets/model-experiments*`, legacy `/api/models*` |
| 组合和执行 | `/api/research-assets/portfolio-*`, `/api/research-assets/risk-*`, `/api/research-assets/rebalance-*`, `/api/research-assets/execution-*` |
| 策略运行 | `/api/research-assets/strategy-graphs*`, legacy `/api/strategies*` |
| 信号诊断 | `/api/research-assets/production-signals*`, legacy `/api/signals/diagnose` |
| QA 和发布 | `/api/research/agent/qa`, `/api/research/agent/promotion` |

每个研究计划建议至少记录：

- baseline 引用
- 当前观察
- 研究假设
- 允许改变的模块
- 不允许改变的模块
- 数据范围和 universe
- 验证切分方式
- trial budget
- 停止条件
- QA 结论

## 3. 总体研究路径图

agent 的研究路径应从“问题归因”开始，而不是从“想优化哪个参数”开始。总体地图分成八层：研究契约、证据诊断、假设工厂、路径选择、最小实验、分层验证、市场迁移检查、发布和学习闭环。

```text
Research Contract
├─ 0. 固定 baseline
│  ├─ 找到可复现 backtest / signal / paper 结果
│  ├─ 绑定 universe、dataset、model、strategy、cost、execution
│  ├─ 记录目标：提升收益、降低回撤、降低换手、提高稳定性、增强可解释性
│  └─ 记录约束：市场、持仓周期、可用数据、trial budget、停止条件
│
├─ 1. 证据诊断层：先判断问题属于哪一类
│  ├─ 数据证据
│  │  ├─ coverage、missing、stale、复权、上市退市、指数成分、停牌和交易限制
│  │  └─ 不可信 -> 路径 A：数据质量与可交易性
│  ├─ 收益来源证据
│  │  ├─ alpha、beta、行业、规模、流动性、少数 ticker、少数日期、少数 regime
│  │  └─ 来源不可解释 -> 先做归因，不进入优化
│  ├─ 信息独立性证据
│  │  ├─ 新信号是否只是已有因子、风险暴露、市场状态或低质量样本的重复表达
│  │  └─ 重复或冲突 -> 路径 C：因子稳健化与合成
│  ├─ 学习过程证据
│  │  ├─ 标签、样本权重、时间切分、purge gap、模型复杂度、窗口稳定性
│  │  └─ 训练强但样本外弱 -> 路径 D/E/I
│  ├─ 策略转化证据
│  │  ├─ 分数排序、阈值、选股数量、仓位映射、约束、再平衡、执行
│  │  └─ 分数有效但组合无效 -> 路径 F/G
│  └─ 可交易性证据
│     ├─ 成本、换手、容量、流动性、价格冲击、持仓延续、成交可达性
│     └─ 成本后失效 -> 路径 G 或拒绝 alpha
│
├─ 2. 假设工厂：只生成能被系统验证的假设
│  ├─ 经济或行为假设
│  │  └─ 价值、质量、盈利、投资、成长、动量、反转、风险、流动性、事件滞后
│  ├─ 信息结构假设
│  │  └─ 滞后、变化率、横截面排序、非线性交互、状态依赖、缺失模式
│  ├─ 标签与目标假设
│  │  └─ 预测 horizon、持仓周期、成本后收益、排序目标、风险调整目标是否一致
│  ├─ 组合表达假设
│  │  └─ alpha 是否在选股、仓位、约束、再平衡或执行中被削弱
│  ├─ 市场微结构假设
│  │  └─ US 与 CN 的交易日历、涨跌停、停牌、流动性、做空约束和微盘容量不同
│  └─ 稳健性假设
│     └─ 提升是否来自大量试验后的偶然 winner
│
├─ 3. 路径选择层：一次只走一条主路径
│  ├─ A 数据质量与可交易性
│  ├─ B 新增或重构因子
│  ├─ C 因子稳健化与合成
│  ├─ D 特征集合与标签目标
│  ├─ E 模型训练与验证
│  ├─ F 分数到仓位映射
│  ├─ G 组合、风控、再平衡和执行
│  ├─ H regime 与分层研究
│  └─ I 稳健性和反过拟合
│
├─ 4. 最小实验层：先证明方向，再扩大搜索
│  ├─ 固定不相关模块，只改变一个主变量
│  ├─ 先做小 universe / 短窗口 / 只读诊断，再做完整回测
│  ├─ 记录所有 trial，不只记录 winner
│  ├─ 对复杂方案保留一个简单对照版本
│  └─ 一旦发现数据或泄漏问题，停止优化并回到诊断层
│
├─ 5. 分层验证层：把“好结果”拆成证据矩阵
│  ├─ 时间层：早期、近期、滚动窗口、压力时期、不同市场周期
│  ├─ 股票层：规模、流动性、行业、价格、数据质量、上市年限
│  ├─ 信号层：IC、rank、单调性、方向稳定、和已有信号相关性
│  ├─ 模型层：训练/OOS 差距、窗口稳定、参数邻域稳定、特征重要性稳定
│  ├─ 策略层：毛收益、净收益、换手、持仓延续、集中度、回撤来源
│  ├─ 执行层：成本、滑点、容量、成交限制、再平衡可达性
│  └─ 复杂度层：简单版本是否足够，复杂版本是否只是拟合历史
│
├─ 6. 市场迁移检查层：US 先可用，CN 后接入，但抽象不能硬接
│  ├─ 可迁移的是研究问题、资产接口、验证协议和审计规则
│  ├─ 不应强行迁移的是股票池构造、breakpoint、成本模型、交易限制和风格解释
│  ├─ US 有效的因子进入 CN 前，先重做 CN-native universe / data / cost / liquidity 诊断
│  ├─ CN 有效的短周期或微盘信号进入 US 前，先检查容量、价差和数据频率差异
│  └─ 跨市场一致性是加分项，不是发布前提；市场内稳健性才是基础门槛
│
└─ 7. 决策和学习层
   ├─ 证据不足 -> 归档 trial，回到诊断层
   ├─ 局部有效 -> 回到路径 H，明确适用边界
   ├─ 成本后无效 -> 回到路径 G 或拒绝 alpha
   ├─ OOS 不稳 -> 回到路径 I 或降低复杂度
   ├─ 可解释、稳健、成本后有效 -> 路径 J：QA、promotion、paper
   └─ 发布后继续监控 -> 新 baseline，进入下一轮 Research Contract
```

### 3.1 路径选择启发式

agent 可以按以下启发式选择路径：

| 观察 | 优先路径 | 不应立即做什么 |
| --- | --- | --- |
| 数据覆盖、股票池、价格或成交量异常 | A | 不要先调模型 |
| 回测收益集中在微盘、低价、低流动性或少数 ticker | A / G | 不要把结果当成可交易 alpha |
| 因子 IC 弱，且不是数据问题 | B | 不要先调仓位 |
| 单因子弱但方向、分层或经济解释稳定 | B / C | 不要直接丢弃 |
| 多个因子各有信息但互相冲突 | C / H | 不要堆复杂策略规则 |
| 预测目标和持仓周期不一致 | D | 不要调模型参数掩盖 label 错位 |
| 线性模型弱，但怀疑存在交互或状态依赖 | D / E / H | 不要先上大规模无约束搜索 |
| 模型训练好但样本外差 | E / I | 不要继续增加模型复杂度 |
| 预测排序有效但回测不提升 | F | 不要马上新增因子 |
| 毛收益好但净收益差 | G | 不要只看不含成本的回测 |
| 某些年份或股票层显著失效 | H | 不要用全局参数硬修 |
| US 有效，准备迁移到 CN | A / H / G | 不要复用 US breakpoint、成本和交易假设 |
| CN 有效，准备迁移到 US | A / G | 不要忽略容量、价差和做空可行性差异 |
| trial 很多但只有一个结果好 | I | 不要 promotion |
| 已经稳定、可解释、成本后有效 | J | 不要继续无边界优化 |

### 3.2 证据权重和试验预算

研究结论不只看指标高低，还要看证据权重。agent 可以把每个候选分为三档：

| 证据档位 | 特征 | 决策 |
| --- | --- | --- |
| 高 | 多窗口、分层、成本后、简单对照、参数邻域都支持，且收益来源可解释 | 可进入 QA / paper |
| 中 | 主样本有效，但适用边界、成本、参数稳定性或市场状态仍需确认 | 继续约束实验 |
| 低 | 只在单一窗口、单一参数、单一股票层、复杂规则或大量 trial winner 中有效 | 归档或降复杂度 |

trial budget 应随假设风险变化：

- 已有经济解释、只做稳健化：允许较小范围参数搜索。
- 新增复杂模型、复杂组合规则或 regime gating：必须更小 trial budget、更强 OOS 和参数邻域检查。
- 每增加一类自由度，必须增加一个独立验证维度。
- 如果探索已经产生大量失败 trial，后续 winner 的 promotion 门槛必须提高。

### 3.3 研究路径的回路

路径不是一次性流程，而是闭环：

```text
baseline -> diagnosis -> hypothesis -> constrained experiment
         -> evidence matrix -> QA decision -> paper / reject / new baseline
```

如果新方案进入 paper 或 production signal，它不再是“终点”，而是下一轮研究的 baseline。agent 后续应关注 live/paper drift、数据漂移、成本漂移、signal decay、universe drift 和 market-regime drift。

### 3.4 研究假设分类

agent 写 hypothesis 时，优先归入以下类别之一：

- `data_quality`: 数据、股票池、交易可行性问题。
- `independent_alpha`: 新信息是否真的独立于已有信号。
- `signal_stability`: 因子是否需要稳健化、滞后、平滑、分层。
- `feature_representation`: 模型是否缺少正确表达。
- `label_alignment`: 预测目标是否与交易目标一致。
- `learning_process`: 训练、采样、验证是否可靠。
- `portfolio_translation`: 分数到仓位是否损失信息。
- `execution_cost`: 成本、换手、容量是否吞掉收益。
- `regime_dependency`: alpha 是否依赖状态。
- `market_portability`: 研究假设是否能在另一个市场重新定义，而不是复制参数。
- `robustness`: 是否存在过拟合、数据挖掘、参数敏感。

没有明确分类的 hypothesis 不应启动大规模 backtest。

### 3.5 总体路径图的使用方式

agent 每次研究前先输出一段简短的 routing decision：

```json
{
  "baseline": "当前可复现结果或资产 id",
  "main_observation": "当前最值得解释的现象",
  "hypothesis_type": "data_quality | independent_alpha | ...",
  "selected_path": "A | B | C | D | E | F | G | H | I",
  "fixed_modules": ["本轮不允许变化的模块"],
  "changed_module": "本轮唯一主变量",
  "validation_matrix": ["必须通过的验证维度"],
  "stop_condition": "何时停止继续试验"
}
```

如果 agent 无法写出 `main_observation`、`changed_module` 和 `stop_condition`，说明当前研究还没有进入可执行状态，应先做只读诊断。

### 3.6 防穿越验证时间线

所有模型和策略研究必须先声明时间线，再启动训练、验证和回测。窗口的核心原则是：模型验证窗口只用于选择模型，策略回测窗口只用于选择策略转化方式，paper 窗口必须在所有因子、特征、标签、模型、策略、组合、成本和执行参数冻结后才允许打开。

防穿越不等于使用过时模型。agent 必须区分两件事：

- **冻结研究规则**：因子定义、特征集合、标签目标、模型类型、训练窗口长度、再训练频率、策略图、组合约束、成本和执行规则在后续窗口前固定。
- **滚动刷新模型权重**：在固定研究规则下，每个训练点只使用当时可见的最近历史重新训练模型，让模型持续学习当前分布。

因此，正式验证的对象不是“某个几年前训练好的静态模型”，而是“一个不会穿越的滚动训练协议”。

#### 推荐窗口结构

以日频或周频低频美股研究为默认场景，建议使用下面的顺序：

```text
Data Start                                                       Latest Completed Trading Day
│                                                                 │
├─ W0 数据和研究准备窗口
│  └─ 用于数据质量诊断、universe 清洗、基础因子可用性检查
│
├─ W1 模型训练 / 因子研究窗口
│  ├─ 默认使用最近 3 年滚动训练窗口；样本太少时可扩大，但必须记录原因
│  ├─ 允许创建因子、构造特征、设计标签、训练模型
│  └─ 只能使用当时可得数据；所有 scaler、rank breakpoint、缺失填充、行业中性化参数都在本窗口拟合
│
├─ Gap-1 标签和执行隔离带
│  └─ 长度 = max(label_horizon, holding_period, rebalance_period, execution_lag)
│
├─ W2 模型验证窗口
│  ├─ 默认不少于 18-24 个月
│  ├─ 只用于选择模型、特征集合、标签定义和训练协议
│  └─ 不允许根据策略回测收益反向修改因子或标签
│
├─ Gap-2 研究决策隔离带
│  └─ 防止 W2 末端标签、持仓或执行收益跨入 W3
│
├─ W3 策略回测窗口
│  ├─ 默认不少于 12-24 个月
│  ├─ 模型和特征已冻结，只允许研究分数到仓位、组合约束、再平衡和执行
│  └─ 每个交易日按 walk-forward 方式生成信号，不能使用未来窗口重新训练或归一化
│
├─ Freeze 冻结点
│  └─ 固定所有代码、数据快照、universe、模型 artifact、策略图、成本和执行参数
│
└─ W4 paper / sealed replay 窗口
   ├─ 默认 1 个完整自然月或不少于 20 个交易日
   ├─ 只能按冻结方案每日推进，不能调参；若允许 retrain，必须按 Freeze 前声明的固定规则执行
   └─ 作为 promotion 前最后一道防穿越闸门
```

#### 快速可用默认切分

如果数据库已经覆盖 `2016-01-01` 到当前最新已结束交易日，且需要快速评估一条候选策略，可以采用下面的默认切分。它适合快速筛选和上线前 provisional 判断；若要 promotion 为正式策略，后续应补充多轮滚动窗口验证。

| 窗口 | 示例日期 | 用途 | 允许决策 | 禁止行为 |
| --- | --- | --- | --- | --- |
| W0 历史准备 | `2016-01-01` 至 W1 开始前 | 数据质量、universe、基础 coverage、候选因子可用性 | 清洗规则、最小流动性和可交易性规则 | 用后续收益挑股票池 |
| W1 滚动训练 | 每个训练点向前最近 3 年 | 因子、特征、标签、模型训练 | 创建候选因子、训练模型、内部 rolling/expanding 验证 | 用固定早期窗口训练一个长期不刷新的模型 |
| Gap-1 | 按 label/持仓周期自动计算 | 标签和执行隔离 | 无 | 用跨窗口标签样本训练 |
| W2 模型验证 | 最近独立 3 个月 | 模型 OOS 验证 | 选择模型族、特征集合、标签目标、滚动训练协议 | 用策略回测结果反向选择模型 |
| Gap-2 | 按 label/持仓周期自动计算 | 决策隔离 | 无 | 让 W2 持仓收益进入 W3 |
| W3 策略回测 | 最近独立 4 个月 | 策略、组合、风控、成本后回测 | 选择仓位映射、风险约束、再平衡、执行参数 | 修改因子、标签、模型或重新打开 W2 |
| Freeze | W3 结束后 | 固定候选方案和滚动 retrain 规则 | 生成 reproducibility bundle | 冻结后继续调参 |
| W4 sealed replay | 最近独立 1 个月，或未来 paper 1 个月 | 模拟交易复盘 / paper gate | 只允许 pass/fail/paper 结论 | 根据 W4 结果优化参数 |

如果当前日期还没有形成一个完整的 W4 历史月，则 W4 应从下一交易日开始进入真实 paper trading，等待 1 个完整自然月或至少 20 个交易日后再做 promotion。

#### 滚动训练协议

agent 每次训练模型时，都应优先使用滚动训练协议，避免模型停留在过早的市场分布中：

```text
for retrain_date in scheduled_retrain_dates:
    train_end = retrain_date - purge_gap
    train_start = train_end - rolling_train_window
    fit model using [train_start, train_end]
    save model artifact with train_start/train_end/retrain_date

for decision_date in signal_dates:
    load latest model whose retrain_date <= decision_date
    compute features using data visible at decision_date
    generate score and pass to StrategyGraph
```

默认建议：

- `rolling_train_window`: 3 年。
- `model_validation_window`: 3 个月，作为快速 sanity check；正式策略应补充多轮滚动验证。
- `strategy_backtest_window`: 4 个月，作为近期策略适配检查；正式策略应补充更长或多段回测。
- `paper_window`: 1 个完整自然月或至少 20 个交易日。
- `retrain_frequency`: 月度优先；如果信号半衰期短或市场状态变化快，可提高到周度，但必须记录成本和稳定性影响。
- `short_term_adapter`: 可选使用最近 3-6 个月做分数校准、阈值校准或 regime 权重调整，但不能改因子定义、标签目标和模型结构。

agent 必须把“允许自动刷新”的内容写清楚。允许刷新的是模型权重、scaler、rank breakpoint、缺失填充统计量和短期校准器；不允许在 W3/W4 临时改变的是因子逻辑、特征集合、标签定义、模型类型、策略规则和成本模型。

#### 单轮验证和多轮滚动验证

`3 年训练 + 3 个月验证 + 4 个月回测 + 1 个月 paper` 可以作为快速可用协议，但它不是最终稳健性证据。更好的做法是把这条短时间线滚动多轮：

```text
Round 1: Train recent 36m -> Validate 3m -> Backtest 4m -> Paper/replay 1m
Round 2: 向前或向后滚动 3-4 个月，重复同一协议
Round 3: 继续滚动，覆盖不同年份、波动状态、行业轮动和市场方向
```

如果单轮有效但多轮不稳，结论应标记为 `provisional` 或 `regime_specific`，不能作为正式全局策略发布。

#### Walk-forward 规则

W2、W3、W4 都必须按时间推进生成信号：

```text
for decision_date in validation_or_backtest_window:
    data_visible_at_t = data where trade_date <= decision_date - execution_lag
    fit_or_load_model using only allowed training history before decision_date
    compute factors/features using only past data
    generate score on decision_date
    execute on next eligible trading day
    evaluate return only after holding window finishes
```

默认规则：

- 因子值必须以 `decision_date` 当时可见数据计算，不能使用未来复权、未来成分股、未来财务公告或未来缺失填充。
- 横截面 rank、标准化、行业中性化、缺失填充和 winsorize 的参数，只能由当前可见样本产生。
- 模型验证阶段可以使用 rolling 或 expanding retrain，但每次 retrain 的训练数据必须早于对应验证日期。
- 策略回测阶段如果声明模型冻结，则必须加载冻结模型；如果声明 walk-forward retrain，则 retrain 规则必须在 W2 结束前确定，不能在 W3 中临时调整。
- paper / sealed replay 阶段禁止 retrain，除非候选方案在 Freeze 前已经声明固定频率的自动 retrain 规则。
- 最终上线模型不应是旧历史窗口训练出的静态 artifact；应是通过验证的滚动训练协议在最新可见数据上生成的 artifact。

#### 窗口污染处理

一旦 agent 在后续窗口中发现问题并回头修改上游模块，原窗口就被污染，不能继续作为独立证据：

| 污染行为 | 处理方式 |
| --- | --- |
| 看了 W2 后新增或删除因子 | W2 不再是最终模型验证窗口，需要向后滚动新验证窗口 |
| 看了 W3 后修改模型、标签或特征 | W3 不再是策略 OOS，只能当研究窗口 |
| 看了 W4 后调参数 | W4 作废，必须等待新的未来 paper 窗口 |
| 在任一窗口发现数据穿越 | 停止 promotion，修复数据协议后重跑完整时间线 |

#### Agent 时间线声明模板

每次模型或策略研究计划必须写入：

```json
{
  "timeline_protocol": "anti_leakage_v1",
  "data_start": "2016-01-01",
  "latest_completed_trade_date": "由数据库最大 trade_date 决定",
  "windows": {
    "train_research": {"type": "rolling", "lookback": "36m"},
    "model_validation": {"length": "3m", "role": "select model protocol"},
    "strategy_backtest": {"length": "4m", "role": "select strategy translation"},
    "paper_or_sealed_replay": {"length": "1m", "role": "frozen protocol gate"}
  },
  "purge_gap_rule": "max(label_horizon, holding_period, rebalance_period, execution_lag)",
  "retrain_policy": {
    "enabled": true,
    "frequency": "monthly",
    "lookback": "36m",
    "frozen_before_strategy_backtest": true,
    "allowed_to_refresh": ["model_weights", "scaler_stats", "rank_breakpoints", "missing_value_stats", "short_term_calibrator"],
    "not_allowed_to_change": ["factor_logic", "feature_set", "label_definition", "model_family", "strategy_graph", "cost_model", "execution_policy"]
  },
  "freeze_before_paper": true,
  "contamination_policy": "downstream result cannot modify upstream module without rolling the validation window"
}
```

## 4. 路径 A：数据质量与可交易性

**研究目的**：确认当前回测不是由脏数据、不可交易资产、低流动性异常、幸存者偏差或股票池错误驱动。

**触发信号**：

- 回测收益集中在少数股票。
- 某些交易日收益异常大。
- coverage 不稳定。
- 换一个 universe 后结果大幅消失。
- 成本或流动性约束后收益明显塌陷。

**探索方法**：

- 检查 universe profile、数据覆盖、缺失、异常价格、成交量、复权。
- 对收益贡献做分解，识别是否由低质量样本贡献。
- 建立 clean universe，与原 universe 做对照。
- 对低流动性、低价格、缺失多、交易不连续资产做分层诊断。

**系统行动**：

- 用 `/api/market-data/projects/{project_id}/status` 查看数据状态。
- 用 `/api/research-assets/universes/{universe_id}/profile` 查看股票池。
- 用 `/api/diagnostics/daily-bars` 做小样本只读诊断。
- 创建 clean universe 后复跑 baseline。

**分支判断**：

```text
数据诊断
├─ clean 后失效 -> 原策略不可采信，回到 alpha 研究
├─ clean 后仍有效 -> 可以继续优化
├─ 收益集中在不可交易资产 -> 加 universe / liquidity 约束
└─ 数据缺口严重 -> 先修数据，不做策略优化
```

## 5. 路径 B：新增或重构因子

**研究目的**：发现新的可解释 alpha，或把现有信号重新表达成更干净的因子。

**触发信号**：

- 当前因子 IC 弱或方向不稳定。
- 模型表现差，但数据和标签没有明显问题。
- 策略依赖少量旧因子，缺少互补信息。
- 某些股票分层或市场状态里有稳定残差收益。

**探索方法**：

- 从价格行为、成交量、波动、质量、价值、成长、流动性、风险、事件滞后、相对强弱等方向产生候选。
- 先做简单因子，不优先做复杂组合。
- 对候选因子做 rank、标准化、滞后、平滑、去极值等稳健化处理。
- 检查新因子是否只是已有因子的重复表达。

**系统行动**：

- 用 `FactorSpec` 创建新因子。
- 先 `preview`，再 `materialize`。
- 用 `factor-runs/{id}/evaluate` 查看因子质量。
- 与 legacy 因子做相关性和回测对照。

**分支判断**：

```text
新增 / 重构因子
├─ coverage 不够 -> 回路径 A
├─ 因子有效但换手高 -> 路径 G
├─ 因子有效但策略无效 -> 路径 F
├─ 因子只在局部有效 -> 路径 H
└─ 因子独立稳定 -> 进入特征或策略研究
```

## 6. 路径 C：因子稳健化与合成

**研究目的**：减少噪声，提取互补信息，让多个弱信号形成更稳的 alpha。

**触发信号**：

- 单因子方向有意义，但波动大。
- 多个因子各自有效，但在策略层组合很混乱。
- 模型或策略对某几个因子过度敏感。
- 因子之间存在明显互补，但没有统一表达。

**探索方法**：

- 先比较因子方向、相关性、缺失区域、适用股票层。
- 对同方向信号做标准化后合成。
- 避免用复杂权重过拟合历史表现。
- 优先测试等权、稳定性权重、简单分组权重。
- 如果因子在不同 regime 中方向不同，考虑分层使用，而不是强行合成。

**系统行动**：

- 使用 factor evaluation 和 feature correlation 分析候选。
- 创建 composite FactorSpec 或新的 feature set。
- 通过 dataset / model / StrategyGraph 验证 composite 是否真的提升回测。

**分支判断**：

```text
因子合成
├─ 高相关 -> 减少重复因子
├─ 方向冲突 -> 分层或放弃合成
├─ 简单合成有效 -> 可进入模型或策略
├─ 复杂权重才有效 -> 高过拟合风险
└─ 合成后解释性变差 -> 降低复杂度
```

## 7. 路径 D：特征集合与标签目标

**研究目的**：让模型看到足够的信息，并让预测目标贴近最终交易目标。

**触发信号**：

- 单因子有弱预测力，但模型没有明显改善。
- 模型预测指标尚可，回测不提升。
- 标签 horizon 与再平衡周期不匹配。
- 模型总是在错误股票或错误时间给出高分。

**探索方法**：

- 扩展特征时，从滞后、滚动统计、横截面 rank、相对变化、交互项、状态特征中选择一个方向。
- 优化标签时，思考策略到底要预测什么：绝对收益、相对收益、排序、方向、风险调整收益、成本后收益。
- 标签窗口必须和决策日期、执行日期、持仓周期一致。
- 特征和标签变化必须单独验证，不和模型调参混在一起。

**系统行动**：

- 创建新 dataset。
- materialize 后看 profile 和 sample。
- 用相同模型结构对比不同特征或标签。
- 把 dataset profile 和训练结果写入 artifact。

**分支判断**：

```text
特征 / 标签
├─ 样本损失大 -> 精简特征
├─ 预测提升但回测不提升 -> 路径 F
├─ 回测提升但预测不提升 -> 检查泄漏或偶然性
├─ 标签换后换手过高 -> 路径 G
└─ 简单特征也有效 -> 优先保留简单版本
```

## 8. 路径 E：模型训练与验证

**研究目的**：让模型稳定提取信息，而不是在训练样本里制造漂亮结果。

**触发信号**：

- 训练表现好，OOS 表现差。
- 不同时间窗口表现差异大。
- 模型对参数很敏感。
- trial 很多，但只有一个特别好。

**探索方法**：

- 先固定 dataset，再比较模型。
- 优先约束模型复杂度。
- 使用时间序列切分、rolling、expanding、purge gap。
- 记录所有 trial，不只记录 winner。
- 如果模型复杂度增加但 OOS 不稳定，回到特征和标签，而不是继续调参。

**系统行动**：

- 用 `/api/research/agent/plans` 限制 trial budget。
- 用 `/api/research-assets/model-experiments/train` 训练。
- 用 trial batch 记录所有实验配置和结果。
- 用 QA 检查过拟合风险。

**分支判断**：

```text
模型训练
├─ 训练好 / OOS 差 -> 减复杂度或回路径 D
├─ OOS 好 / 回测差 -> 路径 F
├─ 多窗口不稳 -> 路径 H
├─ 只有一个 winner -> 高过拟合风险
└─ 多窗口稳定 -> 进入策略转化
```

## 9. 路径 F：分数到仓位映射

**研究目的**：把模型分数或因子强弱转成更合理的 target portfolio。

**触发信号**：

- 模型排序有效，但策略收益弱。
- 高分股票不一定带来高收益。
- 持仓过于集中或过于分散。
- 信号波动导致频繁换仓。

**探索方法**：

- 固定 alpha，只改变分数到仓位的映射。
- 比较截断、排序、阈值、分层、等权、分数权重、波动调整等思路。
- 关注仓位是否表达了信号置信度。
- 如果映射很复杂才有效，优先怀疑过拟合。

**系统行动**：

- 创建 `PortfolioConstructionSpec`。
- 用同一 `alpha_frame` 对比多个 builder。
- 通过 StrategyGraph `simulate-day` 看单日 trace。
- 用 backtest 验证净效果。

**分支判断**：

```text
分数到仓位
├─ 收益来自少数重仓 -> 加集中度约束
├─ 换手过高 -> 加平滑或再平衡带
├─ 低分尾部贡献异常 -> 检查 score 单调性
├─ 简单映射有效 -> 保留简单方案
└─ 复杂映射才有效 -> 拒绝 promotion
```

## 10. 路径 G：组合、风控、再平衡和执行

**研究目的**：减少 alpha 到实际收益之间的损耗，让策略更可交易。

**触发信号**：

- 毛收益好，净收益差。
- turnover 高。
- 回撤来自集中暴露。
- 信号频繁反转。
- 交易成本或滑点假设一变，策略失效。

**探索方法**：

- 固定 alpha 和仓位映射，单独研究风控、再平衡和执行。
- 研究最大单票、持仓数量、行业/风格暴露、换手限制、再平衡带、买入门槛和持有门槛。
- 比较“更少交易但更稳”是否优于“每期追逐最优信号”。
- 不要为了降低回撤加入过多硬规则，先看规则是否有经济含义。

**系统行动**：

- 创建 `RiskControlSpec`、`RebalancePolicySpec`、`ExecutionPolicySpec`。
- 用 `portfolio-runs/compare-builders` 对比。
- 用 StrategyGraph 统一 signal / backtest / paper 的执行口径。

**分支判断**：

```text
组合执行
├─ 成本吞噬收益 -> 降换手 / 调再平衡
├─ 暴露过度集中 -> 加风险约束
├─ 规则一多才有效 -> 降复杂度
├─ 净收益改善但 alpha 不变 -> 可保留执行优化
└─ 净收益仍差 -> 回 alpha 或 universe
```

## 11. 路径 H：regime 与分层研究

**研究目的**：识别策略是否只在某些市场状态或股票分层中有效，并决定是否需要 gating 或分模型。

**触发信号**：

- 某些年份或阶段明显失效。
- 回撤集中在特定市场环境。
- 大盘、小盘、高流动性、低流动性表现差异明显。
- 某些行业长期贡献或拖累。

**探索方法**：

- 先做只读分层诊断，不急着引入复杂 regime 模型。
- 从市场状态、波动状态、流动性层、规模层、行业层、价格层中选择一个维度。
- 如果分层后发现 alpha 方向反转，再考虑 gating。
- 如果只有很窄的分层有效，优先判断容量和过拟合风险。

**系统行动**：

- 用 dataset query、backtest diagnostics、signal diagnose 做分层分析。
- 创建多个 universe 或 StrategyGraph 分支做小范围验证。
- 把分层规则写入 artifact，避免变成隐含手工判断。

**分支判断**：

```text
regime / 分层
├─ 全局都有效 -> 不增加复杂度
├─ 某层失效 -> 加过滤或降权
├─ 某层反向 -> 考虑 gating
├─ 只有窄层有效 -> 标记容量风险
└─ 分层规则不稳定 -> 回到全局简单策略
```

## 12. 路径 I：稳健性和反过拟合

**研究目的**：判断提升是否真实，而不是 trial 挖出来的偶然结果。

**触发信号**：

- 优化来自大量试验后的一个 winner。
- 换一个时间段就失效。
- 参数稍微变化结果大幅改变。
- 结果很难解释。
- 只有 in-sample 曲线漂亮。

**探索方法**：

- 复用相同假设做不同时间窗口验证。
- 做 rolling / expanding / walk-forward。
- 比较 winner 和 trial 分布，不只看 winner。
- 做参数邻域稳定性检查。
- 检查结果是否依赖单一股票、单一行业、单一年份、单一市场状态。

**系统行动**：

- 用 agent plan 记录 trial count。
- 用 trial batch 记录所有候选。
- 用 QA gate 做正式阻断。
- 对 scratch artifact 做 cleanup preview，保留可复现的候选和拒绝原因。

**分支判断**：

```text
稳健性检查
├─ 多窗口有效 -> 可进入 QA
├─ 参数敏感 -> 降复杂度
├─ winner 孤立 -> 拒绝 promotion
├─ 单一 bucket 驱动 -> 回路径 A/H
└─ 成本后不稳 -> 回路径 G
```

## 13. 路径 J：QA、promotion 和 paper

**研究目的**：把候选研究结果从实验资产转成可用策略资产。

**触发条件**：

- 已有明确 baseline。
- 已有完整 lineage。
- OOS、成本后、分层、稳健性检查都没有明显阻断。
- 复杂度和收益改善匹配。
- 可以解释为什么有效、什么时候失效。

**系统行动**：

- `POST /api/research/agent/qa`
- `POST /api/research/agent/promotion`
- 如通过，生成 production signal 或 paper session。
- 导出 reproducibility bundle。

**拒绝条件**：

- 只在样本内有效。
- 只在一个窗口、一个股票池或一个参数点有效。
- 成本后无效。
- clean universe 后无效。
- 无法解释收益来源。
- 没有记录失败 trial。

## 14. 研究计划模板

agent 创建研究计划时，应写成开放但受控的结构：

```json
{
  "hypothesis": "The current model score has usable ranking information, but portfolio conversion may be too unstable.",
  "search_space": {
    "primary_path": "F",
    "allowed_changes": ["portfolio_construction", "rebalance_policy"],
    "frozen_modules": ["factor_specs", "dataset", "model_package", "universe"],
    "diagnostics": ["score_monotonicity", "turnover", "position_concentration", "cost_drag"]
  },
  "budget": {
    "max_trials": 8,
    "max_backtests": 4
  },
  "stop_conditions": {
    "reject_if_oos_worse": true,
    "reject_if_cost_adjusted_worse": true,
    "reject_if_complexity_increases_without_clear_explanation": true
  }
}
```

## 15. Agent 输出要求

每轮研究结束，agent 必须输出：

- 当前 baseline 是什么。
- 选择了哪条路径，为什么。
- 改了什么，没改什么。
- 观察到什么证据。
- 有哪些失败 trial。
- 改善是否跨时间、跨分层、成本后仍然成立。
- 是否建议继续、归档、进入 QA 或 promotion。

## 16. 推荐执行顺序

默认顺序：

1. 数据和 universe 诊断。
2. alpha / 因子诊断。
3. 特征和标签诊断。
4. 模型训练诊断。
5. 分数到仓位诊断。
6. 组合、风控、再平衡、执行诊断。
7. regime / 分层诊断。
8. 稳健性和反过拟合检查。
9. QA / promotion / paper。

agent 可以根据证据跳转路径，但必须记录跳转原因。

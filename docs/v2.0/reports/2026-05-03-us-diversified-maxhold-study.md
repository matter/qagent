# 2026-05-03 美股分散持仓与最长持仓约束研究

## 结论

本轮将用户新增约束作为硬条件，而不是事后解释：

- 回测窗口固定为 `2026-01-05` 到 `2026-04-01`。
- 组合必须分散，合格策略至少持有 `6~7` 只，单票权重上限 `15%~18%`。
- 单笔最长持仓不得超过一个月，本轮按交易日志 `holding_days <= 21` 验收。
- 已持仓只在实际开盘权重偏离目标 `5%~10%` 后调整，避免为维持仓位占比而做无效 T。
- 不把候选不足时的权重重新集中到少数股票；使用 `raw_weight` 保留策略输出权重和现金语义。

旧冠军 `M0502_S327_S31_LEADERKEEP_RISKADJ_R1` 在该约束下无效。它的 Sharpe `19.4821` 主要来自 `LITE` 首月 `100%` 单票和跨月继续持有，合规诊断为：

- 最少持仓数：`1`
- 最大单票权重：`1.0`
- 最大交易持仓天数：`41`
- 分散、单票上限、最长持仓三项均不通过。

当前合规最优更新为：

- Strategy: `45443a56dc81`
- Name: `M0503_S350_COHORT10_QUALITY_AGEONLY_MAX21_RAW_R1`
- Backtest: `6405a89eaa5c`
- Sharpe: `6.1259`
- Total return: `0.267559`
- Annual volatility: `0.264945`
- Max drawdown: `-0.058703`
- Win rate: `0.5294`
- Trades: `44`
- Annual turnover: `21.9201`
- Total cost: `12673.27`
- 合规：最少持仓 `7`，平均持仓 `8.7`，最大单票权重 `0.15`，最大交易持仓天数 `19`。

胜率优先的合规备选：

- Strategy: `e8278d70c755`
- Name: `M0503_S345_COHORT8_FLOOR2_MAX21_RAW_R1`
- Backtest: `a783cfbbcf36`
- Sharpe: `5.8613`
- Total return: `0.264593`
- Max drawdown: `-0.064179`
- Win rate: `0.6087`
- Trades: `53`
- Annual turnover: `30.4457`
- 合规：最少持仓 `6`，平均持仓 `7.2`，最大单票权重 `0.18`，最大交易持仓天数 `19`。

## 设计依据

本轮采用低自由度结构优化，避免围绕本窗口做密集阈值搜索。

- `No-trade band`：交易成本存在时，仓位偏离不大应少调仓。本轮使用 actual-open held-overlap 偏离带，参考 Leland 的交易成本/无交易区思想：[SSRN](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=206871)。
- 动量仍使用 52 周高点和趋势确认，但不允许单票满仓或无限续持，参考 George 和 Hwang 的 52-week high momentum 研究：[HKUST](https://repository.hkust.edu.hk/ir/Record/1783.1-27926)、[PDF](https://www.bauer.uh.edu/TGeorge/papers/gh4-paper.pdf)。
- 风险权重使用已有 safe/keep/risk 因子的平滑惩罚，不用单窗口硬过滤搜索。

## 策略演进

| 策略 | Backtest | 合规 | Sharpe | Return | MaxDD | Win | Trades | Turnover | 说明 |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| S327 leaderkeep | `00ed63ae0b55` | 否 | `19.4821` | `0.847844` | `-0.131662` | `1.0000` | `11` | `12.3407` | 单票满仓、持仓 41 天 |
| S334 diverse weekly | `cbeb9795cd0d` | 是 | `2.8939` | `0.192322` | `-0.096161` | `0.5806` | `69` | `34.3837` | 分散合规，但周频全组合竞争换手高 |
| S345 cohort floor2 | `a783cfbbcf36` | 是 | `5.8613` | `0.264593` | `-0.064179` | `0.6087` | `53` | `30.4457` | 月度 cohort + 广度底线，胜率较好 |
| S348 age-only 8 | `2e3e9c2362fc` | 是 | `6.0008` | `0.259305` | `-0.049709` | `0.5333` | `38` | `22.1676` | 避免 3/27 短持换仓 |
| S350 age-only 10 quality | `6405a89eaa5c` | 是 | `6.1259` | `0.267559` | `-0.058703` | `0.5294` | `44` | `21.9201` | 当前合规最优 |

## 当前最优结构

S350 的核心逻辑：

1. 空仓时只在月末锚点开一个分散 cohort。
2. 已有持仓不因新的月末锚点强制换仓，只在持仓年龄到期或硬风控触发时滚动。
3. 到期滚动时排除当前 cohort，避免同票立即续命导致超过一个月。
4. 如果硬止损导致持仓数跌破底线，只用满足安全/趋势底线的候选补足。
5. 用 `raw_weight` 输出封顶权重，10 只版本单票上限 `15%`。

关键 rebalance：

- `2026-01-30`: 开仓 9 只，最大权重 `0.147391`。
- `2026-02-27`: 首个 cohort 到期，滚动为 7 只，最大权重 `0.15`。
- `2026-03-06`: AES 硬退出，补 APA，持仓数维持 7。
- `2026-03-20`: 第二个 cohort 到期，滚动为 10 只。
- `2026-03-27`、`2026-04-01`: 只 carry，不再因为月底/窗口末端无效换仓。

## 执行带验证

对 S345 和 S347 做 actual-open held-overlap `5pp / 8pp / 10pp` 检查，结果完全一致，说明当前 cohort 结构本身已经显著减少权重维护交易，不依赖窄参数带。

| 策略 | Band | Backtest | Sharpe | Return | Trades | 合规 |
| --- | --- | --- | ---: | ---: | ---: | --- |
| S345 | 5pp | `bbd2c02077e7` | `5.8613` | `0.264593` | `53` | 是 |
| S345 | 8pp | `a783cfbbcf36` | `5.8613` | `0.264593` | `53` | 是 |
| S345 | 10pp | `8f20ae752299` | `5.8613` | `0.264593` | `53` | 是 |

## 产物

- `/Users/m/dev/atlas/tmp/research_0503_diverse_maxhold.py`
- `/Users/m/dev/atlas/tmp/research_0503_diverse_maxhold.json`
- `/Users/m/dev/atlas/tmp/research_0503_lifecycle_maxhold.py`
- `/Users/m/dev/atlas/tmp/research_0503_lifecycle_maxhold.json`
- `/Users/m/dev/atlas/tmp/research_0503_cohort_maxhold.py`
- `/Users/m/dev/atlas/tmp/research_0503_cohort_maxhold.json`
- `/Users/m/dev/atlas/tmp/research_0503_cohort_floor2_maxhold.py`
- `/Users/m/dev/atlas/tmp/research_0503_cohort_floor2_maxhold.json`
- `/Users/m/dev/atlas/tmp/research_0503_cohort_ageonly_maxhold.py`
- `/Users/m/dev/atlas/tmp/research_0503_cohort_ageonly_maxhold.json`
- `/Users/m/dev/atlas/tmp/research_0503_s345_execution_buffers.py`
- `/Users/m/dev/atlas/tmp/research_0503_s345_execution_buffers.json`

## 下一步

1. 继续围绕 S350 做非参数化改进：减少亏损 tactical refill，而不是调整大量阈值。
2. 增加 cohort 级别行业/主题分散诊断，避免单期过度集中能源或半导体。
3. 在相邻窗口和 2026-04-02 之后做 forward-like 复验，确认分散/期限约束下的 Sharpe 不只来自本窗口。

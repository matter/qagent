# 2026-05-02 美股指定窗口低频动量策略优化

## 结论

用户指定窗口统一为 `2026-01-05` 到 `2026-04-01`。本轮纠正了此前把长窗口策略直接当成该窗口最优的问题：指定窗口内前几天的高夏普主要来自月频低换手的 S27/S30 家族，而不是 S294 日频 hysteresis 线。

当前最优策略更新为：

- Strategy: `a522ab8def0d`
- Name: `M0502_S327_S31_LEADERKEEP_RISKADJ_R1`
- Backtest: `00ed63ae0b55`
- Sharpe: `19.4821`
- Total return: `0.847844`
- Annual volatility: `0.595249`
- Max drawdown: `-0.131662`
- Win rate: `1.0`
- Trades: `11`
- Annual turnover: `12.3407`
- Total cost: `9279.81`

相对上一轮最优 `a8ad06399aec` / `M0502_S322_S31_SIGNALWEIGHT_R1`，S327 的 Sharpe 从 `19.3297` 提升到 `19.4821`，收益从 `0.841316` 提升到 `0.847844`，胜率从 `0.6667` 提升到 `1.0`。代价是最大回撤从 `-0.127528` 放宽到 `-0.131662`，但仍好于 S30/S316 线的 `-0.132654` 附近。

## 设计依据

这次只做低自由度结构优化，不做阈值密集搜索：

- 组合层采用 no-trade band 思路：交易成本下存在不交易区域，权重偏离不大时不应强制调仓。参考 Leland 的交易成本/无交易区研究：[SSRN](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=206871)。
- 动量层继续使用 52 周高点作为趋势确认，而不是单独追高过滤。参考 George 和 Hwang 的 52-week high momentum 研究：[HKUST](https://repository.hkust.edu.hk/ir/Record/1783.1-27926)、[Journal of Finance PDF](https://www.bauer.uh.edu/TGeorge/papers/gh4-paper.pdf)。
- 权重层使用平滑风险调整，而不是硬性风险剔除。参考动量波动管理思路：[Momentum Has Its Moments](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2041429)。

## 策略演进

| 策略 | Backtest | Sharpe | Return | MaxDD | Win | Trades | 说明 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `M0415_S27_S12_SOFTGUARD_R7_E7` | `3a44137fbe5d` | `17.0008` | `0.846601` | `-0.150024` | `0.5000` | `6` | 老高夏普月频基线 |
| `M0415_S30_S27_BROAD_POOL_PROMOTE_ANTITRAP_R1` | `6e3e4b938854` | `19.2565` | `0.844650` | `-0.133453` | `1.0000` | `11` | broad/trend 扩池明显降波动 |
| `M0502_S316_S30_SIGNALWEIGHT_R1` | `90180e99669e` | `19.3137` | `0.845229` | `-0.132654` | `1.0000` | `11` | 信号权重替代等权 |
| `M0502_S322_S31_SIGNALWEIGHT_R1` | `b209b209f95b` | `19.3297` | `0.841316` | `-0.127528` | `0.6667` | `11` | S31 entry 降回撤，但 COHR 亏损卖出 |
| `M0502_S327_S31_LEADERKEEP_RISKADJ_R1` | `00ed63ae0b55` | `19.4821` | `0.847844` | `-0.131662` | `1.0000` | `11` | 当前最优 |

## S327 结构

S327 从 S322 出发，只加两类结构：

1. S30 风格 leader retention：已经持有且具备 near52、broad/trend、safe/keep 支撑的票，不因 S31 entry 排名切换而轻易卖出。关键效果是 4/1 不再把 COHR 亏损退出，避免“低位卖出”。
2. 平滑风险调整 sizing：在原始信号分数上乘以 `safe/keep` 正向项、风险 rank 负向项。它不是硬过滤，因此没有新增候选进入/剔除阈值。

S327 的关键调仓：

- `2026-01-30`: LITE 100%。
- `2026-02-27`: 扩为 LITE/DELL/MRNA/SATS/COHR。
- `2026-03-31`: 仅退出 SATS，保留并增配 COHR/DELL/LITE/MRNA。
- `2026-04-01`: 新增 APA，保留 COHR/DELL/LITE/MRNA；没有像 S322 一样卖出 COHR。

交易诊断：

- LITE 已实现盈利约 `637234.71`。
- DELL 已实现小盈利约 `4483.15`。
- SATS 已实现小盈利约 `2702.38`。
- COHR 不再在 `2026-04-01` 亏损卖出，避免了 S322 中约 `-32639.51` 的 realized loss。

## 用户建议的偏离带

对 S327 使用实际开盘权重偏离带：

| 配置 | Backtest | Sharpe | Return | Trades | Turnover | 说明 |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| base | `00ed63ae0b55` | `19.4821` | `0.847844` | `11` | `12.3407` | 当前最优 |
| actual_open 5pp | `14c8f06453eb` | `19.3680` | `0.845312` | `9` | `11.9863` | 降低 2 笔交易，但略降 Sharpe |

结论：偏离带能减少无效做 T，适合作为保守执行版；但在该窗口 Sharpe 目标下，S327 base 更优。

## 失败/消融方向

### 1. 月频不可替代

S30/S316 频率扫描显示，daily/weekly 即使用 8%-10% actual-open overlap guard，仍大幅降低 Sharpe，并产生大量 tactical 亏损。该窗口的核心边际来自“月频持有赢家”，不是更快信号刷新。

### 2. raw cash budget 未超过

新增 `raw_weight` 支持后，现金预算策略可以正式回测，但 S30 raw cash 最好只有：

- `M0502_S318_S30_RAWCASH_COUNT_BAL_R1`
- Backtest `bd986ee36791`
- Sharpe `18.6957`
- Return `0.828530`

降低总仓位压低波动有限，收益牺牲更明显，不作为当前主线。

### 3. S31 path-quality tilt 未超过

`M0502_S328_S31_PATHADJ_SIGNALWEIGHT_R1` 只把已有 path-quality 因子作为小权重 tilt，Sharpe `19.2933`，低于 S322 和 S327。说明此前 S311/S312 因子线对月频 S31/S30 家族增量不足。

### 4. 新因子层扩展未超过

新增标准化因子：

- `exp0502_realized_volatility_20`，id `dd29bf856b91`
- `exp0502_momentum_acceleration_20_60`，id `4767d81b3bae`

结果：

- `M0502_S330_S327_VOLBUDGET_R1`: Sharpe `19.4654`
- `M0502_S331_S327_VOL_ACCEL_R1`: Sharpe `19.4201`

二者都低于 S327。短期不继续增加同类 OHLCV 单票因子。

### 5. S30 风险调整消融未超过

把 S327 的同一风险调整放回 S30/S316：

- Strategy `c595b6615afd` / `M0502_S332_S30_RISKADJ_R1`
- Backtest `46bfa61702c4`
- Sharpe `19.4476`
- Return `0.847802`
- MaxDD `-0.132483`
- Win `1.0`

它低于 S327，说明最优来自 S31 entry、S30 leader retention、风险平滑三者组合，而不是单纯权重公式。

## 下一步

1. 保留 S327 为当前指定窗口冠军，S327 actual-open 5pp 作为保守执行候选。
2. 不继续在 `risk_q/safe/keep` 系数上做密集调参；当前改进已经有结构解释，继续微调容易过拟合。
3. 如果继续提高泛化性，应做 candidate-state label：把 current keep、leader keep、new entry、late extension 分 lane 建模，而不是继续加单票 OHLCV 小因子。
4. 需要更多窗口验证时，优先用滚动月切片和 2026-04-02 之后 forward/live-like 复验，而不是修改本次指定窗口。

## 产物

- `/Users/m/dev/atlas/tmp/research_0502_s27_family_signal_weight.py`
- `/Users/m/dev/atlas/tmp/research_0502_s27_family_signal_weight.json`
- `/Users/m/dev/atlas/tmp/research_0502_s322_structural_robustness.py`
- `/Users/m/dev/atlas/tmp/research_0502_s322_structural_robustness.json`
- `/Users/m/dev/atlas/tmp/research_0502_s327_factor_extension.py`
- `/Users/m/dev/atlas/tmp/research_0502_s327_factor_extension.json`
- `/Users/m/dev/atlas/tmp/research_0502_s30_riskadj_check.py`
- `/Users/m/dev/atlas/tmp/research_0502_s30_riskadj_check.json`
- `/Users/m/dev/atlas/tmp/backtest_00ed63ae0b55_detail.json`
- `/Users/m/dev/atlas/tmp/backtest_14c8f06453eb_detail.json`

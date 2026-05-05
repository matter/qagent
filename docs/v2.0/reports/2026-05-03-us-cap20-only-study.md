# 2026-05-03 美股单票 20% 上限约束研究

## 结论

本轮按用户最新约束重置合规口径：

- 回测窗口固定为 `2026-01-05` 到 `2026-04-01`。
- 只保留硬条件：单票目标权重不超过 `20%`。
- 不再限制最长持仓时间。
- 执行层继续使用 actual-open held-overlap no-trade band：已持仓只在实际开盘权重相对目标偏离约 `5%~10%` 后调整，避免上涨过程中的无效做 T。
- 避免密集参数扫表；本轮有效提升来自低自由度结构改动。

当前 cap20-only 合规最优已经进一步提升到：

- Strategy: `eabd088d75bf`
- Name: `M0503_S413_S402_MONTHEND_SEAT_PATHQ_MIN_CAP20_R1`
- Backtest: `c1165a224cd6`
- Sharpe: `13.5912`
- Total return: `0.456257`
- Annual volatility: `0.271098`
- Max drawdown: `-0.028719`
- Win rate: `0.8182`
- Trades: `27`
- Annual turnover: `19.1819`
- Total cost: `12066.33`
- 合规：最大单票目标权重 `0.20`，满足 `<=20%`；最少持仓 `3`，平均持仓 `4.884`；目标权重合计区间 `0.60~1.0`。

核心改进分三层。第一层是 S390：把 S380 的 cohort 换仓锚点从“周频回测遇到日历日 `>=24`”改成“实际最后一个交易日”，避免 `2026-03-27` 过早滚入新 cohort。第二层是 S402：不替换整套策略规则，只把 attack 信号改成“原 attack 模型 + `F273 pathret20 ddheavy pretrade4` 模型”的较弱侧共识。第三层是 S413：在 S402 的弱侧共识上继续加入 `seatquality_b_pretrade6 / 75abb9134dd9` 与 `compact path-quality / 7c6a99802c81` 两个不同标签家族的 candidate-quality 模型，并允许使用用户给出的 `20%` 单票硬上限。这个增益来自候选质量模型共识，不是窗口内阈值扫表。

## 旧冠军复核

旧高 Sharpe 策略可以作为思路参考，但不能直接作为本轮合规方案：

| 策略 | Backtest | Sharpe | Return | MaxDD | Win | 合规问题 |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| S327 leaderkeep | `00ed63ae0b55` | `19.4821` | `0.847844` | `-0.131662` | `1.0000` | 1 月 `LITE=100%`；3 月 `MRNA=31.8667%` |
| S332/S30 riskadj | `46bfa61702c4` | `19.4476` | `0.847802` | `-0.132483` | `1.0000` | 同样存在 `LITE=100%` 和后续权重超 20% |
| S318 raw-cash | `bd986ee36791` | `18.6957` | `0.828530` | `-0.132652` | `1.0000` | 1 月 `LITE=98%`，后续多票仍超 20% |

直接把旧 S327/S332 权重裁到 `20%` 并保留现金，胜率仍高，但 1 月只有 `20%` 暴露，Sharpe 降至约 `1.97`。因此本轮没有沿“旧冠军封顶”继续调参，而是复用老策略的有效思想：持有强 cohort、减少无效换仓、避免高位追涨，同时补足合规分散。

## 策略演进

| 策略 | Backtest | 合规 | Sharpe | Return | MaxDD | Win | Trades | Turnover | 说明 |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| S413 seat + pathq weak consensus cap20 | `c1165a224cd6` | 是 | `13.5912` | `0.456257` | `-0.028719` | `0.8182` | `27` | `19.1819` | 当前最优；S402 加 seat-quality 与 compact path-quality 弱侧共识 |
| S413 seat + pathq weak consensus cap20 5pp | `93340f07579c` | 是 | `13.5052` | `0.452815` | `-0.028719` | `0.8182` | `28` | `19.1879` | 5% no-trade band 稳定，回撤不变 |
| S407 seat weak consensus cap20 | `e2b36904b15e` | 是 | `13.5735` | `0.443447` | `-0.033412` | `0.8000` | `25` | `19.1819` | seat-quality 弱侧共识；主要修 2 月底 cohort |
| S410 seat weak consensus cap18 | `f421bf480392` | 是 | `13.3066` | `0.438089` | `-0.034202` | `0.8000` | `25` | `18.1158` | 18% cap 下仍超过 S402，说明不是单纯加杠杆 |
| S411 seat score-only cap20 | `fd3ed04fbb09` | 是 | `13.3050` | `0.437236` | `-0.033412` | `0.8750` | `21` | `15.9512` | 只用 seat 做弱侧打分，不扩候选池 |
| S412 seat pool-only cap20 | `6a89bf594e67` | 是 | `12.9669` | `0.447961` | `-0.033412` | `0.7273` | `26` | `19.9119` | 只扩候选池不打分，胜率下降 |
| S409 compact pathq weak consensus cap20 | `f429b872b888` | 是 | `12.6600` | `0.453845` | `-0.028575` | `0.8000` | `24` | `16.6956` | 回撤最低但波动较高，单独使用不如 S413 |
| S406 pure S402 cap20 | `c66caa52abc4` | 是 | `12.5906` | `0.441706` | `-0.033412` | `0.7778` | `22` | `16.6957` | 只把 S402 权重上限从 18% 放到 20% |
| S402 dual attack min | `9707a4989190` | 是 | `12.4823` | `0.436570` | `-0.034202` | `0.7778` | `22` | `15.8756` | 上一版最优；原 attack 与 ddheavy path 模型取较弱侧共识 |
| S402 dual attack min 5pp | `5e6cd67037bb` | 是 | `12.3038` | `0.427482` | `-0.031008` | `0.7778` | `24` | `15.8950` | 5% no-trade band 仍超过 S390 |
| S395 guard ddheavy | `7fea4852a515` | 是 | `12.2323` | `0.427992` | `-0.035303` | `0.7778` | `22` | `15.8756` | guard slot 替换为 ddheavy；主要少买 3/31 的 NWSA |
| S397 attack ddheavy | `b57c9bed2839` | 是 | `12.2202` | `0.432200` | `-0.034201` | `0.7000` | `24` | `17.2990` | attack slot 纯替换为 ddheavy；收益高但换手/波动略高 |
| S390 month-end cap18 | `f3415766baa6` | 是 | `12.0572` | `0.424204` | `-0.035303` | `0.7778` | `23` | `16.6192` | 前一版结构基线；实际月底换仓，避免 3/27 提前 roll |
| S390 month-end cap18 10pp | `4760bf6259a1` | 是 | `12.0572` | `0.424204` | `-0.035303` | `0.7778` | `23` | `16.6192` | 8pp 与 10pp no-trade band 路径一致 |
| S391 month-end cap20 | `8c86ea073eed` | 是 | `11.8589` | `0.422325` | `-0.034642` | `0.7778` | `23` | `17.5220` | 放到 20% 后波动略升，Sharpe 未超过 cap18 |
| S390 month-end cap18 5pp | `a2a44449a028` | 是 | `11.8470` | `0.414303` | `-0.031852` | `0.7778` | `25` | `16.6371` | 更窄 band 多两笔交易，收益略低 |
| S380 no-age struct cap18 | `b76928cdb49a` | 是 | `10.2638` | `0.428558` | `-0.082787` | `0.7778` | `24` | `17.0423` | 强基线；但 3/27 日历锚点提前换仓 |
| S386 no-age struct cap20 | `e44ad0c092e0` | 是 | `10.1451` | `0.424041` | `-0.082037` | `0.7778` | `24` | `17.5314` | 单纯放大到 20% 没有改善 |
| S359 cohort8 quality hold | `97e8180efa82` | 是 | `6.8947` | `0.335646` | `-0.095128` | `0.3333` | `11` | `5.1149` | 早期 cap20-only 最优；收益和胜率不足 |
| old hyst/pathq recheck | `75829aed9c29` | 是 | `4.9305` | `0.257809` | `-0.080957` | `0.4286` | `71` | `41.7190` | 老 hysteresis 线在本固定窗口不成立 |

## 当前最优结构

S413 的核心：

1. 继承 S390 的月底 cohort 骨架：实际最后一个交易日才 open/roll，月内只 carry 或硬风控退出。
2. 继续使用路径质量、防守安全和高位追涨过滤：`near_52w_high`、downside stability、entry trap safety、launch efficiency 等因子用于过滤 late chase 与结构断裂票。
3. 单票权重由 `raw_weight` 输出，硬上限为 `20%`，正好满足用户 `<=20%` 要求。
4. 月内不补仓、不因权重漂移频繁做 T；非锚点日只保留现有持仓，除非硬风控退出。
5. attack 模型不再只看原 `ex10 attack`，而是取原 attack、`F273 pathret20 ddheavy pretrade4`、`seatquality_b_pretrade6`、`compact path-quality` 的较弱侧分位。这个共识机制会压低“短期强但路径、席位质量或回撤质量不被认可”的候选。

关键持仓路径：

- `2026-01-30`: 开仓 `APA / CIEN / COHR / LITE / LRCX / NEM / TER`，最大权重约 `0.192`。相对 S402，用 `APA / TER` 替代 `BKR` 并摊薄部分单票权重，这是 seat/pathq 共识排序结果。
- `2026-02-27`: 月底 roll 到 `AES / DELL / LYB / XOM`，每只 `0.20`；相对 S402，`MRNA` 被替换，组合暴露从 `60%` 提高到 `80%`。
- `2026-03-02`: `AES` 被硬风控退出，剩余 `DELL / LYB / XOM` 继续持有，每只 `0.20`。
- `2026-03-27`: 不再提前 roll，继续持有 `DELL / LYB / XOM`。
- `2026-03-30`: 继续持有原 cohort，避免 S380 的提前 roll 路径。
- `2026-03-31`: 实际月底 roll 到 `FANG / FIX / MRNA / TER / XYZ`，每只 `0.20`；窗口末端这部分未实现盈亏，不能单独视作主要 alpha 来源。
- `2026-04-01`: 继续持有，不在窗口末端做额外优化。

个股实现收益主要来自 `LYB +104586`、`LITE +104015`、`CIEN +54528`、`APA +53293`、`TER +41377`、`COHR +41091`、`DELL +35121`、`NEM +21754`、`XOM +9526`；亏损主要是被快速退出的 `AES -3248` 和 `LRCX -3294`。最差单日从 S402 的 `2026-02-26 -3.4202%` 改善到 S413 的 `2026-02-04 -2.7242%`，最大回撤从 `-0.034202` 降到 `-0.028719`。

## 失败方向

- 纯旧冠军 cap20：只把旧 S327/S332 权重封到 `20%`，初期现金过多，无法接近旧 Sharpe。
- 单纯 cap18 改 cap20：S386/S391 都显示 20% 暴露会略放大波动或换手，Sharpe 不如 18%。
- path-retain/reserve：最佳只有约 `6.9986`，保留了过旧 cohort，错过 2 月底 `DELL / LYB` 的强路径。
- S390 低权重 reserve 补位：`S392/S393` 只多买一只补位票，Sharpe 反而降到约 `11.84~11.85`。说明 2 月底 54% 暴露不是简单现金补满问题，强行增加弱候选会伤害收益。
- S390 launch slot 换成 `F262 launch30 stab6`：Sharpe 降到约 `5.90`，说明先前在 S204/S262 上有效的 launch 模型不能无条件迁移到 S390 月底 cohort。
- dual attack mean：原 attack 与 ddheavy 简单均值共识 Sharpe 只有 `11.36`；较弱侧 `min` 共识更适合当前防追高目标。
- 只扩 seat-quality 候选池而不把它纳入弱侧打分：`S412` 虽然 return 到 `0.447961`，但胜率降到 `0.7273`、换手升到 `19.9119`，说明 candidate-quality 模型不能只当补位来源，必须参与质量约束。
- 只用 conversion-rank 模型做额外弱侧共识：`S408` 与纯 `S406 cap20` 路径一致，没有增量，说明该模型在这个月底 cohort 边界上没有改变 selected set。
- incumbent-vs-challenger competition：Sharpe 约 `3.5`，因为它保留了过峰旧仓并放大 3 月波动。
- old hysteresis/pathq 线：在本次固定窗口 `2026-01-05~2026-04-01` 只有约 `3~5` Sharpe；前几天的高 Sharpe 主要来自不同窗口和不同约束，不能直接迁移。

## 鲁棒性判断

S413 的提升点有明确行为解释：先修正 month-end cohort 的时间语义，再用不同标签家族的弱侧共识降低高位/路径差/席位质量差候选的权重。8pp 与 10pp 结果一致，5pp 仍有 `Sharpe 13.5052 / maxDD -0.028719`，说明收益不是来自 no-trade band 的窄参数拟合。S410 在 `18%` cap 下也有 `Sharpe 13.3066`，说明 seat-quality 共识本身有效，不是单纯把仓位放到 `20%`。

需要保留的风险提示：本窗口较短，S413 仍有 2 月底到 3 月底阶段 3 只、60% 目标暴露的阶段；这符合当前唯一硬约束，但不是充分分散的长期组合。S413 的收益也伴随更高换手和成本，`annual_turnover 19.1819` 高于 S402 的 `15.8756`。低权重 reserve 补位和只扩候选池已经验证为负优化，因此后续若要提高分散度，应继续从 candidate-quality / seat-quality 模型层验证，而不是强行满仓或继续堆阈值。

## 系统问题

本轮没有新增必须写入 backlog 的系统不可用问题。既有问题仍以 `/Users/m/dev/qagent/docs/backlog.md` 为准。

## 产物

- `/Users/m/dev/atlas/tmp/research_0503_cap20_s340_monthend_anchor.py`
- `/Users/m/dev/atlas/tmp/research_0503_cap20_s340_monthend_anchor.json`
- `/Users/m/dev/atlas/tmp/research_0503_cap20_s390_dual_attack.py`
- `/Users/m/dev/atlas/tmp/research_0503_cap20_s390_dual_attack.json`
- `/Users/m/dev/atlas/tmp/research_0503_cap20_s402_leader_carry.py`
- `/Users/m/dev/atlas/tmp/research_0503_cap20_s402_leader_carry.json`
- `/Users/m/dev/atlas/tmp/research_0503_cap20_s402_cap_and_aux.py`
- `/Users/m/dev/atlas/tmp/research_0503_cap20_s402_cap_and_aux.json`
- `/Users/m/dev/atlas/tmp/backtest_9707a4989190_detail.json`
- `/Users/m/dev/atlas/tmp/backtest_c1165a224cd6_detail.json`
- `/Users/m/dev/atlas/tmp/backtest_93340f07579c_detail.json`
- `/Users/m/dev/atlas/tmp/backtest_e2b36904b15e_detail.json`
- `/Users/m/dev/atlas/tmp/research_0503_cap20_s390_model_slots.py`
- `/Users/m/dev/atlas/tmp/research_0503_cap20_s390_model_slots.json`
- `/Users/m/dev/atlas/tmp/research_0503_cap20_s390_robust_reserve.py`
- `/Users/m/dev/atlas/tmp/research_0503_cap20_s390_robust_reserve.json`
- `/Users/m/dev/atlas/tmp/backtest_f3415766baa6_detail.json`
- `/Users/m/dev/atlas/tmp/backtest_f3415766baa6_detail_refetch.json`
- `/Users/m/dev/atlas/tmp/research_0503_cap20_s340_structural_guard.py`
- `/Users/m/dev/atlas/tmp/research_0503_cap20_s340_structural_guard.json`
- `/Users/m/dev/atlas/tmp/research_0503_old_hyst_cap20_recheck.py`
- `/Users/m/dev/atlas/tmp/research_0503_old_hyst_cap20_recheck.json`
- `/Users/m/dev/atlas/tmp/research_0503_cap20_s340_incumbent_compete.py`
- `/Users/m/dev/atlas/tmp/research_0503_cap20_s340_incumbent_compete.json`
- `/Users/m/dev/atlas/tmp/research_0503_cap20_s340_pathretain_reserve.py`
- `/Users/m/dev/atlas/tmp/research_0503_cap20_s340_pathretain_reserve.json`

## 下一步

1. 不建议继续在 `2026-01-05~2026-04-01` 上做窄阈值优化；当前 S413 已经达到 `Sharpe 13.5912`，继续微调容易过拟合。
2. 下一步优先验证 S413 的相邻窗口、前向 paper 和不同股票池稳定性，尤其检查 3 月底新入 `MRNA / TER / FIX / XYZ / FANG` 的窗口末端风险。
3. 因子模型层下一条主线应是更稳健的“月底 cohort / seat-quality”候选质量模型，而不是继续把 reserve 策略规则写得更复杂。

# Russell 3000 Five-Round Research Report

## Scope

- Date: 2026-04-05
- Universe: built-in `russell3000` group (`7806` members in current system)
- Feature set: `R3000_QLIB158_CORE12` (`30d14391a5b6`)
- Backtest window: `2025-01-02` to `2025-09-30`
- Rebalance: `monthly`
- Costs: `0.1%` commission + `0.1%` slippage

## Round Summary

| Round | Model | Label | Strategy focus | Valid IC | Test IC | Total Return | Max DD | Annual Turnover | Notes |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | `b01be4a83b0a` | `fwd_rank_5d` | Top 20, equal weight | 0.0999 | 0.0489 | 3.02% | -17.18% | 19.78 | Best realized PnL |
| 2 | `8bb61862678c` | `fwd_rank_20d` | Top 15 + price floor | 0.1531 | 0.0583 | -4.53% | -16.82% | 19.24 | Better IC, worse portfolio |
| 3 | `3ffb5b83d77a` | `fwd_return_5d_vol_adj` | Top 25 + price floor | -0.0175 | -0.0097 | -59.62% | -59.94% | 18.59 | Signal collapsed; anomaly exposure exploded |
| 4 | `415a7b34906c` | `fwd_excess_10d` | Price band + vol filter | -0.0621 | 0.0102 | -8.70% | -32.82% | 19.51 | Hard filters helped, alpha still weak |
| 5 | `f1c4c9862547` | `fwd_rank_10d` | Price band + vol filter | 0.1179 | 0.0572 | -0.37% | -20.11% | 19.78 | Best filtered result, still below Round 1 |

## Round-by-Round Findings

### Round 1

- Profit side: model alpha translated into a small positive return; top winners were `BW` `+30.2k`, `UHG` `+29.9k`, `HPP` `+23.1k`.
- Loss side: weak loss control in lower-quality names; biggest losers were `DFLI` `-15.5k`, `MOVE` `-13.9k`, `STKH` `-10.2k`.
- Problem: alpha existed, but high turnover (`19.78`) and idiosyncratic losers absorbed most of it.
- Next step: test longer horizon label and add a price floor.

### Round 2

- Profit side: model IC improved materially, but realized gains concentrated in a few names such as `HSBC`, `ALX`, and `FORM`.
- Loss side: portfolio concentration hurt; `VSCO`, `UNH`, and `CHDN` drove outsized losses. Profit/loss ratio fell to `0.54`.
- Problem: stronger ranking signal did not convert to tradable long-only PnL.
- Next step: try a risk-adjusted target and broaden diversification.

### Round 3

- Profit side: none worth keeping.
- Loss side: severe exposure to anomalous instruments and extreme prices; trade log included names priced in the thousands, leading to a `-59.6%` collapse.
- Problem: `vol_adj` target removed useful alpha while the strategy still admitted corrupted candidates.
- Next step: add explicit price ceiling and volatility screen.

### Round 4

- Profit side: filters reduced the blow-up from Round 3 and produced positive months in May to July.
- Loss side: model quality remained unstable; early-period losses were too deep to recover.
- Problem: security filters alone cannot rescue a weak model.
- Next step: revert to a positive rank-style model and keep the filters.

### Round 5

- Profit side: filtered `10d` rank model was the most balanced filtered setup. It recovered late in the sample and finished near flat.
- Loss side: costs stayed high (`27.9k`) and profit/loss ratio remained weak (`0.658`), so the strategy could not beat Round 1.
- Problem: the system still mixes in many non-ideal tradables under the current `russell3000` approximation.
- Next step: do not optimize further until the universe is tightened.

## Conclusion

- Best round by realized return: Round 1 (`3.02%` total return, `-17.18%` max drawdown).
- Best round with hard filters: Round 5 (`-0.37%` total return), which was much safer than Round 3 and cleaner than Round 4, but still not investable.
- Main blocker is not only model tuning. The current built-in `russell3000` group behaves like an approximate all-active US universe and admits abnormal or non-common-stock exposures, which distorts long-only research.

## Recommendation

Before running a new research loop, add one of the following system-level constraints:

1. True Russell 3000 constituent membership instead of the current approximation.
2. Security-type filtering to exclude ETFs, closed-end funds, preferreds, notes, and split-distorted tickers.
3. Liquidity and price sanity filters at universe level, not only inside strategy code.

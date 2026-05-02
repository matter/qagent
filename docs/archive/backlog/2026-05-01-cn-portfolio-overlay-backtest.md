# [2026-05-01] P3 研究功能：正式 CN 策略回测需要支持组合底仓 + 增强卫星的组合层回测

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-02

## 原始问题

- **来源**：agent A 股核心指数并集研究
- **影响范围**：CN 策略研究、组合层回测、`/api/strategies/backtests`、后续 UI 展示
- **复现入口**：
  - UI：策略回测页暂无组合底仓 + 策略增强的配置入口
  - API / MCP：当前正式 backtest 以单个 strategy 为核心，缺少把 `cn_a_core_indices_union` 等权底仓与一个增强策略按固定比例合成的持久化资产
  - 资产 ID：临时研究脚本 `/Users/m/dev/atlas/tmp/cn_core_regime_factor_experiment.py`

## 修复记录

- **commit**：原 backlog 记录为“本次提交”
- **验证命令**：`uv run python -m unittest tests.test_backtest_diagnostics_contracts`；live API CN `portfolio_overlay` 回测 smoke
- **复验结论**：通过。`BacktestService.run_backtest` 支持 `config.portfolio_overlay`，可将 `core_union_equal_weight` / `equal_weight` base leg 与策略 overlay 按权重合成，详情保留 `config.portfolio_overlay`、`summary.trade_diagnostics.portfolio_legs` 和总 NAV 指标。live smoke 返回 `CONFIG_OVERLAY {"base_leg":"core_union_equal_weight","base_weight":0.65,"overlay_weight":0.35}`，临时回测和策略已删除。

## 本轮归档复验

- **复验时间**：2026-05-02
- **复验命令**：`uv run python -m unittest discover tests`；`cd frontend && pnpm build`；`git diff --check`
- **复验结论**：后端全量 unittest `118` 个通过；前端构建通过，仅保留既有动态导入和 chunk size warning；diff whitespace 检查通过。

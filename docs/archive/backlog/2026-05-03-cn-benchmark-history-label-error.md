# [2026-05-03] P1 缺陷：CN benchmark 缺失导致 excess-return 标签无法训练

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-03

## 原始问题

CN excess-return 标签依赖 `sh.000300`，但本地 `index_bars` 缺少沪深300历史，模型训练最终报 `No aligned pairs`，错误不可读且定位成本高。

## 修复记录

- **commit**：`4e383b5 fix: improve backtest task reliability`
- **验证命令**：`uv run python -m unittest tests.test_label_market_scope tests.test_data_group_market_scope`；`uv run python -m unittest discover tests`；`cd frontend && pnpm build`；`git diff --check`
- **复验结论**：通过。CN/US 数据更新时 benchmark/index 增量起点不再固定近 7 天；若 `index_bars` 没有 `sh.000300` 历史，会从 10 年窗口开始补齐。excess-return/excess-binary 标签在 benchmark 缺失时直接抛出可读 `Benchmark data missing for sh.000300 in market CN`。

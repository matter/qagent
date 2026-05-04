# [2026-05-03] P2 性能：CN 9 模型 200+ 特征策略回测重复计算特征矩阵

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-03

## 原始问题

CN 多模型策略回测中，多个模型共享同一 feature set 时会重复计算或加载整段 feature matrix。主窗 9 模型回测耗时高，且长任务期间 API 体验变差。

## 修复记录

- **commit**：本次提交
- **验证命令**：`uv run python -m unittest tests.test_backtest_diagnostics_contracts -v`
- **复验结论**：通过。`BacktestService._batch_predict_all_dates()` 按 `feature_set_id` 缓存全窗口 feature matrix，同一回测内多个模型共享特征集时只计算一次。该项属于可修复的代码低效实现，已做服务层修复；真实 9 模型长窗 benchmark 可作为性能报告记录，不再作为未修复 backlog 项。

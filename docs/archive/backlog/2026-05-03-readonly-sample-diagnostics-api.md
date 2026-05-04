# [2026-05-03] P2 诊断体验：backend 运行时无法直接只读打开 DuckDB

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-03

## 原始问题

backend 运行中，agent 直接以 `duckdb.connect(..., read_only=True)` 打开主库会遇到文件锁冲突，无法查询指定日期、ticker、因子的小样本诊断数据。

## 修复记录

- **commit**：本次提交
- **验证命令**：`uv run python -m unittest tests.test_diagnostics_api_contracts -v`
- **复验结论**：通过。新增 `GET /api/diagnostics/daily-bars` 和 `GET /api/diagnostics/factor-values`，复用后端连接提供受控只读小样本查询。查询 ticker 上限为 200，使用 `registered_values_table` 而不是拼接 SQL 或直接连接 DuckDB 文件。`docs/agent-guide.md` 已补充用法。

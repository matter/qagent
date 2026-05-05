# [2026-05-02] P3 诊断体验：运行中 DuckDB 主库被锁，研究侧缺少官方只读诊断入口

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-02

## 原始问题

- **来源**：agent A 股回测诊断发现
- **影响范围**：本地研究诊断、指数行情核对、回测异常定位、agent 自动化分析
- **复现入口**：qagent 后端运行中直接执行 `duckdb.connect('/Users/m/dev/qagent/data/qagent.duckdb', read_only=True)`，触发 DuckDB 文件锁冲突。

## 修复记录

- **commit**：本次提交
- **验证命令**：`uv run python -m unittest tests.test_data_group_market_scope.DataAndGroupMarketScopeTests.test_index_bars_api_reads_market_scoped_index_history tests.test_data_group_market_scope.DataAndGroupMarketScopeTests.test_group_daily_snapshot_reports_missing_bars_without_duckdb_access`；`cd frontend && pnpm build`
- **复验结论**：通过。新增官方只读 API：`GET /api/data/index-bars/{symbol}` 读取 market-scoped 指数行情；`GET /api/data/groups/{group_id}/daily-snapshot?date=YYYY-MM-DD` 读取股票池当日行情覆盖、缺失数量和缺失 ticker。数据管理页新增“只读诊断”面板，可查看指数行情和股票池横截面覆盖，避免 agent/human 直接连运行中的 DuckDB 主库。

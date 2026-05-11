# [2026-05-08] P2 Concurrent feature matrix cache writes

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-08

## 原始问题

- **范围**：US legacy strategy backtests sharing model prediction / feature matrix cache paths
- **入口**：并发 `POST /api/strategies/{strategy_id}/backtest`
- **现象**：并发回测共享模型预测路径时，feature matrix cache 元数据写入可能触发 DuckDB transient transaction conflict。
- **典型日志**：`TransactionContext Error: Conflict on tuple deletion!`

## 修复记录

- `ResearchCacheService.store_feature_matrix()` 对同一个 `cache_key` 的 parquet 文件写入和 metadata upsert 使用进程内锁串行化。
- metadata 写入对 DuckDB transient conflict 做 3 次指数退避重试。
- transient marker 覆盖：
  - `TransactionContext Error`
  - `Conflict on tuple deletion`
  - `PRIMARY KEY or UNIQUE constraint violation`
- 新增并发同 key 写入测试和一次性 transient conflict 重试测试。

## 验证

- `uv run python -m unittest tests.test_research_cache_service.ResearchCacheServiceTests.test_feature_matrix_store_retries_transient_duckdb_conflict tests.test_research_cache_service.ResearchCacheServiceTests.test_feature_matrix_store_serializes_same_cache_key_writes tests.test_research_cache_service.ResearchCacheServiceTests.test_feature_matrix_cache_round_trips_frames_and_updates_stats -v`
- `uv run python -m unittest tests.test_agent_research_3_service tests.test_research_cache_service tests.test_backtest_engine_contracts tests.test_backtest_diagnostics_contracts -v`

## 残余风险

- 这是同进程同 cache key 的热缓存写安全修复；DuckDB 主库跨进程单写者约束仍由 P0 backlog 项跟踪。

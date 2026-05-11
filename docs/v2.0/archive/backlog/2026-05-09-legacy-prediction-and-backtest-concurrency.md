# [2026-05-09] P1/P2 Legacy prediction and backtest concurrency guardrails

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-09

## 原始问题

- **P1**：并发 `POST /api/models/{model_id}/predict` 请求可能让后端无响应。
- **P2**：并发 legacy backtest 在 `_batch_predict_all_dates` 共享模型/特征集时，可能因为 feature matrix hot cache 元数据写入触发 DuckDB `TransactionContext Error: Conflict on tuple deletion!`。

## 修复记录

- `/api/models/{model_id}/predict` 和 `/api/models/{model_id}/predict-batch` 增加进程内并发闸门。超过并发槽位时返回 `429`，避免多个重预测请求挤满 API worker。
- prediction API 的阻塞预测计算改为 `run_in_threadpool()` 执行，避免同步模型推理和特征计算直接占住 FastAPI event loop。
- legacy `_batch_predict_all_dates` 在构建共享 feature matrix cache 前增加按 `market + feature_set_id + tickers + date_range` 的进程内锁，序列化同键缓存构建/写入，减少并发 backtest 之间的 DuckDB 元数据写冲突。

## 验证

- `uv run python -m unittest tests.test_model_market_scope.ModelMarketScopeTests.test_model_predict_api_rejects_when_prediction_slot_is_full -v`
- `uv run python -m unittest tests.test_model_market_scope.ModelMarketScopeTests.test_model_predict_api_offloads_blocking_prediction_to_threadpool -v`
- `uv run python -m unittest tests.test_model_market_scope.ModelMarketScopeTests.test_model_predict_batch_api_offloads_and_uses_same_concurrency_gate -v`
- `uv run python -m unittest tests.test_backtest_diagnostics_contracts.BacktestDiagnosticsContractTests.test_batch_predict_serializes_shared_feature_matrix_cache_builds -v`

## 残余风险

- 这是单机进程内并发保护；多进程部署仍需要外部队列或数据库级单写者架构。
- DuckDB 单文件写入脆弱性仍作为独立 P0/P2 运维架构问题保留。

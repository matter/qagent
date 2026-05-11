# [2026-05-09] P2 Research cache versioning latest fallback

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-09

## 原始问题

- **范围**：US, CN research feature matrix cache.
- **入口**：`backend/services/research_cache_service.py`
- **现象**：调用方缺少稳定版本时，`default_data_version()` 可退回 `<market>:latest`，导致数据刷新后缓存复用语义不清晰。

## 修复记录

- `default_data_version()` 不再接受缺失 `as_of_date` 的调用。
- 默认版本键改为 `<market>:asof:<as_of_date>`。
- 缺少 as-of 时直接抛错，要求调用方显式传入稳定快照/日期。

## 验证

- `uv run python -m unittest tests.test_research_cache_service.ResearchCacheServiceTests.test_feature_matrix_cache_rejects_unversioned_latest_key -v`

## 残余风险

- 当前修复消除了默认 `latest` 键；更强的数据快照哈希仍可作为后续增强。

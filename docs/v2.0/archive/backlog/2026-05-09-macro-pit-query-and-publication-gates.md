# [2026-05-09] P1 Macro PIT query and data publication gates

- **归档状态**：Done / Boundary Guarded
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-09

## 原始问题

- **范围**：Global macro data, US/CN data quality contracts.
- **入口**：`MacroDataService`, `DataQualityService`, `/api/macro-data/observations`
- **现象**：宏观数据存了 realtime 字段，但查询端没有严格 as-of latest-visible 语义；免费数据源质量在发布级研究中容易被误用。

## 修复记录

- `MacroDataService.query_series_as_of()` 返回每个 observation date 在 `decision_time` 前最新可见版本。
- `/api/macro-data/observations` 支持 `strict_pit=true`，并强制要求 `as_of`。
- FRED 更新支持显式 `realtime_start` / `realtime_end`，agent 可以补本地历史 realtime 窗口。
- `DataQualityService.get_data_quality_contract()` 输出 `publication_gates`。
- 对 US/CN equity free providers，发布门槛显式阻断：
  - `pit_data`
  - `survivorship_safe_universe`
  - `corporate_actions`
- Workbench 展示 publication gates、PIT capability 和质量摘要。

## 验证

- `uv run python -m unittest tests.test_macro_data_service.MacroDataServiceTests tests.test_macro_data_api.MacroDataApiTests tests.test_data_quality_service.DataQualityServiceContractTests -v`
- `cd frontend && pnpm build`

## 残余风险

- 如果本地只下载当前 FRED realtime window，则严格查询只能在已有窗口内 PIT。完整 publication-grade 宏观回放仍要求 agent 下载足够历史 realtime window。
- 免费股票数据源本身仍不具备退市/历史成分/完整公司行为事实，因此相关 equity 发布门槛保持 blocked，并继续在 backlog 中保留数据源级问题。

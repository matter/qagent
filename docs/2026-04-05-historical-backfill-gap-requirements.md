# 历史数据回补缺口需求文档

## 背景

2026-04-05 已通过现有网站数据更新流程执行一次全量历史回补：

- 模式：`full`
- 时间范围：近 10 年，实际落库起点为 `2016-01-04`
- 股票列表：`11936`
- 成功写入 bars 的股票：`11933`
- 总日线条数：`17542190`
- 全量任务耗时：`3450.5s`
- 失败股票：`MDLZ`、`MIND`、`KYLD`

随后执行一次 `incremental`，系统直接返回“Data is already up to date”，未对失败股票做任何重试。

## 现状问题

### 1. 增量更新无法修复局部缺口

当前 `backend/services/data_service.py` 在 `incremental` 模式下先用全库 `daily_bars` 的 `MAX(date)` 判断是否“全局已最新”。只要任意大量股票已经到最新交易日，任务就会直接退出，导致以下情况无法被修复：

- 某些股票完全没有 bars
- 某些股票只回补到较早日期
- 某些活跃股票存在局部断档
- 指数数据（如 `SPY`）拉取失败后不会被后续增量自动补齐

### 2. 数据状态的 `stale_tickers` 语义不清

全量任务完成后 `stale_tickers=23`，但系统没有区分：

- 活跃股票真实缺数
- 已停牌/退市/长期无交易股票
- 本次更新失败股票

这会让“历史数据是否已补齐”难以判断。

### 3. 数据质量检查接口当前不可用

`GET /api/data/quality` 返回 `500`。错误来自 `backend/services/data_service.py` 中 gap 检查 SQL：DuckDB 不允许在 `HAVING` 中直接使用窗口函数。

## 源码修改需求

### A. 修复增量更新判定逻辑

修改 `backend/services/data_service.py`：

- 移除 `incremental` 模式下仅凭全库 `MAX(date)` 直接返回的逻辑。
- 改为按股票逐只判断是否需要更新：
  - 无 bars 的股票必须进入待更新列表
  - `MAX(date) < latest_trading_day` 的股票必须进入待更新列表
  - 可选：对活跃股票增加“最近 N 个交易日缺口”修复逻辑
- 指数数据（至少 `SPY`）也应独立判断是否需要更新，不能被股票全局状态短路。

### B. 增加“修复模式”或失败重试能力

扩展 `UpdateRequest.mode` 和前后端交互，新增一种面向运维/用户的修复能力，至少满足以下之一：

- `repair` 模式：仅重试失败股票、无 bars 股票、落后股票、指数数据
- 或保留 `incremental` 语义，但让其天然具备上述修复能力

前端数据管理页应明确提示本次修复目标和结果。

### C. 改进更新结果与状态展示

修改 `backend/api/data.py`、`backend/services/data_service.py`、必要时修改前端页面：

- 在更新结果中明确返回：
  - `failed_tickers`
  - `missing_bars_tickers_count`
  - `stale_active_tickers_count`
  - `index_update_status`
- 将 `stale_tickers` 拆分为更有解释性的字段，避免把 inactive/长期停牌与真实失败混为一谈。

### D. 修复质量检查 SQL

修改 `backend/services/data_service.py` 中 gap 检查逻辑：

- 先在子查询/CTE 中计算 `LEAD(date)` 与 `gap_days`
- 再在外层 `WHERE gap_days > 7` 过滤
- 确保 `GET /api/data/quality` 返回 `200`，且可在大数据量下正常执行

## 验收标准

1. 全量回补后再执行一次修复/增量，失败股票会被重试，而不是因为全局 `MAX(date)` 提前退出。
2. 对当前数据集复测后，`MDLZ`、`MIND`、`KYLD` 至少会进入重试队列，并在结果中明确成功或失败原因。
3. `SPY` 指数更新失败时，任务结果能明确展示，后续修复可重试。
4. `GET /api/data/quality` 返回 `200`，不再触发 DuckDB Binder Error。
5. 数据状态页能区分“活跃股票缺数据”和“非活跃股票无近期数据”。

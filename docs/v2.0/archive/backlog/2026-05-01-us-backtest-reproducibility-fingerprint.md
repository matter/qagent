# [2026-05-01] P2 可复现性：US 历史最优 S271 回测无法用同策略同配置复现

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-02

## 原始问题

- **来源**：agent live API 复验
- **影响范围**：US 策略研究链路、`POST /api/strategies/{strategy_id}/backtest`、回测资产可复现性、历史 backtest 对比
- **复现入口**：
  - UI：策略回测页选择 US 策略 `M0428_S271_S262_ENTRYSWAP_DDHEAVY_PRETRADE4_R1`
  - API / MCP：`POST /api/strategies/357056b76e3c/backtest` with `market=US`、`universe_group_id=sp500`
  - 资产 ID：历史 backtest `12d7b159c3ad`，当前复跑 backtest `99824267db46`

## 修复记录

- **commit**：原 backlog 记录为“本次提交”
- **验证命令**：`uv run python -m unittest tests.test_backtest_diagnostics_contracts`；live API CN 回测 smoke 检查 `summary.reproducibility_fingerprint.hash`
- **复验结论**：通过。新回测会持久化复现指纹，包含 service/schema 版本、git commit、market、strategy source hash、required factors/models、factor source hash、model metadata hash、config、data watermark、result shape 和稳定 hash；列表接口只暴露轻量 `reproducibility_hash` / `has_reproducibility_fingerprint`。旧历史回测无法补回当时缺失的快照，后续结果已具备可比性诊断基础。

## 本轮归档复验

- **复验时间**：2026-05-02
- **复验命令**：`uv run python -m unittest discover tests`；`cd frontend && pnpm build`；`git diff --check`
- **复验结论**：后端全量 unittest `118` 个通过；前端构建通过，仅保留既有动态导入和 chunk size warning；diff whitespace 检查通过。

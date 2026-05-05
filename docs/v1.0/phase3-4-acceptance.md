# Phase 3 & 4 验收手册

## 前置条件

- Phase 1 & 2 已通过验收
- 系统已启动：`bash scripts/start.sh`
- 已有股票数据（至少一个分组有数据）

---

## Phase 3 验收

### M3.1 特征集 + 回测 PoC

#### 1. 特征集 CRUD

```bash
# 先获取两个因子 ID
FACTORS=$(curl -s http://127.0.0.1:8000/api/factors | python3 -c "
import sys,json; fs=json.load(sys.stdin)
for f in fs[:2]: print(f['id'], f['name'], f['version'])
")
echo "$FACTORS"

# 创建特征集
curl -s -X POST http://127.0.0.1:8000/api/feature-sets \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test_features",
    "description": "测试特征集",
    "factor_refs": [{"factor_id": "<FACTOR_ID_1>", "factor_name": "Momentum_20", "version": 1}],
    "preprocessing": {"missing": "forward_fill", "outlier": "mad", "normalize": "rank", "neutralize": null}
  }' | python3 -m json.tool
```

- [ ] 创建成功

```bash
curl -s http://127.0.0.1:8000/api/feature-sets | python3 -m json.tool
```

- [ ] 列表返回特征集

#### 2. 回测引擎验证

```bash
uv run python -c "
from backend.services.backtest_engine import BacktestEngine, BacktestConfig
import pandas as pd, numpy as np
from datetime import date

# 构造测试信号：固定等权持有3只股
dates = pd.bdate_range('2025-01-01', '2025-03-31')
signals = pd.DataFrame(
    {t: [1/3]*len(dates) for t in ['AAPL','MSFT','GOOG']},
    index=dates
)
config = BacktestConfig(start_date=date(2025,1,1), end_date=date(2025,3,31))
engine = BacktestEngine()
# Note: needs real data in DB to run fully
print('BacktestEngine instantiated OK')
print(f'Config: capital={config.initial_capital}, commission={config.commission_rate}')
"
```

- [ ] BacktestEngine 可实例化，配置正确

### M3.2 模型训练

```bash
# 列出模型
curl -s http://127.0.0.1:8000/api/models | python3 -m json.tool
```

- [ ] 返回模型列表（可能为空）

#### UI 模型训练流程

打开 http://localhost:5173，点击 "模型训练"：

- [ ] "训练配置" Tab 显示：特征集选择、标签选择、分组选择、日期配置、模型类型
- [ ] "模型列表" Tab 显示已训练模型表格

### M3.3 策略 + 回测

```bash
# 列出策略模板
curl -s http://127.0.0.1:8000/api/strategies/templates | python3 -m json.tool | head -20
```

- [ ] 返回 3 个策略模板

#### UI 策略回测流程

点击 "策略回测"：

- [ ] "策略编辑器" Tab：Monaco 编辑器 + 模板选择 + 保存
- [ ] "回测" Tab：策略选择 + 回测配置 + "运行回测" 按钮
- [ ] "策略列表" Tab：所有策略表格
- [ ] "回测历史" Tab：回测记录表格

### M3.4 页面完整性

- [ ] 特征工程页面：因子选择 + 预处理配置 + 特征集列表
- [ ] 模型训练页面：训练配置 + 模型列表
- [ ] 策略回测页面：4 个 Tab 全部有内容

---

## Phase 4 验收

### M4.1 MCP Server

```bash
# 验证 MCP 端点可访问
curl -s http://127.0.0.1:8000/mcp/ | head -5
```

- [ ] 返回响应（非 404）

```bash
# 列出 MCP 工具（通过 MCP SDK）
uv run python -c "
from backend.mcp_server import mcp
tools = list(mcp._tool_manager._tools.keys())
print(f'MCP tools ({len(tools)}):')
for t in sorted(tools): print(f'  - {t}')
"
```

- [ ] 显示 16 个 MCP 工具

#### Claude Desktop / Claude Code 集成测试（可选）

在 Claude Desktop MCP 配置中添加：
```json
{
  "mcpServers": {
    "qagent": {
      "url": "http://127.0.0.1:8000/mcp/"
    }
  }
}
```

- [ ] Agent 可调用 `search_stocks`
- [ ] Agent 可调用 `get_data_status`
- [ ] Agent 可调用 `list_factors`

### M4.2 信号生成

```bash
# 创建一个策略（如果还没有）
curl -s http://127.0.0.1:8000/api/strategies/templates/动量因子策略 | python3 -c "
import sys,json; t=json.load(sys.stdin); print(t['source_code'][:100])
"
```

```bash
# 生成信号（需要先有策略和数据）
STRATEGY_ID=$(curl -s http://127.0.0.1:8000/api/strategies | python3 -c "import sys,json; ss=json.load(sys.stdin); print(ss[0]['id'] if ss else 'none')")
GROUP_ID=$(curl -s http://127.0.0.1:8000/api/groups | python3 -c "import sys,json; gs=json.load(sys.stdin); print(next((g['id'] for g in gs if g['member_count']>0), 'none'))")

curl -s -X POST http://127.0.0.1:8000/api/signals/generate \
  -H "Content-Type: application/json" \
  -d "{\"strategy_id\": \"$STRATEGY_ID\", \"target_date\": \"2026-04-03\", \"universe_group_id\": \"$GROUP_ID\"}" \
  | python3 -m json.tool
```

- [ ] 返回 task_id

```bash
# 查看信号历史
curl -s http://127.0.0.1:8000/api/signals | python3 -m json.tool | head -20
```

- [ ] 信号生成后出现在历史中
- [ ] 包含 result_level 字段

```bash
# 导出 CSV
RUN_ID=$(curl -s http://127.0.0.1:8000/api/signals | python3 -c "import sys,json; rs=json.load(sys.stdin); print(rs[0]['id'] if rs else 'none')")
curl -s "http://127.0.0.1:8000/api/signals/$RUN_ID/export?format=csv" | head -5
```

- [ ] 返回 CSV 格式数据

### M4.3 信号生成页面

点击 "信号生成"：

- [ ] "信号生成" Tab：策略选择 + 分组选择 + 日期选择 + "生成信号" 按钮
- [ ] 生成后显示：结果级别标签（探索性/正式）、信号表格、导出按钮
- [ ] 信号表格：Ticker, Signal（彩色标签）, Weight（百分比）, Strength
- [ ] "导出CSV" / "导出JSON" 可下载文件
- [ ] "信号历史" Tab：历史记录列表，点击可查看详情

### 系统设置页面

点击 "系统设置"：

- [ ] 显示系统版本、Python 版本、数据库路径
- [ ] 显示数据源信息
- [ ] 显示当前配置
- [ ] API 文档链接可点击

### 全链路验收

端到端流程（UI）：

1. [ ] 数据管理：数据已更新，状态正常
2. [ ] 因子研究：选择模板 → 评价 → 保存因子
3. [ ] 特征工程：选择因子 → 配置预处理 → 保存特征集
4. [ ] 模型训练：选择特征集和标签 → 训练 → 查看结果
5. [ ] 策略回测：选择模板 → 保存策略 → 运行回测 → 查看图表
6. [ ] 信号生成：选择策略 → 生成信号 → 查看结果 → 导出

端到端流程（Agent/MCP，可选）：

7. [ ] Agent 调用 list_factors
8. [ ] Agent 调用 evaluate_factor
9. [ ] Agent 调用 generate_signals
10. [ ] Agent 调用 get_task_status 查看结果

---

## 验收总结

| Phase | 里程碑 | 验收项 | 状态 |
|-------|--------|--------|------|
| P3 | M3.1 | 特征集 CRUD | ☐ |
| P3 | M3.1 | 回测引擎实例化 | ☐ |
| P3 | M3.2 | 模型训练 API + UI | ☐ |
| P3 | M3.3 | 策略模板 + 回测 API | ☐ |
| P3 | M3.4 | 3 个前端页面完整 | ☐ |
| P4 | M4.1 | MCP 16 个工具注册 | ☐ |
| P4 | M4.1 | MCP 端点可访问 | ☐ |
| P4 | M4.2 | 信号生成 + 依赖校验 | ☐ |
| P4 | M4.2 | 信号导出 CSV/JSON | ☐ |
| P4 | M4.3 | 信号生成页面 | ☐ |
| P4 | M4.3 | 系统设置页面 | ☐ |
| P4 | M4.3 | 全链路 UI 闭环 | ☐ |
| P4 | M4.3 | 全链路 Agent 闭环 | ☐ |

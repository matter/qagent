# Phase 1 验收手册

## 环境准备

```bash
cd /Users/m/dev/qagent

# 安装 Python 依赖（首次）
uv sync

# 安装前端依赖（首次）
cd frontend && pnpm install && cd ..
```

## 启动系统

```bash
# 启动（前台，Ctrl+C 停止）
bash scripts/start.sh

# 或者分别启动：
# 后端: uv run uvicorn backend.app:app --host 127.0.0.1 --port 8000 --reload
# 前端: cd frontend && pnpm dev
```

启动后：
- 后端 API: http://127.0.0.1:8000
- 前端 UI: http://localhost:5173

## 停止系统

```bash
# 如果用 start.sh 启动的，直接 Ctrl+C
# 或者：
bash scripts/stop.sh
```

---

## M1.1 验收项

### 1. 后端健康检查

```bash
curl http://127.0.0.1:8000/api/health
```

预期返回：`{"status":"ok"}`

### 2. 前端页面

浏览器打开 http://localhost:5173

- [ ] 左侧导航栏显示 8 个菜单项（行情浏览、数据管理、因子研究、特征工程、模型训练、策略回测、信号生成、系统设置）
- [ ] 点击各菜单项可切换页面
- [ ] 页面布局正常（侧边栏 + 主内容区）

### 3. 任务框架

> 注意：DuckDB 是单进程数据库。验收时后端必须处于运行状态，通过 API 验证；不要用独立 Python 脚本直接连接数据库文件，否则会因锁冲突报错。

```bash
# 通过 API 验证任务框架（后端需运行中）
# 触发一次数据更新（会创建任务记录）
curl -X POST http://127.0.0.1:8000/api/data/update \
  -H "Content-Type: application/json" -d '{"mode": "incremental"}'

# 查看任务进度（包含 task_id、status 等字段，证明 task_runs 表工作正常）
curl -s http://127.0.0.1:8000/api/data/update/progress | python3 -m json.tool
```

预期返回包含 `task_id`、`status`（running/completed/failed）等字段。

如果需要直接检查数据库表结构，**先停止后端**再执行：

```bash
bash scripts/stop.sh
uv run python -c "
from backend.db import get_connection, init_db
init_db()
conn = get_connection()
print(conn.execute('DESCRIBE task_runs').fetchall())
conn.close()
"
```

预期输出包含 id, task_type, status, params 等字段。

### 4. 日志

启动后端后检查：
- [ ] `logs/` 目录下有日志文件
- [ ] 日志格式为 JSON

---

## M1.2 验收项

### 1. 数据状态查询

```bash
curl -s http://127.0.0.1:8000/api/data/status | python3 -m json.tool
```

预期返回 JSON，包含 stock_count, total_bars, date_range, latest_trading_day 等字段。

### 2. 触发数据更新（增量）

```bash
curl -X POST http://127.0.0.1:8000/api/data/update \
  -H "Content-Type: application/json" \
  -d '{"mode": "incremental"}'
```

预期返回任务 ID。然后轮询进度：

```bash
curl -s http://127.0.0.1:8000/api/data/update/progress | python3 -m json.tool
```

> 注意：首次增量更新等同于全量，会拉取股票列表和历史数据，耗时较长。
> 建议首次测试时在 UI 的数据管理页操作，可看到进度。

### 3. 验证数据写入

更新完成后：

```bash
curl -s "http://127.0.0.1:8000/api/stocks/search?q=AAPL" | python3 -m json.tool
curl -s "http://127.0.0.1:8000/api/stocks/AAPL/daily?start=2025-01-01&end=2025-12-31" | head -5
```

- [ ] 股票搜索返回 AAPL 信息
- [ ] 日线数据返回 OHLCV 数组

### 4. 中断续传

如果更新过程中 kill 后端，重启后再次触发更新：
- [ ] 应从断点继续而非从头开始（查看日志中的 batch 编号）

### 5. 数据质量检查

```bash
curl -s http://127.0.0.1:8000/api/data/quality | python3 -m json.tool
```

预期返回质量报告（price_jumps, zero_volume, date_gaps 统计）。

---

## M1.3 验收项

### 1. 分组列表

```bash
curl -s http://127.0.0.1:8000/api/groups | python3 -m json.tool
```

- [ ] 默认存在 "全市场" 内置分组

### 2. 创建手动分组

```bash
curl -X POST http://127.0.0.1:8000/api/groups \
  -H "Content-Type: application/json" \
  -d '{"name": "测试分组", "description": "手动测试", "group_type": "manual", "tickers": ["AAPL", "MSFT", "GOOG"]}'
```

- [ ] 返回成功，包含 group_id

### 3. 查看分组详情

```bash
curl -s "http://127.0.0.1:8000/api/groups/<返回的group_id>" | python3 -m json.tool
```

- [ ] 返回分组信息 + 成员列表（AAPL, MSFT, GOOG）

### 4. 删除分组

```bash
curl -X DELETE "http://127.0.0.1:8000/api/groups/<group_id>"
```

- [ ] 删除成功
- [ ] "全市场" 分组不可删除

---

## M1.4 验收项

### 1. 行情浏览页

浏览器打开 http://localhost:5173，点击 "行情浏览"：

- [ ] 搜索框输入 "AAPL" 有下拉候选
- [ ] 选择后展示 K 线图（需要先有数据，即 M1.2 数据更新完成后）
- [ ] K 线图包含：蜡烛图 + 均线（MA5/MA20/MA60）
- [ ] 副图显示成交量柱状图
- [ ] 副图显示 MACD
- [ ] 底部有 dataZoom 滑块可拖动
- [ ] 时间范围按钮（1M/3M/6M/1Y/3Y/ALL）可切换
- [ ] 鼠标悬浮有十字光标和数据提示

### 2. 数据管理页

点击 "数据管理"：

- [ ] 数据状态卡片显示：股票总数、数据条数、日期范围、最后更新时间
- [ ] "增量更新" 按钮点击后触发更新，显示进度
- [ ] 股票分组区域显示分组列表（含 "全市场"）
- [ ] 可创建新分组（弹窗表单）
- [ ] 可删除非内置分组

---

## 验收总结

| 里程碑 | 验收项 | 状态 |
|--------|--------|------|
| M1.1 骨架 | 后端启动+health | ☐ |
| M1.1 骨架 | 前端页面+导航 | ☐ |
| M1.1 骨架 | 任务框架+DuckDB | ☐ |
| M1.1 骨架 | 日志 | ☐ |
| M1.2 数据 | 数据状态查询 | ☐ |
| M1.2 数据 | 触发更新+进度 | ☐ |
| M1.2 数据 | 数据写入验证 | ☐ |
| M1.2 数据 | 中断续传 | ☐ |
| M1.2 数据 | 质量检查 | ☐ |
| M1.3 分组 | 默认全市场分组 | ☐ |
| M1.3 分组 | 创建/查看/删除 | ☐ |
| M1.4 UI | K 线图 | ☐ |
| M1.4 UI | 数据管理页 | ☐ |

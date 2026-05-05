# Phase 2 验收手册

## 前置条件

- Phase 1 已通过验收，数据已更新（至少有部分股票的日线数据）
- 系统已启动：`bash scripts/start.sh`
- 后端: http://127.0.0.1:8000
- 前端: http://localhost:5173

---

## M2.1 验收项

### 1. 技术指标库

```bash
uv run python -c "
from backend.indicators import ta
import pandas as pd
import numpy as np

close = pd.Series(np.random.randn(100).cumsum() + 100)
high = close + abs(np.random.randn(100))
low = close - abs(np.random.randn(100))
volume = pd.Series(np.random.randint(1000, 10000, 100), dtype='float64')

print('RSI:', ta.rsi(close, 14).dropna().iloc[-1])
dif, dea, hist = ta.macd(close)
print('MACD DIF:', round(dif.iloc[-1], 4))
print('SMA:', round(ta.sma(close, 20).iloc[-1], 4))
print('EMA:', round(ta.ema(close, 20).iloc[-1], 4))
print('ADX:', round(ta.adx(high, low, close, 14).dropna().iloc[-1], 4))
print('ATR:', round(ta.atr(high, low, close, 14).dropna().iloc[-1], 4))
upper, mid, lower = ta.bbands(close, 20)
print('BBands upper:', round(upper.iloc[-1], 4))
print('OBV:', round(ta.obv(close, volume).iloc[-1], 4))
print('MFI:', round(ta.mfi(high, low, close, volume, 14).dropna().iloc[-1], 4))
print('ZScore:', round(ta.zscore(close, 20).iloc[-1], 4))
print('LinReg:', round(ta.linreg_slope(close, 20).dropna().iloc[-1], 4))
print('ROC:', round(ta.roc(close, 10).dropna().iloc[-1], 4))
print('All 17 indicators OK')
"
```

- [ ] 全部 17 个指标返回数值，无报错（纯 numpy 回退，不依赖 TA-Lib）

### 2. 标签定义 API

```bash
# 列出预置标签
curl -s http://127.0.0.1:8000/api/labels | python3 -m json.tool
```

- [ ] 返回 4 个预置标签：fwd_return_5d, fwd_return_20d, fwd_rank_5d, fwd_excess_5d
- [ ] 每个标签包含 id, name, target_type, horizon, status 字段

```bash
# 创建自定义标签
curl -s -X POST http://127.0.0.1:8000/api/labels \
  -H "Content-Type: application/json" \
  -d '{"name": "test_return_10d", "target_type": "return", "horizon": 10}' \
  | python3 -m json.tool
```

- [ ] 创建成功，返回完整标签对象

### 3. 因子协议 + 加载器

```bash
uv run python -c "
from backend.factors.loader import load_factor_from_code
from backend.factors.builtins import TEMPLATES

# 加载一个内置模板
code = TEMPLATES['Momentum_20']
factor = load_factor_from_code(code)
print(f'Loaded: {factor.name}, category: {factor.category}')
print(f'Total templates: {len(TEMPLATES)}')

# 测试错误处理
try:
    load_factor_from_code('raise ValueError(\"bad\")')
except Exception as e:
    print(f'Bad code caught: {type(e).__name__}')
print('Factor loader OK')
"
```

- [ ] 内置模板加载成功，显示因子名称和分类
- [ ] 共 12 个模板
- [ ] 错误代码被正确捕获

### 4. 因子模板 API

```bash
# 列出模板
curl -s http://127.0.0.1:8000/api/factors/templates | python3 -m json.tool | head -20
```

- [ ] 返回 12 个因子模板，每个包含 name, category, description, source_code

```bash
# 获取单个模板
curl -s http://127.0.0.1:8000/api/factors/templates/RSI_14 | python3 -m json.tool
```

- [ ] 返回 RSI_14 模板完整源码

### 5. 因子 CRUD API

```bash
# 创建因子（使用模板源码）
SOURCE=$(curl -s http://127.0.0.1:8000/api/factors/templates/Momentum_20 | python3 -c "import sys,json; print(json.load(sys.stdin)['source_code'])")

curl -s -X POST http://127.0.0.1:8000/api/factors \
  -H "Content-Type: application/json" \
  -d "{\"name\": \"My_Momentum\", \"description\": \"test\", \"category\": \"momentum\", \"source_code\": $(echo "$SOURCE" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))')}" \
  | python3 -m json.tool
```

- [ ] 创建成功，version=1, status=draft

```bash
# 列出因子
curl -s http://127.0.0.1:8000/api/factors | python3 -m json.tool | head -20
```

- [ ] 返回因子列表，包含刚创建的因子

---

## M2.2 验收项

### 1. 因子计算

> 需要先有数据（Phase 1 数据更新完成）和至少一个股票分组。

```bash
# 获取一个因子 ID 和分组 ID
FACTOR_ID=$(curl -s http://127.0.0.1:8000/api/factors | python3 -c "import sys,json; fs=json.load(sys.stdin); print(fs[0]['id'] if fs else 'none')")
GROUP_ID=$(curl -s http://127.0.0.1:8000/api/groups | python3 -c "import sys,json; gs=json.load(sys.stdin); print(next((g['id'] for g in gs if g['member_count']>0), 'none'))")

echo "Factor: $FACTOR_ID, Group: $GROUP_ID"

# 触发计算
curl -s -X POST "http://127.0.0.1:8000/api/factors/$FACTOR_ID/compute" \
  -H "Content-Type: application/json" \
  -d "{\"universe_group_id\": \"$GROUP_ID\", \"start_date\": \"2025-01-01\", \"end_date\": \"2025-12-31\"}" \
  | python3 -m json.tool
```

- [ ] 返回 task_id
- [ ] 通过 `GET /api/tasks/{task_id}` 查询状态最终变为 completed

### 2. 因子评价

```bash
# 获取一个标签 ID
LABEL_ID=$(curl -s http://127.0.0.1:8000/api/labels | python3 -c "import sys,json; ls=json.load(sys.stdin); print(ls[0]['id'] if ls else 'none')")

echo "Evaluating factor=$FACTOR_ID with label=$LABEL_ID on group=$GROUP_ID"

# 触发评价
curl -s -X POST "http://127.0.0.1:8000/api/factors/$FACTOR_ID/evaluate" \
  -H "Content-Type: application/json" \
  -d "{\"label_id\": \"$LABEL_ID\", \"universe_group_id\": \"$GROUP_ID\", \"start_date\": \"2025-01-01\", \"end_date\": \"2025-12-31\"}" \
  | python3 -m json.tool
```

- [ ] 返回 task_id

```bash
# 等待完成后查看评价结果
TASK_ID=<上面返回的task_id>
# 轮询直到完成：
curl -s "http://127.0.0.1:8000/api/tasks/$TASK_ID" | python3 -m json.tool
```

- [ ] 任务最终 status=completed

```bash
# 查看评价结果列表
curl -s "http://127.0.0.1:8000/api/factors/$FACTOR_ID/evaluations" | python3 -m json.tool
```

- [ ] 返回评价结果，包含 summary（ic_mean, ir, ic_win_rate, turnover, coverage, long_short_annual_return）

```bash
# 查看完整评价详情
EVAL_ID=$(curl -s "http://127.0.0.1:8000/api/factors/$FACTOR_ID/evaluations" | python3 -c "import sys,json; es=json.load(sys.stdin); print(es[0]['id'] if es else 'none')")
curl -s "http://127.0.0.1:8000/api/factors/evaluations/$EVAL_ID" | python3 -m json.tool | head -30
```

- [ ] 返回完整评价，包含 ic_series（日期+IC值数组）和 group_returns（5组累计收益）

---

## M2.3 验收项

### 1. 因子编辑器页面

浏览器打开 http://localhost:5173，点击左侧 "因子研究"：

- [ ] 显示 3 个 Tab：因子编辑器、因子库、评价历史
- [ ] 默认在"因子编辑器" Tab

**编辑器功能：**

- [ ] 左侧 Monaco Editor 显示 Python 代码编辑区，有语法高亮
- [ ] 模板下拉框可选择 12 个内置模板
- [ ] 选择模板后代码自动填入编辑器
- [ ] 可输入因子名称、描述、选择分类
- [ ] "保存因子" 按钮可点击，保存后提示成功

**评价功能：**

- [ ] 右侧有标签定义下拉框（显示 4+ 个标签）
- [ ] 有股票分组下拉框
- [ ] 有日期范围选择器
- [ ] 点击 "运行评价" 后显示加载状态
- [ ] 评价完成后显示：
  - [ ] 6 个指标卡片（IC Mean, IR, IC Win Rate, Coverage, Turnover, L/S Return）
  - [ ] IC 时序图（柱状图，红绿色，有均值线）
  - [ ] 分组收益图（5 条线 + long-short 虚线）

### 2. 因子库页面

点击 "因子库" Tab：

- [ ] 表格显示所有因子
- [ ] 列包含：名称、版本、分类（彩色 Tag）、状态（彩色 Tag）、IR、创建时间、操作
- [ ] 可按分类和状态筛选
- [ ] 点击因子名称可跳转到编辑器 Tab 并加载代码
- [ ] 删除按钮可删除因子

### 3. 评价历史页面

点击 "评价历史" Tab：

- [ ] 表格显示所有评价记录
- [ ] 列包含：因子名称、标签、IC Mean、IR、Win Rate、L/S Return、日期
- [ ] 点击某行可查看完整评价详情（弹窗或展开）

---

## 验收总结

| 里程碑 | 验收项 | 状态 |
|--------|--------|------|
| M2.1 指标库 | 17 个指标正常计算 | ☐ |
| M2.1 标签 | 预置标签存在 + CRUD | ☐ |
| M2.1 因子协议 | 模板加载 + 错误捕获 | ☐ |
| M2.1 因子 API | CRUD + 模板接口 | ☐ |
| M2.2 计算 | 因子计算任务完成 | ☐ |
| M2.2 评价 | IC/IR/分组收益等指标输出 | ☐ |
| M2.2 评价详情 | ic_series + group_returns 完整 | ☐ |
| M2.3 编辑器 | Monaco + 模板 + 保存 | ☐ |
| M2.3 评价 UI | 运行评价 + 图表展示 | ☐ |
| M2.3 因子库 | 列表 + 筛选 + 跳转 | ☐ |
| M2.3 评价历史 | 全局记录 + 详情查看 | ☐ |

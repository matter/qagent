<p align="center">
  <img src="docs/assets/logo.png" alt="QAgent" width="120" />
</p>

<h1 align="center">QAgent</h1>

<p align="center">
  <strong>让 AI Agent 像量化研究员一样工作的全链路基础设施</strong>
</p>

<p align="center">
  <a href="#quick-start">快速开始</a> ·
  <a href="#agent-patterns">Agent 模式</a> ·
  <a href="#architecture">架构</a> ·
  <a href="#screenshots">界面</a> ·
  <a href="#self-evolution">自演进</a>
</p>

---

## 为什么做这个项目

量化研究的本质是一个**假设→实验→验证→迭代**的闭环。每一轮迭代都涉及因子挖掘、特征工程、模型训练、策略编写、回测验证、模拟交易，链路长且环环相扣。

这正是 AI Agent 最擅长的场景——**不是替代人类的判断，而是把判断落地为可执行、可验证、可追溯的自动化链路**。

QAgent 不是一个"AI 帮你炒股"的工具。它是一套**量化研究 Agent 的运行基础设施**：

- 🧠 **Agent 思考的外化**：每一步决策都有对应的 API、数据和可视化，Agent 不是黑盒操作，而是透明地走完完整研究流程
- 🔄 **Agent 闭环的载体**：从因子假设到模拟交易 P&L，每一步都有数值验收，Agent 可以根据结果自主决定下一步
- 📊 **Agent 记忆的沉淀**：所有实验结果持久化在 DuckDB + Obsidian Vault 中，跨会话可追溯

---

## 核心理念：Agent-Native 量化系统

传统量化平台为人类设计 GUI。QAgent 为 Agent 设计 API，同时为人类提供可视化验收界面。

```
┌─────────────────────────────────────────────────────┐
│                    Human (你)                        │
│         审阅结果 · 制定方向 · 最终决策                    │
└──────────────────────┬──────────────────────────────┘
                       │ 自然语言指令
┌──────────────────────▼──────────────────────────────┐
│              Atlas (Agent 调度层)                     │
│    MCP Server · 37 量化工具 · Obsidian 知识沉淀         │
└──────────────────────┬──────────────────────────────┘
                       │ REST API / MCP Protocol
┌──────────────────────▼──────────────────────────────┐
│               QAgent (本项目)                        │
│   数据 · 因子 · 模型 · 策略 · 回测 · 信号 · 模拟交易     │
│   DuckDB 持久化 · FastAPI 服务 · React 可视化          │
└─────────────────────────────────────────────────────┘
```

**Atlas**（[github.com/your/atlas](https://github.com/your/atlas)）是上层 Agent 调度项目，通过 MCP 协议调用 QAgent 的 37 个量化工具。QAgent 专注于做好一件事：**成为 Agent 可以信赖的量化执行引擎**。

---

<a id="agent-patterns"></a>
## Agent 模式在量化全链路中的体现

QAgent 的设计贯穿了 7 种经典 Agent 模式：

### 1. 🔗 Chain — 全链路串联

量化研究天然是一条长链路。Agent 按顺序推进，每一步的输出是下一步的输入：

```
数据就绪 → 因子挖掘 → 因子评估 → 特征集组装 → 模型训练 → 策略编写 → 回测验证 → 模拟交易
```

QAgent 的 API 设计完全遵循这条链路——不跳步、不黑盒、每一步都有独立验收点。

### 2. 🔄 ReAct — 观察-思考-行动

Agent 不是一次性执行完整链路，而是在每一步都**观察结果、判断是否继续**：

```
Agent: 评估因子 RSI_14 的 IC
QAgent: IC_mean=0.032, IR=0.45
Agent: IR 不够高，换一个因子试试
Agent: 评估因子 Momentum_20 的 IC
QAgent: IC_mean=0.058, IR=1.12
Agent: 这个因子值得入池
```

每个 API 返回结构化数值结果，Agent 据此做出下一步决策。这就是 ReAct 模式的核心：**行动-观察-反思-再行动**。

### 3. 🛠️ Tool Use — Agent 即工具调用者

QAgent 暴露 37+ API 端点作为 Agent 工具集。通过 Atlas 的 MCP Server，Agent 可以像调用函数一样调用量化能力：

```python
# Agent 的视角（通过 MCP 工具调用）
data_status()                    # 检查数据是否就绪
create_factor(name, code)        # 创建因子
evaluate_factor(factor_id)       # 评估因子效果
train_model(features, label)     # 训练模型
run_backtest(strategy_id)        # 回测策略
advance_paper_session(id, 1)     # 推进模拟交易 1 天
```

### 4. 📋 Plan & Execute — 长任务异步编排

模型训练和回测可能需要数分钟。QAgent 用 **TaskExecutor** 将长任务异步化：

```
Agent: train_model(...)
QAgent: → task_id: "abc123", status: "running"

Agent: task_status("abc123")     # 轮询
QAgent: → status: "running", progress: 65%

Agent: task_status("abc123")     # 继续轮询
QAgent: → status: "completed", result: { model_id: "xyz", metrics: {...} }
```

Agent 提交任务后不阻塞，可以去做其他事，定期回来检查。这是 Plan & Execute 模式的经典实现。

### 5. 🔍 Reflection — 自我验证与反思

每个环节都内置了**数值验收机制**，Agent 可以自动判断实验是否达标：

| 环节 | 验收指标 | Agent 判断逻辑 |
|------|---------|-------------|
| 因子评估 | IC, IR, IC 胜率 | IR < 0.5 → 不入池 |
| 模型训练 | IC, AUC, Sharpe | IC_mean < 0.03 → 重新调参 |
| 回测验证 | Sharpe, 最大回撤, 年化收益 | Sharpe < 2 → 调整策略 |
| 模拟交易 | 累计收益, 回撤, 换手率 | 回撤 > 15% → 暂停会话 |

这不是人为设定的规则——Agent 通过阅读 `memory.md` 中的历史经验，自己形成判断标准。

### 6. 📝 Memory — 跨会话经验沉淀

Agent 每天产出 4 份结构化文档（详见[自演进](#self-evolution)一节），沉淀到 Obsidian Vault：

- **工作总结**：今天做了什么、得出什么结论
- **实验记录**：完整实验数据表，可复现
- **经验反思**：哪些假设被证伪，哪些经验可复用
- **下一步计划**：明天应该优先做什么

下一次 Agent 启动时，先读取 `memory.md`，直接继承所有历史经验。这就是 Agent 的长期记忆。

### 7. 🤖 Autonomous — 自治闭环

将上述模式组合在一起，Agent 可以在**最少人类干预下完成完整研究周期**：

```
每日自动流程（通过 Atlas 调度器）:
  07:00  更新行情数据
  07:05  检查模拟交易会话状态
  07:10  推进所有活跃模拟交易至最新
  07:15  生成 T+1 操作计划
  07:20  汇总结果 → 写入 Obsidian

人类只需要在适当时候:
  - 审阅 T+1 操作计划
  - 决定是否调整研究方向
  - 批准新策略上线模拟
```

---

<a id="quick-start"></a>
## 快速开始

### 环境要求

- Python ≥ 3.12, [uv](https://docs.astral.sh/uv/)
- Node.js ≥ 18, [pnpm](https://pnpm.io/)

### 安装

```bash
git clone https://github.com/your/qagent.git
cd qagent

# 后端
uv sync

# 前端
cd frontend && pnpm install && cd ..
```

### 启动

```bash
# 一键启动（后端 :8000 + 前端 :5173）
scripts/start.sh

# 或分别启动
uv run uvicorn backend.app:app --host 127.0.0.1 --port 8000 --reload
cd frontend && pnpm dev
```

### 停止

```bash
scripts/stop.sh
```

### 验证

```bash
# 运行端到端演示（需要已启动的服务）
uv run python scripts/e2e_demo.py
```

---

<a id="architecture"></a>
## 系统架构

### 技术栈

| 层 | 技术 | 说明 |
|---|------|-----|
| **数据库** | DuckDB | 单文件嵌入式分析数据库，零运维 |
| **后端** | FastAPI + Python | 26K+ 行，覆盖完整量化链路 |
| **前端** | React + Ant Design + ECharts | 10 个功能页面，全链路可视化 |
| **数据源** | yfinance | 美股日线数据，NYSE 交易日历 |
| **ML** | LightGBM + scikit-learn | 回归/分类双模式，22 种内置标签 |
| **Agent 接口** | REST API + MCP | 37+ 工具端点，支持 Atlas 调度 |

### 模块结构

```
qagent/
├── backend/
│   ├── api/           # FastAPI 路由（薄层，委托给 services）
│   │   ├── data.py          # 数据管理 API
│   │   ├── factors.py       # 因子 CRUD + 计算 + 评估
│   │   ├── features.py      # 特征集管理
│   │   ├── labels.py        # 标签定义（回归/分类）
│   │   ├── models.py        # 模型训练/预测
│   │   ├── strategies.py    # 策略编辑/模板
│   │   ├── signals.py       # 信号生成
│   │   └── paper_trading.py # 模拟交易全生命周期
│   │
│   ├── services/      # 核心业务逻辑
│   │   ├── data_service.py        # 行情数据增量更新
│   │   ├── factor_engine.py       # 因子计算引擎（批量缓存）
│   │   ├── feature_service.py     # 特征预处理管线
│   │   ├── model_service.py       # 训练/预测/评估
│   │   ├── signal_service.py      # 12步信号生成管线
│   │   ├── backtest_engine.py     # 回测撮合引擎
│   │   └── paper_trading_service.py  # T+1 模拟交易
│   │
│   ├── factors/       # 因子基类 + 287 内置因子
│   ├── models/        # 模型基类 + LightGBM 实现
│   ├── strategies/    # 策略基类 + 模板
│   ├── tasks/         # 异步任务执行器
│   └── providers/     # 数据源抽象（yfinance）
│
├── frontend/src/
│   ├── pages/         # 10 个路由页面
│   │   ├── DataManagePage     # 数据管理
│   │   ├── FactorResearch     # 因子研究
│   │   ├── FeatureEngineering # 特征工程
│   │   ├── ModelTraining      # 模型训练
│   │   ├── StrategyBacktest   # 策略回测
│   │   ├── SignalGeneration   # 信号生成
│   │   ├── PaperTrading       # 模拟交易
│   │   └── MarketPage         # 市场总览
│   │
│   ├── components/    # 可复用 UI 组件
│   └── api/           # 类型安全的 API 客户端
│
├── config.yaml        # 运行时配置
├── scripts/           # 启停/备份/演示脚本
└── data/              # DuckDB 数据库 + 模型文件（gitignore）
```

---

<a id="screenshots"></a>
## 界面展示

QAgent 的每个页面对应量化研究链路的一个环节，同时也是 Agent 操作的可视化验收面板。

### 因子研究

在线编辑因子代码，计算因子值，评估 IC/IR/多空收益。Agent 创建的因子会立即出现在这里，人类可以审阅图表判断因子质量。

【截图：因子研究页面 - 左侧因子列表 + 右侧 IC 时序图和分组收益柱状图】

### 模型训练

选择特征集、学习目标（22 种内置标签，回归/分类双模式）、训练区间，一键训练。标签选择器带 `[回归]` / `[分类]` 标签，清晰区分任务类型。

【截图：模型训练页面 - 数据配置区（特征集/标签/分组）+ 训练配置区 + 模型列表】

### 策略回测

在线编辑策略代码（引用因子和模型），运行回测查看净值曲线、Sharpe、最大回撤、每笔交易明细。

【截图：策略回测页面 - 净值曲线 + 指标面板 + 交易记录表】

### 模拟交易

前向测试策略。逐日推进，查看 T+1 操作计划（买入/卖出/持有），跟踪实际 P&L。这是防止回测过拟合的关键验证环节。

【截图：模拟交易页面 - 会话列表 + 持仓卡片 + T+1 操作计划 + 净值曲线】

### 数据管理

查看数据覆盖状态，增量更新行情数据，管理股票分组（SP500/NASDAQ100/SP400 等内置分组）。

【截图：数据管理页面 - 数据状态卡片 + 股票分组列表 + 更新按钮】

---

## 量化全链路

一次完整的量化研究，从假设到验证：

```
              ┌──────────────────────────────────────────┐
              │          Phase 1: 数据基础                 │
              │  更新行情 → 确认覆盖 → 管理股票池             │
              └────────────────┬─────────────────────────┘
                               ▼
              ┌──────────────────────────────────────────┐
              │          Phase 2: 因子研究                 │
              │  创建因子 → 计算因子值 → 评估 IC/IR           │
              │  287 内置因子 + 自定义因子代码                 │
              └────────────────┬─────────────────────────┘
                               ▼
              ┌──────────────────────────────────────────┐
              │          Phase 3: 模型训练                 │
              │  组装特征集 → 选择标签 → 训练 LightGBM        │
              │  22 种标签 (回归 + 分类)                    │
              │  自动评估 IC/AUC/Sharpe/多空收益             │
              └────────────────┬─────────────────────────┘
                               ▼
              ┌──────────────────────────────────────────┐
              │          Phase 4: 策略验证                 │
              │  编写策略 → 回测 → 分析交易 → 调优            │
              └────────────────┬─────────────────────────┘
                               ▼
              ┌──────────────────────────────────────────┐
              │          Phase 5: 前向测试                 │
              │  创建模拟交易 → 逐日推进 → T+1 操作计划       │
              │  跟踪 P&L → 验证策略在未见数据上的表现         │
              └──────────────────────────────────────────┘
```

### 内置研究原语

| 类别 | 数量 | 说明 |
|------|------|------|
| 内置因子 | 287 | Alpha360 统计因子 + 经典技术指标 |
| 学习标签 | 22 | 回归 (return/rank/excess) + 分类 (binary/top_quantile/bottom_quantile/large_move/excess_binary) |
| 策略模板 | 多种 | 模型预测策略、多因子策略等 |
| 股票分组 | 4 | SP500, NASDAQ100, SP MidCap 400, Russell 3000 |

---

<a id="self-evolution"></a>
## Agent 自演进框架

QAgent 不只是一个静态工具——它是 **Agent 自主研究和持续演进的基础设施**。

### 每日 4 文档：Agent 的结构化记忆

通过 Atlas 调度，Agent 每天结束时自动产出 4 份文档，沉淀到 Obsidian Vault：

```
~/ObsidianVault/Atlas/quant/reports/
├── 2026-04-13-quant-work-summary.md        # 工作总结
├── 2026-04-13-quant-experiment-record.md    # 实验记录
├── 2026-04-13-quant-retrospective.md        # 经验反思
└── 2026-04-13-quant-next-day-plan.md        # 下一步计划
```

| 文档 | 核心内容 | Agent 如何使用 |
|------|---------|-------------|
| **工作总结** | 一句话结论、核心成绩、关键判断、风险 | 回顾时快速定位当天结论 |
| **实验记录** | 完整实验参数、对比表格、指标数值 | 复现实验、避免重复工作 |
| **经验反思** | 哪些假设被证伪、哪些经验可复用 | 积累到 `memory.md`，长期指导决策 |
| **下一步计划** | 明天的目标、重点工作、验收标准 | 次日启动时直接读取，无缝衔接 |

### memory.md：Agent 的长期记忆

每条经过实验验证的经验，最终沉淀为 `memory.md` 中的一条规则。Agent 在每次会话开始时读取这份文档，直接继承所有历史认知：

```markdown
# memory.md 中的真实条目（节选）

- 低频优化里，weekly 对当前 ex20d + V6 体系通常是有效折中，
  monthly 会明显损伤 alpha

- 分类模型在策略上下文里仍返回硬分类结果，不适合做主排序模型；
  除非后端开放概率输出，否则分类模型默认只做弱 gate

- 如果一个新辅助模型的正向效果只在很窄的阈值上出现，
  不应当直接沉淀成经验
```

这些不是预设的规则——它们全部来自 Agent 自己的实验和反思。

### Backlog：Agent 的改进需求

Agent 在使用 QAgent 的过程中，会主动发现系统的不足并记录到 `backlog.md`。人类开发者根据 backlog 改进 QAgent，形成**使用者→基础设施**的反向演进循环：

```
Agent 使用 QAgent 做研究
    ↓
发现问题（如：分类模型缺少概率输出）
    ↓
记录到 backlog.md
    ↓
人类开发者修复（如：本项目新增 predict_proba 支持）
    ↓
Agent 下次使用时自动受益
    ↓
新经验沉淀到 memory.md
```

这就是 **Agent-driven development**：Agent 是 QAgent 最重要的用户和需求来源。

---

## 性能优化

QAgent 为 Agent 的高频使用场景做了专门优化：

| 场景 | 优化前 | 优化后 | 方案 |
|------|--------|--------|------|
| 模拟交易推进 1 天 | ~3 分钟 | ~18 秒 | 264 个因子单次批量查询替代 528 次 DB 往返 |
| 模拟交易推进 3 天 | ~9 分钟 | ~20 秒 | 批量信号生成 + 价格缓存复用 |
| T+1 操作计划 | ~3 分钟 | ~20 秒 | 预计算特征直传模型，跳过重复因子计算 |
| 数据增量更新 | 逐条更新 | 智能分批 | 按日期范围动态调整批大小 |

---

## 配置

`config.yaml` 控制运行时行为：

```yaml
data:
  provider: yfinance           # 数据源
  db_path: ./data/qagent.duckdb  # 数据库路径

server:
  host: 127.0.0.1
  port: 8000

backtest:
  default_initial_capital: 1000000
  default_commission_rate: 0.001
  default_slippage_rate: 0.001
  default_benchmark: SPY

market:
  calendar: NYSE               # 交易日历
```

---

## 与 Atlas 集成

QAgent 设计为独立运行，但与 [Atlas](https://github.com/your/atlas) 集成时释放完整 Agent 能力：

```
Atlas 调度层
├── atlas-quant MCP Server     # 37 个量化工具，调用 QAgent API
├── atlas-knowledge Server     # Obsidian Vault 读写
├── atlas-content Server       # 内容生成
└── scheduler.py               # 定时任务（每日数据更新、周复盘）

QAgent 执行层
├── REST API (127.0.0.1:8000)  # 被 Atlas 调用
├── React UI (localhost:5173)  # 人类审阅和验收
└── DuckDB                     # 持久化所有实验数据
```

**典型 Agent 工作流**：

1. Atlas 调度器在工作日 7:00 自动触发数据更新
2. Agent 读取昨天的 `next-day-plan.md`，继续研究
3. 通过 MCP 工具调用 QAgent API 执行实验
4. 人类在 React UI 上审阅回测曲线和模拟交易结果
5. Agent 产出当天 4 份文档，沉淀经验
6. 循环

---

## 开发

```bash
# 后端开发（hot reload）
uv run uvicorn backend.app:app --host 127.0.0.1 --port 8000 --reload

# 前端开发（hot reload）
cd frontend && pnpm dev

# TypeScript 类型检查
cd frontend && pnpm build

# 数据备份/恢复
scripts/backup_data.sh
scripts/restore_data.sh
```

### 代码风格

- Python: 4 空格缩进，`snake_case`
- TypeScript: 2 空格缩进，`PascalCase` 组件文件名
- API 路由保持精简，业务逻辑放在 `services/`

---

## License

MIT

---

<p align="center">
  <sub>Built with 🤖 by human + agent collaboration</sub>
</p>

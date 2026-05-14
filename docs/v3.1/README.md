# QAgent V3.1

V3.1 迭代聚焦一个明确的执行语义升级：在现有 T 日决策、T+1 日执行框架下，新增“计划交易模型”，让策略和回测不再只能假设 T+1 开盘价成交。

## 文档

- [3.1-planned-trading-design.md](./3.1-planned-trading-design.md): 需求方案、交易语义、影响范围、风险评判和最佳实践。
- [3.1-planned-trading-implementation-plan.md](./3.1-planned-trading-implementation-plan.md): 面向 agent/工程实现的分阶段开发计划、测试策略和验收标准。
- [3.1-strategy-parameter-and-execution-plan.md](./3.1-strategy-parameter-and-execution-plan.md): 策略默认参数、计划价兜底和后续动态执行意图的开发计划。

## 版本边界

- V3.1 不替换 V3.0 research kernel、StrategyGraph、artifact、lineage 等架构目标。
- V3.1 先把计划交易作为显式可选执行模式接入 legacy 回测、legacy paper trading 和 V3.0 StrategyGraph。
- 现有默认执行模式暂保持 `next_open`，完成对照验证后再单独评估是否切换默认值。

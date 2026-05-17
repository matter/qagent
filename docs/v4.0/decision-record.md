# QAgent Next 决策记录

> 文档状态：开发过程决策记录  
> 创建日期：2026-05-17  
> 用途：无人值守开发过程中记录关键技术、产品和取舍决策。

## 决策模板

```text
ID：
日期：
状态：PROPOSED / ACCEPTED / SUPERSEDED
背景：
决策：
理由：
影响：
关联任务：
```

## 决策列表

### ADR-001：从清理后空仓库开始重建新系统

ID：ADR-001  
日期：2026-05-17  
状态：ACCEPTED  
背景：旧系统代码已经按新项目准备要求清理，当前仓库只保留 v4.0 文档。  
决策：Phase 0 不复用旧 backend/frontend 目录结构，按新需求重新建立低耦合包边界。  
理由：新系统要求从零开发，不兼容旧接口、旧数据库和旧 UI；复用旧结构会继续携带策略膨胀、多市场补丁和任务并发问题。  
影响：需要重新建立 `qagent_domain`、`qagent_storage`、`qagent_task`、`qagent_api`、`qagent_cli`、`qagent_mcp`、`qagent_ui`。  
关联任务：P0-T01

### ADR-002：Phase 0 使用本地进程内任务执行器，不引入 Ray

ID：ADR-002  
日期：2026-05-17  
状态：ACCEPTED  
背景：需求文档建议 Ray 作为可选 executor backend，但 Phase 0 目标是稳定任务合约和资源 lease。  
决策：Phase 0 先实现本地线程/进程内执行器和清晰的 executor adapter 接口，Ray 作为后续 P7 优化。  
理由：提前引入 Ray 会增加部署和调试成本；当前关键风险是任务状态、取消、晚到结果隔离、writer fence 和 idempotency。  
影响：Phase 0 验收以本地 executor 为准，但接口必须保留替换空间。  
关联任务：P0-T05、P0-T06

### ADR-003：UI 先采用最小 HTML/React 工作台

ID：ADR-003  
日期：2026-05-17  
状态：ACCEPTED  
背景：用户明确要求 UI 部分先用最简单实现，后续功能完善后会大改版。  
决策：Phase 0 UI 只实现任务提交、任务列表和任务状态轮询，不引入复杂组件库和设计系统。  
理由：当前阶段核心是服务层、任务系统和低耦合架构；复杂 UI 会降低迭代速度。  
影响：UI 代码保持薄层，不复制业务规则。  
关联任务：P0-T08

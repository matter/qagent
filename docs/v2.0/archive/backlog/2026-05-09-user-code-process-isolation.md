# [2026-05-09] P0 User factor/strategy code process isolation

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-09

## 原始问题

- **范围**：US, CN custom factor / custom strategy execution.
- **入口**：`backend/factors/loader.py`, `backend/strategies/loader.py`
- **现象**：自定义因子和策略源代码在后端进程内 `exec()`，静态检查只能挡住明显危险语法，不能隔离 CPU/内存/卡死风险。

## 修复记录

- 新增 `backend/services/custom_code_runner.py`，通过子进程执行用户代码。
- factor metadata、factor `compute()`、strategy metadata、strategy `generate_signals()` 都改为通过隔离子进程执行。
- 子进程路径保留现有白名单 import 和静态安全检查。
- 运行层支持超时终止，Unix 环境下应用 CPU 和内存 resource limit。
- 父进程从队列读取执行结果，超时后终止子进程，避免用户代码阻塞 API worker。

## 验证

- `uv run python -m unittest tests.test_custom_code_safety.CustomCodeSafetyContractTests -v`

## 残余风险

- 这是单机本地隔离，不是多租户安全沙箱。
- 网络和文件系统主要依赖 import/builtin 限制与静态检查；如未来允许更多库，需要继续收紧 worker 权限。

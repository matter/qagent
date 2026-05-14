# QAgent Multi-Agent Strategy Optimization Protocol

This document defines how the coordinator agent runs multiple independent Codex threads for strategy optimization. The coordination model is agent-layer first: QAgent is the research execution and evidence system, not the owner of agent orchestration. Detailed experiment outputs should be captured in QAgent research plans, trials, artifacts, QA reports, and promotion records when useful, but the coordinator's authority to dispatch top-model agents does not depend on QAgent implementing a control plane.

## 1. Operating Model

The coordinator is accountable for final strategy quality. Specialist agents are execution units, not collaborators with each other.

```text
Human owner
└─ Coordinator agent
   ├─ Factor agent
   ├─ Feature-model agent
   ├─ Strategy-execution agent
   └─ Audit-review agent
```

Rules:

- The coordinator starts and manages all specialist Codex threads.
- The coordinator has authority to dispatch top-model specialist agents, with `gpt-5.5` and `xhigh` as the default for serious research assignments.
- Specialist agents run independently and do not coordinate with each other.
- Specialist agents receive a bounded task packet and return a structured result packet.
- The coordinator reads all results, validates evidence, chooses the next research direction, and asks the human owner for confirmation before the next optimization round.
- QAgent integration is deliberately thin: use REST/MCP/tasks/research records for execution and evidence, but do not require QAgent to know every Codex thread detail before the workflow is useful.
- Specialist agents must not directly write DuckDB or bypass REST/MCP service paths.
- One QAgent backend should be shared by all threads. Do not start multiple backend processes against the same DuckDB file.

## 2. Role Boundaries

### Coordinator

Responsibilities:

- Own the baseline, research contract, trial budget, and acceptance criteria.
- Dispatch specialist agents with narrow, independent tasks using the strongest model needed for the assignment.
- Keep all communication through coordinator-only summaries.
- Monitor `/api/tasks`, research plan budgets, task failures, queue congestion, and evidence completeness.
- Decide whether results are rejected, continued, expanded, sent to QA, or proposed for paper/promotion.
- Maintain the concise coordination record in this document.

The coordinator must not let headline backtest metrics override time-series correctness, reproducibility, costs, or failed-trial evidence.

### Factor Agent

Scope:

- Factor discovery, refactoring, FactorSpec creation/preview/materialization planning, factor evaluation, coverage, IC, rank IC, stability, correlation, and alpha independence.

Frozen modules:

- Feature set, label, model, strategy, portfolio construction, risk controls, cost model, and execution model.

### Feature-Model Agent

Scope:

- Feature set, dataset, label target, split policy, purge gap, model experiment, model package, OOS prediction diagnostics, and overfit checks.

Frozen modules:

- Factor logic, universe, strategy conversion, portfolio construction, risk controls, cost model, and execution model.

### Strategy-Execution Agent

Scope:

- Score-to-portfolio mapping, selection thresholds, position sizing, constraints, rebalance policy, execution policy, planned-price diagnostics, backtest, and signal explainability.

Frozen modules:

- Factor logic, feature set, label, model family, model package, and training protocol.

### Audit-Review Agent

Scope:

- Read-only review of trial matrix, failed trials, reproducibility fingerprints, task status, evidence package, data quality gates, leakage risk, and promotion readiness.

Frozen modules:

- All research assets. The audit agent does not create or modify experiment assets.

## 3. Research Round Lifecycle

Each round has one coordinator-owned research contract.

1. Baseline lock
   - Identify market, universe, baseline strategy/model/backtest, data range, cost model, execution model, and primary metrics.
   - Record known limitations, especially non-PIT/free-source data limitations.

2. Task packet dispatch
   - Coordinator creates independent task packets for specialist agents.
   - Each task packet states allowed changes, frozen modules, budget, stop conditions, required outputs, and result recording rules.

3. Concurrent execution
   - Specialist agents run in parallel.
   - They do not wait for other specialists unless the coordinator explicitly issues a follow-up task.
   - Long work must use QAgent task APIs and return `task_id` references.

4. Result intake
   - Coordinator reads specialist result packets, QAgent trial records, artifacts, task records, and compact summaries.
   - Results missing required evidence are rejected or returned for one focused correction.

5. Coordinator decision
   - Choose exactly one next primary path: data, factor, feature-label, model, strategy-execution, regime, robustness, QA/paper.
   - Do not merge multiple simultaneous changes into one accepted improvement.

6. Human checkpoint
   - Coordinator reports the round summary and proposed next plan.
   - The next optimization round starts only after human confirmation.

## 4. Specialist Task Packet

Use this structure when launching an independent Codex thread:

```markdown
Role:
Round:
Model requirement: gpt-5.5, xhigh
Workspace: /Users/m/dev/qagent

Objective:
Baseline refs:
Allowed changes:
Frozen modules:
Market/universe/date scope:
Trial budget:
Task/API constraints:
Required diagnostics:
Required QAgent records:
Stop conditions:
Output packet format:
```

Minimum task constraints:

- Do not coordinate with other agents.
- Do not direct-write DuckDB or generated artifacts.
- Do not start or stop backend services unless explicitly assigned.
- Do not run broad data refreshes.
- Record failed trials.
- Return compact evidence and asset IDs, not large raw payloads.

## 5. Specialist Result Packet

Every specialist returns:

```markdown
Status: completed | blocked | rejected | needs-coordinator-decision
Role:
Round:
Changed module:
Frozen modules respected: yes | no
Baseline refs:
Trials recorded:
Tasks submitted:
Artifacts / asset refs:
Headline metrics:
Diagnostics:
Failed trials:
Evidence quality:
Risks:
Recommendation:
Next suggested task:
```

The coordinator rejects results when:

- The specialist changed a frozen module.
- Failed trials were omitted.
- A result cannot be tied to task/trial/artifact IDs.
- A timeout/cancelled task has `late_result_quarantined=true`.
- A promotion-like claim lacks QA evidence.
- The result is a single winner from a broad unrecorded search.

## 6. Coordinator Round Report

Use this concise report for human checkpoints:

```markdown
Round:
Baseline:
Primary question:
Agents dispatched:
What changed:
What stayed frozen:
Result summary:
Rejected or weak evidence:
Operational issues:
Coordinator decision:
Next proposed round:
Human confirmation needed:
```

## 7. Coordinator Work Record

The coordinator keeps one compact rolling record. It should be concise enough to read before each dispatch, with details stored in QAgent trials, artifacts, tasks, and QA reports.

```markdown
## Round: YYYY-MM-DD-RNN

### Objective
- Goal:
- Market/scope:
- Baseline refs:
- Frozen modules:
- Success criteria:

### Dispatch Board
| Agent ID | Specialist Role | Codex Thread | Model | Status | Scope | Plan/Task IDs | Output |
| --- | --- | --- | --- | --- | --- | --- | --- |

### Decisions
| Decision | Evidence | Accepted/Rejected | Next Action |
| --- | --- | --- | --- |

### Integration Notes
- Accepted:
- Rejected:
- Conflicts or duplicates:
- Needs human decision:

### Backlog Candidates
| Issue | Why It Is System Work | Severity | Write To Backlog? |
| --- | --- | --- | --- |

### Next Round Queue
1. Specialist task:
2. Verification task:
3. Asset/documentation work:
```

## 8. Immediate Round 0 Plan

Purpose: diagnose where strategy performance is currently constrained before optimizing.

Coordinator setup:

- Use one running backend and the existing DuckDB.
- Inspect current baseline strategy/model/backtest availability.
- Create or select one agent research plan for the round.
- Freeze market, universe, date windows, cost model, execution model, and baseline refs.

Parallel specialist assignments:

- Factor agent: assess existing alpha/factor quality, overlap, coverage, and whether weak alpha is the likely bottleneck.
- Feature-model agent: assess feature/label/model OOS strength, split policy, purge gap, and overfit risk.
- Strategy-execution agent: assess whether score-to-portfolio conversion, constraints, turnover, costs, or planned execution explain performance loss.
- Audit-review agent: assess task health, trial completeness, reproducibility, data quality limitations, and whether any result is promotion-like.

Round 0 output:

- Coordinator chooses one next path and proposes a bounded Round 1 experiment.

## 9. Optional System Support

The protocol works without QAgent owning agent orchestration. The coordinator can run the workflow from the Codex layer today, using QAgent only for research execution and evidence. System upgrades below are optional automation supports: they reduce manual bookkeeping and operational risk, but they are not prerequisites for the coordinator to dispatch specialist agents.

- Lightweight metadata support: research plans/trials/artifacts can optionally record `round`, `agent_role`, `model`, and `result_status` for easier querying.
- Structured result artifact: a standard JSON artifact schema can store specialist outputs without requiring a new assignment subsystem.
- Coordinator acceptance marker: accepted/rejected can live in a coordinator summary artifact or trial metadata; it does not need to block QAgent task completion.
- Operational guardrails: keep the existing single-backend rule, task pause rules, and no-direct-DuckDB rule. Add system scheduling only where real conflicts occur.
- Observability support: a compact API/UI view for tasks, budgets, pending evidence, and quarantined results is useful, but the coordinator can also maintain a manual dispatch board.
- MCP convenience: MCP should make it easy to write plan metadata and JSON artifacts, but specialist agent dispatch remains controlled by the coordinator outside QAgent.

## 10. Coordination Log

### 2026-05-14

- Established coordinator-led operating model: specialist agents do not coordinate with each other; all communication flows through the coordinator.
- Confirmed current QAgent foundations: shared REST/MCP service layer, TaskExecutor, task pause rules, research plans, trial matrix, artifacts, QA, and promotion gates.
- Refined boundary: the coordinator's authority to dispatch top-model agents is external to QAgent. QAgent should support evidence capture and operational safety, not own the full agent control plane.
- Immediate recommendation: run Round 0 through coordinator-managed Codex threads using existing QAgent records, while treating coordinator-native system features as optional automation backlog.

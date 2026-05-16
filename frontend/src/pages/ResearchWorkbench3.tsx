import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Alert,
  Button,
  Descriptions,
  Drawer,
  Empty,
  Form,
  Input,
  InputNumber,
  message,
  Popconfirm,
  Select,
  Space,
  Spin,
  Statistic,
  Table,
  Tabs,
  Tag,
  Typography,
} from "antd";
import {
  AuditOutlined,
  BarChartOutlined,
  BranchesOutlined,
  CheckCircleOutlined,
  ClearOutlined,
  DatabaseOutlined,
  ExperimentOutlined,
  FileSearchOutlined,
  ReloadOutlined,
} from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import {
  archiveResearchArtifact3,
  backtestStrategyGraph3,
  evaluatePromotion3,
  getBootstrapProject3,
  getDataQualityContract3,
  getProjectDataStatus3,
  getStrategyGraphBacktest3,
  getResearchLineage3,
  listAgentPlaybooks3,
  listAgentResearchPlans3,
  listDatasets3,
  listMacroSeries,
  listPaperSessions3,
  listProductionSignalRuns3,
  listPromotionRecords3,
  listProviderCapabilities3,
  listQaReports3,
  listResearchArtifacts3,
  listResearchRuns3,
  listStrategyGraphBacktests3,
  listStrategyGraphs3,
  listUniverses3,
  previewArtifactCleanup3,
} from "../api";
import type {
  AgentPlaybook3,
  AgentResearchPlan3,
  BacktestRun3,
  CleanupPreview3,
  DataQualityContract3,
  Dataset3,
  MacroSeries,
  PaperSession3,
  ProductionSignalRun3,
  ProjectDataStatus3,
  PromotionRecord3,
  ProviderCapability3,
  QaReport3,
  ResearchArtifact3,
  ResearchLineage3,
  ResearchProject3,
  ResearchRun3,
  StrategyGraph3,
  Universe3,
} from "../api";

const { Text, Paragraph } = Typography;

const dateRule = {
  pattern: /^\d{4}-\d{2}-\d{2}$/,
  message: "YYYY-MM-DD",
};

interface BacktestFormValues {
  start_date: string;
  end_date: string;
  initial_capital: number;
  price_field: string;
}

interface WorkbenchState {
  project: ResearchProject3 | null;
  dataStatus: ProjectDataStatus3 | null;
  runs: ResearchRun3[];
  artifacts: ResearchArtifact3[];
  playbooks: AgentPlaybook3[];
  plans: AgentResearchPlan3[];
  qaReports: QaReport3[];
  promotions: PromotionRecord3[];
  universes: Universe3[];
  datasets: Dataset3[];
  strategyGraphs: StrategyGraph3[];
  productionSignals: ProductionSignalRun3[];
  paperSessions: PaperSession3[];
  providerCapabilities: ProviderCapability3[];
  qualityContract: DataQualityContract3 | null;
  macroSeries: MacroSeries[];
}

const emptyState: WorkbenchState = {
  project: null,
  dataStatus: null,
  runs: [],
  artifacts: [],
  playbooks: [],
  plans: [],
  qaReports: [],
  promotions: [],
  universes: [],
  datasets: [],
  strategyGraphs: [],
  productionSignals: [],
  paperSessions: [],
  providerCapabilities: [],
  qualityContract: null,
  macroSeries: [],
};

function statusColor(status: string) {
  if (["completed", "pass", "promoted", "active", "published", "validated"].includes(status)) return "success";
  if (["running", "queued", "warning", "candidate", "draft"].includes(status)) return "processing";
  if (["failed", "fail", "rejected"].includes(status)) return "error";
  if (["archived", "scratch"].includes(status)) return "default";
  return "blue";
}

function shortJson(value: unknown) {
  if (value == null) return "-";
  const text = JSON.stringify(value);
  return text.length > 180 ? `${text.slice(0, 180)}...` : text;
}

function timeText(value: string | null | undefined) {
  return value ? value.slice(0, 19) : "-";
}

function bytesText(value: number) {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / 1024 / 1024).toFixed(1)} MB`;
}

export default function ResearchWorkbench3() {
  const [state, setState] = useState<WorkbenchState>(emptyState);
  const [loading, setLoading] = useState(false);
  const [selectedRun, setSelectedRun] = useState<ResearchRun3 | null>(null);
  const [selectedArtifact, setSelectedArtifact] = useState<ResearchArtifact3 | null>(null);
  const [selectedQa, setSelectedQa] = useState<QaReport3 | null>(null);
  const [lineage, setLineage] = useState<ResearchLineage3 | null>(null);
  const [cleanupPreview, setCleanupPreview] = useState<CleanupPreview3 | null>(null);
  const [selectedGraph, setSelectedGraph] = useState<StrategyGraph3 | null>(null);
  const [graphBacktests, setGraphBacktests] = useState<BacktestRun3[]>([]);
  const [selectedBacktest, setSelectedBacktest] = useState<BacktestRun3 | null>(null);
  const [backtestTaskId, setBacktestTaskId] = useState<string | null>(null);
  const [backtestLoading, setBacktestLoading] = useState(false);
  const [actionLoading, setActionLoading] = useState(false);
  const [messageApi, contextHolder] = message.useMessage();
  const [backtestForm] = Form.useForm<BacktestFormValues>();

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const project = await getBootstrapProject3();
      const [
        dataStatus,
        runs,
        artifacts,
        playbooks,
        plans,
        qaReports,
        promotions,
        universes,
        datasets,
        strategyGraphs,
        productionSignals,
        paperSessions,
        providerCapabilities,
        qualityContract,
        macroSeries,
      ] = await Promise.all([
        getProjectDataStatus3(project.id).catch(() => null),
        listResearchRuns3({ project_id: project.id, limit: 50 }),
        listResearchArtifacts3({ project_id: project.id, limit: 50 }),
        listAgentPlaybooks3(),
        listAgentResearchPlans3({ project_id: project.id, limit: 50 }),
        listQaReports3({ limit: 50 }),
        listPromotionRecords3({ project_id: project.id, limit: 50 }),
        listUniverses3({ project_id: project.id, limit: 50 }),
        listDatasets3({ project_id: project.id, limit: 50 }),
        listStrategyGraphs3({ project_id: project.id }),
        listProductionSignalRuns3({ limit: 20 }),
        listPaperSessions3({ limit: 20 }),
        listProviderCapabilities3(),
        getDataQualityContract3({ market_profile_id: project.market_profile_id }).catch(() => null),
        listMacroSeries("fred", 100).catch(() => []),
      ]);
      setState({
        project,
        dataStatus,
        runs,
        artifacts,
        playbooks,
        plans,
        qaReports,
        promotions,
        universes,
        datasets,
        strategyGraphs,
        productionSignals,
        paperSessions,
        providerCapabilities,
        qualityContract,
        macroSeries,
      });
    } catch {
      messageApi.error("加载研究工作台失败");
    } finally {
      setLoading(false);
    }
  }, [messageApi]);

  useEffect(() => {
    load();
  }, [load]);

  const openRun = async (run: ResearchRun3) => {
    setSelectedRun(run);
    setLineage(null);
    try {
      setLineage(await getResearchLineage3(run.id));
    } catch {
      messageApi.warning("Lineage 加载失败");
    }
  };

  const openArtifact = (artifact: ResearchArtifact3) => {
    setSelectedArtifact(artifact);
  };

  const archiveArtifact = async (artifact: ResearchArtifact3) => {
    setActionLoading(true);
    try {
      const archived = await archiveResearchArtifact3(artifact.id, {
        archive_reason: "archived from research workbench",
      });
      messageApi.success("Artifact 已归档");
      setSelectedArtifact(archived);
      await load();
    } catch {
      messageApi.error("归档失败");
    } finally {
      setActionLoading(false);
    }
  };

  const previewCleanup = async () => {
    if (!state.project) return;
    setActionLoading(true);
    try {
      setCleanupPreview(await previewArtifactCleanup3({
        project_id: state.project.id,
        lifecycle_stage: "scratch",
        retention_class: "scratch",
        limit: 100,
      }));
    } catch {
      messageApi.error("清理预览失败");
    } finally {
      setActionLoading(false);
    }
  };

  const promoteQaSource = async (qa: QaReport3) => {
    setActionLoading(true);
    try {
      const record = await evaluatePromotion3({
        source_type: qa.source_type,
        source_id: qa.source_id,
        qa_report_id: qa.id,
        metrics: qa.metrics,
        approved_by: "ui",
        rationale: "reviewed from research workbench",
      });
      messageApi.success(record.decision === "promoted" ? "已通过 Promotion" : "Promotion 未通过");
      await load();
    } catch {
      messageApi.error("Promotion 评估失败");
    } finally {
      setActionLoading(false);
    }
  };

  const openStrategyGraph = async (graph: StrategyGraph3) => {
    setSelectedGraph(graph);
    setSelectedBacktest(null);
    setBacktestTaskId(null);
    backtestForm.setFieldsValue(defaultBacktestFormValues(state.dataStatus));
    await loadGraphBacktests(graph.id);
  };

  const loadGraphBacktests = async (strategyGraphId: string) => {
    setBacktestLoading(true);
    try {
      setGraphBacktests(await listStrategyGraphBacktests3(strategyGraphId, { limit: 20 }));
    } catch {
      messageApi.error("回测记录加载失败");
    } finally {
      setBacktestLoading(false);
    }
  };

  const submitGraphBacktest = async (values: BacktestFormValues) => {
    if (!selectedGraph) return;
    setBacktestLoading(true);
    try {
      const response = await backtestStrategyGraph3(selectedGraph.id, {
        start_date: values.start_date,
        end_date: values.end_date,
        initial_capital: Number(values.initial_capital),
        price_field: values.price_field,
        lifecycle_stage: selectedGraph.lifecycle_stage || "experiment",
      });
      setBacktestTaskId(response.task_id);
      messageApi.success(`回测任务已提交：${response.task_id}`);
      await loadGraphBacktests(selectedGraph.id);
    } catch {
      messageApi.error("回测任务提交失败");
    } finally {
      setBacktestLoading(false);
    }
  };

  const openBacktestRun = async (backtest: BacktestRun3) => {
    setBacktestLoading(true);
    try {
      setSelectedBacktest(await getStrategyGraphBacktest3(backtest.id));
    } catch {
      setSelectedBacktest(backtest);
      messageApi.warning("回测详情加载失败，显示列表缓存");
    } finally {
      setBacktestLoading(false);
    }
  };

  const summary = useMemo(() => {
    const protectedArtifacts = state.artifacts.filter((item) =>
      ["validated", "published"].includes(item.lifecycle_stage),
    ).length;
    return {
      runCount: state.runs.length,
      artifactCount: state.artifacts.length,
      protectedArtifacts,
      qaBlocking: state.qaReports.filter((item) => item.blocking).length,
      strategyCount: state.strategyGraphs.length,
      signalCount: state.productionSignals.length,
      nonPitSources: state.providerCapabilities.filter((item) => !item.pit_supported).length,
      evidenceBlocking: state.qaReports.filter((item) =>
        item.findings.some((finding) => finding.check === "evidence_package" && Boolean(finding.blocking)),
      ).length,
    };
  }, [state]);

  const runColumns: ColumnsType<ResearchRun3> = [
    {
      title: "Run",
      dataIndex: "id",
      width: 110,
      render: (value: string, row) => <Button type="link" size="small" onClick={() => openRun(row)}>{value}</Button>,
    },
    { title: "类型", dataIndex: "run_type", width: 190 },
    {
      title: "状态",
      dataIndex: "status",
      width: 100,
      render: (value: string) => <Tag color={statusColor(value)}>{value}</Tag>,
    },
    {
      title: "阶段",
      dataIndex: "lifecycle_stage",
      width: 110,
      render: (value: string) => <Tag>{value}</Tag>,
    },
    { title: "来源", dataIndex: "created_by", width: 140 },
    { title: "创建时间", dataIndex: "created_at", width: 170, render: timeText },
  ];

  const artifactColumns: ColumnsType<ResearchArtifact3> = [
    {
      title: "Artifact",
      dataIndex: "id",
      width: 110,
      render: (value: string, row) => <Button type="link" size="small" onClick={() => openArtifact(row)}>{value}</Button>,
    },
    { title: "类型", dataIndex: "artifact_type", width: 180 },
    {
      title: "阶段",
      dataIndex: "lifecycle_stage",
      width: 110,
      render: (value: string) => <Tag color={statusColor(value)}>{value}</Tag>,
    },
    { title: "保留", dataIndex: "retention_class", width: 110 },
    { title: "大小", dataIndex: "byte_size", width: 100, render: bytesText },
    { title: "创建时间", dataIndex: "created_at", width: 170, render: timeText },
  ];

  const qaColumns: ColumnsType<QaReport3> = [
    {
      title: "QA",
      dataIndex: "id",
      width: 110,
      render: (value: string, row) => <Button type="link" size="small" onClick={() => setSelectedQa(row)}>{value}</Button>,
    },
    { title: "来源", dataIndex: "source_type", width: 160 },
    { title: "Source ID", dataIndex: "source_id", width: 150 },
    {
      title: "状态",
      dataIndex: "status",
      width: 90,
      render: (value: string) => <Tag color={statusColor(value)}>{value}</Tag>,
    },
    {
      title: "阻断",
      dataIndex: "blocking",
      width: 90,
      render: (value: boolean) => <Tag color={value ? "error" : "success"}>{value ? "yes" : "no"}</Tag>,
    },
    { title: "创建时间", dataIndex: "created_at", width: 170, render: timeText },
  ];

  const providerColumns: ColumnsType<ProviderCapability3> = [
    { title: "Provider", dataIndex: "provider", width: 110 },
    { title: "Dataset", dataIndex: "dataset", width: 170 },
    { title: "Market", dataIndex: "market_profile_id", width: 120 },
    { title: "Capability", dataIndex: "capability", width: 170 },
    {
      title: "Quality",
      dataIndex: "quality_level",
      width: 130,
      render: (value: string) => <Tag color={statusColor(value)}>{value}</Tag>,
    },
    {
      title: "PIT",
      dataIndex: "pit_supported",
      width: 80,
      render: (value: boolean) => <Tag color={value ? "success" : "warning"}>{value ? "yes" : "no"}</Tag>,
    },
    { title: "License", dataIndex: "license_scope", width: 170 },
    { title: "Availability", dataIndex: "availability", width: 190 },
  ];

  const graphColumns: ColumnsType<StrategyGraph3> = [
    {
      title: "StrategyGraph",
      dataIndex: "id",
      width: 150,
      render: (value: string, row) => <Button type="link" size="small" onClick={() => openStrategyGraph(row)}>{value}</Button>,
    },
    { title: "Name", dataIndex: "name", width: 220, ellipsis: true },
    { title: "Type", dataIndex: "graph_type", width: 150 },
    {
      title: "Status",
      dataIndex: "status",
      width: 100,
      render: (value: string) => <Tag color={statusColor(value)}>{value}</Tag>,
    },
    {
      title: "Lifecycle",
      dataIndex: "lifecycle_stage",
      width: 120,
      render: (value: string) => <Tag>{value}</Tag>,
    },
    {
      title: "Action",
      key: "action",
      width: 110,
      render: (_, row) => (
        <Button size="small" icon={<BarChartOutlined />} onClick={() => openStrategyGraph(row)}>
          回测
        </Button>
      ),
    },
  ];

  const backtestColumns: ColumnsType<BacktestRun3> = [
    {
      title: "Backtest",
      dataIndex: "id",
      width: 130,
      render: (value: string, row) => <Button type="link" size="small" onClick={() => openBacktestRun(row)}>{value}</Button>,
    },
    { title: "Start", dataIndex: "start_date", width: 110 },
    { title: "End", dataIndex: "end_date", width: 110 },
    {
      title: "Status",
      dataIndex: "status",
      width: 100,
      render: (value: string) => <Tag color={statusColor(value)}>{value}</Tag>,
    },
    {
      title: "Return",
      key: "return",
      width: 100,
      render: (_, row) => percentText(numberField(row.summary, "total_return")),
    },
    {
      title: "Final NAV",
      key: "final_nav",
      width: 120,
      render: (_, row) => currencyText(numberField(row.summary, "final_nav")),
    },
    {
      title: "Costs",
      key: "total_cost",
      width: 100,
      render: (_, row) => currencyText(numberField(row.summary, "total_cost")),
    },
    {
      title: "Created",
      dataIndex: "created_at",
      width: 170,
      render: timeText,
    },
  ];

  const macroColumns: ColumnsType<MacroSeries> = [
    { title: "Series", dataIndex: "series_id", width: 120 },
    { title: "Title", dataIndex: "title", width: 260, ellipsis: true },
    { title: "Frequency", dataIndex: "frequency", width: 120 },
    { title: "Units", dataIndex: "units", width: 160, ellipsis: true },
    { title: "Updated", dataIndex: "updated_at", width: 170, render: timeText },
  ];

  return (
    <Spin spinning={loading}>
      {contextHolder}
      <Space orientation="vertical" size={16} style={{ width: "100%" }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 16 }}>
          <Space orientation="vertical" size={2}>
            <Text strong style={{ fontSize: 18 }}>3.0 Research Workbench</Text>
            <Text type="secondary">
              {state.project ? `${state.project.name} / ${state.project.market_profile_id}` : "loading project"}
            </Text>
          </Space>
          <Space>
            <Button icon={<ClearOutlined />} onClick={previewCleanup} loading={actionLoading}>
              清理预览
            </Button>
            <Button type="primary" icon={<ReloadOutlined />} onClick={load}>
              刷新
            </Button>
          </Space>
        </div>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
            gap: 12,
          }}
        >
          <Statistic title="Runs" value={summary.runCount} prefix={<ExperimentOutlined />} />
          <Statistic title="Artifacts" value={summary.artifactCount} prefix={<FileSearchOutlined />} />
          <Statistic title="Protected" value={summary.protectedArtifacts} prefix={<CheckCircleOutlined />} />
          <Statistic title="QA Blocking" value={summary.qaBlocking} prefix={<AuditOutlined />} />
          <Statistic title="Strategy Graphs" value={summary.strategyCount} prefix={<BranchesOutlined />} />
          <Statistic title="Signals" value={summary.signalCount} prefix={<DatabaseOutlined />} />
          <Statistic title="Non-PIT Sources" value={summary.nonPitSources} prefix={<DatabaseOutlined />} />
          <Statistic title="Evidence Blocks" value={summary.evidenceBlocking} prefix={<AuditOutlined />} />
        </div>

        {state.dataStatus ? (
          <Alert
            type="info"
            showIcon
            title={`数据覆盖：${state.dataStatus.coverage.asset_count} assets / ${state.dataStatus.coverage.total_bars} bars`}
            description={`Latest trading day: ${state.dataStatus.latest_trading_day}; provider: ${state.dataStatus.provider}`}
          />
        ) : null}

        <Tabs
          items={[
            {
              key: "runs",
              label: "Runs",
              children: (
                <Table
                  rowKey="id"
                  size="small"
                  columns={runColumns}
                  dataSource={state.runs}
                  pagination={{ pageSize: 10 }}
                  scroll={{ x: 900 }}
                />
              ),
            },
            {
              key: "artifacts",
              label: "Artifacts",
              children: (
                <Table
                  rowKey="id"
                  size="small"
                  columns={artifactColumns}
                  dataSource={state.artifacts}
                  pagination={{ pageSize: 10 }}
                  scroll={{ x: 820 }}
                />
              ),
            },
            {
              key: "qa",
              label: "QA / Promotion",
              children: (
                <Space orientation="vertical" size={12} style={{ width: "100%" }}>
                  <Alert
                    type={summary.evidenceBlocking > 0 ? "warning" : "info"}
                    showIcon
                    message={`Promotion evidence blocks: ${summary.evidenceBlocking}`}
                    description="Promotion-like QA now requires lineage, data-quality contract, PIT status, split policy, dependency snapshot, valuation diagnostics, artifact hashes, and reviewer decision."
                  />
                  <Table
                    rowKey="id"
                    size="small"
                    columns={qaColumns}
                    dataSource={state.qaReports}
                    pagination={{ pageSize: 8 }}
                    scroll={{ x: 820 }}
                  />
                  <SimpleList title="Promotion Records" empty={state.promotions.length === 0}>
                    {state.promotions.map((item) => (
                      <SimpleListItem key={item.id}>
                        <Space wrap>
                          <Tag color={statusColor(item.decision)}>{item.decision}</Tag>
                          <Text>{item.source_type}</Text>
                          <Text code>{item.source_id}</Text>
                          <Text type="secondary">{timeText(item.created_at)}</Text>
                        </Space>
                      </SimpleListItem>
                    ))}
                  </SimpleList>
                </Space>
              ),
            },
            {
              key: "review",
              label: "Data Quality",
              children: (
                <Space orientation="vertical" size={12} style={{ width: "100%" }}>
                  <Alert
                    type={hasBlockedPublicationGate(state.qualityContract) ? "warning" : "info"}
                    showIcon
                    message={`Data quality contract: ${String(state.qualityContract?.summary?.highest_quality_level ?? "unknown")}`}
                    description={`Publication grade: ${String(state.qualityContract?.summary?.publication_grade ?? false)}; PIT-supported capabilities: ${state.providerCapabilities.filter((item) => item.pit_supported).length} / ${state.providerCapabilities.length}.`}
                  />
                  <SimpleList title="Publication Gates" empty={(state.qualityContract?.publication_gates ?? []).length === 0}>
                    {(state.qualityContract?.publication_gates ?? []).map((item, index) => (
                      <SimpleListItem key={`${String(item.gate ?? "gate")}-${index}`}>
                        <Space orientation="vertical" size={2}>
                          <Space wrap>
                            <Tag color={statusColor(String(item.status ?? ""))}>{String(item.status ?? "unknown")}</Tag>
                            <Text strong>{String(item.gate ?? "gate")}</Text>
                          </Space>
                          <Text type="secondary">{String(item.reason ?? "")}</Text>
                        </Space>
                      </SimpleListItem>
                    ))}
                  </SimpleList>
                  <Table
                    rowKey={(row) => `${row.provider}:${row.dataset}:${row.market_profile_id}:${row.capability}`}
                    size="small"
                    columns={providerColumns}
                    dataSource={state.providerCapabilities}
                    pagination={{ pageSize: 8 }}
                    scroll={{ x: 1130 }}
                  />
                  <Table
                    rowKey={(row) => `${row.provider}:${row.series_id}`}
                    size="small"
                    title={() => "FRED Macro Series"}
                    columns={macroColumns}
                    dataSource={state.macroSeries}
                    pagination={{ pageSize: 6 }}
                    scroll={{ x: 830 }}
                  />
                </Space>
              ),
            },
            {
              key: "assets",
              label: "Assets",
              children: (
                <Space orientation="vertical" size={12} style={{ width: "100%" }}>
                  <Table
                    rowKey="id"
                    size="small"
                    title={() => "Strategy Graphs"}
                    columns={graphColumns}
                    dataSource={state.strategyGraphs}
                    pagination={{ pageSize: 8 }}
                    scroll={{ x: 850 }}
                  />
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))", gap: 12 }}>
                    <AssetList title="Universes" items={state.universes.map((item) => [item.name, item.status, item.id])} />
                    <AssetList title="Datasets" items={state.datasets.map((item) => [item.name, item.status, item.id])} />
                    <AssetList title="Production Signals" items={state.productionSignals.map((item) => [item.strategy_graph_id, item.status, item.id])} />
                    <AssetList title="Paper Sessions" items={state.paperSessions.map((item) => [item.name, item.status, item.id])} />
                  </div>
                </Space>
              ),
            },
            {
              key: "agent",
              label: "Agent Plans",
              children: (
                <div style={{ display: "grid", gridTemplateColumns: "minmax(320px, 1fr) minmax(320px, 1fr)", gap: 12 }}>
                  <SimpleList title="Research Plans" empty={state.plans.length === 0}>
                    {state.plans.map((item) => (
                      <SimpleListItem key={item.id}>
                        <Space orientation="vertical" size={2} style={{ width: "100%" }}>
                          <Space wrap>
                            <Tag color={statusColor(item.status)}>{item.status}</Tag>
                            <Text code>{item.id}</Text>
                            <Text type="secondary">
                              trials {Number(item.budget_state?.used_trials ?? 0)} / {Number(item.budget_state?.max_trials ?? 0)}
                            </Text>
                          </Space>
                          <Paragraph ellipsis={{ rows: 2 }} style={{ margin: 0 }}>
                            {item.hypothesis}
                          </Paragraph>
                        </Space>
                      </SimpleListItem>
                    ))}
                  </SimpleList>
                  <SimpleList title="Playbooks" empty={state.playbooks.length === 0}>
                    {state.playbooks.map((item) => (
                      <SimpleListItem key={item.id}>
                        <Space orientation="vertical" size={2}>
                          <Space wrap>
                            <Tag>{item.category}</Tag>
                            <Text strong>{item.name}</Text>
                          </Space>
                          <Text type="secondary">{item.description}</Text>
                        </Space>
                      </SimpleListItem>
                    ))}
                  </SimpleList>
                </div>
              ),
            },
          ]}
        />
      </Space>

      <Drawer
        title={selectedRun ? `Run ${selectedRun.id}` : "Run"}
        open={!!selectedRun}
        onClose={() => setSelectedRun(null)}
        size={720}
      >
        {selectedRun ? (
          <Space orientation="vertical" size={16} style={{ width: "100%" }}>
            <Descriptions size="small" bordered column={1}>
              <Descriptions.Item label="Type">{selectedRun.run_type}</Descriptions.Item>
              <Descriptions.Item label="Status"><Tag color={statusColor(selectedRun.status)}>{selectedRun.status}</Tag></Descriptions.Item>
              <Descriptions.Item label="Lifecycle">{selectedRun.lifecycle_stage}</Descriptions.Item>
              <Descriptions.Item label="Params">{shortJson(selectedRun.params)}</Descriptions.Item>
              <Descriptions.Item label="Metrics">{shortJson(selectedRun.metrics_summary)}</Descriptions.Item>
            </Descriptions>
            <SimpleList title="Lineage" empty={(lineage?.edges ?? []).length === 0}>
              {(lineage?.edges ?? []).map((item) => (
                <SimpleListItem key={item.id}>
                  <Text>{item.from_type}:{item.from_id} -&gt; {item.to_type}:{item.to_id}</Text>
                </SimpleListItem>
              ))}
            </SimpleList>
          </Space>
        ) : null}
      </Drawer>

      <Drawer
        title={selectedArtifact ? `Artifact ${selectedArtifact.id}` : "Artifact"}
        open={!!selectedArtifact}
        onClose={() => setSelectedArtifact(null)}
        size={720}
        extra={
          selectedArtifact && selectedArtifact.lifecycle_stage !== "archived" ? (
            <Popconfirm
              title="确认归档这个 artifact？"
              onConfirm={() => archiveArtifact(selectedArtifact)}
            >
              <Button danger loading={actionLoading}>归档</Button>
            </Popconfirm>
          ) : null
        }
      >
        {selectedArtifact ? (
          <Descriptions size="small" bordered column={1}>
            <Descriptions.Item label="Type">{selectedArtifact.artifact_type}</Descriptions.Item>
            <Descriptions.Item label="Lifecycle"><Tag color={statusColor(selectedArtifact.lifecycle_stage)}>{selectedArtifact.lifecycle_stage}</Tag></Descriptions.Item>
            <Descriptions.Item label="Retention">{selectedArtifact.retention_class}</Descriptions.Item>
            <Descriptions.Item label="Size">{bytesText(selectedArtifact.byte_size)}</Descriptions.Item>
            <Descriptions.Item label="URI">{selectedArtifact.uri}</Descriptions.Item>
            <Descriptions.Item label="Metadata">{shortJson(selectedArtifact.metadata)}</Descriptions.Item>
          </Descriptions>
        ) : null}
      </Drawer>

      <Drawer
        title={selectedQa ? `QA ${selectedQa.id}` : "QA"}
        open={!!selectedQa}
        onClose={() => setSelectedQa(null)}
        size={720}
        extra={
          selectedQa && !selectedQa.blocking ? (
            <Button type="primary" loading={actionLoading} onClick={() => promoteQaSource(selectedQa)}>
              Promotion 评估
            </Button>
          ) : null
        }
      >
        {selectedQa ? (
          <Space orientation="vertical" size={16} style={{ width: "100%" }}>
            <Descriptions size="small" bordered column={1}>
              <Descriptions.Item label="Source">{selectedQa.source_type}:{selectedQa.source_id}</Descriptions.Item>
              <Descriptions.Item label="Status"><Tag color={statusColor(selectedQa.status)}>{selectedQa.status}</Tag></Descriptions.Item>
              <Descriptions.Item label="Blocking">{selectedQa.blocking ? "yes" : "no"}</Descriptions.Item>
              <Descriptions.Item label="Metrics">{shortJson(selectedQa.metrics)}</Descriptions.Item>
            </Descriptions>
            <SimpleList title="Findings" empty={selectedQa.findings.length === 0}>
              {selectedQa.findings.map((item, index) => (
                <SimpleListItem key={`${String(item.check ?? "check")}-${index}`}>
                  <Space orientation="vertical" size={2}>
                    <Space>
                      <Tag color={statusColor(String(item.severity ?? ""))}>{String(item.severity ?? "info")}</Tag>
                      <Text>{String(item.check ?? "check")}</Text>
                    </Space>
                    <Text type="secondary">{String(item.message ?? "")}</Text>
                  </Space>
                </SimpleListItem>
              ))}
            </SimpleList>
          </Space>
        ) : null}
      </Drawer>

      <Drawer
        title="Artifact Cleanup Preview"
        open={!!cleanupPreview}
        onClose={() => setCleanupPreview(null)}
        size={720}
      >
        {cleanupPreview ? (
          <Space orientation="vertical" size={16} style={{ width: "100%" }}>
            <Descriptions size="small" bordered column={2}>
              <Descriptions.Item label="Candidates">{cleanupPreview.summary.candidate_count}</Descriptions.Item>
              <Descriptions.Item label="Protected">{cleanupPreview.summary.protected_count}</Descriptions.Item>
              <Descriptions.Item label="Candidate Bytes">{bytesText(cleanupPreview.summary.candidate_bytes)}</Descriptions.Item>
              <Descriptions.Item label="Protected Bytes">{bytesText(cleanupPreview.summary.protected_bytes)}</Descriptions.Item>
            </Descriptions>
            <SimpleList title="Candidates" empty={cleanupPreview.candidates.length === 0}>
              {cleanupPreview.candidates.map((item) => (
                <SimpleListItem key={item.id}>
                  <Space>
                    <Text code>{item.id}</Text>
                    <Text>{item.artifact_type}</Text>
                    <Tag>{bytesText(item.byte_size)}</Tag>
                  </Space>
                </SimpleListItem>
              ))}
            </SimpleList>
          </Space>
        ) : null}
      </Drawer>

      <Drawer
        title={selectedGraph ? `StrategyGraph ${selectedGraph.id}` : "StrategyGraph"}
        open={!!selectedGraph}
        onClose={() => {
          setSelectedGraph(null);
          setGraphBacktests([]);
          setSelectedBacktest(null);
          setBacktestTaskId(null);
        }}
        size={860}
      >
        {selectedGraph ? (
          <Spin spinning={backtestLoading}>
            <Space orientation="vertical" size={16} style={{ width: "100%" }}>
              <Descriptions size="small" bordered column={1}>
                <Descriptions.Item label="Name">{selectedGraph.name}</Descriptions.Item>
                <Descriptions.Item label="Market">{selectedGraph.market_profile_id}</Descriptions.Item>
                <Descriptions.Item label="Type">{selectedGraph.graph_type}</Descriptions.Item>
                <Descriptions.Item label="Status"><Tag color={statusColor(selectedGraph.status)}>{selectedGraph.status}</Tag></Descriptions.Item>
                <Descriptions.Item label="Dependencies">{shortJson(selectedGraph.dependency_refs)}</Descriptions.Item>
              </Descriptions>
              <Form
                form={backtestForm}
                layout="inline"
                initialValues={defaultBacktestFormValues(state.dataStatus)}
                onFinish={submitGraphBacktest}
                style={{ rowGap: 8 }}
              >
                <Form.Item
                  name="start_date"
                  rules={[{ required: true, message: "start date required" }, dateRule]}
                >
                  <Input style={{ width: 130 }} placeholder="YYYY-MM-DD" />
                </Form.Item>
                <Form.Item
                  name="end_date"
                  rules={[{ required: true, message: "end date required" }, dateRule]}
                >
                  <Input style={{ width: 130 }} placeholder="YYYY-MM-DD" />
                </Form.Item>
                <Form.Item
                  name="initial_capital"
                  rules={[{ required: true, message: "capital required" }]}
                >
                  <InputNumber min={1000} step={10000} style={{ width: 150 }} />
                </Form.Item>
                <Form.Item name="price_field" rules={[{ required: true }]}>
                  <Select
                    style={{ width: 110 }}
                    options={[
                      { value: "open", label: "open" },
                      { value: "close", label: "close" },
                    ]}
                  />
                </Form.Item>
                <Button type="primary" htmlType="submit" icon={<BarChartOutlined />} loading={backtestLoading}>
                  运行回测
                </Button>
              </Form>
              {backtestTaskId ? (
                <Alert type="success" showIcon message="Backtest task queued" description={backtestTaskId} />
              ) : null}
              <Table
                rowKey="id"
                size="small"
                columns={backtestColumns}
                dataSource={graphBacktests}
                pagination={{ pageSize: 8 }}
                scroll={{ x: 940 }}
              />
              <BacktestSummaryPanel backtest={selectedBacktest ?? graphBacktests[0] ?? null} />
            </Space>
          </Spin>
        ) : null}
      </Drawer>
    </Spin>
  );
}

function BacktestSummaryPanel({ backtest }: { backtest: BacktestRun3 | null }) {
  if (!backtest) {
    return <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} />;
  }
  const diagnostics = objectField(backtest.summary, "fill_diagnostics");
  const executionModel = String(diagnostics.execution_model ?? "-");
  const executionModelCounts = objectField(diagnostics, "execution_model_counts");
  const pathWarnings = arrayField(diagnostics, "path_assumption_warnings");
  const positionDiagnostics = objectField(backtest.summary, "position_diagnostics");
  const driftRows = arrayField(positionDiagnostics, "drift");
  const warnings = arrayField(backtest.summary, "valuation_warnings");
  return (
    <Space orientation="vertical" size={12} style={{ width: "100%" }}>
      <Descriptions size="small" bordered column={2}>
        <Descriptions.Item label="Selected">{backtest.id}</Descriptions.Item>
        <Descriptions.Item label="Status"><Tag color={statusColor(backtest.status)}>{backtest.status}</Tag></Descriptions.Item>
        <Descriptions.Item label="Return">{percentText(numberField(backtest.summary, "total_return"))}</Descriptions.Item>
        <Descriptions.Item label="Final NAV">{currencyText(numberField(backtest.summary, "final_nav"))}</Descriptions.Item>
        <Descriptions.Item label="Total Cost">{currencyText(numberField(backtest.summary, "total_cost"))}</Descriptions.Item>
        <Descriptions.Item label="Days">{String(numberField(backtest.summary, "days_processed") ?? "-")}</Descriptions.Item>
      </Descriptions>
      <Descriptions size="small" bordered column={2} title="Fill Diagnostics">
        <Descriptions.Item label="Execution Model">
          <Tag color={executionModel === "mixed" ? "gold" : "blue"}>{executionModel}</Tag>
        </Descriptions.Item>
        <Descriptions.Item label="Fill Rate">{percentText(numberField(diagnostics, "fill_rate"))}</Descriptions.Item>
        <Descriptions.Item label="Filled">{String(numberField(diagnostics, "filled_order_count") ?? "-")}</Descriptions.Item>
        <Descriptions.Item label="Blocked">{String(numberField(diagnostics, "blocked_order_count") ?? "-")}</Descriptions.Item>
        <Descriptions.Item label="Path Warnings">
          {String(numberField(diagnostics, "path_assumption_warning_count") ?? 0)}
        </Descriptions.Item>
        <Descriptions.Item label="Model Counts">{shortJson(executionModelCounts)}</Descriptions.Item>
      </Descriptions>
      <SimpleList title="Daily-Bar Path Warnings" empty={pathWarnings.length === 0}>
        {pathWarnings.slice(0, 8).map((item, index) => (
          <SimpleListItem key={index}>
            <Text>{shortJson(item)}</Text>
          </SimpleListItem>
        ))}
      </SimpleList>
      <Descriptions size="small" bordered column={2} title="Position Diagnostics">
        <Descriptions.Item label="Skipped Rebalance">
          {String(numberField(positionDiagnostics, "skipped_rebalance_count") ?? 0)}
        </Descriptions.Item>
        <Descriptions.Item label="Turnover Saved">
          {percentText(numberField(positionDiagnostics, "turnover_saved"))}
        </Descriptions.Item>
        <Descriptions.Item label="Target Turnover">
          {percentText(numberField(positionDiagnostics, "turnover_before"))}
        </Descriptions.Item>
        <Descriptions.Item label="Executed Turnover">
          {percentText(numberField(positionDiagnostics, "turnover_after"))}
        </Descriptions.Item>
      </Descriptions>
      <SimpleList title="Drift / Skipped Rebalance" empty={driftRows.length === 0}>
        {driftRows.slice(0, 8).map((item, index) => (
          <SimpleListItem key={index}>
            <Text>{shortJson(item)}</Text>
          </SimpleListItem>
        ))}
      </SimpleList>
      <SimpleList title="Valuation Warnings" empty={warnings.length === 0}>
        {warnings.map((item, index) => (
          <SimpleListItem key={index}>
            <Text>{shortJson(item)}</Text>
          </SimpleListItem>
        ))}
      </SimpleList>
    </Space>
  );
}

function AssetList({ title, items }: { title: string; items: Array<[string, string, string]> }) {
  return (
    <SimpleList title={title} empty={items.length === 0}>
      {items.map(([name, status, id]) => (
        <SimpleListItem key={id}>
          <Space orientation="vertical" size={2}>
            <Space wrap>
              <Tag color={statusColor(status)}>{status}</Tag>
              <Text strong>{name}</Text>
            </Space>
            <Text type="secondary" code>{id}</Text>
          </Space>
        </SimpleListItem>
      ))}
    </SimpleList>
  );
}

function SimpleList({
  title,
  empty,
  children,
}: {
  title: string;
  empty: boolean;
  children: React.ReactNode;
}) {
  return (
    <div
      style={{
        border: "1px solid rgba(255,255,255,0.08)",
        borderRadius: 6,
        overflow: "hidden",
        background: "rgba(255,255,255,0.02)",
      }}
    >
      <div
        style={{
          padding: "8px 12px",
          borderBottom: "1px solid rgba(255,255,255,0.08)",
          fontWeight: 600,
        }}
      >
        {title}
      </div>
      {empty ? (
        <div style={{ padding: 16 }}>
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} />
        </div>
      ) : (
        <div>{children}</div>
      )}
    </div>
  );
}

function SimpleListItem({ children }: { children: React.ReactNode }) {
  return (
    <div
      style={{
        padding: "10px 12px",
        borderBottom: "1px solid rgba(255,255,255,0.06)",
      }}
    >
      {children}
    </div>
  );
}

function defaultBacktestFormValues(dataStatus: ProjectDataStatus3 | null): BacktestFormValues {
  const end = dataStatus?.latest_trading_day ?? todayText();
  const start = dataStatus?.coverage.date_range.min ?? end;
  return {
    start_date: start,
    end_date: end,
    initial_capital: 1_000_000,
    price_field: "open",
  };
}

function todayText() {
  return new Date().toISOString().slice(0, 10);
}

function numberField(source: Record<string, unknown>, key: string): number | null {
  const value = source[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function objectField(source: Record<string, unknown>, key: string): Record<string, unknown> {
  const value = source[key];
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function arrayField(source: Record<string, unknown>, key: string): unknown[] {
  const value = source[key];
  return Array.isArray(value) ? value : [];
}

function percentText(value: number | null) {
  return value == null ? "-" : `${(value * 100).toFixed(2)}%`;
}

function currencyText(value: number | null) {
  return value == null ? "-" : value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function hasBlockedPublicationGate(contract: DataQualityContract3 | null) {
  const gates = contract?.publication_gates;
  return Array.isArray(gates) && gates.some((item) => String(item.status ?? "") === "blocked");
}

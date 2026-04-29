import { useState, useEffect, useCallback, useRef } from "react";
import {
  Button,
  Card,
  Col,
  DatePicker,
  Input,
  message,
  Row,
  Select,
  Space,
  Spin,
  Typography,
  Alert,
} from "antd";
import {
  SaveOutlined,
  PlayCircleOutlined,
} from "@ant-design/icons";
import Editor from "@monaco-editor/react";
import dayjs from "dayjs";
import type { Dayjs } from "dayjs";
import {
  listTemplates,
  getTemplate,
  listLabels,
  listGroups,
  createFactor,
  updateFactor,
  evaluateFactor,
  getTaskStatus,
  getEvaluation,
} from "../../api";
import type {
  Factor,
  FactorTemplate,
  LabelDefinition,
  StockGroup,
  FactorEvalDetail,
} from "../../api";
import { EvalSummaryCards, ICSeriesChart, GroupReturnsChart } from "./EvalCharts";

const { Text } = Typography;
const { RangePicker } = DatePicker;

const CATEGORY_OPTIONS = [
  { value: "momentum", label: "动量 (Momentum)" },
  { value: "volatility", label: "波动率 (Volatility)" },
  { value: "volume", label: "成交量 (Volume)" },
  { value: "trend", label: "趋势 (Trend)" },
  { value: "statistical", label: "统计 (Statistical)" },
  { value: "custom", label: "自定义 (Custom)" },
];

import type { EvalRestoreConfig } from "./EvalHistory";

interface FactorEditorProps {
  editingFactor: Factor | null;
  evalConfig?: EvalRestoreConfig | null;
  onFactorSaved?: (factor: Factor) => void;
}

export default function FactorEditor({ editingFactor, evalConfig, onFactorSaved }: FactorEditorProps) {
  // Editor state
  const [code, setCode] = useState<string>("");
  const [factorName, setFactorName] = useState("");
  const [description, setDescription] = useState("");
  const [category, setCategory] = useState("custom");

  // Template state
  const [templates, setTemplates] = useState<FactorTemplate[]>([]);

  // Eval config state
  const [labels, setLabels] = useState<LabelDefinition[]>([]);
  const [groups, setGroups] = useState<StockGroup[]>([]);
  const [selectedLabel, setSelectedLabel] = useState<string>("");
  const [selectedGroup, setSelectedGroup] = useState<string>("");
  const [dateRange, setDateRange] = useState<[Dayjs, Dayjs]>([
    dayjs().subtract(2, "year"),
    dayjs(),
  ]);

  // Save/Eval state
  const [saving, setSaving] = useState(false);
  const [evaluating, setEvaluating] = useState(false);
  const [evalError, setEvalError] = useState<string | null>(null);

  // Current factor (after save)
  const [currentFactor, setCurrentFactor] = useState<Factor | null>(null);

  // Evaluation result
  const [evalResult, setEvalResult] = useState<FactorEvalDetail | null>(null);

  const [messageApi, contextHolder] = message.useMessage();
  const pollRef = useRef<ReturnType<typeof setInterval> | undefined>(undefined);

  // Load when editing factor changes
  useEffect(() => {
    if (editingFactor) {
      setCode(editingFactor.source_code);
      setFactorName(editingFactor.name);
      setDescription(editingFactor.description ?? "");
      setCategory(editingFactor.category);
      setCurrentFactor(editingFactor);
      setEvalResult(null);
      setEvalError(null);
    }
  }, [editingFactor]);

  // Restore eval config from history
  useEffect(() => {
    if (evalConfig) {
      setSelectedLabel(evalConfig.labelId);
      setSelectedGroup(evalConfig.groupId);
      if (evalConfig.startDate && evalConfig.endDate) {
        setDateRange([dayjs(evalConfig.startDate), dayjs(evalConfig.endDate)]);
      }
    }
  }, [evalConfig]);

  // Load templates, labels, groups on mount
  useEffect(() => {
    listTemplates().then(setTemplates).catch(() => {});
    listLabels().then(setLabels).catch(() => {});
    listGroups().then(setGroups).catch(() => {});
  }, []);

  // Cleanup poll on unmount
  useEffect(() => {
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, []);

  const handleTemplateSelect = useCallback(
    async (templateName: string) => {
      try {
        const tpl = await getTemplate(templateName);
        setCode(tpl.source_code ?? "");
        // Extract name from template
        setFactorName(templateName);
        setCurrentFactor(null);
        setEvalResult(null);
        setEvalError(null);
      } catch {
        messageApi.error("加载模板失败");
      }
    },
    [messageApi],
  );

  const handleSave = async (): Promise<Factor | null> => {
    if (!factorName.trim()) {
      messageApi.warning("请输入因子名称");
      return null;
    }
    if (!code.trim()) {
      messageApi.warning("请输入因子源代码");
      return null;
    }

    setSaving(true);
    try {
      let factor: Factor;
      if (currentFactor) {
        // Update existing factor
        factor = await updateFactor(currentFactor.id, {
          source_code: code,
          description: description || undefined,
          category,
        });
      } else {
        // Try create; if name already exists (400), find it and update instead
        try {
          factor = await createFactor({
            name: factorName,
            source_code: code,
            description: description || undefined,
            category,
          });
        } catch {
          // Name conflict - find existing factor and update it
          const { listFactors } = await import("../../api");
          const all = await listFactors();
          const existing = all.find((f) => f.name === factorName);
          if (existing) {
            factor = await updateFactor(existing.id, {
              source_code: code,
              description: description || undefined,
              category,
            });
          } else {
            throw new Error("保存失败：因子名称冲突");
          }
        }
      }
      setCurrentFactor(factor);
      messageApi.success("因子已保存");
      onFactorSaved?.(factor);
      return factor;
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "保存失败";
      messageApi.error(msg);
      return null;
    } finally {
      setSaving(false);
    }
  };

  const handleEvaluate = async () => {
    if (!selectedLabel) {
      messageApi.warning("请选择标签");
      return;
    }
    if (!selectedGroup) {
      messageApi.warning("请选择股票分组");
      return;
    }
    if (!dateRange[0] || !dateRange[1]) {
      messageApi.warning("请选择日期范围");
      return;
    }

    setEvaluating(true);
    setEvalError(null);
    setEvalResult(null);

    try {
      // Save factor first
      const factor = await handleSave();
      if (!factor) {
        setEvaluating(false);
        return;
      }

      // Trigger evaluation
      const { task_id } = await evaluateFactor(factor.id, {
        label_id: selectedLabel,
        universe_group_id: selectedGroup,
        start_date: dateRange[0].format("YYYY-MM-DD"),
        end_date: dateRange[1].format("YYYY-MM-DD"),
      });

      // Poll task status
      if (pollRef.current) clearInterval(pollRef.current);
      pollRef.current = setInterval(async () => {
        try {
          const status = await getTaskStatus(task_id);
          if (status.status === "completed") {
            if (pollRef.current) clearInterval(pollRef.current);
            pollRef.current = undefined;

            // Fetch the evaluation result from the task result
            const evalId = (status.result as Record<string, string> | null)?.id;
            if (evalId) {
              const detail = await getEvaluation(evalId);
              setEvalResult(detail);
            }
            setEvaluating(false);
            messageApi.success("评价完成");
          } else if (status.status === "failed") {
            if (pollRef.current) clearInterval(pollRef.current);
            pollRef.current = undefined;
            setEvalError(status.error ?? "评价失败");
            setEvaluating(false);
          }
        } catch {
          // Keep polling on transient errors
        }
      }, 2000);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "评价失败";
      setEvalError(msg);
      setEvaluating(false);
    }
  };

  return (
    <>
      {contextHolder}
      <Row gutter={16} style={{ height: "100%" }}>
        {/* Left: Editor Panel */}
        <Col span={10}>
          <Space orientation="vertical" style={{ width: "100%" }} size="small">
            <Select
              placeholder="选择模板..."
              style={{ width: "100%" }}
              allowClear
              showSearch
              options={templates.map((t) => ({ value: t.name, label: t.name }))}
              onChange={(val) => { if (val) handleTemplateSelect(val); }}
            />
            <Card
              size="small"
              style={{ background: "rgba(0,0,0,0.3)" }}
              styles={{ body: { padding: 0 } }}
            >
              <Editor
                height="400px"
                language="python"
                theme="vs-dark"
                value={code}
                onChange={(val) => setCode(val ?? "")}
                options={{
                  minimap: { enabled: false },
                  fontSize: 13,
                  lineNumbers: "on",
                  scrollBeyondLastLine: false,
                  automaticLayout: true,
                  tabSize: 4,
                }}
              />
            </Card>
            <Space.Compact style={{ width: "100%" }}>
              <Button disabled>名称</Button>
              <Input
                placeholder="因子名称"
                value={factorName}
                onChange={(e) => setFactorName(e.target.value)}
              />
            </Space.Compact>
            <Space.Compact style={{ width: "100%" }}>
              <Button disabled>描述</Button>
              <Input
                placeholder="描述 (可选)"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
              />
            </Space.Compact>
            <Select
              style={{ width: "100%" }}
              value={category}
              onChange={setCategory}
              options={CATEGORY_OPTIONS}
              placeholder="分类"
            />
            <Button
              type="primary"
              icon={<SaveOutlined />}
              loading={saving}
              onClick={handleSave}
              block
            >
              保存因子
            </Button>
          </Space>
        </Col>

        {/* Right: Eval Config + Results */}
        <Col span={14}>
          <Space orientation="vertical" style={{ width: "100%" }} size="small">
            <Card title="评价配置" size="small">
              <Space orientation="vertical" style={{ width: "100%" }} size="small">
                <Row gutter={8}>
                  <Col span={12}>
                    <Text type="secondary" style={{ fontSize: 12 }}>标签</Text>
                    <Select
                      style={{ width: "100%" }}
                      placeholder="选择标签..."
                      value={selectedLabel || undefined}
                      onChange={setSelectedLabel}
                      options={labels.map((l) => ({
                        value: l.id,
                        label: `${l.name} (${l.target_type}, H=${l.horizon})`,
                      }))}
                      showSearch
                      optionFilterProp="label"
                    />
                  </Col>
                  <Col span={12}>
                    <Text type="secondary" style={{ fontSize: 12 }}>股票分组</Text>
                    <Select
                      style={{ width: "100%" }}
                      placeholder="选择分组..."
                      value={selectedGroup || undefined}
                      onChange={setSelectedGroup}
                      options={groups.map((g) => ({
                        value: g.id,
                        label: `${g.name} (${g.member_count})`,
                      }))}
                      showSearch
                      optionFilterProp="label"
                    />
                  </Col>
                </Row>
                <Row gutter={8} align="middle">
                  <Col flex="auto">
                    <Text type="secondary" style={{ fontSize: 12 }}>日期范围</Text>
                    <RangePicker
                      style={{ width: "100%" }}
                      value={dateRange}
                      onChange={(vals) => {
                        if (vals && vals[0] && vals[1]) {
                          setDateRange([vals[0], vals[1]]);
                        }
                      }}
                    />
                  </Col>
                  <Col>
                    <div style={{ marginTop: 18 }}>
                      <Button
                        type="primary"
                        icon={<PlayCircleOutlined />}
                        loading={evaluating}
                        onClick={handleEvaluate}
                        style={{ background: "#52c41a", borderColor: "#52c41a" }}
                      >
                        运行评价
                      </Button>
                    </div>
                  </Col>
                </Row>
              </Space>
            </Card>

            {/* Evaluation status */}
            {evaluating && (
              <Card size="small">
                <div style={{ textAlign: "center", padding: 24 }}>
                  <Spin size="large" />
                  <div style={{ marginTop: 12 }}>
                    <Text type="secondary">正在运行评价...</Text>
                  </div>
                </div>
              </Card>
            )}

            {evalError && (
              <Alert
                type="error"
                showIcon
                message="评价错误"
                description={evalError}
                closable
                onClose={() => setEvalError(null)}
              />
            )}

            {/* Evaluation results */}
            {evalResult && (
              <Space orientation="vertical" style={{ width: "100%" }} size="small">
                <EvalSummaryCards summary={evalResult.summary} />
                <ICSeriesChart icSeries={evalResult.ic_series} />
                <GroupReturnsChart groupReturns={evalResult.group_returns} />
              </Space>
            )}
          </Space>
        </Col>
      </Row>
    </>
  );
}

import { useState, useEffect, useCallback } from "react";
import {
  Button,
  Card,
  Col,
  Input,
  message,
  Row,
  Select,
  Space,
  Typography,
} from "antd";
import { SaveOutlined, CopyOutlined } from "@ant-design/icons";
import Editor from "@monaco-editor/react";
import {
  listStrategyTemplates,
  getStrategyTemplate,
  listStrategies,
  createStrategy,
  updateStrategy,
} from "../../api";
import type { Strategy, StrategyTemplate } from "../../api";

const { Text } = Typography;

const POSITION_OPTIONS = [
  { value: "equal_weight", label: "等权 (Equal Weight)" },
  { value: "value_weight", label: "市值加权 (Value Weight)" },
  { value: "risk_parity", label: "风险平价 (Risk Parity)" },
  { value: "custom", label: "自定义 (Custom)" },
];

interface StrategyEditorPanelProps {
  editingStrategy?: Strategy | null;
  onStrategySaved?: () => void;
}

export default function StrategyEditorPanel({
  editingStrategy,
  onStrategySaved,
}: StrategyEditorPanelProps) {
  const [code, setCode] = useState("");
  const [strategyName, setStrategyName] = useState("");
  const [description, setDescription] = useState("");
  const [positionSizing, setPositionSizing] = useState("equal_weight");
  const [templates, setTemplates] = useState<StrategyTemplate[]>([]);
  const [saving, setSaving] = useState(false);
  const [currentStrategy, setCurrentStrategy] = useState<Strategy | null>(null);
  const [messageApi, contextHolder] = message.useMessage();

  useEffect(() => {
    listStrategyTemplates().then(setTemplates).catch(() => {});
  }, []);

  useEffect(() => {
    if (editingStrategy) {
      setCode(editingStrategy.source_code);
      setStrategyName(editingStrategy.name);
      setDescription(editingStrategy.description ?? "");
      setPositionSizing(editingStrategy.position_sizing);
      setCurrentStrategy(editingStrategy);
    }
  }, [editingStrategy]);

  const handleTemplateSelect = useCallback(
    async (templateName: string) => {
      try {
        const tpl = await getStrategyTemplate(templateName);
        setCode(tpl.source_code ?? "");
        setStrategyName(templateName);
        setCurrentStrategy(null);
      } catch {
        messageApi.error("加载模板失败");
      }
    },
    [messageApi],
  );

  const handleSave = async () => {
    if (!strategyName.trim()) {
      messageApi.warning("请输入策略名称");
      return;
    }
    if (!code.trim()) {
      messageApi.warning("请输入策略代码");
      return;
    }

    setSaving(true);
    try {
      if (currentStrategy) {
        const updated = await updateStrategy(currentStrategy.id, {
          source_code: code,
          description: description || undefined,
          position_sizing: positionSizing,
        });
        setCurrentStrategy(updated);
      } else {
        // Try create; if name already exists (400), find it and update instead
        try {
          const created = await createStrategy({
            name: strategyName,
            source_code: code,
            description: description || undefined,
            position_sizing: positionSizing,
          });
          setCurrentStrategy(created);
        } catch {
          const all = await listStrategies();
          const existing = all.find((s) => s.name === strategyName);
          if (existing) {
            const updated = await updateStrategy(existing.id, {
              source_code: code,
              description: description || undefined,
              position_sizing: positionSizing,
            });
            setCurrentStrategy(updated);
          } else {
            throw new Error("保存失败");
          }
        }
      }
      messageApi.success("策略已保存");
      onStrategySaved?.();
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "保存失败";
      messageApi.error(msg);
    } finally {
      setSaving(false);
    }
  };

  const handleSaveAs = async () => {
    if (!code.trim()) {
      messageApi.warning("请输入策略代码");
      return;
    }

    const newName = strategyName.trim()
      ? `${strategyName.trim()} (副本)`
      : "未命名策略 (副本)";

    setStrategyName(newName);
    setCurrentStrategy(null);

    setSaving(true);
    try {
      try {
        const created = await createStrategy({
          name: newName,
          source_code: code,
          description: description || undefined,
          position_sizing: positionSizing,
        });
        setCurrentStrategy(created);
      } catch {
        const all = await listStrategies();
        const existing = all.find((s) => s.name === newName);
        if (existing) {
          const updated = await updateStrategy(existing.id, {
            source_code: code,
            description: description || undefined,
            position_sizing: positionSizing,
          });
          setCurrentStrategy(updated);
        } else {
          throw new Error("另存为失败");
        }
      }
      messageApi.success("已另存为新策略");
      onStrategySaved?.();
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "另存为失败";
      messageApi.error(msg);
    } finally {
      setSaving(false);
    }
  };

  return (
    <>
      {contextHolder}
      <Row gutter={16}>
        <Col span={14}>
          <Space orientation="vertical" style={{ width: "100%" }} size="small">
            <Select
              placeholder="选择策略模板..."
              style={{ width: "100%" }}
              allowClear
              showSearch
              options={templates.map((t) => ({ value: t.name, label: t.name }))}
              onChange={(val) => {
                if (val) handleTemplateSelect(val);
              }}
            />
            <Card
              size="small"
              style={{ background: "rgba(0,0,0,0.3)" }}
              styles={{ body: { padding: 0 } }}
            >
              <Editor
                height="480px"
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
          </Space>
        </Col>

        <Col span={10}>
          <Space orientation="vertical" style={{ width: "100%" }} size="small">
            <Card title="策略元信息" size="small">
              <Space orientation="vertical" style={{ width: "100%" }} size="small">
                <div>
                  <Text type="secondary" style={{ fontSize: 12 }}>名称</Text>
                  <Input
                    placeholder="策略名称"
                    value={strategyName}
                    onChange={(e) => setStrategyName(e.target.value)}
                  />
                </div>
                <div>
                  <Text type="secondary" style={{ fontSize: 12 }}>描述</Text>
                  <Input.TextArea
                    placeholder="策略描述 (可选)"
                    value={description}
                    onChange={(e) => setDescription(e.target.value)}
                    rows={3}
                  />
                </div>
                <div>
                  <Text type="secondary" style={{ fontSize: 12 }}>仓位管理</Text>
                  <Select
                    style={{ width: "100%" }}
                    value={positionSizing}
                    onChange={setPositionSizing}
                    options={POSITION_OPTIONS}
                  />
                </div>
              </Space>
            </Card>

            <Button
              type="primary"
              icon={<SaveOutlined />}
              loading={saving}
              onClick={handleSave}
              block
            >
              保存策略
            </Button>
            <Button
              icon={<CopyOutlined />}
              loading={saving}
              onClick={handleSaveAs}
              block
            >
              另存为新策略
            </Button>
          </Space>
        </Col>
      </Row>
    </>
  );
}

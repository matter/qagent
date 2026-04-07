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
import { SaveOutlined } from "@ant-design/icons";
import {
  listFactors,
  listGroups,
  createFeatureSet,
  getCorrelationMatrix,
} from "../../api";
import type {
  Factor,
  StockGroup,
  CorrelationMatrix,
} from "../../api";
import CorrelationHeatmap from "./CorrelationHeatmap";

const { Text } = Typography;

const MISSING_OPTIONS = [
  { value: "ffill", label: "前向填充 (ffill)" },
  { value: "drop", label: "删除 (drop)" },
  { value: "median", label: "中位数填充 (median)" },
  { value: "zero", label: "零填充 (zero)" },
];

const OUTLIER_OPTIONS = [
  { value: "winsorize", label: "缩尾 (winsorize)" },
  { value: "clip", label: "截断 (clip)" },
  { value: "zscore", label: "Z-Score" },
  { value: "none", label: "不处理" },
];

const NORMALIZE_OPTIONS = [
  { value: "zscore", label: "Z-Score" },
  { value: "minmax", label: "Min-Max" },
  { value: "rank", label: "排名 (Rank)" },
  { value: "none", label: "不处理" },
];

const NEUTRALIZE_OPTIONS = [
  { value: "industry", label: "行业中性化" },
  { value: "market", label: "市值中性化" },
  { value: "both", label: "行业+市值" },
  { value: "none", label: "不处理" },
];

import type { FeatureSetRestoreConfig } from "./FeatureSetList";

interface FeatureSetCreatorProps {
  onCreated?: () => void;
  restoreConfig?: FeatureSetRestoreConfig | null;
}

export default function FeatureSetCreator({ onCreated, restoreConfig }: FeatureSetCreatorProps) {
  const [factors, setFactors] = useState<Factor[]>([]);
  const [groups, setGroups] = useState<StockGroup[]>([]);
  const [selectedFactorIds, setSelectedFactorIds] = useState<string[]>([]);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [missing, setMissing] = useState("ffill");
  const [outlier, setOutlier] = useState("winsorize");
  const [normalize, setNormalize] = useState("zscore");
  const [neutralize, setNeutralize] = useState("none");
  const [saving, setSaving] = useState(false);
  const [corrData, setCorrData] = useState<CorrelationMatrix | null>(null);
  const [corrLoading, setCorrLoading] = useState(false);
  const [corrGroup, setCorrGroup] = useState<string>("");
  const [messageApi, contextHolder] = message.useMessage();

  useEffect(() => {
    listFactors().then(setFactors).catch(() => {});
    listGroups().then(setGroups).catch(() => {});
  }, []);

  // Restore config from feature set list
  useEffect(() => {
    if (restoreConfig) {
      setSelectedFactorIds(restoreConfig.factorIds);
      const pp = restoreConfig.preprocessing;
      if (pp) {
        setMissing(pp.missing ?? "ffill");
        setOutlier(pp.outlier ?? "winsorize");
        setNormalize(pp.normalize ?? "zscore");
        setNeutralize(pp.neutralize ?? "none");
      }
    }
  }, [restoreConfig]);

  const selectedFactors = factors.filter((f) => selectedFactorIds.includes(f.id));

  const handleComputeCorrelation = useCallback(async (fsId: string, groupId: string) => {
    setCorrLoading(true);
    try {
      const data = await getCorrelationMatrix(fsId, {
        universe_group_id: groupId,
        start_date: "2020-01-01",
        end_date: "2024-01-01",
      });
      setCorrData(data);
    } catch {
      messageApi.error("计算相关性矩阵失败");
    } finally {
      setCorrLoading(false);
    }
  }, [messageApi]);

  const handleSave = async () => {
    if (!name.trim()) {
      messageApi.warning("请输入特征集名称");
      return;
    }
    if (selectedFactorIds.length === 0) {
      messageApi.warning("请至少选择一个因子");
      return;
    }

    setSaving(true);
    try {
      const factorRefs = selectedFactors.map((f) => ({
        factor_id: f.id,
        factor_name: f.name,
        version: f.version,
      }));
      const fs = await createFeatureSet({
        name,
        description: description || undefined,
        factor_refs: factorRefs,
        preprocessing: { missing, outlier, normalize, neutralize },
      });

      messageApi.success("特征集已保存");

      // Optionally compute correlation if group selected
      if (corrGroup) {
        handleComputeCorrelation(fs.id, corrGroup);
      }

      setName("");
      setDescription("");
      setSelectedFactorIds([]);
      setCorrData(null);
      onCreated?.();
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "保存失败";
      messageApi.error(msg);
    } finally {
      setSaving(false);
    }
  };

  return (
    <>
      {contextHolder}
      <Card title="创建特征集" size="small">
        <Space direction="vertical" style={{ width: "100%" }} size="middle">
          <div>
            <Text type="secondary" style={{ fontSize: 12 }}>选择因子</Text>
            <Select
              mode="multiple"
              style={{ width: "100%" }}
              placeholder="从因子库中选择..."
              value={selectedFactorIds}
              onChange={setSelectedFactorIds}
              optionFilterProp="label"
              showSearch
              options={factors.map((f) => ({
                value: f.id,
                label: `${f.name} v${f.version} [${f.category}]`,
              }))}
            />
          </div>

          {selectedFactorIds.length > 1 && (
            <div>
              <Text type="secondary" style={{ fontSize: 12 }}>相关性分析 (选择股票分组后可计算)</Text>
              <Row gutter={8}>
                <Col flex="auto">
                  <Select
                    style={{ width: "100%" }}
                    placeholder="选择股票分组..."
                    value={corrGroup || undefined}
                    onChange={setCorrGroup}
                    options={groups.map((g) => ({
                      value: g.id,
                      label: `${g.name} (${g.member_count})`,
                    }))}
                    allowClear
                  />
                </Col>
                <Col>
                  <Button
                    loading={corrLoading}
                    disabled={!corrGroup || selectedFactorIds.length < 2}
                    onClick={() => {
                      // Need to save first to get an ID, or show info
                      messageApi.info("请先保存特征集，然后在列表中查看相关性");
                    }}
                  >
                    计算相关性
                  </Button>
                </Col>
              </Row>
              {corrData && <CorrelationHeatmap data={corrData} />}
            </div>
          )}

          <Row gutter={[12, 12]}>
            <Col span={6}>
              <Text type="secondary" style={{ fontSize: 12 }}>缺失值处理</Text>
              <Select
                style={{ width: "100%" }}
                value={missing}
                onChange={setMissing}
                options={MISSING_OPTIONS}
              />
            </Col>
            <Col span={6}>
              <Text type="secondary" style={{ fontSize: 12 }}>异常值处理</Text>
              <Select
                style={{ width: "100%" }}
                value={outlier}
                onChange={setOutlier}
                options={OUTLIER_OPTIONS}
              />
            </Col>
            <Col span={6}>
              <Text type="secondary" style={{ fontSize: 12 }}>标准化</Text>
              <Select
                style={{ width: "100%" }}
                value={normalize}
                onChange={setNormalize}
                options={NORMALIZE_OPTIONS}
              />
            </Col>
            <Col span={6}>
              <Text type="secondary" style={{ fontSize: 12 }}>中性化</Text>
              <Select
                style={{ width: "100%" }}
                value={neutralize}
                onChange={setNeutralize}
                options={NEUTRALIZE_OPTIONS}
              />
            </Col>
          </Row>

          <Row gutter={12}>
            <Col span={12}>
              <Input
                placeholder="特征集名称"
                value={name}
                onChange={(e) => setName(e.target.value)}
                addonBefore="名称"
              />
            </Col>
            <Col span={12}>
              <Input
                placeholder="描述 (可选)"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                addonBefore="描述"
              />
            </Col>
          </Row>

          <Button
            type="primary"
            icon={<SaveOutlined />}
            loading={saving}
            onClick={handleSave}
            block
          >
            保存特征集
          </Button>
        </Space>
      </Card>
    </>
  );
}

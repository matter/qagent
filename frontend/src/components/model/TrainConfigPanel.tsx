import { useState, useEffect, useRef } from "react";
import {
  Button,
  Card,
  Col,
  Collapse,
  DatePicker,
  Input,
  InputNumber,
  message,
  Row,
  Select,
  Space,
  Spin,
  Typography,
  Alert,
} from "antd";
import { PlayCircleOutlined } from "@ant-design/icons";
import dayjs from "dayjs";
import type { Dayjs } from "dayjs";
import {
  listFeatureSets,
  listLabels,
  listGroups,
  trainModel,
  getTaskStatus,
} from "../../api";
import type {
  FeatureSet,
  LabelDefinition,
  StockGroup,
} from "../../api";

const { Text } = Typography;

interface TrainConfigPanelProps {
  onTrainComplete?: () => void;
}

export default function TrainConfigPanel({ onTrainComplete }: TrainConfigPanelProps) {
  const [featureSets, setFeatureSets] = useState<FeatureSet[]>([]);
  const [labels, setLabels] = useState<LabelDefinition[]>([]);
  const [groups, setGroups] = useState<StockGroup[]>([]);

  const [selectedFS, setSelectedFS] = useState<string>("");
  const [selectedLabel, setSelectedLabel] = useState<string>("");
  const [selectedGroup, setSelectedGroup] = useState<string>("");
  const [modelName, setModelName] = useState("");

  const [trainStart, setTrainStart] = useState<Dayjs>(dayjs("2018-01-01"));
  const [trainEnd, setTrainEnd] = useState<Dayjs>(dayjs("2021-12-31"));
  const [validStart, setValidStart] = useState<Dayjs>(dayjs("2022-01-01"));
  const [validEnd, setValidEnd] = useState<Dayjs>(dayjs("2022-12-31"));
  const [testStart, setTestStart] = useState<Dayjs>(dayjs("2023-01-01"));
  const [testEnd, setTestEnd] = useState<Dayjs>(dayjs("2023-12-31"));
  const [purgeGap, setPurgeGap] = useState<number>(5);

  // LightGBM model parameters
  const [nEstimators, setNEstimators] = useState<number>(200);
  const [maxDepth, setMaxDepth] = useState<number>(6);
  const [learningRate, setLearningRate] = useState<number>(0.05);
  const [numLeaves, setNumLeaves] = useState<number>(31);
  const [minChildSamples, setMinChildSamples] = useState<number>(20);
  const [subsample, setSubsample] = useState<number>(0.8);
  const [colsampleBytree, setColsampleBytree] = useState<number>(0.8);
  const [regAlpha, setRegAlpha] = useState<number>(0.0);
  const [regLambda, setRegLambda] = useState<number>(0.0);

  const [training, setTraining] = useState(false);
  const [trainError, setTrainError] = useState<string | null>(null);

  const [messageApi, contextHolder] = message.useMessage();
  const pollRef = useRef<ReturnType<typeof setInterval> | undefined>(undefined);

  useEffect(() => {
    listFeatureSets().then(setFeatureSets).catch(() => {});
    listLabels().then(setLabels).catch(() => {});
    listGroups().then(setGroups).catch(() => {});
  }, []);

  useEffect(() => {
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, []);

  const handleTrain = async () => {
    if (!selectedFS) { messageApi.warning("请选择特征集"); return; }
    if (!selectedLabel) { messageApi.warning("请选择标签"); return; }
    if (!selectedGroup) { messageApi.warning("请选择股票分组"); return; }
    if (!modelName.trim()) { messageApi.warning("请输入模型名称"); return; }

    setTraining(true);
    setTrainError(null);

    try {
      const { task_id } = await trainModel({
        name: modelName,
        feature_set_id: selectedFS,
        label_id: selectedLabel,
        model_type: "lightgbm",
        model_params: {
          n_estimators: nEstimators,
          max_depth: maxDepth,
          learning_rate: learningRate,
          num_leaves: numLeaves,
          min_child_samples: minChildSamples,
          subsample,
          colsample_bytree: colsampleBytree,
          reg_alpha: regAlpha,
          reg_lambda: regLambda,
        },
        train_config: {
          method: "single_split",
          train_period: {
            start: trainStart.format("YYYY-MM-DD"),
            end: trainEnd.format("YYYY-MM-DD"),
          },
          valid_period: {
            start: validStart.format("YYYY-MM-DD"),
            end: validEnd.format("YYYY-MM-DD"),
          },
          test_period: {
            start: testStart.format("YYYY-MM-DD"),
            end: testEnd.format("YYYY-MM-DD"),
          },
          purge_gap: purgeGap,
        },
        universe_group_id: selectedGroup,
      });

      // Poll task status
      if (pollRef.current) clearInterval(pollRef.current);
      pollRef.current = setInterval(async () => {
        try {
          const status = await getTaskStatus(task_id);
          if (status.status === "completed") {
            if (pollRef.current) clearInterval(pollRef.current);
            pollRef.current = undefined;
            setTraining(false);
            messageApi.success("模型训练完成");
            onTrainComplete?.();
          } else if (status.status === "failed") {
            if (pollRef.current) clearInterval(pollRef.current);
            pollRef.current = undefined;
            setTrainError(status.error ?? "训练失败");
            setTraining(false);
          }
        } catch {
          // Keep polling on transient errors
        }
      }, 3000);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "训练失败";
      setTrainError(msg);
      setTraining(false);
    }
  };

  return (
    <>
      {contextHolder}
      <Space direction="vertical" style={{ width: "100%" }} size="middle">
        <Card title="数据配置" size="small">
          <Row gutter={[12, 12]}>
            <Col span={8}>
              <Text type="secondary" style={{ fontSize: 12 }}>特征集</Text>
              <Select
                style={{ width: "100%" }}
                placeholder="选择特征集..."
                value={selectedFS || undefined}
                onChange={setSelectedFS}
                options={featureSets.map((fs) => ({
                  value: fs.id,
                  label: `${fs.name} (${fs.factor_refs?.length ?? 0}因子)`,
                }))}
                showSearch
                optionFilterProp="label"
              />
            </Col>
            <Col span={8}>
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
            <Col span={8}>
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
        </Card>

        <Card title="训练配置" size="small">
          <Space direction="vertical" style={{ width: "100%" }} size="small">
            <Row gutter={12}>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: 12 }}>模型类型</Text>
                <Select
                  style={{ width: "100%" }}
                  value="lightgbm"
                  disabled
                  options={[{ value: "lightgbm", label: "LightGBM" }]}
                />
              </Col>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: 12 }}>划分方法</Text>
                <Select
                  style={{ width: "100%" }}
                  value="single_split"
                  disabled
                  options={[{ value: "single_split", label: "单次划分 (Single Split)" }]}
                />
              </Col>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: 12 }}>Purge Gap</Text>
                <InputNumber
                  style={{ width: "100%" }}
                  value={purgeGap}
                  onChange={(v) => setPurgeGap(v ?? 5)}
                  min={0}
                  max={60}
                />
              </Col>
            </Row>

            <Row gutter={12}>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: 12 }}>训练区间</Text>
                <Space.Compact style={{ width: "100%" }}>
                  <DatePicker
                    value={trainStart}
                    onChange={(v) => { if (v) setTrainStart(v); }}
                    style={{ width: "50%" }}
                  />
                  <DatePicker
                    value={trainEnd}
                    onChange={(v) => { if (v) setTrainEnd(v); }}
                    style={{ width: "50%" }}
                  />
                </Space.Compact>
              </Col>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: 12 }}>验证区间</Text>
                <Space.Compact style={{ width: "100%" }}>
                  <DatePicker
                    value={validStart}
                    onChange={(v) => { if (v) setValidStart(v); }}
                    style={{ width: "50%" }}
                  />
                  <DatePicker
                    value={validEnd}
                    onChange={(v) => { if (v) setValidEnd(v); }}
                    style={{ width: "50%" }}
                  />
                </Space.Compact>
              </Col>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: 12 }}>测试区间</Text>
                <Space.Compact style={{ width: "100%" }}>
                  <DatePicker
                    value={testStart}
                    onChange={(v) => { if (v) setTestStart(v); }}
                    style={{ width: "50%" }}
                  />
                  <DatePicker
                    value={testEnd}
                    onChange={(v) => { if (v) setTestEnd(v); }}
                    style={{ width: "50%" }}
                  />
                </Space.Compact>
              </Col>
            </Row>
          </Space>
        </Card>

        <Collapse
          size="small"
          items={[
            {
              key: "model_params",
              label: "模型参数 (LightGBM)",
              children: (
                <Space direction="vertical" style={{ width: "100%" }} size="small">
                  <Row gutter={12}>
                    <Col span={8}>
                      <Text type="secondary" style={{ fontSize: 12 }}>n_estimators</Text>
                      <InputNumber
                        style={{ width: "100%" }}
                        value={nEstimators}
                        onChange={(v) => setNEstimators(v ?? 200)}
                        min={10}
                        max={2000}
                      />
                    </Col>
                    <Col span={8}>
                      <Text type="secondary" style={{ fontSize: 12 }}>max_depth</Text>
                      <InputNumber
                        style={{ width: "100%" }}
                        value={maxDepth}
                        onChange={(v) => setMaxDepth(v ?? 6)}
                        min={2}
                        max={15}
                      />
                    </Col>
                    <Col span={8}>
                      <Text type="secondary" style={{ fontSize: 12 }}>learning_rate</Text>
                      <InputNumber
                        style={{ width: "100%" }}
                        value={learningRate}
                        onChange={(v) => setLearningRate(v ?? 0.05)}
                        min={0.001}
                        max={1.0}
                        step={0.01}
                      />
                    </Col>
                  </Row>
                  <Row gutter={12}>
                    <Col span={8}>
                      <Text type="secondary" style={{ fontSize: 12 }}>num_leaves</Text>
                      <InputNumber
                        style={{ width: "100%" }}
                        value={numLeaves}
                        onChange={(v) => setNumLeaves(v ?? 31)}
                        min={8}
                        max={256}
                      />
                    </Col>
                    <Col span={8}>
                      <Text type="secondary" style={{ fontSize: 12 }}>min_child_samples</Text>
                      <InputNumber
                        style={{ width: "100%" }}
                        value={minChildSamples}
                        onChange={(v) => setMinChildSamples(v ?? 20)}
                        min={5}
                        max={200}
                      />
                    </Col>
                    <Col span={8}>
                      <Text type="secondary" style={{ fontSize: 12 }}>subsample</Text>
                      <InputNumber
                        style={{ width: "100%" }}
                        value={subsample}
                        onChange={(v) => setSubsample(v ?? 0.8)}
                        min={0.1}
                        max={1.0}
                        step={0.05}
                      />
                    </Col>
                  </Row>
                  <Row gutter={12}>
                    <Col span={8}>
                      <Text type="secondary" style={{ fontSize: 12 }}>colsample_bytree</Text>
                      <InputNumber
                        style={{ width: "100%" }}
                        value={colsampleBytree}
                        onChange={(v) => setColsampleBytree(v ?? 0.8)}
                        min={0.1}
                        max={1.0}
                        step={0.05}
                      />
                    </Col>
                    <Col span={8}>
                      <Text type="secondary" style={{ fontSize: 12 }}>reg_alpha</Text>
                      <InputNumber
                        style={{ width: "100%" }}
                        value={regAlpha}
                        onChange={(v) => setRegAlpha(v ?? 0.0)}
                        min={0}
                        max={10}
                        step={0.01}
                      />
                    </Col>
                    <Col span={8}>
                      <Text type="secondary" style={{ fontSize: 12 }}>reg_lambda</Text>
                      <InputNumber
                        style={{ width: "100%" }}
                        value={regLambda}
                        onChange={(v) => setRegLambda(v ?? 0.0)}
                        min={0}
                        max={10}
                        step={0.01}
                      />
                    </Col>
                  </Row>
                </Space>
              ),
            },
          ]}
        />

        <Card size="small">
          <Row gutter={12} align="middle">
            <Col flex="auto">
              <Input
                placeholder="模型名称"
                value={modelName}
                onChange={(e) => setModelName(e.target.value)}
                addonBefore="名称"
              />
            </Col>
            <Col>
              <Button
                type="primary"
                icon={<PlayCircleOutlined />}
                loading={training}
                onClick={handleTrain}
                style={{ background: "#52c41a", borderColor: "#52c41a" }}
              >
                开始训练
              </Button>
            </Col>
          </Row>
        </Card>

        {training && (
          <Card size="small">
            <div style={{ textAlign: "center", padding: 24 }}>
              <Spin size="large" />
              <div style={{ marginTop: 12 }}>
                <Text type="secondary">正在训练模型...</Text>
              </div>
            </div>
          </Card>
        )}

        {trainError && (
          <Alert
            type="error"
            showIcon
            message="训练错误"
            description={trainError}
            closable
            onClose={() => setTrainError(null)}
          />
        )}
      </Space>
    </>
  );
}

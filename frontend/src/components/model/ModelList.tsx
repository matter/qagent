import { useState, useEffect, useCallback } from "react";
import {
  Button,
  Card,
  Col,
  Descriptions,
  Input,
  message,
  Modal,
  Popconfirm,
  Row,
  Space,
  Statistic,
  Table,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import { ReloadOutlined, DeleteOutlined, RollbackOutlined, SearchOutlined } from "@ant-design/icons";
import { listModels, getModel, deleteModel } from "../../api";
import type { Model } from "../../api";
import FeatureImportanceChart from "./FeatureImportanceChart";

const { Text } = Typography;

const STATUS_TAG: Record<string, { color: string; label: string }> = {
  training: { color: "processing", label: "训练中" },
  trained: { color: "success", label: "已训练" },
  ready: { color: "success", label: "就绪" },
  failed: { color: "error", label: "失败" },
  archived: { color: "warning", label: "已归档" },
};

function metricVal(r: Model, key: string): number | undefined {
  const em = r.eval_metrics as Record<string, unknown> | null;
  if (!em) return undefined;
  const v = em[key] as number | undefined;
  return v ?? undefined;
}

export interface ModelRestoreConfig {
  featureSetId: string;
  labelId: string;
  groupId: string;
  modelParams: Record<string, unknown> | null;
  trainConfig: Record<string, unknown> | null;
}

interface ModelListProps {
  refreshKey?: number;
  onRestoreConfig?: (config: ModelRestoreConfig) => void;
}

export default function ModelList({ refreshKey, onRestoreConfig }: ModelListProps) {
  const [models, setModels] = useState<Model[]>([]);
  const [loading, setLoading] = useState(false);
  const [idSearch, setIdSearch] = useState<string>("");
  const [detailOpen, setDetailOpen] = useState(false);
  const [detailModel, setDetailModel] = useState<Model | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [messageApi, contextHolder] = message.useMessage();

  const fetchModels = useCallback(async () => {
    setLoading(true);
    try {
      const data = await listModels();
      setModels(data);
    } catch {
      messageApi.error("加载模型列表失败");
    } finally {
      setLoading(false);
    }
  }, [messageApi]);

  useEffect(() => {
    fetchModels();
  }, [fetchModels, refreshKey]);

  const handleDelete = async (id: string) => {
    try {
      await deleteModel(id);
      messageApi.success("模型已删除");
      fetchModels();
    } catch {
      messageApi.error("删除失败");
    }
  };

  const handleRowClick = async (record: Model) => {
    setDetailOpen(true);
    setDetailLoading(true);
    setDetailModel(null);
    try {
      const detail = await getModel(record.id);
      setDetailModel(detail);
    } catch {
      messageApi.error("加载模型详情失败");
      setDetailOpen(false);
    } finally {
      setDetailLoading(false);
    }
  };

  const metrics = detailModel?.eval_metrics as Record<string, unknown> | null;
  const featureImportance = (metrics?.feature_importance ?? null) as Record<string, number> | null;
  const trainConfig = detailModel?.train_config as Record<string, string> | null;
  const modelParams = detailModel?.model_params as Record<string, string | number> | null;

  const handleRestore = (record: Model, e: React.MouseEvent) => {
    e.stopPropagation();
    if (!onRestoreConfig) return;
    const tc = record.train_config as Record<string, unknown> | null;
    onRestoreConfig({
      featureSetId: record.feature_set_id,
      labelId: record.label_id,
      groupId: (tc?.universe_group_id as string) ?? "",
      modelParams: record.model_params,
      trainConfig: record.train_config,
    });
    messageApi.success("已还原训练配置");
  };

  const displayData = idSearch
    ? models.filter((m) => m.id.toLowerCase().includes(idSearch.toLowerCase()))
    : models;

  const columns = [
    {
      title: "ID",
      dataIndex: "id",
      key: "id",
      width: 90,
      ellipsis: true,
      render: (id: string) => (
        <Tooltip title={id}>
          <Text copyable={{ text: id }} style={{ fontSize: 12 }}>
            {id.slice(0, 8)}
          </Text>
        </Tooltip>
      ),
    },
    {
      title: "名称",
      dataIndex: "name",
      key: "name",
      render: (name: string, record: Model) => (
        <a onClick={(e) => { e.stopPropagation(); handleRowClick(record); }}>{name}</a>
      ),
    },
    {
      title: "类型",
      dataIndex: "model_type",
      key: "model_type",
      width: 90,
      render: (v: string) => <Tag color="blue">{v}</Tag>,
    },
    {
      title: "状态",
      dataIndex: "status",
      key: "status",
      width: 80,
      render: (s: string) => {
        const cfg = STATUS_TAG[s] ?? { color: "default", label: s };
        return <Tag color={cfg.color}>{cfg.label}</Tag>;
      },
    },
    {
      title: "IC",
      key: "ic_mean",
      width: 80,
      sorter: (a: Model, b: Model) => (metricVal(a, "ic_mean") ?? 0) - (metricVal(b, "ic_mean") ?? 0),
      render: (_: unknown, r: Model) => {
        const v = metricVal(r, "ic_mean");
        if (v === undefined) return <Text type="secondary">-</Text>;
        return (
          <Text style={{ color: v > 0 ? "#52c41a" : "#ff4d4f" }}>
            {v.toFixed(4)}
          </Text>
        );
      },
    },
    {
      title: "IR",
      key: "ir",
      width: 70,
      sorter: (a: Model, b: Model) => (metricVal(a, "ir") ?? 0) - (metricVal(b, "ir") ?? 0),
      render: (_: unknown, r: Model) => {
        const v = metricVal(r, "ir");
        if (v === undefined) return <Text type="secondary">-</Text>;
        return (
          <Text style={{ color: v > 1 ? "#52c41a" : v > 0 ? "#1677ff" : "#ff4d4f" }}>
            {v.toFixed(3)}
          </Text>
        );
      },
    },
    {
      title: "Sharpe",
      key: "sharpe",
      width: 80,
      sorter: (a: Model, b: Model) => (metricVal(a, "sharpe") ?? 0) - (metricVal(b, "sharpe") ?? 0),
      render: (_: unknown, r: Model) => {
        const v = metricVal(r, "sharpe");
        if (v === undefined) return <Text type="secondary">-</Text>;
        return (
          <Text style={{ color: v > 1 ? "#52c41a" : v > 0 ? "#1677ff" : "#ff4d4f" }}>
            {v.toFixed(3)}
          </Text>
        );
      },
    },
    {
      title: "年化收益",
      key: "annual_return",
      width: 90,
      sorter: (a: Model, b: Model) => (metricVal(a, "annual_return") ?? 0) - (metricVal(b, "annual_return") ?? 0),
      render: (_: unknown, r: Model) => {
        const v = metricVal(r, "annual_return");
        if (v === undefined) return <Text type="secondary">-</Text>;
        return (
          <Text style={{ color: v > 0 ? "#52c41a" : "#ff4d4f" }}>
            {(v * 100).toFixed(2)}%
          </Text>
        );
      },
    },
    {
      title: "最大回撤",
      key: "max_drawdown",
      width: 90,
      sorter: (a: Model, b: Model) => (metricVal(a, "max_drawdown") ?? 0) - (metricVal(b, "max_drawdown") ?? 0),
      render: (_: unknown, r: Model) => {
        const v = metricVal(r, "max_drawdown");
        if (v === undefined) return <Text type="secondary">-</Text>;
        return (
          <Text style={{ color: "#ff4d4f" }}>
            {(v * 100).toFixed(2)}%
          </Text>
        );
      },
    },
    {
      title: "Calmar",
      key: "calmar",
      width: 80,
      sorter: (a: Model, b: Model) => (metricVal(a, "calmar") ?? 0) - (metricVal(b, "calmar") ?? 0),
      render: (_: unknown, r: Model) => {
        const v = metricVal(r, "calmar");
        if (v === undefined) return <Text type="secondary">-</Text>;
        return (
          <Text style={{ color: v > 1 ? "#52c41a" : v > 0 ? "#1677ff" : "#ff4d4f" }}>
            {v.toFixed(3)}
          </Text>
        );
      },
    },
    {
      title: "创建时间",
      dataIndex: "created_at",
      key: "created_at",
      width: 160,
      ellipsis: true,
      sorter: (a: Model, b: Model) => (a.created_at ?? "").localeCompare(b.created_at ?? ""),
      defaultSortOrder: "descend" as const,
      render: (d: string) => d?.slice(0, 19) ?? "-",
    },
    {
      title: "操作",
      key: "actions",
      width: 120,
      render: (_: unknown, record: Model) => (
        <Space size={4}>
          <Tooltip title="还原训练配置">
            <Button
              size="small"
              icon={<RollbackOutlined />}
              onClick={(e) => handleRestore(record, e)}
              disabled={!onRestoreConfig}
            />
          </Tooltip>
          <Popconfirm title="确定删除此模型?" onConfirm={(e) => { e?.stopPropagation(); handleDelete(record.id); }}>
            <Button size="small" danger icon={<DeleteOutlined />} onClick={(e) => e.stopPropagation()} />
          </Popconfirm>
        </Space>
      ),
    },
  ];

  // Detail modal metric cards config
  const metricCards: Array<{
    key: string;
    title: string;
    precision: number;
    suffix?: string;
    multiply?: boolean;
    colorFn?: (v: number) => string;
  }> = [
    { key: "ic_mean", title: "IC Mean", precision: 4, colorFn: (v) => (v > 0 ? "#52c41a" : "#ff4d4f") },
    { key: "ic_std", title: "IC Std", precision: 4 },
    { key: "ir", title: "IR", precision: 4, colorFn: (v) => (v > 1 ? "#52c41a" : v > 0 ? "#1677ff" : "#ff4d4f") },
    { key: "sharpe", title: "Sharpe", precision: 3, colorFn: (v) => (v > 1 ? "#52c41a" : v > 0 ? "#1677ff" : "#ff4d4f") },
    { key: "annual_return", title: "年化收益", precision: 2, suffix: "%", multiply: true, colorFn: (v) => (v > 0 ? "#52c41a" : "#ff4d4f") },
    { key: "max_drawdown", title: "最大回撤", precision: 2, suffix: "%", multiply: true, colorFn: () => "#ff4d4f" },
    { key: "calmar", title: "Calmar", precision: 3, colorFn: (v) => (v > 1 ? "#52c41a" : v > 0 ? "#1677ff" : "#ff4d4f") },
    { key: "valid_ic", title: "验证IC", precision: 4, colorFn: (v) => (v > 0 ? "#52c41a" : "#ff4d4f") },
    { key: "test_ic", title: "测试IC", precision: 4, colorFn: (v) => (v > 0 ? "#52c41a" : "#ff4d4f") },
    { key: "valid_rmse", title: "验证RMSE", precision: 4 },
    { key: "test_rmse", title: "测试RMSE", precision: 4 },
  ];

  return (
    <>
      {contextHolder}
      <Card
        title="模型列表"
        extra={
          <Space>
            <Input
              prefix={<SearchOutlined />}
              placeholder="搜索 ID"
              allowClear
              style={{ width: 160 }}
              value={idSearch}
              onChange={(e) => setIdSearch(e.target.value)}
            />
            <Button icon={<ReloadOutlined />} size="small" onClick={fetchModels}>
              刷新
            </Button>
          </Space>
        }
      >
        <Table
          dataSource={displayData}
          columns={columns}
          rowKey="id"
          loading={loading}
          size="small"
          pagination={{ pageSize: 15 }}
          scroll={{ x: 1100 }}
          onRow={(record) => ({
            onClick: () => handleRowClick(record),
            style: { cursor: "pointer" },
          })}
        />
      </Card>

      <Modal
        title="模型详情"
        open={detailOpen}
        onCancel={() => setDetailOpen(false)}
        footer={null}
        width={960}
        destroyOnClose
      >
        {detailLoading ? (
          <div style={{ textAlign: "center", padding: 48 }}>
            <Text type="secondary">加载中...</Text>
          </div>
        ) : detailModel ? (
          <Space direction="vertical" style={{ width: "100%" }} size="middle">
            {/* Evaluation metrics cards */}
            {metrics && (
              <Row gutter={[12, 12]}>
                {metricCards.map((item) => {
                  const raw = metrics[item.key] as number | undefined;
                  if (raw === undefined || raw === null) return null;
                  const v = item.multiply ? raw * 100 : raw;
                  const color = item.colorFn ? item.colorFn(raw) : undefined;
                  return (
                    <Col xs={12} sm={8} md={6} lg={4} key={item.key}>
                      <Card size="small">
                        <Statistic
                          title={item.title}
                          value={v}
                          precision={item.precision}
                          suffix={item.suffix}
                          valueStyle={{ fontSize: 18, color }}
                        />
                      </Card>
                    </Col>
                  );
                })}
              </Row>
            )}

            {/* Train config + model params */}
            {(trainConfig || modelParams) && (
              <Card title="训练配置" size="small">
                <Row gutter={24}>
                  {trainConfig && (
                    <Col span={12}>
                      <Descriptions column={1} size="small" bordered>
                        {trainConfig.train_start && (
                          <Descriptions.Item label="训练区间">
                            {String(trainConfig.train_start)} ~ {String(trainConfig.train_end)}
                          </Descriptions.Item>
                        )}
                        {trainConfig.valid_start && (
                          <Descriptions.Item label="验证区间">
                            {String(trainConfig.valid_start)} ~ {String(trainConfig.valid_end)}
                          </Descriptions.Item>
                        )}
                        {trainConfig.test_start && (
                          <Descriptions.Item label="测试区间">
                            {String(trainConfig.test_start)} ~ {String(trainConfig.test_end)}
                          </Descriptions.Item>
                        )}
                        {trainConfig.purge_gap !== undefined && (
                          <Descriptions.Item label="Purge Gap">
                            {String(trainConfig.purge_gap)} 天
                          </Descriptions.Item>
                        )}
                        {metrics?.train_samples !== undefined && (
                          <Descriptions.Item label="样本量">
                            训练 {String(metrics.train_samples)} / 验证 {String(metrics.valid_samples)} / 测试 {String(metrics.test_samples)}
                          </Descriptions.Item>
                        )}
                      </Descriptions>
                    </Col>
                  )}
                  {modelParams && Object.keys(modelParams).length > 0 && (
                    <Col span={12}>
                      <Descriptions column={1} size="small" bordered title="模型参数">
                        {Object.entries(modelParams).map(([k, v]) => (
                          <Descriptions.Item key={k} label={k}>
                            {String(v)}
                          </Descriptions.Item>
                        ))}
                      </Descriptions>
                    </Col>
                  )}
                </Row>
              </Card>
            )}

            {featureImportance && Object.keys(featureImportance).length > 0 && (
              <FeatureImportanceChart data={featureImportance} />
            )}
          </Space>
        ) : null}
      </Modal>
    </>
  );
}

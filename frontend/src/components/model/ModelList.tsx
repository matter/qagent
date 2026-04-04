import { useState, useEffect, useCallback } from "react";
import {
  Button,
  Card,
  Col,
  message,
  Modal,
  Popconfirm,
  Row,
  Space,
  Statistic,
  Table,
  Tag,
  Typography,
} from "antd";
import { ReloadOutlined, DeleteOutlined } from "@ant-design/icons";
import { listModels, getModel, deleteModel } from "../../api";
import type { Model } from "../../api";
import FeatureImportanceChart from "./FeatureImportanceChart";

const { Text } = Typography;

const STATUS_TAG: Record<string, { color: string; label: string }> = {
  training: { color: "processing", label: "训练中" },
  ready: { color: "success", label: "就绪" },
  failed: { color: "error", label: "失败" },
  archived: { color: "warning", label: "已归档" },
};

interface ModelListProps {
  refreshKey?: number;
}

export default function ModelList({ refreshKey }: ModelListProps) {
  const [models, setModels] = useState<Model[]>([]);
  const [loading, setLoading] = useState(false);
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

  const columns = [
    {
      title: "名称",
      dataIndex: "name",
      key: "name",
      render: (name: string, record: Model) => (
        <a onClick={() => handleRowClick(record)}>{name}</a>
      ),
    },
    {
      title: "特征集",
      dataIndex: "feature_set_id",
      key: "feature_set_id",
      width: 120,
      ellipsis: true,
      render: (v: string) => <Text type="secondary">{v?.slice(0, 8)}...</Text>,
    },
    {
      title: "标签",
      dataIndex: "label_id",
      key: "label_id",
      width: 120,
      ellipsis: true,
      render: (v: string) => <Text type="secondary">{v?.slice(0, 8)}...</Text>,
    },
    {
      title: "类型",
      dataIndex: "model_type",
      key: "model_type",
      width: 100,
      render: (v: string) => <Tag color="blue">{v}</Tag>,
    },
    {
      title: "状态",
      dataIndex: "status",
      key: "status",
      width: 90,
      render: (s: string) => {
        const cfg = STATUS_TAG[s] ?? { color: "default", label: s };
        return <Tag color={cfg.color}>{cfg.label}</Tag>;
      },
    },
    {
      title: "IC",
      key: "ic",
      width: 80,
      render: (_: unknown, r: Model) => {
        const em = r.eval_metrics as Record<string, unknown> | null;
        const v = em?.ic_mean as number | undefined;
        if (v === undefined || v === null) return <Text type="secondary">-</Text>;
        return (
          <Text style={{ color: v > 0 ? "#52c41a" : "#ff4d4f" }}>
            {v.toFixed(4)}
          </Text>
        );
      },
    },
    {
      title: "Sharpe",
      key: "sharpe",
      width: 80,
      render: (_: unknown, r: Model) => {
        const em = r.eval_metrics as Record<string, unknown> | null;
        const v = em?.sharpe as number | undefined;
        if (v === undefined || v === null) return <Text type="secondary">-</Text>;
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
      render: (d: string) => d?.slice(0, 19) ?? "-",
    },
    {
      title: "操作",
      key: "actions",
      width: 80,
      render: (_: unknown, record: Model) => (
        <Popconfirm title="确定删除此模型?" onConfirm={() => handleDelete(record.id)}>
          <Button size="small" danger icon={<DeleteOutlined />} />
        </Popconfirm>
      ),
    },
  ];

  return (
    <>
      {contextHolder}
      <Card
        title="模型列表"
        extra={
          <Button icon={<ReloadOutlined />} size="small" onClick={fetchModels}>
            刷新
          </Button>
        }
      >
        <Table
          dataSource={models}
          columns={columns}
          rowKey="id"
          loading={loading}
          size="small"
          pagination={{ pageSize: 15 }}
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
        width={900}
        destroyOnClose
      >
        {detailLoading ? (
          <div style={{ textAlign: "center", padding: 48 }}>
            <Text type="secondary">加载中...</Text>
          </div>
        ) : detailModel ? (
          <Space direction="vertical" style={{ width: "100%" }} size="middle">
            {metrics && (
              <Row gutter={[12, 12]}>
                {(["ic_mean", "ic_std", "ir", "sharpe", "annual_return", "max_drawdown"] as const).map((key) => {
                  const v = metrics[key] as number | undefined;
                  if (v === undefined || v === null) return null;
                  const labels: Record<string, string> = {
                    ic_mean: "IC Mean",
                    ic_std: "IC Std",
                    ir: "IR",
                    sharpe: "Sharpe",
                    annual_return: "年化收益",
                    max_drawdown: "最大回撤",
                  };
                  const isPercent = key === "annual_return" || key === "max_drawdown";
                  return (
                    <Col xs={12} sm={8} md={4} key={key}>
                      <Card size="small">
                        <Statistic
                          title={labels[key]}
                          value={isPercent ? v * 100 : v}
                          precision={isPercent ? 2 : 4}
                          suffix={isPercent ? "%" : undefined}
                          valueStyle={{ fontSize: 18 }}
                        />
                      </Card>
                    </Col>
                  );
                })}
              </Row>
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

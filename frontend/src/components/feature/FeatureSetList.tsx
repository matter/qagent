import { useState, useEffect, useCallback } from "react";
import {
  Button,
  Card,
  message,
  Popconfirm,
  Space,
  Table,
  Tag,
  Typography,
} from "antd";
import { ReloadOutlined, DeleteOutlined } from "@ant-design/icons";
import { listFeatureSets, deleteFeatureSet } from "../../api";
import type { FeatureSet } from "../../api";

const { Text } = Typography;

const STATUS_TAG: Record<string, { color: string; label: string }> = {
  draft: { color: "default", label: "草稿" },
  active: { color: "processing", label: "活跃" },
  ready: { color: "success", label: "就绪" },
  archived: { color: "warning", label: "已归档" },
};

interface FeatureSetListProps {
  refreshKey?: number;
}

export default function FeatureSetList({ refreshKey }: FeatureSetListProps) {
  const [featureSets, setFeatureSets] = useState<FeatureSet[]>([]);
  const [loading, setLoading] = useState(false);
  const [messageApi, contextHolder] = message.useMessage();

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const data = await listFeatureSets();
      setFeatureSets(data);
    } catch {
      messageApi.error("加载特征集列表失败");
    } finally {
      setLoading(false);
    }
  }, [messageApi]);

  useEffect(() => {
    fetchData();
  }, [fetchData, refreshKey]);

  const handleDelete = async (id: string) => {
    try {
      await deleteFeatureSet(id);
      messageApi.success("特征集已删除");
      fetchData();
    } catch {
      messageApi.error("删除失败");
    }
  };

  const columns = [
    {
      title: "名称",
      dataIndex: "name",
      key: "name",
    },
    {
      title: "因子数量",
      key: "factor_count",
      width: 100,
      render: (_: unknown, record: FeatureSet) =>
        record.factor_refs?.length ?? 0,
    },
    {
      title: "预处理",
      key: "preprocessing",
      width: 240,
      render: (_: unknown, record: FeatureSet) => {
        const pp = record.preprocessing;
        if (!pp) return <Text type="secondary">-</Text>;
        return (
          <Space size={4} wrap>
            {pp.missing && pp.missing !== "none" && (
              <Tag>{`缺失:${pp.missing}`}</Tag>
            )}
            {pp.outlier && pp.outlier !== "none" && (
              <Tag>{`异常:${pp.outlier}`}</Tag>
            )}
            {pp.normalize && pp.normalize !== "none" && (
              <Tag>{`标准:${pp.normalize}`}</Tag>
            )}
            {pp.neutralize && pp.neutralize !== "none" && (
              <Tag>{`中性:${pp.neutralize}`}</Tag>
            )}
          </Space>
        );
      },
    },
    {
      title: "状态",
      dataIndex: "status",
      key: "status",
      width: 100,
      render: (s: string) => {
        const cfg = STATUS_TAG[s] ?? { color: "default", label: s };
        return <Tag color={cfg.color}>{cfg.label}</Tag>;
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
      render: (_: unknown, record: FeatureSet) => (
        <Popconfirm title="确定删除此特征集?" onConfirm={() => handleDelete(record.id)}>
          <Button size="small" danger icon={<DeleteOutlined />} />
        </Popconfirm>
      ),
    },
  ];

  return (
    <>
      {contextHolder}
      <Card
        title="特征集列表"
        extra={
          <Button icon={<ReloadOutlined />} size="small" onClick={fetchData}>
            刷新
          </Button>
        }
      >
        <Table
          dataSource={featureSets}
          columns={columns}
          rowKey="id"
          loading={loading}
          size="small"
          pagination={{ pageSize: 15 }}
        />
      </Card>
    </>
  );
}

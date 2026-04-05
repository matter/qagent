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
import { ReloadOutlined, DeleteOutlined, EyeOutlined } from "@ant-design/icons";
import { listStrategies, deleteStrategy } from "../../api";
import type { Strategy } from "../../api";

const { Text } = Typography;

const STATUS_TAG: Record<string, { color: string; label: string }> = {
  draft: { color: "default", label: "草稿" },
  active: { color: "processing", label: "活跃" },
  published: { color: "success", label: "已发布" },
  archived: { color: "warning", label: "已归档" },
};

const SIZING_LABEL: Record<string, string> = {
  equal_weight: "等权",
  value_weight: "市值加权",
  risk_parity: "风险平价",
  custom: "自定义",
};

interface StrategyListProps {
  refreshKey?: number;
  onViewStrategy?: (strategy: Strategy) => void;
}

export default function StrategyList({ refreshKey, onViewStrategy }: StrategyListProps) {
  const [strategies, setStrategies] = useState<Strategy[]>([]);
  const [loading, setLoading] = useState(false);
  const [messageApi, contextHolder] = message.useMessage();

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const data = await listStrategies();
      setStrategies(data);
    } catch {
      messageApi.error("加载策略列表失败");
    } finally {
      setLoading(false);
    }
  }, [messageApi]);

  useEffect(() => {
    fetchData();
  }, [fetchData, refreshKey]);

  const handleDelete = async (id: string) => {
    try {
      await deleteStrategy(id);
      messageApi.success("策略已删除");
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
      title: "版本",
      dataIndex: "version",
      key: "version",
      width: 80,
      render: (v: number) => <Text type="secondary">v{v}</Text>,
    },
    {
      title: "仓位管理",
      dataIndex: "position_sizing",
      key: "position_sizing",
      width: 120,
      render: (v: string) => SIZING_LABEL[v] ?? v,
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
      width: 120,
      render: (_: unknown, record: Strategy) => (
        <Space size="small">
          <Button
            size="small"
            icon={<EyeOutlined />}
            onClick={() => onViewStrategy?.(record)}
          >
            查看
          </Button>
          <Popconfirm title="确定删除此策略?" onConfirm={() => handleDelete(record.id)}>
            <Button size="small" danger icon={<DeleteOutlined />} />
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <>
      {contextHolder}
      <Card
        title="策略列表"
        extra={
          <Button icon={<ReloadOutlined />} size="small" onClick={fetchData}>
            刷新
          </Button>
        }
      >
        <Table
          dataSource={strategies}
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

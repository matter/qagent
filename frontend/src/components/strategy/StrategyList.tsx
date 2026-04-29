import { useState, useEffect, useCallback } from "react";
import {
  Button,
  Card,
  Input,
  message,
  Popconfirm,
  Space,
  Table,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import { ReloadOutlined, DeleteOutlined, EyeOutlined, SearchOutlined } from "@ant-design/icons";
import { listStrategies, deleteStrategy } from "../../api";
import type { Market, Strategy } from "../../api";

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
  const [idSearch, setIdSearch] = useState<string>("");
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

  const handleDelete = async (id: string, market?: string) => {
    try {
      await deleteStrategy(id, market);
      messageApi.success("策略已删除");
      fetchData();
    } catch {
      messageApi.error("删除失败");
    }
  };

  const displayData = idSearch
    ? strategies.filter((s) => s.id.toLowerCase().includes(idSearch.toLowerCase()))
    : strategies;

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
      title: "Market",
      dataIndex: "market",
      key: "market",
      width: 80,
      render: (m: Market) => <Tag color={m === "CN" ? "red" : "blue"}>{m}</Tag>,
    },
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
      sorter: (a: Strategy, b: Strategy) => (a.created_at ?? "").localeCompare(b.created_at ?? ""),
      defaultSortOrder: "descend" as const,
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
          <Popconfirm title="确定删除此策略?" onConfirm={() => handleDelete(record.id, record.market)}>
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
          <Space>
            <Input
              prefix={<SearchOutlined />}
              placeholder="搜索 ID"
              allowClear
              style={{ width: 160 }}
              value={idSearch}
              onChange={(e) => setIdSearch(e.target.value)}
            />
            <Button icon={<ReloadOutlined />} size="small" onClick={fetchData}>
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
          scroll={{ x: 900 }}
        />
      </Card>
    </>
  );
}

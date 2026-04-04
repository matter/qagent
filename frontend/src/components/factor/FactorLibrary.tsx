import { useState, useEffect, useCallback } from "react";
import {
  Button,
  Card,
  message,
  Popconfirm,
  Select,
  Space,
  Table,
  Tag,
  Typography,
} from "antd";
import {
  ReloadOutlined,
  DeleteOutlined,
  EyeOutlined,
} from "@ant-design/icons";
import { listFactors, deleteFactor, listEvaluations } from "../../api";
import type { Factor } from "../../api";

const { Text } = Typography;

const STATUS_TAG: Record<string, { color: string; label: string }> = {
  draft: { color: "default", label: "草稿" },
  active: { color: "processing", label: "活跃" },
  validated: { color: "processing", label: "已验证" },
  published: { color: "success", label: "已发布" },
  archived: { color: "warning", label: "已归档" },
};

const CATEGORY_TAG_COLOR: Record<string, string> = {
  momentum: "blue",
  volatility: "orange",
  volume: "purple",
  trend: "green",
  statistical: "cyan",
  custom: "default",
};

interface FactorLibraryProps {
  onViewFactor: (factor: Factor) => void;
  refreshKey?: number;
}

export default function FactorLibrary({ onViewFactor, refreshKey }: FactorLibraryProps) {
  const [factors, setFactors] = useState<Factor[]>([]);
  const [loading, setLoading] = useState(false);
  const [categoryFilter, setCategoryFilter] = useState<string>("");
  const [statusFilter, setStatusFilter] = useState<string>("");
  const [irMap, setIrMap] = useState<Record<string, number | null>>({});
  const [messageApi, contextHolder] = message.useMessage();

  const fetchFactors = useCallback(async () => {
    setLoading(true);
    try {
      const data = await listFactors(
        categoryFilter || undefined,
        statusFilter || undefined,
      );
      setFactors(data);

      // Fetch latest IR for each factor
      const irEntries: Record<string, number | null> = {};
      await Promise.all(
        data.map(async (f) => {
          try {
            const evals = await listEvaluations(f.id);
            if (evals.length > 0) {
              irEntries[f.id] = evals[0].summary?.ir ?? null;
            } else {
              irEntries[f.id] = null;
            }
          } catch {
            irEntries[f.id] = null;
          }
        }),
      );
      setIrMap(irEntries);
    } catch {
      messageApi.error("加载因子列表失败");
    } finally {
      setLoading(false);
    }
  }, [categoryFilter, statusFilter, messageApi]);

  useEffect(() => {
    fetchFactors();
  }, [fetchFactors, refreshKey]);

  const handleDelete = async (id: string) => {
    try {
      await deleteFactor(id);
      messageApi.success("因子已删除");
      fetchFactors();
    } catch {
      messageApi.error("删除失败");
    }
  };

  const columns = [
    {
      title: "名称",
      dataIndex: "name",
      key: "name",
      render: (name: string, record: Factor) => (
        <a onClick={() => onViewFactor(record)}>{name}</a>
      ),
    },
    {
      title: "版本",
      dataIndex: "version",
      key: "version",
      width: 80,
      render: (v: number) => <Text type="secondary">v{v}</Text>,
    },
    {
      title: "分类",
      dataIndex: "category",
      key: "category",
      width: 110,
      render: (cat: string) => (
        <Tag color={CATEGORY_TAG_COLOR[cat] ?? "default"}>{cat}</Tag>
      ),
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
      title: "IR",
      key: "ir",
      width: 80,
      render: (_: unknown, record: Factor) => {
        const ir = irMap[record.id];
        if (ir === undefined || ir === null) return <Text type="secondary">-</Text>;
        return (
          <Text style={{ color: ir > 0.5 ? "#52c41a" : ir > 0 ? "#1677ff" : "#ff4d4f" }}>
            {ir.toFixed(3)}
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
      width: 100,
      render: (_: unknown, record: Factor) => (
        <Space size="small">
          <Button size="small" icon={<EyeOutlined />} onClick={() => onViewFactor(record)} />
          <Popconfirm title="确定删除此因子?" onConfirm={() => handleDelete(record.id)}>
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
        title="因子库"
        extra={
          <Space>
            <Select
              style={{ width: 140 }}
              placeholder="分类"
              allowClear
              value={categoryFilter || undefined}
              onChange={(v) => setCategoryFilter(v ?? "")}
              options={[
                { value: "momentum", label: "动量" },
                { value: "volatility", label: "波动率" },
                { value: "volume", label: "成交量" },
                { value: "trend", label: "趋势" },
                { value: "statistical", label: "统计" },
                { value: "custom", label: "自定义" },
              ]}
            />
            <Select
              style={{ width: 110 }}
              placeholder="状态"
              allowClear
              value={statusFilter || undefined}
              onChange={(v) => setStatusFilter(v ?? "")}
              options={[
                { value: "draft", label: "草稿" },
                { value: "active", label: "活跃" },
                { value: "validated", label: "已验证" },
                { value: "published", label: "已发布" },
                { value: "archived", label: "已归档" },
              ]}
            />
            <Button icon={<ReloadOutlined />} size="small" onClick={fetchFactors}>
              刷新
            </Button>
          </Space>
        }
      >
        <Table
          dataSource={factors}
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

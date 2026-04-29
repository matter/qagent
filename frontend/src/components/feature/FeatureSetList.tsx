import { useState, useEffect, useCallback } from "react";
import {
  Button,
  Card,
  message,
  Popconfirm,
  Space,
  Table,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import { ReloadOutlined, DeleteOutlined, RollbackOutlined } from "@ant-design/icons";
import { listFeatureSets, deleteFeatureSet } from "../../api";
import type { FeatureSet, Market } from "../../api";

const { Text } = Typography;

const STATUS_TAG: Record<string, { color: string; label: string }> = {
  draft: { color: "default", label: "草稿" },
  active: { color: "processing", label: "活跃" },
  ready: { color: "success", label: "就绪" },
  archived: { color: "warning", label: "已归档" },
};

export interface FeatureSetRestoreConfig {
  factorIds: string[];
  preprocessing: Record<string, string> | null;
}

interface FeatureSetListProps {
  refreshKey?: number;
  onRestoreConfig?: (config: FeatureSetRestoreConfig) => void;
}

export default function FeatureSetList({ refreshKey, onRestoreConfig }: FeatureSetListProps) {
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

  const handleDelete = async (id: string, market?: string) => {
    try {
      await deleteFeatureSet(id, market);
      messageApi.success("特征集已删除");
      fetchData();
    } catch {
      messageApi.error("删除失败");
    }
  };

  const handleRestore = (record: FeatureSet) => {
    if (!onRestoreConfig) return;
    onRestoreConfig({
      factorIds: record.factor_refs?.map((r) => r.factor_id) ?? [],
      preprocessing: record.preprocessing,
    });
    messageApi.success("已还原特征集配置");
  };

  const columns = [
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
      sorter: (a: FeatureSet, b: FeatureSet) => (a.created_at ?? "").localeCompare(b.created_at ?? ""),
      defaultSortOrder: "descend" as const,
      render: (d: string) => d?.slice(0, 19) ?? "-",
    },
    {
      title: "操作",
      key: "actions",
      width: 120,
      render: (_: unknown, record: FeatureSet) => (
        <Space size={4}>
          <Tooltip title="还原配置">
            <Button
              size="small"
              icon={<RollbackOutlined />}
              onClick={() => handleRestore(record)}
              disabled={!onRestoreConfig}
            />
          </Tooltip>
          <Popconfirm title="确定删除此特征集?" onConfirm={() => handleDelete(record.id, record.market)}>
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
          scroll={{ x: 900 }}
        />
      </Card>
    </>
  );
}

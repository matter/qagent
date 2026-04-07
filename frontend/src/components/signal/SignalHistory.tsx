import { useState, useEffect, useCallback } from "react";
import {
  Alert,
  Button,
  Card,
  Descriptions,
  message,
  Modal,
  Space,
  Table,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import {
  DownloadOutlined,
  ReloadOutlined,
  RollbackOutlined,
} from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import {
  listSignalRuns,
  getSignalRun,
  exportSignals,
} from "../../api";
import type { SignalRun } from "../../api";
import SignalTable from "./SignalTable";

const { Text } = Typography;

export interface SignalRestoreConfig {
  strategyId: string;
  groupId: string;
  targetDate: string;
}

interface SignalHistoryProps {
  refreshKey: number;
  onRestoreConfig?: (config: SignalRestoreConfig) => void;
}

export default function SignalHistory({ refreshKey, onRestoreConfig }: SignalHistoryProps) {
  const [runs, setRuns] = useState<SignalRun[]>([]);
  const [loading, setLoading] = useState(false);
  const [detailOpen, setDetailOpen] = useState(false);
  const [detailRun, setDetailRun] = useState<SignalRun | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [messageApi, contextHolder] = message.useMessage();

  const fetchRuns = useCallback(async () => {
    setLoading(true);
    try {
      const data = await listSignalRuns();
      setRuns(data);
    } catch {
      // noop
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchRuns();
  }, [fetchRuns, refreshKey]);

  const showDetail = async (record: SignalRun) => {
    setDetailLoading(true);
    setDetailOpen(true);
    try {
      const detail = await getSignalRun(record.id);
      setDetailRun(detail);
    } catch {
      messageApi.error("获取信号详情失败");
      setDetailOpen(false);
    } finally {
      setDetailLoading(false);
    }
  };

  const handleExport = async (runId: string, format: "csv" | "json") => {
    try {
      await exportSignals(runId, format);
      messageApi.success(`导出${format.toUpperCase()}成功`);
    } catch {
      messageApi.error("导出失败");
    }
  };

  const handleRestore = (record: SignalRun, e: React.MouseEvent) => {
    e.stopPropagation();
    if (!onRestoreConfig) return;
    onRestoreConfig({
      strategyId: record.strategy_id,
      groupId: record.universe_group_id,
      targetDate: record.target_date,
    });
    messageApi.success("已还原信号配置");
  };

  const columns: ColumnsType<SignalRun> = [
    {
      title: "策略",
      dataIndex: "strategy_id",
      key: "strategy_id",
      ellipsis: true,
      render: (id: string, record) => (
        <a onClick={() => showDetail(record)}>
          {id.slice(0, 8)}... v{record.strategy_version}
        </a>
      ),
    },
    {
      title: "目标日期",
      dataIndex: "target_date",
      key: "target_date",
      width: 120,
      sorter: (a, b) => a.target_date.localeCompare(b.target_date),
    },
    {
      title: "信号数",
      dataIndex: "signal_count",
      key: "signal_count",
      width: 80,
      align: "right" as const,
    },
    {
      title: "结果等级",
      dataIndex: "result_level",
      key: "result_level",
      width: 100,
      render: (level: string) => (
        <Tag color={level === "formal" ? "green" : "orange"}>
          {level === "formal" ? "正式" : "探索性"}
        </Tag>
      ),
    },
    {
      title: "创建时间",
      dataIndex: "created_at",
      key: "created_at",
      width: 170,
      ellipsis: true,
      sorter: (a, b) => a.created_at.localeCompare(b.created_at),
      defaultSortOrder: "descend" as const,
    },
    {
      title: "操作",
      key: "actions",
      width: 60,
      render: (_: unknown, record: SignalRun) => (
        <Tooltip title="还原信号配置">
          <Button
            size="small"
            icon={<RollbackOutlined />}
            onClick={(e) => handleRestore(record, e)}
            disabled={!onRestoreConfig}
          />
        </Tooltip>
      ),
    },
  ];

  return (
    <>
      {contextHolder}
      <Card
        title="信号历史"
        extra={
          <Button icon={<ReloadOutlined />} size="small" onClick={fetchRuns} loading={loading}>
            刷新
          </Button>
        }
      >
        <Table
          dataSource={runs}
          columns={columns}
          rowKey="id"
          loading={loading}
          size="small"
          pagination={{ pageSize: 15, showTotal: (total) => `共 ${total} 条` }}
          onRow={(record) => ({
            onClick: () => showDetail(record),
            style: { cursor: "pointer" },
          })}
        />
      </Card>

      <Modal
        title="信号详情"
        open={detailOpen}
        onCancel={() => {
          setDetailOpen(false);
          setDetailRun(null);
        }}
        footer={null}
        width={900}
        destroyOnClose
      >
        {detailLoading ? (
          <div style={{ textAlign: "center", padding: 48 }}>
            <Text type="secondary">加载中...</Text>
          </div>
        ) : detailRun ? (
          <Space direction="vertical" style={{ width: "100%" }} size="middle">
            <Space size="middle">
              <Text strong>结果等级:</Text>
              <Tag color={detailRun.result_level === "formal" ? "green" : "orange"}>
                {detailRun.result_level === "formal" ? "正式" : "探索性"}
              </Tag>
              <Text type="secondary">
                共 {detailRun.signal_count} 条信号
              </Text>
            </Space>

            {detailRun.warnings && detailRun.warnings.length > 0 && (
              <Alert
                type="warning"
                showIcon
                message="警告信息"
                description={
                  <ul style={{ margin: 0, paddingLeft: 20 }}>
                    {detailRun.warnings.map((w, i) => (
                      <li key={i}>{w}</li>
                    ))}
                  </ul>
                }
              />
            )}

            {detailRun.dependency_snapshot && (
              <Descriptions size="small" column={2} bordered>
                <Descriptions.Item label="策略版本">
                  {(detailRun.dependency_snapshot as Record<string, unknown>).strategy_version as string ?? detailRun.strategy_version}
                </Descriptions.Item>
                <Descriptions.Item label="目标日期">
                  {detailRun.target_date}
                </Descriptions.Item>
                {(detailRun.dependency_snapshot as Record<string, unknown>).factors_used != null && (
                  <Descriptions.Item label="使用因子" span={2}>
                    {String((detailRun.dependency_snapshot as Record<string, unknown>).factors_used)}
                  </Descriptions.Item>
                )}
                {(detailRun.dependency_snapshot as Record<string, unknown>).models_used != null && (
                  <Descriptions.Item label="使用模型" span={2}>
                    {String((detailRun.dependency_snapshot as Record<string, unknown>).models_used)}
                  </Descriptions.Item>
                )}
              </Descriptions>
            )}

            <SignalTable signals={detailRun.signals ?? []} />

            <Space>
              <Button
                icon={<DownloadOutlined />}
                onClick={() => handleExport(detailRun.id, "csv")}
              >
                导出CSV
              </Button>
              <Button
                icon={<DownloadOutlined />}
                onClick={() => handleExport(detailRun.id, "json")}
              >
                导出JSON
              </Button>
            </Space>
          </Space>
        ) : null}
      </Modal>
    </>
  );
}

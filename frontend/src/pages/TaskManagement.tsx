import { useState, useEffect, useCallback, useRef } from "react";
import {
  Button,
  Card,
  message,
  Modal,
  Popconfirm,
  Select,
  Space,
  Table,
  Tag,
  Typography,
} from "antd";
import { ReloadOutlined, StopOutlined } from "@ant-design/icons";
import { listTasks, cancelTask } from "../api";
import type { TaskStatus as TaskStatusType } from "../api";

const { Text, Paragraph } = Typography;

const STATUS_TAG: Record<string, { color: string; label: string }> = {
  queued: { color: "default", label: "排队中" },
  running: { color: "processing", label: "运行中" },
  completed: { color: "success", label: "已完成" },
  failed: { color: "error", label: "失败" },
  timeout: { color: "warning", label: "超时" },
};

const TASK_TYPE_LABEL: Record<string, string> = {
  data_update: "数据更新",
  stock_list_refresh: "股票池刷新",
  factor_compute: "因子计算",
  factor_evaluate: "因子评估",
  model_train: "模型训练",
  backtest: "回测",
  signal_generate: "信号生成",
};

export default function TaskManagement() {
  const [tasks, setTasks] = useState<TaskStatusType[]>([]);
  const [loading, setLoading] = useState(false);
  const [statusFilter, setStatusFilter] = useState<string | undefined>(undefined);
  const [typeFilter, setTypeFilter] = useState<string | undefined>(undefined);
  const [errorModal, setErrorModal] = useState<{ open: boolean; error: string; taskId: string }>({
    open: false,
    error: "",
    taskId: "",
  });
  const [messageApi, contextHolder] = message.useMessage();
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchTasks = useCallback(async () => {
    setLoading(true);
    try {
      const data = await listTasks({
        status: statusFilter,
        task_type: typeFilter,
        limit: 200,
      });
      setTasks(data);
    } catch {
      messageApi.error("加载任务列表失败");
    } finally {
      setLoading(false);
    }
  }, [messageApi, statusFilter, typeFilter]);

  useEffect(() => {
    fetchTasks();
  }, [fetchTasks]);

  // Auto-refresh every 5s if there are running/queued tasks
  useEffect(() => {
    const hasActive = tasks.some((t) => t.status === "running" || t.status === "queued");
    if (hasActive) {
      intervalRef.current = setInterval(fetchTasks, 5000);
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [tasks, fetchTasks]);

  const handleCancel = async (taskId: string) => {
    try {
      await cancelTask(taskId);
      messageApi.success("任务已取消");
      fetchTasks();
    } catch {
      messageApi.error("取消失败");
    }
  };

  const formatDuration = (task: TaskStatusType) => {
    if (!task.started_at) return "-";
    const start = new Date(task.started_at).getTime();
    const end = task.completed_at ? new Date(task.completed_at).getTime() : Date.now();
    const sec = Math.round((end - start) / 1000);
    if (sec < 60) return `${sec}s`;
    const min = Math.floor(sec / 60);
    const rem = sec % 60;
    return `${min}m${rem}s`;
  };

  const columns = [
    {
      title: "任务类型",
      dataIndex: "task_type",
      key: "task_type",
      width: 120,
      render: (v: string) => (
        <Tag color="blue">{TASK_TYPE_LABEL[v] ?? v}</Tag>
      ),
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
      title: "耗时",
      key: "duration",
      width: 80,
      render: (_: unknown, record: TaskStatusType) => formatDuration(record),
    },
    {
      title: "创建时间",
      dataIndex: "created_at",
      key: "created_at",
      width: 160,
      render: (d: string | null) => d?.slice(0, 19) ?? "-",
    },
    {
      title: "开始时间",
      dataIndex: "started_at",
      key: "started_at",
      width: 160,
      render: (d: string | null) => d?.slice(0, 19) ?? "-",
    },
    {
      title: "完成时间",
      dataIndex: "completed_at",
      key: "completed_at",
      width: 160,
      render: (d: string | null) => d?.slice(0, 19) ?? "-",
    },
    {
      title: "结果/错误",
      key: "result_error",
      ellipsis: true,
      render: (_: unknown, record: TaskStatusType) => {
        if (record.error) {
          return (
            <a
              onClick={() =>
                setErrorModal({ open: true, error: record.error!, taskId: record.task_id })
              }
            >
              <Text type="danger" ellipsis style={{ maxWidth: 200 }}>
                {record.error.split("\n").pop() || record.error}
              </Text>
            </a>
          );
        }
        if (record.result) {
          const keys = Object.keys(record.result);
          const preview = keys.slice(0, 3).map((k) => `${k}: ${record.result![k]}`).join(", ");
          return (
            <Text type="secondary" ellipsis style={{ maxWidth: 200 }}>
              {preview}
            </Text>
          );
        }
        return <Text type="secondary">-</Text>;
      },
    },
    {
      title: "来源",
      dataIndex: "source",
      key: "source",
      width: 70,
      render: (v: string) => v ?? "-",
    },
    {
      title: "操作",
      key: "actions",
      width: 80,
      render: (_: unknown, record: TaskStatusType) => {
        const canCancel = record.status === "running" || record.status === "queued";
        if (!canCancel) return null;
        return (
          <Popconfirm title="确定取消此任务?" onConfirm={() => handleCancel(record.task_id)}>
            <Button size="small" danger icon={<StopOutlined />}>
              取消
            </Button>
          </Popconfirm>
        );
      },
    },
  ];

  const runningCount = tasks.filter((t) => t.status === "running").length;
  const queuedCount = tasks.filter((t) => t.status === "queued").length;

  return (
    <>
      {contextHolder}
      <Space orientation="vertical" style={{ width: "100%" }} size="middle">
        {/* Summary */}
        {(runningCount > 0 || queuedCount > 0) && (
          <Card size="small">
            <Space size="large">
              {runningCount > 0 && (
                <Text>
                  <Tag color="processing">运行中</Tag> {runningCount} 个任务
                </Text>
              )}
              {queuedCount > 0 && (
                <Text>
                  <Tag color="default">排队中</Tag> {queuedCount} 个任务
                </Text>
              )}
            </Space>
          </Card>
        )}

        {/* Task list */}
        <Card
          title="任务列表"
          extra={
            <Space>
              <Select
                allowClear
                placeholder="任务类型"
                style={{ width: 130 }}
                value={typeFilter}
                onChange={setTypeFilter}
                options={Object.entries(TASK_TYPE_LABEL).map(([k, v]) => ({ value: k, label: v }))}
              />
              <Select
                allowClear
                placeholder="状态"
                style={{ width: 100 }}
                value={statusFilter}
                onChange={setStatusFilter}
                options={Object.entries(STATUS_TAG).map(([k, v]) => ({ value: k, label: v.label }))}
              />
              <Button icon={<ReloadOutlined />} size="small" onClick={fetchTasks}>
                刷新
              </Button>
            </Space>
          }
        >
          <Table
            dataSource={tasks}
            columns={columns}
            rowKey="task_id"
            loading={loading}
            size="small"
            pagination={{ pageSize: 30 }}
            scroll={{ x: 1200 }}
          />
        </Card>
      </Space>

      {/* Error detail modal */}
      <Modal
        title={`错误详情 - ${errorModal.taskId}`}
        open={errorModal.open}
        onCancel={() => setErrorModal({ open: false, error: "", taskId: "" })}
        footer={null}
        width={700}
      >
        <Paragraph>
          <pre style={{ whiteSpace: "pre-wrap", fontSize: 12, maxHeight: 400, overflow: "auto" }}>
            {errorModal.error}
          </pre>
        </Paragraph>
      </Modal>
    </>
  );
}

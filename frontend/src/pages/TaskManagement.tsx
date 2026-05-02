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
import { DeleteOutlined, PauseCircleOutlined, ReloadOutlined, StopOutlined } from "@ant-design/icons";
import {
  bulkCancelTasks,
  cancelTask,
  createTaskPauseRule,
  deleteTaskPauseRule,
  listTaskPauseRules,
  listTasks,
} from "../api";
import type { TaskPauseRule, TaskStatus as TaskStatusType } from "../api";

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
  const [sourceFilter, setSourceFilter] = useState<string | undefined>(undefined);
  const [marketFilter, setMarketFilter] = useState<string | undefined>(undefined);
  const [pauseRules, setPauseRules] = useState<TaskPauseRule[]>([]);
  const [pauseRuleLoading, setPauseRuleLoading] = useState(false);
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
        source: sourceFilter,
        market: marketFilter,
        limit: 200,
      });
      setTasks(data);
    } catch {
      messageApi.error("加载任务列表失败");
    } finally {
      setLoading(false);
    }
  }, [messageApi, statusFilter, typeFilter, sourceFilter, marketFilter]);

  const fetchPauseRules = useCallback(async () => {
    setPauseRuleLoading(true);
    try {
      setPauseRules(await listTaskPauseRules(true));
    } catch {
      messageApi.error("加载暂停规则失败");
    } finally {
      setPauseRuleLoading(false);
    }
  }, [messageApi]);

  useEffect(() => {
    fetchTasks();
    fetchPauseRules();
  }, [fetchTasks, fetchPauseRules]);

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

  const handleBulkCancel = async () => {
    try {
      const result = await bulkCancelTasks({
        status: statusFilter,
        task_type: typeFilter,
        source: sourceFilter,
        market: marketFilter,
      });
      messageApi.success(`已取消 ${result.cancelled_count} 个任务`);
      fetchTasks();
    } catch {
      messageApi.error("批量取消失败");
    }
  };

  const handleCreatePauseRule = async () => {
    if (!typeFilter && !sourceFilter && !marketFilter) {
      messageApi.warning("请至少选择任务类型、来源或市场之一");
      return;
    }
    try {
      await createTaskPauseRule({
        task_type: typeFilter,
        source: sourceFilter,
        market: marketFilter,
        reason: "created from task management page",
      });
      messageApi.success("已暂停匹配的新任务提交");
      fetchPauseRules();
    } catch {
      messageApi.error("创建暂停规则失败");
    }
  };

  const handleDeletePauseRule = async (ruleId: string) => {
    try {
      await deleteTaskPauseRule(ruleId);
      messageApi.success("暂停规则已删除");
      fetchPauseRules();
    } catch {
      messageApi.error("删除暂停规则失败");
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
        if (record.late_result_id || record.late_model_id || record.late_run_id || record.late_signal_run_id) {
          return (
            <Text type="warning" ellipsis style={{ maxWidth: 200 }}>
              late result: {record.late_result_id ?? record.late_model_id ?? record.late_run_id ?? record.late_signal_run_id}
            </Text>
          );
        }
        if (record.date_adjustment) {
          return (
            <Text type="secondary" ellipsis style={{ maxWidth: 200 }}>
              {record.requested_start_date} -&gt; {record.effective_start_date}
            </Text>
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
              <Select
                allowClear
                placeholder="来源"
                style={{ width: 90 }}
                value={sourceFilter}
                onChange={setSourceFilter}
                options={[
                  { value: "ui", label: "UI" },
                  { value: "agent", label: "Agent" },
                  { value: "system", label: "System" },
                ]}
              />
              <Select
                allowClear
                placeholder="市场"
                style={{ width: 90 }}
                value={marketFilter}
                onChange={setMarketFilter}
                options={[
                  { value: "US", label: "US" },
                  { value: "CN", label: "CN" },
                ]}
              />
              <Popconfirm title="按当前筛选批量取消排队/运行任务?" onConfirm={handleBulkCancel}>
                <Button icon={<StopOutlined />} size="small" danger>
                  批量取消
                </Button>
              </Popconfirm>
              <Popconfirm title="暂停匹配当前筛选的新任务提交?" onConfirm={handleCreatePauseRule}>
                <Button icon={<PauseCircleOutlined />} size="small">
                  暂停提交
                </Button>
              </Popconfirm>
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

        <Card title="任务提交暂停规则" size="small">
          <Table
            dataSource={pauseRules}
            rowKey="id"
            loading={pauseRuleLoading}
            size="small"
            pagination={false}
            columns={[
              {
                title: "任务类型",
                dataIndex: "task_type",
                key: "task_type",
                render: (v: string | null) => v ? (TASK_TYPE_LABEL[v] ?? v) : "全部",
              },
              {
                title: "来源",
                dataIndex: "source",
                key: "source",
                render: (v: string | null) => v ?? "全部",
              },
              {
                title: "市场",
                dataIndex: "market",
                key: "market",
                render: (v: string | null) => v ?? "全部",
              },
              {
                title: "原因",
                dataIndex: "reason",
                key: "reason",
                ellipsis: true,
              },
              {
                title: "操作",
                key: "action",
                width: 90,
                render: (_: unknown, record: TaskPauseRule) => (
                  <Popconfirm title="删除此暂停规则?" onConfirm={() => handleDeletePauseRule(record.id)}>
                    <Button size="small" icon={<DeleteOutlined />} />
                  </Popconfirm>
                ),
              },
            ]}
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

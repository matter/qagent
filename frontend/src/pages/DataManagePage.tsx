import { useState, useEffect, useCallback, useRef } from "react";
import {
  Card,
  Button,
  Table,
  Space,
  Modal,
  Form,
  Input,
  Select,
  Tag,
  Typography,
  Statistic,
  Row,
  Col,
  message,
  Popconfirm,
  Descriptions,
  Spin,
  Progress,
  Alert,
  Collapse,
} from "antd";
import {
  ReloadOutlined,
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  CloudDownloadOutlined,
  SyncOutlined,
  ExclamationCircleOutlined,
} from "@ant-design/icons";
import {
  getDataStatus,
  triggerUpdate,
  getUpdateProgress,
  updateGroupData,
  listGroups,
  createGroup,
  updateGroup,
  deleteGroup,
  getGroup,
  refreshIndexGroups,
} from "../api";
import type { DataStatus, UpdateProgress, StockGroup } from "../api";

const { Text, Paragraph } = Typography;
const { TextArea } = Input;

// ---- Data Status Section ----

function DataStatusSection() {
  const [status, setStatus] = useState<DataStatus | null>(null);
  const [loading, setLoading] = useState(false);

  const fetchStatus = useCallback(async () => {
    setLoading(true);
    try {
      const data = await getDataStatus();
      setStatus(data);
    } catch {
      /* noop */
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchStatus();
  }, [fetchStatus]);

  return (
    <Card
      title="数据概览"
      extra={
        <Button icon={<ReloadOutlined />} size="small" onClick={fetchStatus} loading={loading}>
          刷新
        </Button>
      }
    >
      {status ? (
        <Row gutter={[16, 16]}>
          <Col xs={12} sm={8} md={4}>
            <Statistic title="股票总数" value={status.stock_count} />
          </Col>
          <Col xs={12} sm={8} md={4}>
            <Statistic title="有数据股票" value={status.tickers_with_bars} />
          </Col>
          <Col xs={12} sm={8} md={4}>
            <Statistic title="总K线数" value={status.total_bars} />
          </Col>
          <Col xs={12} sm={8} md={4}>
            <Statistic title="过期股票" value={status.stale_tickers} />
          </Col>
          <Col xs={12} sm={8} md={6}>
            <Statistic
              title="日期范围"
              value={
                status.date_range.min && status.date_range.max
                  ? `${status.date_range.min} ~ ${status.date_range.max}`
                  : "无数据"
              }
              valueStyle={{ fontSize: 14 }}
            />
          </Col>
          <Col xs={12} sm={8} md={6}>
            <Statistic
              title="最近更新"
              value={status.last_update.completed_at ?? "从未更新"}
              valueStyle={{ fontSize: 14 }}
            />
          </Col>
        </Row>
      ) : loading ? (
        <Spin />
      ) : (
        <Text type="secondary">加载中...</Text>
      )}
    </Card>
  );
}

// ---- Data Update Section ----

function formatElapsed(startedAt: string): string {
  const start = new Date(startedAt).getTime();
  const now = Date.now();
  const diff = Math.max(0, Math.floor((now - start) / 1000));
  const m = Math.floor(diff / 60);
  const s = diff % 60;
  return m > 0 ? `${m}分${s}秒` : `${s}秒`;
}

function DataUpdateSection() {
  const [status, setStatus] = useState<DataStatus | null>(null);
  const [progress, setProgress] = useState<UpdateProgress | null>(null);
  const [triggering, setTriggering] = useState(false);
  const [elapsed, setElapsed] = useState("");
  const pollTimer = useRef<ReturnType<typeof setInterval> | undefined>(undefined);
  const elapsedTimer = useRef<ReturnType<typeof setInterval> | undefined>(undefined);
  const [messageApi, contextHolder] = message.useMessage();

  const isRunning = progress?.status === "running" || progress?.status === "queued";

  // Fetch data status for confirmation dialog
  const fetchStatus = useCallback(async () => {
    try {
      const data = await getDataStatus();
      setStatus(data);
    } catch {
      /* noop */
    }
  }, []);

  const startElapsedTimer = useCallback((startedAt: string) => {
    if (elapsedTimer.current) clearInterval(elapsedTimer.current);
    setElapsed(formatElapsed(startedAt));
    elapsedTimer.current = setInterval(() => {
      setElapsed(formatElapsed(startedAt));
    }, 1000);
  }, []);

  const stopElapsedTimer = useCallback(() => {
    if (elapsedTimer.current) {
      clearInterval(elapsedTimer.current);
      elapsedTimer.current = undefined;
    }
  }, []);

  const pollProgress = useCallback(async () => {
    try {
      const data = await getUpdateProgress();
      setProgress(data);
      if (data.status === "running" && data.started_at) {
        startElapsedTimer(data.started_at);
      }
      if (data.status !== "running" && data.status !== "queued") {
        if (pollTimer.current) {
          clearInterval(pollTimer.current);
          pollTimer.current = undefined;
        }
        stopElapsedTimer();
      }
    } catch {
      /* noop */
    }
  }, [startElapsedTimer, stopElapsedTimer]);

  const startPoll = useCallback(() => {
    if (pollTimer.current) clearInterval(pollTimer.current);
    pollTimer.current = setInterval(pollProgress, 2000);
  }, [pollProgress]);

  useEffect(() => {
    fetchStatus();
    pollProgress();
    return () => {
      if (pollTimer.current) clearInterval(pollTimer.current);
      stopElapsedTimer();
    };
  }, [fetchStatus, pollProgress, stopElapsedTimer]);

  const confirmAndUpdate = (mode: "incremental" | "full") => {
    const lastUpdate = status?.last_update?.completed_at ?? "从未更新";
    const stockCount = status?.stock_count ?? "未知";
    const desc =
      mode === "incremental"
        ? "将更新至最新交易日的数据"
        : "将重新拉取全部历史数据（约30-60分钟）";

    Modal.confirm({
      title: "确认数据更新",
      icon: <ExclamationCircleOutlined />,
      content: (
        <Space direction="vertical" style={{ width: "100%", marginTop: 8 }}>
          <Descriptions size="small" column={1} bordered>
            <Descriptions.Item label="最近更新">{lastUpdate}</Descriptions.Item>
            <Descriptions.Item label="股票总数">{stockCount}</Descriptions.Item>
            <Descriptions.Item label="更新模式">
              <Tag color={mode === "incremental" ? "blue" : "orange"}>
                {mode === "incremental" ? "增量更新" : "全量更新"}
              </Tag>
            </Descriptions.Item>
          </Descriptions>
          <Text type="secondary">{desc}</Text>
        </Space>
      ),
      okText: "确认更新",
      cancelText: "取消",
      onOk: () => handleUpdate(mode),
    });
  };

  const handleUpdate = async (mode: "incremental" | "full") => {
    setTriggering(true);
    try {
      await triggerUpdate(mode);
      messageApi.success(`${mode === "incremental" ? "增量" : "全量"}更新已提交`);
      startPoll();
      // immediate poll
      setTimeout(pollProgress, 500);
    } catch {
      messageApi.error("提交失败");
    } finally {
      setTriggering(false);
    }
  };

  // Determine progress percent for visual bar
  const progressPercent = !progress
    ? 0
    : progress.status === "completed"
      ? 100
      : progress.status === "failed"
        ? 100
        : progress.status === "running"
          ? 50
          : 0;

  const progressStatus =
    progress?.status === "completed"
      ? "success"
      : progress?.status === "failed"
        ? "exception"
        : progress?.status === "running"
          ? "active"
          : "normal";

  return (
    <Card title="数据更新">
      {contextHolder}
      <Space direction="vertical" style={{ width: "100%" }} size="middle">
        <Space wrap>
          <Button
            type="primary"
            icon={<SyncOutlined spin={isRunning} />}
            loading={triggering}
            disabled={isRunning}
            onClick={() => confirmAndUpdate("incremental")}
          >
            增量更新
          </Button>
          <Button
            icon={<CloudDownloadOutlined />}
            loading={triggering}
            disabled={isRunning}
            onClick={() => confirmAndUpdate("full")}
          >
            全量更新
          </Button>
        </Space>

        {isRunning && (
          <div>
            <Space style={{ marginBottom: 8 }}>
              <SyncOutlined spin />
              <Text strong>更新进行中...</Text>
              {elapsed && <Text type="secondary">已运行 {elapsed}</Text>}
            </Space>
            <Progress percent={progressPercent} status={progressStatus} />
          </div>
        )}

        {progress && progress.status !== "no_updates" && (
          <Collapse
            size="small"
            items={[
              {
                key: "log",
                label: "查看日志",
                children: (
                  <Descriptions size="small" column={3} bordered>
                    <Descriptions.Item label="状态">
                      <Tag
                        color={
                          progress.status === "completed"
                            ? "green"
                            : progress.status === "failed"
                              ? "red"
                              : progress.status === "running"
                                ? "blue"
                                : "default"
                        }
                      >
                        {progress.status}
                      </Tag>
                    </Descriptions.Item>
                    <Descriptions.Item label="开始时间">
                      {progress.started_at ?? "-"}
                    </Descriptions.Item>
                    <Descriptions.Item label="完成时间">
                      {progress.completed_at ?? "-"}
                    </Descriptions.Item>
                    {progress.message && (
                      <Descriptions.Item label="消息" span={3}>
                        <Text>{progress.message}</Text>
                      </Descriptions.Item>
                    )}
                    {progress.error && (
                      <Descriptions.Item label="错误" span={3}>
                        <Text type="danger">{progress.error}</Text>
                      </Descriptions.Item>
                    )}
                  </Descriptions>
                ),
              },
            ]}
          />
        )}
      </Space>
    </Card>
  );
}

// ---- Stock Groups Section ----

interface GroupFormValues {
  name: string;
  description?: string;
  group_type: string;
  tickers_text?: string;
  filter_expr?: string;
}

function StockGroupsSection() {
  const [groups, setGroups] = useState<StockGroup[]>([]);
  const [loading, setLoading] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [editingGroup, setEditingGroup] = useState<StockGroup | null>(null);
  const [detailGroup, setDetailGroup] = useState<StockGroup | null>(null);
  const [detailOpen, setDetailOpen] = useState(false);
  const [refreshingIndices, setRefreshingIndices] = useState(false);
  const [updatingGroupId, setUpdatingGroupId] = useState<string | null>(null);
  const [form] = Form.useForm<GroupFormValues>();
  const [submitting, setSubmitting] = useState(false);
  const [messageApi, contextHolder] = message.useMessage();

  const fetchGroups = useCallback(async () => {
    setLoading(true);
    try {
      const data = await listGroups();
      setGroups(data);
    } catch {
      /* noop */
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchGroups();
  }, [fetchGroups]);

  const openCreate = () => {
    setEditingGroup(null);
    form.resetFields();
    form.setFieldsValue({ group_type: "manual" });
    setModalOpen(true);
  };

  const openEdit = (record: StockGroup) => {
    setEditingGroup(record);
    form.setFieldsValue({
      name: record.name,
      description: record.description ?? "",
      group_type: record.group_type,
      filter_expr: record.filter_expr ?? "",
      tickers_text: "",
    });
    // Load tickers if manual
    if (record.group_type === "manual") {
      getGroup(record.id).then((g) => {
        form.setFieldsValue({ tickers_text: (g.tickers ?? []).join(", ") });
      });
    }
    setModalOpen(true);
  };

  const handleDelete = async (id: string) => {
    try {
      await deleteGroup(id);
      messageApi.success("分组已删除");
      fetchGroups();
    } catch {
      messageApi.error("删除失败");
    }
  };

  const handleSubmit = async () => {
    try {
      const values = await form.validateFields();
      setSubmitting(true);

      const tickers = values.tickers_text
        ? values.tickers_text
            .split(/[,\s\n]+/)
            .map((t: string) => t.trim().toUpperCase())
            .filter(Boolean)
        : undefined;

      if (editingGroup) {
        await updateGroup(editingGroup.id, {
          name: values.name,
          description: values.description,
          tickers: values.group_type === "manual" ? tickers : undefined,
          filter_expr: values.group_type === "filter" ? values.filter_expr : undefined,
        });
        messageApi.success("分组已更新");
      } else {
        await createGroup({
          name: values.name,
          description: values.description,
          group_type: values.group_type,
          tickers: values.group_type === "manual" ? tickers : undefined,
          filter_expr: values.group_type === "filter" ? values.filter_expr : undefined,
        });
        messageApi.success("分组已创建");
      }

      setModalOpen(false);
      fetchGroups();
    } catch {
      messageApi.error("操作失败");
    } finally {
      setSubmitting(false);
    }
  };

  const showDetail = async (record: StockGroup) => {
    try {
      const data = await getGroup(record.id);
      setDetailGroup(data);
      setDetailOpen(true);
    } catch {
      messageApi.error("获取详情失败");
    }
  };

  const handleRefreshIndices = async () => {
    setRefreshingIndices(true);
    try {
      await refreshIndexGroups();
      messageApi.success("指数成分刷新成功");
      fetchGroups();
    } catch {
      messageApi.error("指数成分刷新失败");
    } finally {
      setRefreshingIndices(false);
    }
  };

  const handleUpdateGroupData = async (groupId: string, groupName: string) => {
    setUpdatingGroupId(groupId);
    try {
      await updateGroupData(groupId);
      messageApi.success(`${groupName} 数据更新已提交`);
      // Poll for completion
      const poll = setInterval(async () => {
        try {
          const p = await getUpdateProgress();
          if (p.status !== "running" && p.status !== "queued") {
            clearInterval(poll);
            setUpdatingGroupId(null);
            if (p.status === "completed") {
              messageApi.success(`${groupName} 数据更新完成`);
            } else if (p.error) {
              messageApi.error(`更新失败: ${p.error}`);
            }
          }
        } catch {
          clearInterval(poll);
          setUpdatingGroupId(null);
        }
      }, 3000);
    } catch (err: unknown) {
      const detail =
        err && typeof err === "object" && "response" in err
          ? ((err as Record<string, Record<string, unknown>>).response?.data as Record<string, string>)?.detail
          : undefined;
      messageApi.error(detail || "更新提交失败");
      setUpdatingGroupId(null);
    }
  };

  const groupType = Form.useWatch("group_type", form);

  const columns = [
    {
      title: "名称",
      dataIndex: "name",
      key: "name",
      render: (text: string, record: StockGroup) => (
        <a onClick={() => showDetail(record)}>{text}</a>
      ),
    },
    {
      title: "描述",
      dataIndex: "description",
      key: "description",
      ellipsis: true,
    },
    {
      title: "类型",
      dataIndex: "group_type",
      key: "group_type",
      width: 90,
      render: (t: string) => (
        <Tag color={t === "builtin" ? "gold" : t === "filter" ? "blue" : "default"}>
          {t === "builtin" ? "内置" : t === "filter" ? "筛选" : "手动"}
        </Tag>
      ),
    },
    {
      title: "成员数",
      dataIndex: "member_count",
      key: "member_count",
      width: 80,
    },
    {
      title: "更新时间",
      dataIndex: "updated_at",
      key: "updated_at",
      width: 170,
      ellipsis: true,
    },
    {
      title: "操作",
      key: "actions",
      width: 160,
      render: (_: unknown, record: StockGroup) => (
        <Space size="small">
          <Button
            size="small"
            icon={<CloudDownloadOutlined />}
            loading={updatingGroupId === record.id}
            disabled={updatingGroupId !== null && updatingGroupId !== record.id}
            onClick={() => handleUpdateGroupData(record.id, record.name)}
          >
            更新数据
          </Button>
          {record.group_type !== "builtin" && (
            <>
              <Button size="small" icon={<EditOutlined />} onClick={() => openEdit(record)} />
              <Popconfirm title="确定删除此分组?" onConfirm={() => handleDelete(record.id)}>
                <Button size="small" danger icon={<DeleteOutlined />} />
              </Popconfirm>
            </>
          )}
        </Space>
      ),
    },
  ];

  return (
    <>
      {contextHolder}
      <Card
        title="股票分组"
        extra={
          <Space>
            <Button
              icon={<SyncOutlined spin={refreshingIndices} />}
              size="small"
              loading={refreshingIndices}
              onClick={handleRefreshIndices}
            >
              刷新指数成分
            </Button>
            <Button icon={<ReloadOutlined />} size="small" onClick={fetchGroups}>
              刷新
            </Button>
            <Button type="primary" icon={<PlusOutlined />} size="small" onClick={openCreate}>
              新建分组
            </Button>
          </Space>
        }
      >
        <Table
          dataSource={groups}
          columns={columns}
          rowKey="id"
          loading={loading}
          size="small"
          pagination={false}
        />
      </Card>

      {/* Create / Edit Modal */}
      <Modal
        title={editingGroup ? "编辑分组" : "新建分组"}
        open={modalOpen}
        onOk={handleSubmit}
        onCancel={() => setModalOpen(false)}
        confirmLoading={submitting}
        destroyOnClose
      >
        <Form form={form} layout="vertical" initialValues={{ group_type: "manual" }}>
          <Form.Item name="name" label="名称" rules={[{ required: true, message: "请输入分组名称" }]}>
            <Input placeholder="分组名称" />
          </Form.Item>
          <Form.Item name="description" label="描述">
            <Input placeholder="可选描述" />
          </Form.Item>
          {!editingGroup && (
            <Form.Item name="group_type" label="类型">
              <Select
                options={[
                  { value: "manual", label: "手动 (指定股票列表)" },
                  { value: "filter", label: "筛选 (SQL WHERE 条件)" },
                ]}
              />
            </Form.Item>
          )}
          {(groupType === "manual" || editingGroup?.group_type === "manual") && (
            <Form.Item name="tickers_text" label="股票列表" help="用逗号或空格分隔股票代码">
              <TextArea rows={3} placeholder="AAPL, MSFT, GOOG" />
            </Form.Item>
          )}
          {(groupType === "filter" || editingGroup?.group_type === "filter") && (
            <>
              <Form.Item
                name="filter_expr"
                label="筛选条件"
                help="SQL WHERE 子句，例如: sector = 'Technology'"
              >
                <TextArea rows={2} placeholder="sector = 'Technology'" />
              </Form.Item>
              <Alert
                type="info"
                showIcon
                message="筛选条件说明"
                description={
                  <div>
                    <Text type="secondary">可用字段: ticker, name, exchange, sector, status</Text>
                    <br />
                    <Text type="secondary">示例:</Text>
                    <ul style={{ margin: "4px 0", paddingLeft: 20 }}>
                      <li><Text code>exchange = 'NYSE'</Text> — NYSE 上市股票</li>
                      <li><Text code>exchange IN ('NYSE', 'NASDAQ')</Text> — 主要交易所</li>
                      <li><Text code>sector = 'Technology'</Text> — 科技行业</li>
                      <li><Text code>status = 'active'</Text> — 活跃状态</li>
                    </ul>
                  </div>
                }
                style={{ marginBottom: 16 }}
              />
            </>
          )}
        </Form>
      </Modal>

      {/* Detail Modal */}
      <Modal
        title={detailGroup ? `分组详情: ${detailGroup.name}` : "分组详情"}
        open={detailOpen}
        onCancel={() => setDetailOpen(false)}
        footer={null}
        width={600}
      >
        {detailGroup && (
          <Space direction="vertical" style={{ width: "100%" }} size="middle">
            <Descriptions size="small" column={2} bordered>
              <Descriptions.Item label="名称">{detailGroup.name}</Descriptions.Item>
              <Descriptions.Item label="类型">
                <Tag>{detailGroup.group_type}</Tag>
              </Descriptions.Item>
              <Descriptions.Item label="描述" span={2}>
                {detailGroup.description ?? "-"}
              </Descriptions.Item>
              {detailGroup.filter_expr && (
                <Descriptions.Item label="筛选条件" span={2}>
                  <code>{detailGroup.filter_expr}</code>
                </Descriptions.Item>
              )}
              <Descriptions.Item label="成员数">{detailGroup.member_count}</Descriptions.Item>
              <Descriptions.Item label="更新时间">{detailGroup.updated_at}</Descriptions.Item>
            </Descriptions>
            <Paragraph strong>成员列表:</Paragraph>
            <div style={{ maxHeight: 200, overflowY: "auto" }}>
              <Space wrap size={[4, 4]}>
                {(detailGroup.tickers ?? []).map((t) => (
                  <Tag key={t}>{t}</Tag>
                ))}
                {(detailGroup.tickers ?? []).length === 0 && (
                  <Text type="secondary">暂无成员</Text>
                )}
              </Space>
            </div>
          </Space>
        )}
      </Modal>
    </>
  );
}

// ---- Main Page ----

export default function DataManagePage() {
  return (
    <Space direction="vertical" style={{ width: "100%" }} size="middle">
      <DataStatusSection />
      <DataUpdateSection />
      <StockGroupsSection />
    </Space>
  );
}

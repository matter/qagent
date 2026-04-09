import { useState, useEffect, useCallback } from "react";
import {
  Button,
  Card,
  Col,
  DatePicker,
  Input,
  InputNumber,
  message,
  Modal,
  Popconfirm,
  Row,
  Select,
  Space,
  Statistic,
  Table,
  Tabs,
  Tag,
  Typography,
} from "antd";
import {
  PauseCircleOutlined,
  DeleteOutlined,
  PlusOutlined,
  ReloadOutlined,
  FastForwardOutlined,
  CaretRightOutlined,
} from "@ant-design/icons";
import dayjs from "dayjs";
import {
  listStrategies,
  listGroups,
  listPaperSessions,
  createPaperSession,
  deletePaperSession,
  pausePaperSession,
  resumePaperSession,
  advancePaperSession,
  getPaperDailySeries,
  getPaperPositions,
  getPaperTrades,
  getPaperSummary,
} from "../api";
import type {
  Strategy,
  StockGroup,
  PaperTradingSession,
  PaperDailyRecord,
  PaperPosition,
  PaperTrade,
} from "../api";

const { Text } = Typography;

export default function PaperTrading() {
  const [sessions, setSessions] = useState<PaperTradingSession[]>([]);
  const [loading, setLoading] = useState(false);
  const [createOpen, setCreateOpen] = useState(false);
  const [selectedSession, setSelectedSession] = useState<string | null>(null);
  const [messageApi, contextHolder] = message.useMessage();

  const fetchSessions = useCallback(async () => {
    setLoading(true);
    try {
      const data = await listPaperSessions();
      setSessions(data);
    } catch {
      messageApi.error("加载模拟交易会话失败");
    } finally {
      setLoading(false);
    }
  }, [messageApi]);

  useEffect(() => {
    fetchSessions();
  }, [fetchSessions]);

  return (
    <>
      {contextHolder}
      <Space direction="vertical" style={{ width: "100%" }} size="middle">
        <Card
          title="模拟交易"
          extra={
            <Space>
              <Button icon={<ReloadOutlined />} size="small" onClick={fetchSessions}>
                刷新
              </Button>
              <Button
                type="primary"
                icon={<PlusOutlined />}
                size="small"
                onClick={() => setCreateOpen(true)}
              >
                新建会话
              </Button>
            </Space>
          }
        >
          <SessionTable
            sessions={sessions}
            loading={loading}
            onSelect={setSelectedSession}
            onRefresh={fetchSessions}
            messageApi={messageApi}
          />
        </Card>

        {selectedSession && (
          <SessionDetail
            sessionId={selectedSession}
            messageApi={messageApi}
          />
        )}
      </Space>

      <CreateSessionModal
        open={createOpen}
        onClose={() => setCreateOpen(false)}
        onCreated={() => {
          setCreateOpen(false);
          fetchSessions();
        }}
        messageApi={messageApi}
      />
    </>
  );
}

// ---- Session Table ----

function SessionTable({
  sessions,
  loading,
  onSelect,
  onRefresh,
  messageApi,
}: {
  sessions: PaperTradingSession[];
  loading: boolean;
  onSelect: (id: string) => void;
  onRefresh: () => void;
  messageApi: ReturnType<typeof message.useMessage>[0];
}) {
  const [advancing, setAdvancing] = useState<string | null>(null);

  const handleAdvance = async (id: string) => {
    setAdvancing(id);
    try {
      const result = await advancePaperSession(id);
      if (result.days_processed > 0) {
        messageApi.success(`推进了 ${result.days_processed} 个交易日，${result.new_trades ?? 0} 笔交易`);
      } else {
        messageApi.info(result.message ?? "已是最新");
      }
      onRefresh();
    } catch {
      messageApi.error("推进失败");
    } finally {
      setAdvancing(null);
    }
  };

  const handleDelete = async (id: string) => {
    try {
      await deletePaperSession(id);
      messageApi.success("已删除");
      onRefresh();
    } catch {
      messageApi.error("删除失败");
    }
  };

  const handlePause = async (id: string) => {
    try {
      await pausePaperSession(id);
      onRefresh();
    } catch {
      messageApi.error("暂停失败");
    }
  };

  const handleResume = async (id: string) => {
    try {
      await resumePaperSession(id);
      onRefresh();
    } catch {
      messageApi.error("恢复失败");
    }
  };

  const columns = [
    {
      title: "名称",
      dataIndex: "name",
      key: "name",
      ellipsis: true,
    },
    {
      title: "策略",
      dataIndex: "strategy_name",
      key: "strategy_name",
      width: 150,
      render: (v: string | null) => v ?? "-",
    },
    {
      title: "状态",
      dataIndex: "status",
      key: "status",
      width: 80,
      render: (s: string) => {
        const colorMap: Record<string, string> = {
          active: "green",
          paused: "orange",
          stopped: "red",
        };
        return <Tag color={colorMap[s] ?? "default"}>{s}</Tag>;
      },
    },
    {
      title: "起始日",
      dataIndex: "start_date",
      key: "start_date",
      width: 110,
    },
    {
      title: "当前日",
      dataIndex: "current_date",
      key: "current_date",
      width: 110,
      render: (v: string | null) => v ?? "未开始",
    },
    {
      title: "净值",
      key: "nav",
      width: 120,
      render: (_: unknown, r: PaperTradingSession) => {
        if (!r.current_nav || !r.initial_capital) return "-";
        const ret = r.current_nav / r.initial_capital - 1;
        const color = ret >= 0 ? "#52c41a" : "#ff4d4f";
        return (
          <Text style={{ color }}>
            {(ret * 100).toFixed(2)}%
          </Text>
        );
      },
    },
    {
      title: "交易数",
      dataIndex: "total_trades",
      key: "total_trades",
      width: 80,
    },
    {
      title: "操作",
      key: "actions",
      width: 180,
      render: (_: unknown, r: PaperTradingSession) => (
        <Space size="small">
          <Button
            size="small"
            type="primary"
            icon={<FastForwardOutlined />}
            loading={advancing === r.id}
            disabled={r.status !== "active"}
            onClick={(e) => {
              e.stopPropagation();
              handleAdvance(r.id);
            }}
          >
            推进
          </Button>
          {r.status === "active" ? (
            <Button
              size="small"
              icon={<PauseCircleOutlined />}
              onClick={(e) => {
                e.stopPropagation();
                handlePause(r.id);
              }}
            />
          ) : r.status === "paused" ? (
            <Button
              size="small"
              icon={<CaretRightOutlined />}
              onClick={(e) => {
                e.stopPropagation();
                handleResume(r.id);
              }}
            />
          ) : null}
          <Popconfirm
            title="确定删除？"
            onConfirm={(e) => {
              e?.stopPropagation();
              handleDelete(r.id);
            }}
            onCancel={(e) => e?.stopPropagation()}
          >
            <Button
              size="small"
              danger
              icon={<DeleteOutlined />}
              onClick={(e) => e.stopPropagation()}
            />
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <Table
      dataSource={sessions}
      columns={columns}
      rowKey="id"
      loading={loading}
      size="small"
      pagination={false}
      onRow={(r) => ({
        onClick: () => onSelect(r.id),
        style: { cursor: "pointer" },
      })}
    />
  );
}

// ---- Session Detail ----

function SessionDetail({
  sessionId,
  messageApi,
}: {
  sessionId: string;
  messageApi: ReturnType<typeof message.useMessage>[0];
}) {
  const [dailySeries, setDailySeries] = useState<PaperDailyRecord[]>([]);
  const [positions, setPositions] = useState<PaperPosition[]>([]);
  const [trades, setTrades] = useState<PaperTrade[]>([]);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const [summary, setSummary] = useState<any>(null);
  const [loading, setLoading] = useState(false);

  const fetchDetail = useCallback(async () => {
    setLoading(true);
    try {
      const [s, d, p, t] = await Promise.all([
        getPaperSummary(sessionId),
        getPaperDailySeries(sessionId),
        getPaperPositions(sessionId),
        getPaperTrades(sessionId),
      ]);
      setSummary(s);
      setDailySeries(d);
      setPositions(p);
      setTrades(t);
    } catch {
      messageApi.error("加载详情失败");
    } finally {
      setLoading(false);
    }
  }, [sessionId, messageApi]);

  useEffect(() => {
    fetchDetail();
  }, [fetchDetail]);

  const tabItems = [
    {
      key: "nav",
      label: "净值曲线",
      children: <NavTable data={dailySeries} initialCapital={summary?.initial_capital ?? 1000000} />,
    },
    {
      key: "positions",
      label: `持仓 (${positions.length})`,
      children: <PositionsTable data={positions} />,
    },
    {
      key: "trades",
      label: `交易记录 (${trades.length})`,
      children: <TradesTable data={trades} />,
    },
  ];

  const totalReturn = summary?.total_return ?? 0;
  const maxDD = summary?.max_drawdown ?? 0;
  const tradingDays = summary?.trading_days ?? 0;
  const latestNav = summary?.latest_nav;

  return (
    <Card
      title={`会话详情: ${summary?.name ?? sessionId}`}
      loading={loading}
      extra={
        <Button icon={<ReloadOutlined />} size="small" onClick={fetchDetail}>
          刷新
        </Button>
      }
    >
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={6}>
          <Statistic
            title="总收益"
            value={totalReturn * 100}
            precision={2}
            suffix="%"
            valueStyle={{ color: totalReturn >= 0 ? "#52c41a" : "#ff4d4f" }}
          />
        </Col>
        <Col span={6}>
          <Statistic
            title="最大回撤"
            value={maxDD * 100}
            precision={2}
            suffix="%"
            valueStyle={{ color: "#ff4d4f" }}
          />
        </Col>
        <Col span={6}>
          <Statistic title="交易天数" value={tradingDays} />
        </Col>
        <Col span={6}>
          <Statistic
            title="当前净值"
            value={latestNav ?? "-"}
            precision={0}
          />
        </Col>
      </Row>
      <Tabs items={tabItems} />
    </Card>
  );
}

// ---- Sub-components ----

function NavTable({ data, initialCapital }: { data: PaperDailyRecord[]; initialCapital: number }) {
  const columns = [
    { title: "日期", dataIndex: "date", key: "date", width: 120 },
    {
      title: "净值",
      dataIndex: "nav",
      key: "nav",
      render: (v: number) => v?.toLocaleString(undefined, { maximumFractionDigits: 0 }),
    },
    {
      title: "收益率",
      key: "return",
      render: (_: unknown, r: PaperDailyRecord) => {
        const ret = r.nav / initialCapital - 1;
        return (
          <Text style={{ color: ret >= 0 ? "#52c41a" : "#ff4d4f" }}>
            {(ret * 100).toFixed(2)}%
          </Text>
        );
      },
    },
    {
      title: "现金",
      dataIndex: "cash",
      key: "cash",
      render: (v: number) => v?.toLocaleString(undefined, { maximumFractionDigits: 0 }),
    },
  ];
  return (
    <Table
      dataSource={data}
      columns={columns}
      rowKey="date"
      size="small"
      pagination={{ pageSize: 30 }}
    />
  );
}

function PositionsTable({ data }: { data: PaperPosition[] }) {
  const columns = [
    { title: "股票", dataIndex: "ticker", key: "ticker", width: 100 },
    {
      title: "持仓数量",
      dataIndex: "shares",
      key: "shares",
      render: (v: number) => v?.toFixed(2),
    },
    {
      title: "均价",
      dataIndex: "avg_price",
      key: "avg_price",
      render: (v: number) => v?.toFixed(2),
    },
  ];
  return (
    <Table
      dataSource={data}
      columns={columns}
      rowKey="ticker"
      size="small"
      pagination={{ pageSize: 50 }}
    />
  );
}

function TradesTable({ data }: { data: PaperTrade[] }) {
  const columns = [
    { title: "日期", dataIndex: "date", key: "date", width: 110 },
    { title: "股票", dataIndex: "ticker", key: "ticker", width: 80 },
    {
      title: "方向",
      dataIndex: "action",
      key: "action",
      width: 60,
      render: (v: string) => (
        <Tag color={v === "buy" ? "green" : "red"}>
          {v === "buy" ? "买入" : "卖出"}
        </Tag>
      ),
    },
    {
      title: "数量",
      dataIndex: "shares",
      key: "shares",
      render: (v: number) => v?.toFixed(2),
    },
    {
      title: "价格",
      dataIndex: "price",
      key: "price",
      render: (v: number) => v?.toFixed(2),
    },
    {
      title: "成本",
      dataIndex: "cost",
      key: "cost",
      render: (v: number) => v?.toFixed(2),
    },
  ];
  return (
    <Table
      dataSource={data}
      columns={columns}
      rowKey={(r, i) => `${r.date}-${r.ticker}-${i}`}
      size="small"
      pagination={{ pageSize: 50 }}
    />
  );
}

// ---- Create Session Modal ----

function CreateSessionModal({
  open,
  onClose,
  onCreated,
  messageApi,
}: {
  open: boolean;
  onClose: () => void;
  onCreated: () => void;
  messageApi: ReturnType<typeof message.useMessage>[0];
}) {
  const [strategies, setStrategies] = useState<Strategy[]>([]);
  const [groups, setGroups] = useState<StockGroup[]>([]);
  const [strategyId, setStrategyId] = useState("");
  const [groupId, setGroupId] = useState("");
  const [startDate, setStartDate] = useState(dayjs().subtract(30, "day"));
  const [name, setName] = useState("");
  const [initialCapital, setInitialCapital] = useState(1000000);
  const [maxPositions, setMaxPositions] = useState(50);
  const [commission, setCommission] = useState(0.001);
  const [slippage, setSlippage] = useState(0.001);
  const [creating, setCreating] = useState(false);

  useEffect(() => {
    if (open) {
      listStrategies().then(setStrategies).catch(() => {});
      listGroups().then(setGroups).catch(() => {});
    }
  }, [open]);

  const handleCreate = async () => {
    if (!strategyId || !groupId) {
      messageApi.warning("请选择策略和股票分组");
      return;
    }
    setCreating(true);
    try {
      await createPaperSession({
        strategy_id: strategyId,
        universe_group_id: groupId,
        start_date: startDate.format("YYYY-MM-DD"),
        name: name || undefined,
        config: {
          initial_capital: initialCapital,
          max_positions: maxPositions,
          commission_rate: commission,
          slippage_rate: slippage,
        },
      });
      messageApi.success("模拟交易会话已创建");
      onCreated();
    } catch {
      messageApi.error("创建失败");
    } finally {
      setCreating(false);
    }
  };

  return (
    <Modal
      title="新建模拟交易会话"
      open={open}
      onCancel={onClose}
      onOk={handleCreate}
      confirmLoading={creating}
      okText="创建"
      width={600}
      destroyOnClose
    >
      <Space direction="vertical" style={{ width: "100%" }} size="middle">
        <div>
          <Text type="secondary" style={{ fontSize: 12 }}>会话名称</Text>
          <Input
            placeholder="可选，留空自动生成"
            value={name}
            onChange={(e) => setName(e.target.value)}
          />
        </div>
        <Row gutter={12}>
          <Col span={12}>
            <Text type="secondary" style={{ fontSize: 12 }}>策略</Text>
            <Select
              style={{ width: "100%" }}
              placeholder="选择策略..."
              value={strategyId || undefined}
              onChange={setStrategyId}
              options={strategies.map((s) => ({
                value: s.id,
                label: `${s.name} v${s.version}`,
              }))}
              showSearch
              optionFilterProp="label"
            />
          </Col>
          <Col span={12}>
            <Text type="secondary" style={{ fontSize: 12 }}>股票分组</Text>
            <Select
              style={{ width: "100%" }}
              placeholder="选择分组..."
              value={groupId || undefined}
              onChange={setGroupId}
              options={groups.map((g) => ({
                value: g.id,
                label: `${g.name} (${g.member_count})`,
              }))}
              showSearch
              optionFilterProp="label"
            />
          </Col>
        </Row>
        <Row gutter={12}>
          <Col span={8}>
            <Text type="secondary" style={{ fontSize: 12 }}>模拟起始日期</Text>
            <DatePicker
              style={{ width: "100%" }}
              value={startDate}
              onChange={(v) => { if (v) setStartDate(v); }}
            />
          </Col>
          <Col span={8}>
            <Text type="secondary" style={{ fontSize: 12 }}>初始资金</Text>
            <InputNumber
              style={{ width: "100%" }}
              value={initialCapital}
              onChange={(v) => setInitialCapital(v ?? 1000000)}
              min={10000}
              step={100000}
            />
          </Col>
          <Col span={8}>
            <Text type="secondary" style={{ fontSize: 12 }}>最大持仓数</Text>
            <InputNumber
              style={{ width: "100%" }}
              value={maxPositions}
              onChange={(v) => setMaxPositions(v ?? 50)}
              min={1}
              max={500}
            />
          </Col>
        </Row>
        <Row gutter={12}>
          <Col span={8}>
            <Text type="secondary" style={{ fontSize: 12 }}>佣金费率</Text>
            <InputNumber
              style={{ width: "100%" }}
              value={commission}
              onChange={(v) => setCommission(v ?? 0.001)}
              min={0}
              max={0.1}
              step={0.0001}
            />
          </Col>
          <Col span={8}>
            <Text type="secondary" style={{ fontSize: 12 }}>滑点费率</Text>
            <InputNumber
              style={{ width: "100%" }}
              value={slippage}
              onChange={(v) => setSlippage(v ?? 0.001)}
              min={0}
              max={0.1}
              step={0.0001}
            />
          </Col>
        </Row>
      </Space>
    </Modal>
  );
}

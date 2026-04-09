import { useState, useEffect, useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import {
  Button,
  Card,
  Col,
  DatePicker,
  Dropdown,
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
  StepForwardOutlined,
  LineChartOutlined,
} from "@ant-design/icons";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
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
  getPaperLatestSignals,
  getPaperStockChart,
} from "../api";
import type {
  Strategy,
  StockGroup,
  StockChartData,
  PaperTradingSession,
  PaperDailyRecord,
  PaperPosition,
  PaperTrade,
  PaperActionPlan,
} from "../api";

const { Text } = Typography;

export interface PaperTradingPrefill {
  strategyId?: string;
  groupId?: string;
  initialCapital?: number;
  maxPositions?: number;
  commission?: number;
  slippage?: number;
}

export default function PaperTrading() {
  const [sessions, setSessions] = useState<PaperTradingSession[]>([]);
  const [loading, setLoading] = useState(false);
  const [createOpen, setCreateOpen] = useState(false);
  const [selectedSession, setSelectedSession] = useState<string | null>(null);
  const [messageApi, contextHolder] = message.useMessage();
  const [searchParams, setSearchParams] = useSearchParams();

  // Read prefill params from URL (e.g. coming from backtest history)
  const [prefill, setPrefill] = useState<PaperTradingPrefill | null>(null);
  useEffect(() => {
    const sid = searchParams.get("strategy_id");
    if (sid) {
      setPrefill({
        strategyId: sid,
        groupId: searchParams.get("group_id") ?? undefined,
        initialCapital: searchParams.get("initial_capital") ? Number(searchParams.get("initial_capital")) : undefined,
        maxPositions: searchParams.get("max_positions") ? Number(searchParams.get("max_positions")) : undefined,
        commission: searchParams.get("commission") ? Number(searchParams.get("commission")) : undefined,
        slippage: searchParams.get("slippage") ? Number(searchParams.get("slippage")) : undefined,
      });
      setCreateOpen(true);
      setSearchParams({}, { replace: true });
    }
  }, [searchParams, setSearchParams]);

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
        onClose={() => { setCreateOpen(false); setPrefill(null); }}
        onCreated={() => {
          setCreateOpen(false);
          setPrefill(null);
          fetchSessions();
        }}
        messageApi={messageApi}
        prefill={prefill}
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

  const handleAdvance = async (id: string, steps?: number) => {
    setAdvancing(id);
    try {
      const result = await advancePaperSession(id, undefined, steps);
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
      width: 220,
      render: (_: unknown, r: PaperTradingSession) => (
        <Space size="small">
          <Dropdown
            menu={{
              items: [
                {
                  key: "step",
                  icon: <StepForwardOutlined />,
                  label: "推进 1 天",
                  onClick: () => handleAdvance(r.id, 1),
                },
                {
                  key: "all",
                  icon: <FastForwardOutlined />,
                  label: "推进至最新",
                  onClick: () => handleAdvance(r.id),
                },
              ],
            }}
            disabled={r.status !== "active" || advancing === r.id}
          >
            <Button
              size="small"
              type="primary"
              icon={<FastForwardOutlined />}
              loading={advancing === r.id}
              disabled={r.status !== "active"}
              onClick={(e) => {
                e.stopPropagation();
              }}
            >
              推进
            </Button>
          </Dropdown>
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

  // Signals / T+1
  const [actionPlan, setActionPlan] = useState<PaperActionPlan[]>([]);
  const [signalTargetDate, setSignalTargetDate] = useState<string | null>(null);
  const [signalsLoading, setSignalsLoading] = useState(false);

  // Stock chart modal
  const [chartOpen, setChartOpen] = useState(false);
  const [chartTicker, setChartTicker] = useState("");
  const [chartData, setChartData] = useState<StockChartData | null>(null);
  const [chartLoading, setChartLoading] = useState(false);

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

  const fetchSignals = useCallback(async () => {
    setSignalsLoading(true);
    try {
      const result = await getPaperLatestSignals(sessionId);
      setActionPlan(result.action_plan);
      setSignalTargetDate(result.target_date);
    } catch {
      messageApi.error("加载信号失败");
    } finally {
      setSignalsLoading(false);
    }
  }, [sessionId, messageApi]);

  useEffect(() => {
    fetchDetail();
  }, [fetchDetail]);

  const openStockChart = useCallback(async (ticker: string) => {
    setChartTicker(ticker);
    setChartOpen(true);
    setChartLoading(true);
    setChartData(null);
    try {
      const data = await getPaperStockChart(sessionId, ticker);
      setChartData(data);
    } catch {
      messageApi.error("加载股票数据失败");
    } finally {
      setChartLoading(false);
    }
  }, [sessionId, messageApi]);

  // Collect unique traded tickers from trade history
  const tradedTickers = [...new Set(trades.map(t => t.ticker))].sort();

  const tabItems = [
    {
      key: "nav",
      label: "净值曲线",
      children: <NavTable data={dailySeries} initialCapital={summary?.initial_capital ?? 1000000} />,
    },
    {
      key: "positions",
      label: `持仓 (${positions.length})`,
      children: <PositionsTable data={positions} onTickerClick={openStockChart} />,
    },
    {
      key: "trades",
      label: `交易记录 (${trades.length})`,
      children: <TradesTable data={trades} onTickerClick={openStockChart} />,
    },
    {
      key: "signals",
      label: "T+1 操作计划",
      children: (
        <SignalsPanel
          actionPlan={actionPlan}
          targetDate={signalTargetDate}
          loading={signalsLoading}
          onRefresh={fetchSignals}
          onTickerClick={openStockChart}
        />
      ),
    },
    {
      key: "chart",
      label: `买卖打点 (${tradedTickers.length})`,
      children: (
        <TradedTickersList
          tickers={tradedTickers}
          onTickerClick={openStockChart}
        />
      ),
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
      <Tabs
        items={tabItems}
        onChange={(key) => {
          if (key === "signals" && actionPlan.length === 0) fetchSignals();
        }}
      />

      {/* Stock chart modal */}
      <StockChartModal
        open={chartOpen}
        onClose={() => setChartOpen(false)}
        ticker={chartTicker}
        data={chartData}
        loading={chartLoading}
      />
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

function PositionsTable({ data, onTickerClick }: { data: PaperPosition[]; onTickerClick: (t: string) => void }) {
  const columns = [
    {
      title: "股票",
      dataIndex: "ticker",
      key: "ticker",
      width: 100,
      render: (v: string) => (
        <Button type="link" size="small" onClick={() => onTickerClick(v)} style={{ padding: 0 }}>
          {v}
        </Button>
      ),
    },
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

function TradesTable({ data, onTickerClick }: { data: PaperTrade[]; onTickerClick: (t: string) => void }) {
  const columns = [
    { title: "日期", dataIndex: "date", key: "date", width: 110 },
    {
      title: "股票",
      dataIndex: "ticker",
      key: "ticker",
      width: 80,
      render: (v: string) => (
        <Button type="link" size="small" onClick={() => onTickerClick(v)} style={{ padding: 0 }}>
          {v}
        </Button>
      ),
    },
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

// ---- Signals / T+1 Plan ----

function SignalsPanel({
  actionPlan,
  targetDate,
  loading,
  onRefresh,
  onTickerClick,
}: {
  actionPlan: PaperActionPlan[];
  targetDate: string | null;
  loading: boolean;
  onRefresh: () => void;
  onTickerClick: (t: string) => void;
}) {
  const columns = [
    {
      title: "股票",
      dataIndex: "ticker",
      key: "ticker",
      width: 100,
      render: (v: string) => (
        <Button type="link" size="small" onClick={() => onTickerClick(v)} style={{ padding: 0 }}>
          {v}
        </Button>
      ),
    },
    {
      title: "操作",
      dataIndex: "action",
      key: "action",
      width: 80,
      render: (v: string) => {
        const map: Record<string, { color: string; label: string }> = {
          buy: { color: "green", label: "买入" },
          sell: { color: "red", label: "卖出" },
          hold: { color: "blue", label: "持有" },
        };
        const c = map[v] ?? { color: "default", label: v };
        return <Tag color={c.color}>{c.label}</Tag>;
      },
    },
    {
      title: "当前持仓",
      dataIndex: "current_shares",
      key: "current_shares",
      render: (v: number) => v > 0 ? v.toFixed(2) : "-",
    },
    {
      title: "目标权重",
      dataIndex: "target_weight",
      key: "target_weight",
      render: (v: number) => v > 0 ? `${(v * 100).toFixed(2)}%` : "-",
    },
  ];

  return (
    <div>
      <Space style={{ marginBottom: 12 }}>
        <Text type="secondary">
          {targetDate ? `T+1 目标日期: ${targetDate}` : "暂无信号"}
        </Text>
        <Button size="small" icon={<ReloadOutlined />} onClick={onRefresh} loading={loading}>
          刷新信号
        </Button>
      </Space>
      <Table
        dataSource={actionPlan}
        columns={columns}
        rowKey="ticker"
        size="small"
        loading={loading}
        pagination={{ pageSize: 50 }}
      />
    </div>
  );
}

// ---- Traded tickers list for chart browsing ----

function TradedTickersList({
  tickers,
  onTickerClick,
}: {
  tickers: string[];
  onTickerClick: (t: string) => void;
}) {
  return (
    <div>
      <Text type="secondary" style={{ display: "block", marginBottom: 12 }}>
        点击股票代码查看 K 线及买卖标记
      </Text>
      <Space wrap>
        {tickers.map((t) => (
          <Button
            key={t}
            size="small"
            icon={<LineChartOutlined />}
            onClick={() => onTickerClick(t)}
          >
            {t}
          </Button>
        ))}
        {tickers.length === 0 && <Text type="secondary">暂无交易记录</Text>}
      </Space>
    </div>
  );
}

// ---- Stock Chart Modal (reuses backtest chart logic) ----

function StockChartModal({
  open,
  onClose,
  ticker,
  data,
  loading,
}: {
  open: boolean;
  onClose: () => void;
  ticker: string;
  data: StockChartData | null;
  loading: boolean;
}) {
  const buildOption = (d: StockChartData): EChartsOption => {
    const dates = d.daily_bars.map((b) => b.date);
    const ohlc = d.daily_bars.map((b) => [b.open, b.close, b.low, b.high]);
    const volumes = d.daily_bars.map((b) => b.volume);

    const buyMarkers = d.trades
      .filter((t) => t.action === "buy")
      .map((t) => ({
        name: "BUY",
        coord: [t.date, t.price],
        value: t.price.toFixed(2),
        itemStyle: { color: "#52c41a" },
      }));

    const sellMarkers = d.trades
      .filter((t) => t.action === "sell")
      .map((t) => ({
        name: "SELL",
        coord: [t.date, t.price],
        value: t.price.toFixed(2),
        itemStyle: { color: "#ff4d4f" },
      }));

    return {
      animation: false,
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "cross" },
        backgroundColor: "rgba(30,30,30,0.9)",
        borderColor: "#555",
        textStyle: { color: "#eee", fontSize: 12 },
      },
      legend: {
        data: [ticker, "成交量"],
        textStyle: { color: "#aaa", fontSize: 11 },
        top: 5,
      },
      grid: [
        { left: 60, right: 20, top: 50, height: "55%" },
        { left: 60, right: 20, top: "75%", height: "15%" },
      ],
      xAxis: [
        {
          type: "category",
          data: dates,
          axisLine: { lineStyle: { color: "#555" } },
          axisLabel: { color: "#aaa", fontSize: 10 },
          splitLine: { show: false },
          boundaryGap: true,
          axisPointer: { z: 100 },
        },
        {
          type: "category",
          gridIndex: 1,
          data: dates,
          axisLine: { lineStyle: { color: "#555" } },
          axisLabel: { show: false },
          splitLine: { show: false },
          boundaryGap: true,
        },
      ],
      yAxis: [
        {
          type: "value",
          scale: true,
          axisLine: { lineStyle: { color: "#555" } },
          axisLabel: { color: "#aaa", fontSize: 10 },
          splitLine: { lineStyle: { color: "rgba(255,255,255,0.06)" } },
        },
        {
          type: "value",
          gridIndex: 1,
          scale: true,
          axisLine: { lineStyle: { color: "#555" } },
          axisLabel: { color: "#aaa", fontSize: 9 },
          splitLine: { show: false },
        },
      ],
      dataZoom: [
        { type: "inside", xAxisIndex: [0, 1], start: 0, end: 100 },
        {
          type: "slider", xAxisIndex: [0, 1], top: "92%", height: 20,
          borderColor: "#555", textStyle: { color: "#aaa" },
        },
      ],
      series: [
        {
          name: ticker,
          type: "candlestick",
          data: ohlc,
          itemStyle: {
            color: "#ef5350",
            color0: "#26a69a",
            borderColor: "#ef5350",
            borderColor0: "#26a69a",
          },
          markPoint: {
            symbol: "triangle",
            symbolSize: 12,
            data: [
              ...buyMarkers.map((m) => ({
                ...m,
                symbolRotate: 0,
                symbolSize: 14,
                label: {
                  show: true, position: "bottom" as const, formatter: "B",
                  fontSize: 9, color: "#52c41a", fontWeight: "bold" as const,
                },
              })),
              ...sellMarkers.map((m) => ({
                ...m,
                symbolRotate: 180,
                symbolSize: 14,
                label: {
                  show: true, position: "top" as const, formatter: "S",
                  fontSize: 9, color: "#ff4d4f", fontWeight: "bold" as const,
                },
              })),
            ],
          },
        },
        {
          name: "成交量",
          type: "bar",
          xAxisIndex: 1,
          yAxisIndex: 1,
          data: volumes,
          itemStyle: {
            color: (params: { dataIndex: number }) => {
              const idx = params.dataIndex;
              const item = ohlc[idx];
              return item && item[1] >= item[0]
                ? "rgba(239,83,80,0.5)"
                : "rgba(38,166,154,0.5)";
            },
          },
        },
      ],
    };
  };

  return (
    <Modal
      title={`${ticker} K线 & 交易标记`}
      open={open}
      onCancel={onClose}
      footer={null}
      width={1100}
      destroyOnClose
    >
      {loading && (
        <div style={{ textAlign: "center", padding: 48 }}>
          <Text type="secondary">加载中...</Text>
        </div>
      )}
      {!loading && data && (
        <ReactECharts
          option={buildOption(data)}
          style={{ height: 520 }}
          notMerge
          lazyUpdate
        />
      )}
      {!loading && !data && (
        <div style={{ textAlign: "center", padding: 48 }}>
          <Text type="secondary">暂无数据</Text>
        </div>
      )}
    </Modal>
  );
}

// ---- Create Session Modal ----

function CreateSessionModal({
  open,
  onClose,
  onCreated,
  messageApi,
  prefill,
}: {
  open: boolean;
  onClose: () => void;
  onCreated: () => void;
  messageApi: ReturnType<typeof message.useMessage>[0];
  prefill?: PaperTradingPrefill | null;
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

  // Apply prefill values from backtest history
  useEffect(() => {
    if (!prefill) return;
    if (prefill.strategyId) setStrategyId(prefill.strategyId);
    if (prefill.groupId) setGroupId(prefill.groupId);
    if (prefill.initialCapital) setInitialCapital(prefill.initialCapital);
    if (prefill.maxPositions) setMaxPositions(prefill.maxPositions);
    if (prefill.commission) setCommission(prefill.commission);
    if (prefill.slippage) setSlippage(prefill.slippage);
  }, [prefill]);

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

import { useState, useMemo } from "react";
import { Card, Col, Row, Statistic, Table, Tag, Select, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import type { TradeRecord, StockPnL } from "../../api";
import StockTradeChart from "./StockTradeChart";

const { Text } = Typography;

// ---- Helpers to normalize API data shapes ----

interface NavPoint { date: string; value: number }
type NavInput = NavPoint[] | { dates: string[]; values: number[] } | null | undefined;

function normalizeTimeSeries(input: NavInput): NavPoint[] {
  if (!input) return [];
  if (Array.isArray(input)) return input;
  // {dates: [...], values: [...]} format
  if (input.dates && input.values) {
    return input.dates.map((d: string, i: number) => ({ date: d, value: input.values[i] }));
  }
  return [];
}

type MonthlyInput = Array<{ year: number; month: number; return: number }> | Record<string, Record<string, number>> | null | undefined;

function normalizeMonthlyReturns(input: MonthlyInput): Record<string, Record<string, number>> {
  if (!input) return {};
  if (Array.isArray(input)) {
    // [{year, month, return}, ...] -> {year: {month: value}}
    const result: Record<string, Record<string, number>> = {};
    for (const item of input) {
      const y = String(item.year);
      if (!result[y]) result[y] = {};
      result[y][String(item.month)] = item.return;
    }
    return result;
  }
  return input;
}

// Normalize summary keys (API uses sharpe_ratio, calmar_ratio, etc.)
function normalizeSummaryKey(summary: Record<string, number>, key: string): number | undefined {
  if (summary[key] !== undefined) return summary[key];
  // Try _ratio suffix
  if (summary[key + "_ratio"] !== undefined) return summary[key + "_ratio"];
  return undefined;
}

// ---- Summary Cards ----

interface BacktestSummaryCardsProps {
  summary: Record<string, number>;
}

export function BacktestSummaryCards({ summary }: BacktestSummaryCardsProps) {
  const items: Array<{
    key: string;
    title: string;
    precision: number;
    suffix?: string;
    multiply?: boolean;
    colorFn?: (v: number) => string;
  }> = [
    {
      key: "annual_return",
      title: "年化收益",
      precision: 2,
      suffix: "%",
      multiply: true,
      colorFn: (v) => (v > 0 ? "#52c41a" : "#ff4d4f"),
    },
    {
      key: "sharpe",
      title: "Sharpe",
      precision: 3,
      colorFn: (v) => (v > 1 ? "#52c41a" : v > 0 ? "#1677ff" : "#ff4d4f"),
    },
    {
      key: "max_drawdown",
      title: "最大回撤",
      precision: 2,
      suffix: "%",
      multiply: true,
      colorFn: () => "#ff4d4f",
    },
    {
      key: "calmar",
      title: "Calmar",
      precision: 3,
      colorFn: (v) => (v > 1 ? "#52c41a" : v > 0 ? "#1677ff" : "#ff4d4f"),
    },
    {
      key: "sortino",
      title: "Sortino",
      precision: 3,
      colorFn: (v) => (v > 1 ? "#52c41a" : v > 0 ? "#1677ff" : "#ff4d4f"),
    },
    {
      key: "win_rate",
      title: "胜率",
      precision: 1,
      suffix: "%",
      multiply: true,
    },
    {
      key: "turnover",
      title: "换手率",
      precision: 4,
    },
    {
      key: "total_cost",
      title: "总成本",
      precision: 0,
    },
  ];

  return (
    <Row gutter={[12, 12]}>
      {items.map((item) => {
        const raw = normalizeSummaryKey(summary, item.key);
        if (raw === undefined || raw === null) return null;
        const v = item.multiply ? raw * 100 : raw;
        const color = item.colorFn ? item.colorFn(raw) : undefined;
        return (
          <Col xs={12} sm={8} md={6} lg={3} key={item.key}>
            <Card size="small">
              <Statistic
                title={item.title}
                value={v}
                precision={item.precision}
                suffix={item.suffix}
                valueStyle={{ fontSize: 18, color }}
              />
            </Card>
          </Col>
        );
      })}
    </Row>
  );
}

// ---- NAV Curve Chart ----

export function NavCurveChart({
  navSeries,
  benchmarkNav,
}: {
  navSeries: NavInput;
  benchmarkNav: NavInput;
}) {
  const nav = normalizeTimeSeries(navSeries);
  const bench = normalizeTimeSeries(benchmarkNav);
  if (nav.length === 0) return null;

  const dates = nav.map((d) => d.date);
  const strategyValues = nav.map((d) => d.value);
  const benchmarkValues = bench.map((d) => d.value);

  const option: EChartsOption = {
    animation: false,
    tooltip: {
      trigger: "axis",
      backgroundColor: "rgba(30,30,30,0.9)",
      borderColor: "#555",
      textStyle: { color: "#eee", fontSize: 12 },
    },
    legend: {
      data: ["策略", "基准"],
      textStyle: { color: "#aaa", fontSize: 11 },
      top: 5,
    },
    grid: { left: 60, right: 20, top: 40, bottom: 40 },
    xAxis: {
      type: "category",
      data: dates,
      axisLine: { lineStyle: { color: "#555" } },
      axisLabel: { color: "#aaa", fontSize: 10, rotate: 30 },
      splitLine: { show: false },
    },
    yAxis: {
      type: "value",
      axisLine: { lineStyle: { color: "#555" } },
      axisLabel: { color: "#aaa", fontSize: 10 },
      splitLine: { lineStyle: { color: "rgba(255,255,255,0.06)" } },
    },
    series: [
      {
        name: "策略",
        type: "line",
        data: strategyValues,
        symbol: "none",
        lineStyle: { width: 2, color: "#1677ff" },
        itemStyle: { color: "#1677ff" },
      },
      ...(benchmarkValues.length > 0
        ? [
            {
              name: "基准",
              type: "line" as const,
              data: benchmarkValues,
              symbol: "none" as const,
              lineStyle: { width: 1.5, color: "#999" },
              itemStyle: { color: "#999" },
            },
          ]
        : []),
    ],
  };

  return (
    <Card
      title="净值曲线"
      size="small"
      style={{ background: "rgba(0,0,0,0.2)" }}
      styles={{ body: { padding: 8 } }}
    >
      <ReactECharts option={option} style={{ height: 320 }} notMerge lazyUpdate />
    </Card>
  );
}

// ---- Drawdown Chart ----

export function DrawdownChart({
  drawdownSeries,
}: {
  drawdownSeries: NavInput;
}) {
  const dd = normalizeTimeSeries(drawdownSeries);
  if (dd.length === 0) return null;

  const dates = dd.map((d) => d.date);
  const values = dd.map((d) => +(d.value * 100).toFixed(3));

  const option: EChartsOption = {
    animation: false,
    tooltip: {
      trigger: "axis",
      backgroundColor: "rgba(30,30,30,0.9)",
      borderColor: "#555",
      textStyle: { color: "#eee", fontSize: 12 },
      formatter(params: unknown) {
        const p = Array.isArray(params) ? params[0] : params;
        const item = p as { name: string; value: number };
        return `${item.name}<br/>回撤: ${item.value.toFixed(2)}%`;
      },
    },
    grid: { left: 60, right: 20, top: 20, bottom: 40 },
    xAxis: {
      type: "category",
      data: dates,
      axisLine: { lineStyle: { color: "#555" } },
      axisLabel: { color: "#aaa", fontSize: 10, rotate: 30 },
      splitLine: { show: false },
    },
    yAxis: {
      type: "value",
      axisLine: { lineStyle: { color: "#555" } },
      axisLabel: {
        color: "#aaa",
        fontSize: 10,
        formatter: (v: number) => `${v.toFixed(1)}%`,
      },
      splitLine: { lineStyle: { color: "rgba(255,255,255,0.06)" } },
    },
    series: [
      {
        name: "回撤",
        type: "line",
        data: values,
        symbol: "none",
        lineStyle: { width: 1, color: "#ef5350" },
        areaStyle: { color: "rgba(239,83,80,0.3)" },
        itemStyle: { color: "#ef5350" },
      },
    ],
  };

  return (
    <Card
      title="回撤"
      size="small"
      style={{ background: "rgba(0,0,0,0.2)" }}
      styles={{ body: { padding: 8 } }}
    >
      <ReactECharts option={option} style={{ height: 220 }} notMerge lazyUpdate />
    </Card>
  );
}

// ---- Monthly Returns Heatmap ----

export function MonthlyReturnsHeatmap({
  monthlyReturns,
}: {
  monthlyReturns: MonthlyInput;
}) {
  const normalized = normalizeMonthlyReturns(monthlyReturns);
  if (Object.keys(normalized).length === 0) return null;

  const years = Object.keys(normalized).sort();
  const months = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"];
  const monthLabels = ["1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月"];

  const heatData: [number, number, number | null][] = [];
  let minVal = 0;
  let maxVal = 0;

  for (let yi = 0; yi < years.length; yi++) {
    const yearData = normalized[years[yi]];
    for (let mi = 0; mi < 12; mi++) {
      const v = yearData?.[months[mi]] ?? null;
      if (v !== null) {
        const pct = v * 100;
        heatData.push([mi, yi, +pct.toFixed(2)]);
        if (pct < minVal) minVal = pct;
        if (pct > maxVal) maxVal = pct;
      } else {
        heatData.push([mi, yi, null]);
      }
    }
  }

  const absMax = Math.max(Math.abs(minVal), Math.abs(maxVal), 5);

  const option: EChartsOption = {
    animation: false,
    tooltip: {
      position: "top",
      backgroundColor: "rgba(30,30,30,0.9)",
      borderColor: "#555",
      textStyle: { color: "#eee", fontSize: 12 },
      formatter(params: unknown) {
        const p = params as { value: [number, number, number | null] };
        const [mi, yi, v] = p.value;
        if (v === null) return `${years[yi]} ${monthLabels[mi]}: N/A`;
        return `${years[yi]} ${monthLabels[mi]}: ${v.toFixed(2)}%`;
      },
    },
    grid: { left: 60, right: 40, top: 10, bottom: 60 },
    xAxis: {
      type: "category",
      data: monthLabels,
      axisLabel: { color: "#aaa", fontSize: 10 },
      axisLine: { lineStyle: { color: "#555" } },
      splitLine: { show: false },
      position: "top",
    },
    yAxis: {
      type: "category",
      data: years,
      axisLabel: { color: "#aaa", fontSize: 10 },
      axisLine: { lineStyle: { color: "#555" } },
      splitLine: { show: false },
    },
    visualMap: {
      min: -absMax,
      max: absMax,
      calculable: true,
      orient: "horizontal",
      left: "center",
      bottom: 0,
      inRange: {
        color: ["#d73027", "#f46d43", "#fdae61", "#fee08b", "#ffffbf", "#d9ef8b", "#a6d96a", "#66bd63", "#1a9850"],
      },
      textStyle: { color: "#aaa" },
    },
    series: [
      {
        name: "月度收益",
        type: "heatmap",
        data: heatData.filter((d) => d[2] !== null) as [number, number, number][],
        label: {
          show: true,
          color: "#fff",
          fontSize: 10,
          formatter(params: unknown) {
            const p = params as { value: [number, number, number] };
            return `${p.value[2].toFixed(1)}`;
          },
        },
        emphasis: {
          itemStyle: { shadowBlur: 10, shadowColor: "rgba(0,0,0,0.5)" },
        },
      },
    ],
  };

  const height = Math.max(200, years.length * 40 + 80);

  return (
    <Card
      title="月度收益热力图"
      size="small"
      style={{ background: "rgba(0,0,0,0.2)" }}
      styles={{ body: { padding: 8 } }}
    >
      <ReactECharts option={option} style={{ height }} notMerge lazyUpdate />
    </Card>
  );
}

// ---- Trade Log Table ----

interface TradeLogProps {
  trades: TradeRecord[] | null;
}

export function TradeLogTable({ trades }: TradeLogProps) {
  const [tickerFilter, setTickerFilter] = useState<string | undefined>(undefined);

  const allTickers = useMemo(() => {
    if (!trades) return [];
    const s = new Set(trades.map((t) => t.ticker));
    return Array.from(s).sort();
  }, [trades]);

  const filtered = useMemo(() => {
    if (!trades) return [];
    if (!tickerFilter) return trades;
    return trades.filter((t) => t.ticker === tickerFilter);
  }, [trades, tickerFilter]);

  if (!trades || trades.length === 0) return null;

  const columns: ColumnsType<TradeRecord> = [
    {
      title: "日期",
      dataIndex: "date",
      key: "date",
      width: 110,
      sorter: (a, b) => a.date.localeCompare(b.date),
    },
    {
      title: "股票",
      dataIndex: "ticker",
      key: "ticker",
      width: 90,
    },
    {
      title: "方向",
      dataIndex: "action",
      key: "action",
      width: 70,
      render: (action: string) =>
        action === "buy" ? (
          <Tag color="green">买入</Tag>
        ) : (
          <Tag color="red">卖出</Tag>
        ),
    },
    {
      title: "数量",
      dataIndex: "shares",
      key: "shares",
      width: 100,
      align: "right",
      render: (v: number) => v.toFixed(2),
    },
    {
      title: "价格",
      dataIndex: "price",
      key: "price",
      width: 100,
      align: "right",
      render: (v: number) => v.toFixed(2),
    },
    {
      title: "费用",
      dataIndex: "cost",
      key: "cost",
      width: 90,
      align: "right",
      render: (v: number) => v.toFixed(2),
    },
  ];

  return (
    <Card
      title={`交易记录 (${trades.length} 条)`}
      size="small"
      style={{ background: "rgba(0,0,0,0.2)" }}
      extra={
        <Select
          allowClear
          placeholder="筛选股票"
          style={{ width: 140 }}
          size="small"
          value={tickerFilter}
          onChange={setTickerFilter}
          options={allTickers.map((t) => ({ value: t, label: t }))}
          showSearch
        />
      }
    >
      <Table
        dataSource={filtered}
        columns={columns}
        rowKey={(_, idx) => String(idx)}
        size="small"
        pagination={{ pageSize: 20, showSizeChanger: true, pageSizeOptions: [20, 50, 100] }}
      />
    </Card>
  );
}

// ---- Stock P&L Table ----

interface StockPnLTableProps {
  stockPnl: StockPnL[] | null;
  backtestId?: string;
}

export function StockPnLTable({ stockPnl, backtestId }: StockPnLTableProps) {
  const [chartOpen, setChartOpen] = useState(false);
  const [selectedTicker, setSelectedTicker] = useState("");

  if (!stockPnl || stockPnl.length === 0) return null;

  const handleTickerClick = (ticker: string) => {
    if (!backtestId) return;
    setSelectedTicker(ticker);
    setChartOpen(true);
  };

  const columns: ColumnsType<StockPnL> = [
    {
      title: "股票",
      dataIndex: "ticker",
      key: "ticker",
      width: 90,
      render: (ticker: string) => (
        <a
          onClick={(e) => {
            e.stopPropagation();
            handleTickerClick(ticker);
          }}
          style={{ cursor: backtestId ? "pointer" : "default" }}
        >
          {ticker}
        </a>
      ),
    },
    {
      title: "买入次数",
      dataIndex: "buy_count",
      key: "buy_count",
      width: 80,
      align: "right",
    },
    {
      title: "卖出次数",
      dataIndex: "sell_count",
      key: "sell_count",
      width: 80,
      align: "right",
    },
    {
      title: "买入总额",
      dataIndex: "total_buy_value",
      key: "total_buy_value",
      width: 120,
      align: "right",
      render: (v: number) => v.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
    },
    {
      title: "卖出总额",
      dataIndex: "total_sell_value",
      key: "total_sell_value",
      width: 120,
      align: "right",
      render: (v: number) => v.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
    },
    {
      title: "已实现盈亏",
      dataIndex: "realized_pnl",
      key: "realized_pnl",
      width: 120,
      align: "right",
      defaultSortOrder: "descend",
      sorter: (a, b) => a.realized_pnl - b.realized_pnl,
      render: (v: number) => (
        <Text style={{ color: v > 0 ? "#52c41a" : v < 0 ? "#ff4d4f" : "#aaa" }}>
          {v.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
        </Text>
      ),
    },
    {
      title: "盈亏%",
      dataIndex: "pnl_pct",
      key: "pnl_pct",
      width: 90,
      align: "right",
      sorter: (a, b) => a.pnl_pct - b.pnl_pct,
      render: (v: number) => (
        <Text style={{ color: v > 0 ? "#52c41a" : v < 0 ? "#ff4d4f" : "#aaa" }}>
          {v.toFixed(2)}%
        </Text>
      ),
    },
    {
      title: "胜/负",
      key: "win_loss",
      width: 80,
      align: "center",
      render: (_: unknown, r: StockPnL) => (
        <span>
          <Text style={{ color: "#52c41a" }}>{r.win_count}</Text>
          {" / "}
          <Text style={{ color: "#ff4d4f" }}>{r.loss_count}</Text>
        </span>
      ),
    },
  ];

  return (
    <>
      <Card
        title={`个股盈亏 (${stockPnl.length} 只)`}
        size="small"
        style={{ background: "rgba(0,0,0,0.2)" }}
      >
        <Table
          dataSource={stockPnl}
          columns={columns}
          rowKey="ticker"
          size="small"
          pagination={{ pageSize: 20, showSizeChanger: true, pageSizeOptions: [20, 50, 100] }}
        />
      </Card>
      {backtestId && (
        <StockTradeChart
          open={chartOpen}
          onClose={() => setChartOpen(false)}
          backtestId={backtestId}
          ticker={selectedTicker}
        />
      )}
    </>
  );
}

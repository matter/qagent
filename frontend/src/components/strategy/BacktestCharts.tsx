import { Card, Col, Row, Statistic } from "antd";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import type { BacktestResultDetail } from "../../api";

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
        const raw = summary[item.key];
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
  navSeries: BacktestResultDetail["nav_series"];
  benchmarkNav: BacktestResultDetail["benchmark_nav"];
}) {
  if (!navSeries || navSeries.length === 0) return null;

  const dates = navSeries.map((d) => d.date);
  const strategyValues = navSeries.map((d) => d.value);
  const benchmarkValues = benchmarkNav?.map((d) => d.value) ?? [];

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
  drawdownSeries: BacktestResultDetail["drawdown_series"];
}) {
  if (!drawdownSeries || drawdownSeries.length === 0) return null;

  const dates = drawdownSeries.map((d) => d.date);
  const values = drawdownSeries.map((d) => +(d.value * 100).toFixed(3));

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
  monthlyReturns: BacktestResultDetail["monthly_returns"];
}) {
  if (!monthlyReturns || Object.keys(monthlyReturns).length === 0) return null;

  const years = Object.keys(monthlyReturns).sort();
  const months = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"];
  const monthLabels = ["1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月"];

  const heatData: [number, number, number | null][] = [];
  let minVal = 0;
  let maxVal = 0;

  for (let yi = 0; yi < years.length; yi++) {
    const yearData = monthlyReturns[years[yi]];
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

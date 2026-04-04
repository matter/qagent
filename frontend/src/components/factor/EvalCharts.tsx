import { Card, Col, Row, Statistic } from "antd";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import type { FactorEvalSummary, FactorEvalDetail } from "../../api";

// ---- Summary Cards ----

export function EvalSummaryCards({ summary }: { summary: FactorEvalSummary }) {
  return (
    <Row gutter={[12, 12]}>
      <Col xs={12} sm={8} md={4}>
        <Card size="small">
          <Statistic
            title="IC Mean"
            value={summary.ic_mean}
            precision={4}
            valueStyle={{ color: summary.ic_mean > 0 ? "#52c41a" : "#ff4d4f", fontSize: 18 }}
          />
        </Card>
      </Col>
      <Col xs={12} sm={8} md={4}>
        <Card size="small">
          <Statistic
            title="IR"
            value={summary.ir}
            precision={3}
            valueStyle={{ color: summary.ir > 0.5 ? "#52c41a" : summary.ir > 0 ? "#1677ff" : "#ff4d4f", fontSize: 18 }}
          />
        </Card>
      </Col>
      <Col xs={12} sm={8} md={4}>
        <Card size="small">
          <Statistic
            title="IC Win Rate"
            value={summary.ic_win_rate * 100}
            precision={1}
            suffix="%"
            valueStyle={{ fontSize: 18 }}
          />
        </Card>
      </Col>
      <Col xs={12} sm={8} md={4}>
        <Card size="small">
          <Statistic
            title="覆盖率"
            value={summary.coverage * 100}
            precision={1}
            suffix="%"
            valueStyle={{ fontSize: 18 }}
          />
        </Card>
      </Col>
      <Col xs={12} sm={8} md={4}>
        <Card size="small">
          <Statistic
            title="换手率"
            value={summary.turnover}
            precision={4}
            valueStyle={{ fontSize: 18 }}
          />
        </Card>
      </Col>
      <Col xs={12} sm={8} md={4}>
        <Card size="small">
          <Statistic
            title="多空年化"
            value={summary.long_short_annual_return * 100}
            precision={2}
            suffix="%"
            valueStyle={{
              color: summary.long_short_annual_return > 0 ? "#52c41a" : "#ff4d4f",
              fontSize: 18,
            }}
          />
        </Card>
      </Col>
    </Row>
  );
}

// ---- IC Time Series Chart ----

function buildICChartOption(
  icSeries: FactorEvalDetail["ic_series"],
): EChartsOption {
  const dates = icSeries.map((d) => d.date);
  const values = icSeries.map((d) => d.ic);
  const validValues = values.filter((v): v is number => v !== null);
  const icMean = validValues.length > 0
    ? validValues.reduce((a, b) => a + b, 0) / validValues.length
    : 0;

  return {
    animation: false,
    tooltip: {
      trigger: "axis",
      backgroundColor: "rgba(30,30,30,0.9)",
      borderColor: "#555",
      textStyle: { color: "#eee", fontSize: 12 },
      formatter(params: unknown) {
        const p = Array.isArray(params) ? params[0] : params;
        const item = p as { name: string; value: number | null };
        const val = item.value !== null ? (item.value as number).toFixed(4) : "N/A";
        return `${item.name}<br/>IC: ${val}`;
      },
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
        name: "IC",
        type: "bar",
        data: values.map((v) => ({
          value: v,
          itemStyle: {
            color: (v ?? 0) >= 0 ? "rgba(82,196,26,0.7)" : "rgba(255,77,79,0.7)",
          },
        })),
        barMaxWidth: 6,
      },
      {
        name: "IC Mean",
        type: "line",
        data: dates.map(() => +icMean.toFixed(6)),
        symbol: "none",
        lineStyle: { width: 1.5, color: "#f5c842", type: "dashed" },
        tooltip: { show: false },
      },
      {
        name: "Zero",
        type: "line",
        data: dates.map(() => 0),
        symbol: "none",
        lineStyle: { width: 1, color: "rgba(255,255,255,0.2)", type: "solid" },
        tooltip: { show: false },
      },
    ],
  };
}

export function ICSeriesChart({
  icSeries,
}: {
  icSeries: FactorEvalDetail["ic_series"];
}) {
  const option = buildICChartOption(icSeries);
  return (
    <Card
      title="IC 时间序列"
      size="small"
      style={{ background: "rgba(0,0,0,0.2)" }}
      styles={{ body: { padding: 8 } }}
    >
      <ReactECharts option={option} style={{ height: 280 }} notMerge lazyUpdate />
    </Card>
  );
}

// ---- Group Returns Chart ----

const GROUP_COLORS: Record<string, string> = {
  G1: "#ef5350",
  G2: "#ff9800",
  G3: "#ffc107",
  G4: "#66bb6a",
  G5: "#26a69a",
  long_short: "#42a5f5",
};

function buildGroupReturnsOption(
  groupReturns: FactorEvalDetail["group_returns"],
): EChartsOption {
  const { dates, groups } = groupReturns;

  const series = Object.entries(groups).map(([name, values]) => ({
    name: name === "long_short" ? "多空" : name,
    type: "line" as const,
    data: values.map((v) => +(v * 100).toFixed(3)),
    symbol: "none",
    lineStyle: {
      width: name === "long_short" ? 2.5 : 1.5,
      color: GROUP_COLORS[name] ?? "#999",
      type: name === "long_short" ? ("dashed" as const) : ("solid" as const),
    },
    itemStyle: { color: GROUP_COLORS[name] ?? "#999" },
  }));

  return {
    animation: false,
    tooltip: {
      trigger: "axis",
      backgroundColor: "rgba(30,30,30,0.9)",
      borderColor: "#555",
      textStyle: { color: "#eee", fontSize: 12 },
    },
    legend: {
      data: series.map((s) => s.name),
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
      axisLabel: {
        color: "#aaa",
        fontSize: 10,
        formatter: (v: number) => `${v.toFixed(1)}%`,
      },
      splitLine: { lineStyle: { color: "rgba(255,255,255,0.06)" } },
    },
    series,
  };
}

export function GroupReturnsChart({
  groupReturns,
}: {
  groupReturns: FactorEvalDetail["group_returns"];
}) {
  const option = buildGroupReturnsOption(groupReturns);
  return (
    <Card
      title="分组累计收益"
      size="small"
      style={{ background: "rgba(0,0,0,0.2)" }}
      styles={{ body: { padding: 8 } }}
    >
      <ReactECharts option={option} style={{ height: 300 }} notMerge lazyUpdate />
    </Card>
  );
}

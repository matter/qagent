import { Card } from "antd";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";

interface FeatureImportanceChartProps {
  data: Record<string, number>;
}

export default function FeatureImportanceChart({ data }: FeatureImportanceChartProps) {
  const entries = Object.entries(data)
    .sort((a, b) => a[1] - b[1]) // ascending so largest at top in horizontal bar
    .slice(-30); // top 30

  const names = entries.map(([k]) => k);
  const values = entries.map(([, v]) => v);

  const option: EChartsOption = {
    animation: false,
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "shadow" },
      backgroundColor: "rgba(30,30,30,0.9)",
      borderColor: "#555",
      textStyle: { color: "#eee", fontSize: 12 },
    },
    grid: { left: 140, right: 30, top: 10, bottom: 20 },
    xAxis: {
      type: "value",
      axisLine: { lineStyle: { color: "#555" } },
      axisLabel: { color: "#aaa", fontSize: 10 },
      splitLine: { lineStyle: { color: "rgba(255,255,255,0.06)" } },
    },
    yAxis: {
      type: "category",
      data: names,
      axisLine: { lineStyle: { color: "#555" } },
      axisLabel: { color: "#aaa", fontSize: 10 },
    },
    series: [
      {
        name: "Importance",
        type: "bar",
        data: values.map((v) => ({
          value: v,
          itemStyle: { color: "#1677ff" },
        })),
        barMaxWidth: 16,
      },
    ],
  };

  const height = Math.max(200, entries.length * 22 + 40);

  return (
    <Card
      title="特征重要性"
      size="small"
      style={{ background: "rgba(0,0,0,0.2)" }}
      styles={{ body: { padding: 8 } }}
    >
      <ReactECharts option={option} style={{ height }} notMerge lazyUpdate />
    </Card>
  );
}

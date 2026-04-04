import { Card } from "antd";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import type { CorrelationMatrix } from "../../api";

function buildHeatmapOption(corr: CorrelationMatrix): EChartsOption {
  const { factor_names, matrix } = corr;
  const n = factor_names.length;

  const heatData: [number, number, number][] = [];
  for (let i = 0; i < n; i++) {
    for (let j = 0; j < n; j++) {
      heatData.push([j, i, +(matrix[i][j]).toFixed(3)]);
    }
  }

  return {
    animation: false,
    tooltip: {
      position: "top",
      backgroundColor: "rgba(30,30,30,0.9)",
      borderColor: "#555",
      textStyle: { color: "#eee", fontSize: 12 },
      formatter(params: unknown) {
        const p = params as { value: [number, number, number] };
        const [x, y, v] = p.value;
        return `${factor_names[y]} vs ${factor_names[x]}<br/>Corr: ${v.toFixed(3)}`;
      },
    },
    grid: { left: 120, right: 40, top: 20, bottom: 80 },
    xAxis: {
      type: "category",
      data: factor_names,
      axisLabel: { color: "#aaa", fontSize: 10, rotate: 45 },
      axisLine: { lineStyle: { color: "#555" } },
      splitLine: { show: false },
    },
    yAxis: {
      type: "category",
      data: factor_names,
      axisLabel: { color: "#aaa", fontSize: 10 },
      axisLine: { lineStyle: { color: "#555" } },
      splitLine: { show: false },
    },
    visualMap: {
      min: -1,
      max: 1,
      calculable: true,
      orient: "horizontal",
      left: "center",
      bottom: 0,
      inRange: {
        color: ["#313695", "#4575b4", "#74add1", "#abd9e9", "#e0f3f8", "#ffffbf", "#fee090", "#fdae61", "#f46d43", "#d73027", "#a50026"],
      },
      textStyle: { color: "#aaa" },
    },
    series: [
      {
        name: "Correlation",
        type: "heatmap",
        data: heatData,
        label: {
          show: n <= 10,
          color: "#fff",
          fontSize: 10,
          formatter(params: unknown) {
            const p = params as { value: [number, number, number] };
            return p.value[2].toFixed(2);
          },
        },
        emphasis: {
          itemStyle: { shadowBlur: 10, shadowColor: "rgba(0,0,0,0.5)" },
        },
      },
    ],
  };
}

export default function CorrelationHeatmap({ data }: { data: CorrelationMatrix }) {
  const option = buildHeatmapOption(data);
  const height = Math.max(300, data.factor_names.length * 30 + 100);

  return (
    <Card
      title="因子相关性矩阵"
      size="small"
      style={{ background: "rgba(0,0,0,0.2)" }}
      styles={{ body: { padding: 8 } }}
    >
      <ReactECharts option={option} style={{ height }} notMerge lazyUpdate />
    </Card>
  );
}

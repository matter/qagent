import { useEffect, useState } from "react";
import { Modal, Spin, Typography } from "antd";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import { getBacktestStockChart } from "../../api";
import type { StockChartData } from "../../api";

const { Text } = Typography;

interface StockTradeChartProps {
  open: boolean;
  onClose: () => void;
  backtestId: string;
  ticker: string;
}

export default function StockTradeChart({
  open,
  onClose,
  backtestId,
  ticker,
}: StockTradeChartProps) {
  const [loading, setLoading] = useState(false);
  const [chartData, setChartData] = useState<StockChartData | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open || !backtestId || !ticker) return;
    setLoading(true);
    setError(null);
    setChartData(null);

    getBacktestStockChart(backtestId, ticker)
      .then(setChartData)
      .catch(() => setError("加载股票数据失败"))
      .finally(() => setLoading(false));
  }, [open, backtestId, ticker]);

  const buildOption = (data: StockChartData): EChartsOption => {
    const dates = data.daily_bars.map((b) => b.date);
    const ohlc = data.daily_bars.map((b) => [b.open, b.close, b.low, b.high]);
    const volumes = data.daily_bars.map((b) => b.volume);

    // Build buy/sell marker data
    const buyMarkers = data.trades
      .filter((t) => t.action === "buy")
      .map((t) => ({
        name: "BUY",
        coord: [t.date, t.price],
        value: t.price.toFixed(2),
        itemStyle: { color: "#52c41a" },
      }));

    const sellMarkers = data.trades
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
        {
          type: "inside",
          xAxisIndex: [0, 1],
          start: 0,
          end: 100,
        },
        {
          type: "slider",
          xAxisIndex: [0, 1],
          top: "92%",
          height: 20,
          borderColor: "#555",
          textStyle: { color: "#aaa" },
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
                  show: true,
                  position: "bottom" as const,
                  formatter: "B",
                  fontSize: 9,
                  color: "#52c41a",
                  fontWeight: "bold" as const,
                },
              })),
              ...sellMarkers.map((m) => ({
                ...m,
                symbolRotate: 180,
                symbolSize: 14,
                label: {
                  show: true,
                  position: "top" as const,
                  formatter: "S",
                  fontSize: 9,
                  color: "#ff4d4f",
                  fontWeight: "bold" as const,
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
              // item = [open, close, low, high]
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
          <Spin size="large" />
        </div>
      )}
      {error && (
        <div style={{ textAlign: "center", padding: 48 }}>
          <Text type="danger">{error}</Text>
        </div>
      )}
      {chartData && !loading && (
        <ReactECharts
          option={buildOption(chartData)}
          style={{ height: 520 }}
          notMerge
          lazyUpdate
        />
      )}
    </Modal>
  );
}

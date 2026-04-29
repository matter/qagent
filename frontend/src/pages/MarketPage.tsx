import { useState, useEffect, useCallback, useRef } from "react";
import { AutoComplete, Space, Button, Card, Spin, Typography, Empty, message, Tooltip, Tag } from "antd";
import { SearchOutlined, SyncOutlined } from "@ant-design/icons";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import dayjs from "dayjs";
import { searchStocks, getDailyBars, updateTickers, getUpdateProgress } from "../api";
import { getActiveMarket } from "../api/client";
import type { DailyBar, Market, StockSearchResult } from "../api";

const { Text } = Typography;

const RANGE_OPTIONS = [
  { label: "1M", months: 1 },
  { label: "3M", months: 3 },
  { label: "6M", months: 6 },
  { label: "1Y", months: 12 },
  { label: "3Y", months: 36 },
  { label: "ALL", months: 0 },
] as const;

const DEFAULT_TICKER_BY_MARKET: Record<Market, string> = {
  US: "SPY",
  CN: "sh.600000",
};

// ---- MA calculation ----
function calcMA(data: number[], period: number): (number | null)[] {
  const result: (number | null)[] = [];
  for (let i = 0; i < data.length; i++) {
    if (i < period - 1) {
      result.push(null);
    } else {
      let sum = 0;
      for (let j = 0; j < period; j++) sum += data[i - j];
      result.push(+(sum / period).toFixed(2));
    }
  }
  return result;
}

// ---- MACD calculation ----
function calcMACD(closes: number[]): {
  dif: (number | null)[];
  dea: (number | null)[];
  histogram: (number | null)[];
} {
  const dif: number[] = [];
  const dea: number[] = [];
  const histogram: number[] = [];

  let ema12 = closes[0] ?? 0;
  let ema26 = closes[0] ?? 0;
  let emaDea = 0;

  for (let i = 0; i < closes.length; i++) {
    const c = closes[i];
    ema12 = ema12 * 11 / 13 + c * 2 / 13;
    ema26 = ema26 * 25 / 27 + c * 2 / 27;
    const d = ema12 - ema26;
    dif.push(d);
    emaDea = emaDea * 8 / 10 + d * 2 / 10;
    dea.push(emaDea);
    histogram.push((d - emaDea) * 2);
  }

  return {
    dif: dif.map((v) => +v.toFixed(4)),
    dea: dea.map((v) => +v.toFixed(4)),
    histogram: histogram.map((v) => +v.toFixed(4)),
  };
}

function buildChartOption(bars: DailyBar[]): EChartsOption {
  if (bars.length === 0) return {};

  const dates = bars.map((b) => b.date);
  const ohlc = bars.map((b) => [b.open, b.close, b.low, b.high]);
  const volumes = bars.map((b) => b.volume);
  const closes = bars.map((b) => b.close);

  const ma5 = calcMA(closes, 5);
  const ma20 = calcMA(closes, 20);
  const ma60 = calcMA(closes, 60);
  const { dif, dea, histogram } = calcMACD(closes);

  const volumeColors = bars.map((b) =>
    b.close >= b.open ? "#26a69a" : "#ef5350",
  );

  return {
    animation: false,
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "cross" },
      backgroundColor: "rgba(30,30,30,0.9)",
      borderColor: "#555",
      textStyle: { color: "#eee", fontSize: 12 },
    },
    axisPointer: {
      link: [{ xAxisIndex: [0, 1, 2] }],
    },
    grid: [
      { left: 60, right: 20, top: 20, height: "48%" },
      { left: 60, right: 20, top: "72%", height: "10%" },
      { left: 60, right: 20, top: "85%", height: "10%" },
    ],
    xAxis: [
      {
        type: "category",
        data: dates,
        gridIndex: 0,
        axisLine: { lineStyle: { color: "#555" } },
        axisLabel: { show: false },
        splitLine: { show: false },
        boundaryGap: true,
        min: "dataMin",
        max: "dataMax",
      },
      {
        type: "category",
        data: dates,
        gridIndex: 1,
        axisLine: { lineStyle: { color: "#555" } },
        axisLabel: { show: false },
        splitLine: { show: false },
        boundaryGap: true,
        min: "dataMin",
        max: "dataMax",
      },
      {
        type: "category",
        data: dates,
        gridIndex: 2,
        axisLine: { lineStyle: { color: "#555" } },
        axisLabel: { color: "#aaa", fontSize: 10 },
        splitLine: { show: false },
        boundaryGap: true,
        min: "dataMin",
        max: "dataMax",
      },
    ],
    yAxis: [
      {
        scale: true,
        gridIndex: 0,
        splitLine: { lineStyle: { color: "rgba(255,255,255,0.06)" } },
        axisLine: { lineStyle: { color: "#555" } },
        axisLabel: { color: "#aaa", fontSize: 10 },
      },
      {
        scale: true,
        gridIndex: 1,
        splitNumber: 2,
        splitLine: { show: false },
        axisLine: { lineStyle: { color: "#555" } },
        axisLabel: { color: "#aaa", fontSize: 10 },
      },
      {
        scale: true,
        gridIndex: 2,
        splitNumber: 2,
        splitLine: { show: false },
        axisLine: { lineStyle: { color: "#555" } },
        axisLabel: { color: "#aaa", fontSize: 10 },
      },
    ],
    dataZoom: [
      {
        type: "slider",
        xAxisIndex: [0, 1, 2],
        bottom: 5,
        height: 20,
        start: bars.length > 120 ? 100 - (120 / bars.length) * 100 : 0,
        end: 100,
        borderColor: "#555",
        textStyle: { color: "#aaa" },
        dataBackground: {
          lineStyle: { color: "#555" },
          areaStyle: { color: "rgba(255,255,255,0.05)" },
        },
        fillerColor: "rgba(22,119,255,0.15)",
        handleStyle: { color: "#1677ff" },
      },
      {
        type: "inside",
        xAxisIndex: [0, 1, 2],
        start: bars.length > 120 ? 100 - (120 / bars.length) * 100 : 0,
        end: 100,
      },
    ],
    series: [
      {
        name: "K线",
        type: "candlestick",
        xAxisIndex: 0,
        yAxisIndex: 0,
        data: ohlc,
        itemStyle: {
          color: "#26a69a",
          color0: "#ef5350",
          borderColor: "#26a69a",
          borderColor0: "#ef5350",
        },
      },
      {
        name: "MA5",
        type: "line",
        xAxisIndex: 0,
        yAxisIndex: 0,
        data: ma5,
        smooth: true,
        symbol: "none",
        lineStyle: { width: 1, color: "#f5c842" },
      },
      {
        name: "MA20",
        type: "line",
        xAxisIndex: 0,
        yAxisIndex: 0,
        data: ma20,
        smooth: true,
        symbol: "none",
        lineStyle: { width: 1, color: "#42a5f5" },
      },
      {
        name: "MA60",
        type: "line",
        xAxisIndex: 0,
        yAxisIndex: 0,
        data: ma60,
        smooth: true,
        symbol: "none",
        lineStyle: { width: 1, color: "#ab47bc" },
      },
      {
        name: "成交量",
        type: "bar",
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: volumes.map((v, i) => ({
          value: v,
          itemStyle: { color: volumeColors[i] },
        })),
      },
      {
        name: "DIF",
        type: "line",
        xAxisIndex: 2,
        yAxisIndex: 2,
        data: dif,
        symbol: "none",
        lineStyle: { width: 1, color: "#42a5f5" },
      },
      {
        name: "DEA",
        type: "line",
        xAxisIndex: 2,
        yAxisIndex: 2,
        data: dea,
        symbol: "none",
        lineStyle: { width: 1, color: "#f5c842" },
      },
      {
        name: "MACD",
        type: "bar",
        xAxisIndex: 2,
        yAxisIndex: 2,
        data: histogram!.map((v) => ({
          value: v,
          itemStyle: {
            color: (v ?? 0) >= 0 ? "#26a69a" : "#ef5350",
          },
        })),
      },
    ],
  };
}

export default function MarketPage() {
  const market = getActiveMarket();
  const defaultTicker = DEFAULT_TICKER_BY_MARKET[market];
  const [ticker, setTicker] = useState(defaultTicker);
  const [searchValue, setSearchValue] = useState(defaultTicker);
  const [options, setOptions] = useState<{ value: string; label: React.ReactNode }[]>([]);
  const [bars, setBars] = useState<DailyBar[]>([]);
  const [loading, setLoading] = useState(false);
  const [range, setRange] = useState("1Y");
  const [error, setError] = useState<string | null>(null);
  const [updating, setUpdating] = useState(false);
  const searchTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const [messageApi, contextHolder] = message.useMessage();

  const fetchBars = useCallback(
    async (t: string, rangeKey: string) => {
      setLoading(true);
      setError(null);
      try {
        const end = dayjs().format("YYYY-MM-DD");
        const opt = RANGE_OPTIONS.find((r) => r.label === rangeKey)!;
        const start = opt.months > 0 ? dayjs().subtract(opt.months, "month").format("YYYY-MM-DD") : undefined;
        const data = await getDailyBars(t, start, end, market);
        setBars(data);
      } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : "加载失败";
        setError(msg);
        setBars([]);
      } finally {
        setLoading(false);
      }
    },
    [market],
  );

  useEffect(() => {
    fetchBars(ticker, range);
  }, [ticker, range, fetchBars]);

  const handleUpdateTicker = async () => {
    setUpdating(true);
    try {
      await updateTickers([ticker], market);
      messageApi.success(`${ticker} 数据更新已提交`);
      // Poll for completion then refresh chart
      const poll = setInterval(async () => {
        try {
          const p = await getUpdateProgress();
          if (p.status !== "running" && p.status !== "queued") {
            clearInterval(poll);
            setUpdating(false);
            fetchBars(ticker, range);
            if (p.status === "completed") {
              messageApi.success(`${ticker} 数据已更新`);
            } else if (p.error) {
              messageApi.error(`更新失败: ${p.error}`);
            }
          }
        } catch {
          clearInterval(poll);
          setUpdating(false);
        }
      }, 2000);
    } catch {
      messageApi.error("更新提交失败");
      setUpdating(false);
    }
  };

  const handleSearch = (value: string) => {
    setSearchValue(value);
    if (searchTimer.current) clearTimeout(searchTimer.current);
    if (!value.trim()) {
      setOptions([]);
      return;
    }
    searchTimer.current = setTimeout(async () => {
      try {
        const results: StockSearchResult[] = await searchStocks(value, 10, market);
        setOptions(
          results.map((r) => ({
            value: r.ticker,
            label: (
              <Space>
                <Text strong>{r.ticker}</Text>
                <Tag color={r.market === "CN" ? "red" : "blue"} style={{ marginInlineEnd: 0 }}>
                  {r.market}
                </Tag>
                <Text type="secondary">{r.name}</Text>
                <Text type="secondary" style={{ fontSize: 11 }}>
                  {r.exchange}
                </Text>
              </Space>
            ),
          })),
        );
      } catch {
        setOptions([]);
      }
    }, 300);
  };

  const handleSelect = (value: string) => {
    setTicker(value);
    setSearchValue(value);
    setOptions([]);
  };

  const chartOption = buildChartOption(bars);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      {contextHolder}
      {/* Search + Range controls */}
      <Space wrap>
        <AutoComplete
          style={{ width: 320 }}
          value={searchValue}
          options={options}
          onSearch={handleSearch}
          onSelect={handleSelect}
          placeholder="输入股票代码或名称搜索..."
          allowClear
          suffixIcon={<SearchOutlined />}
        />
        <Space.Compact>
          {RANGE_OPTIONS.map((r) => (
            <Button
              key={r.label}
              type={range === r.label ? "primary" : "default"}
              size="small"
              onClick={() => setRange(r.label)}
            >
              {r.label}
            </Button>
          ))}
        </Space.Compact>
        <Text strong style={{ fontSize: 16, marginLeft: 8 }}>
          {ticker}
        </Text>
        <Tag color={market === "CN" ? "red" : "blue"}>{market}</Tag>
        <Tooltip title={`更新 ${ticker} 行情数据`}>
          <Button
            icon={<SyncOutlined spin={updating} />}
            size="small"
            loading={updating}
            onClick={handleUpdateTicker}
          >
            更新数据
          </Button>
        </Tooltip>
      </Space>

      {/* Chart */}
      <Card
        styles={{ body: { padding: 8 } }}
        style={{ background: "rgba(0,0,0,0.2)" }}
      >
        {loading ? (
          <div style={{ display: "flex", justifyContent: "center", alignItems: "center", height: 560 }}>
            <Spin size="large" description="加载数据中..." />
          </div>
        ) : error ? (
          <div style={{ display: "flex", justifyContent: "center", alignItems: "center", height: 560 }}>
            <Empty description={error} />
          </div>
        ) : bars.length === 0 ? (
          <div style={{ display: "flex", justifyContent: "center", alignItems: "center", height: 560 }}>
            <Empty description={`${market} 暂无 ${ticker} 数据`} />
          </div>
        ) : (
          <ReactECharts
            option={chartOption}
            style={{ height: 560 }}
            notMerge
            lazyUpdate
          />
        )}
      </Card>
    </div>
  );
}

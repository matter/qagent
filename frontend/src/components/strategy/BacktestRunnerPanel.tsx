import { useState, useEffect, useRef } from "react";
import {
  Alert,
  Button,
  Card,
  Col,
  DatePicker,
  InputNumber,
  message,
  Radio,
  Row,
  Select,
  Space,
  Spin,
  Typography,
} from "antd";
import { PlayCircleOutlined } from "@ant-design/icons";
import dayjs from "dayjs";
import type { Dayjs } from "dayjs";
import {
  listStrategies,
  listGroups,
  runBacktest,
  getTaskStatus,
  getBacktest,
} from "../../api";
import type {
  Strategy,
  StockGroup,
  BacktestResultDetail,
} from "../../api";
import {
  BacktestSummaryCards,
  NavCurveChart,
  DrawdownChart,
  MonthlyReturnsHeatmap,
  RebalanceDiagnosticsTable,
  TradeLogTable,
  StockPnLTable,
} from "./BacktestCharts";

const { Text } = Typography;

import type { BacktestRestoreConfig } from "./BacktestHistory";

interface BacktestRunnerPanelProps {
  onBacktestComplete?: () => void;
  restoreConfig?: BacktestRestoreConfig | null;
}

export default function BacktestRunnerPanel({ onBacktestComplete, restoreConfig }: BacktestRunnerPanelProps) {
  const [strategies, setStrategies] = useState<Strategy[]>([]);
  const [groups, setGroups] = useState<StockGroup[]>([]);

  const [selectedStrategy, setSelectedStrategy] = useState<string>("");
  const [selectedGroup, setSelectedGroup] = useState<string>("");
  const [startDate, setStartDate] = useState<Dayjs>(dayjs("2020-01-01"));
  const [endDate, setEndDate] = useState<Dayjs>(dayjs("2023-12-31"));
  const [initialCapital, setInitialCapital] = useState<number>(1000000);
  const [commission, setCommission] = useState<number>(0.001);
  const [slippage, setSlippage] = useState<number>(0.001);
  const [maxPositions, setMaxPositions] = useState<number>(50);
  const [benchmark, setBenchmark] = useState("SPY");
  const [rebalanceFreq, setRebalanceFreq] = useState("daily");
  const [rebalanceBuffer, setRebalanceBuffer] = useState<number>(0);
  const [minHoldingDays, setMinHoldingDays] = useState<number>(0);
  const [reentryCooldownDays, setReentryCooldownDays] = useState<number>(0);

  const [running, setRunning] = useState(false);
  const [runError, setRunError] = useState<string | null>(null);
  const [result, setResult] = useState<BacktestResultDetail | null>(null);

  const [messageApi, contextHolder] = message.useMessage();
  const pollRef = useRef<ReturnType<typeof setInterval> | undefined>(undefined);

  useEffect(() => {
    listStrategies().then(setStrategies).catch(() => {});
    listGroups().then(setGroups).catch(() => {});
  }, []);

  // Restore config from backtest history
  useEffect(() => {
    if (!restoreConfig) return;
    setSelectedStrategy(restoreConfig.strategyId);
    if (restoreConfig.groupId) setSelectedGroup(restoreConfig.groupId);
    if (restoreConfig.startDate) setStartDate(dayjs(restoreConfig.startDate));
    if (restoreConfig.endDate) setEndDate(dayjs(restoreConfig.endDate));
    if (restoreConfig.initialCapital) setInitialCapital(restoreConfig.initialCapital);
    if (restoreConfig.commission) setCommission(restoreConfig.commission);
    if (restoreConfig.slippage) setSlippage(restoreConfig.slippage);
    if (restoreConfig.maxPositions) setMaxPositions(restoreConfig.maxPositions);
    if (restoreConfig.benchmark) setBenchmark(restoreConfig.benchmark);
    if (restoreConfig.rebalanceFreq) setRebalanceFreq(restoreConfig.rebalanceFreq);
    if (restoreConfig.rebalanceBuffer != null) setRebalanceBuffer(restoreConfig.rebalanceBuffer);
    if (restoreConfig.minHoldingDays != null) setMinHoldingDays(restoreConfig.minHoldingDays);
    if (restoreConfig.reentryCooldownDays != null) setReentryCooldownDays(restoreConfig.reentryCooldownDays);
  }, [restoreConfig]);

  useEffect(() => {
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, []);

  const handleRunBacktest = async () => {
    if (!selectedStrategy) {
      messageApi.warning("请选择策略");
      return;
    }
    if (!selectedGroup) {
      messageApi.warning("请选择股票分组");
      return;
    }

    setRunning(true);
    setRunError(null);
    setResult(null);

    try {
      const { task_id } = await runBacktest(selectedStrategy, {
        config: {
          start_date: startDate.format("YYYY-MM-DD"),
          end_date: endDate.format("YYYY-MM-DD"),
          initial_capital: initialCapital,
          commission_rate: commission,
          slippage_rate: slippage,
          max_positions: maxPositions,
          benchmark,
          rebalance_freq: rebalanceFreq,
          rebalance_buffer: rebalanceBuffer,
          min_holding_days: minHoldingDays,
          reentry_cooldown_days: reentryCooldownDays,
        },
        universe_group_id: selectedGroup,
      });

      // Poll task status
      if (pollRef.current) clearInterval(pollRef.current);
      pollRef.current = setInterval(async () => {
        try {
          const status = await getTaskStatus(task_id);
          if (status.status === "completed") {
            if (pollRef.current) clearInterval(pollRef.current);
            pollRef.current = undefined;

            const backtestId = (status.result as Record<string, string> | null)?.id;
            if (backtestId) {
              const detail = await getBacktest(backtestId);
              setResult(detail);
            }
            setRunning(false);
            messageApi.success("回测完成");
            onBacktestComplete?.();
          } else if (status.status === "failed") {
            if (pollRef.current) clearInterval(pollRef.current);
            pollRef.current = undefined;
            setRunError(status.error ?? "回测失败");
            setRunning(false);
          }
        } catch {
          // Keep polling on transient errors
        }
      }, 3000);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "回测失败";
      setRunError(msg);
      setRunning(false);
    }
  };

  return (
    <>
      {contextHolder}
      <Space orientation="vertical" style={{ width: "100%" }} size="middle">
        <Card title="回测配置" size="small">
          <Space orientation="vertical" style={{ width: "100%" }} size="small">
            <Row gutter={12}>
              <Col span={12}>
                <Text type="secondary" style={{ fontSize: 12 }}>策略</Text>
                <Select
                  style={{ width: "100%" }}
                  placeholder="选择策略..."
                  value={selectedStrategy || undefined}
                  onChange={setSelectedStrategy}
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
                  value={selectedGroup || undefined}
                  onChange={setSelectedGroup}
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
              <Col span={6}>
                <Text type="secondary" style={{ fontSize: 12 }}>开始日期</Text>
                <DatePicker
                  style={{ width: "100%" }}
                  value={startDate}
                  onChange={(v) => {
                    if (v) setStartDate(v);
                  }}
                />
              </Col>
              <Col span={6}>
                <Text type="secondary" style={{ fontSize: 12 }}>结束日期</Text>
                <DatePicker
                  style={{ width: "100%" }}
                  value={endDate}
                  onChange={(v) => {
                    if (v) setEndDate(v);
                  }}
                />
              </Col>
              <Col span={6}>
                <Text type="secondary" style={{ fontSize: 12 }}>初始资金</Text>
                <InputNumber
                  style={{ width: "100%" }}
                  value={initialCapital}
                  onChange={(v) => setInitialCapital(v ?? 1000000)}
                  min={10000}
                  step={100000}
                  formatter={(v) =>
                    `${v}`.replace(/\B(?=(\d{3})+(?!\d))/g, ",")
                  }
                />
              </Col>
              <Col span={6}>
                <Text type="secondary" style={{ fontSize: 12 }}>基准</Text>
                <Select
                  style={{ width: "100%" }}
                  value={benchmark}
                  onChange={setBenchmark}
                  options={[
                    { value: "SPY", label: "SPY" },
                    { value: "QQQ", label: "QQQ" },
                    { value: "IWM", label: "IWM" },
                  ]}
                />
              </Col>
            </Row>

            <Row gutter={12}>
              <Col span={6}>
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
              <Col span={6}>
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
              <Col span={6}>
                <Text type="secondary" style={{ fontSize: 12 }}>最大持仓数</Text>
                <InputNumber
                  style={{ width: "100%" }}
                  value={maxPositions}
                  onChange={(v) => setMaxPositions(v ?? 50)}
                  min={1}
                  max={500}
                />
              </Col>
              <Col span={6}>
                <Text type="secondary" style={{ fontSize: 12 }}>调仓频率</Text>
                <Radio.Group
                  value={rebalanceFreq}
                  onChange={(e) => setRebalanceFreq(e.target.value)}
                  optionType="button"
                  buttonStyle="solid"
                  size="small"
                  style={{ marginTop: 4 }}
                >
                  <Radio.Button value="daily">每日</Radio.Button>
                  <Radio.Button value="weekly">每周</Radio.Button>
                  <Radio.Button value="monthly">每月</Radio.Button>
                </Radio.Group>
              </Col>
            </Row>

            <Row gutter={12}>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: 12 }}>调仓缓冲带</Text>
                <InputNumber
                  style={{ width: "100%" }}
                  value={rebalanceBuffer}
                  onChange={(v) => setRebalanceBuffer(v ?? 0)}
                  min={0}
                  max={0.5}
                  step={0.01}
                  placeholder="权重变化低于此值不交易"
                />
              </Col>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: 12 }}>最小持仓天数</Text>
                <InputNumber
                  style={{ width: "100%" }}
                  value={minHoldingDays}
                  onChange={(v) => setMinHoldingDays(v ?? 0)}
                  min={0}
                  max={60}
                />
              </Col>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: 12 }}>再入场冷却天数</Text>
                <InputNumber
                  style={{ width: "100%" }}
                  value={reentryCooldownDays}
                  onChange={(v) => setReentryCooldownDays(v ?? 0)}
                  min={0}
                  max={60}
                />
              </Col>
            </Row>
          </Space>
        </Card>

        <Button
          type="primary"
          icon={<PlayCircleOutlined />}
          loading={running}
          onClick={handleRunBacktest}
          block
          style={{ background: "#52c41a", borderColor: "#52c41a" }}
          size="large"
        >
          运行回测
        </Button>

        {running && (
          <Card size="small">
            <div style={{ textAlign: "center", padding: 24 }}>
              <Spin size="large" />
              <div style={{ marginTop: 12 }}>
                <Text type="secondary">正在运行回测...</Text>
              </div>
            </div>
          </Card>
        )}

        {runError && (
          <Alert
            type="error"
            showIcon
            message="回测错误"
            description={runError}
            closable
            onClose={() => setRunError(null)}
          />
        )}

        {result && result.summary && (
          <Space orientation="vertical" style={{ width: "100%" }} size="middle">
            <BacktestSummaryCards summary={result.summary} />
            <NavCurveChart navSeries={result.nav_series} benchmarkNav={result.benchmark_nav} />
            <DrawdownChart drawdownSeries={result.drawdown_series} />
            <MonthlyReturnsHeatmap monthlyReturns={result.monthly_returns} />
            <RebalanceDiagnosticsTable diagnostics={result.rebalance_diagnostics} />
            <StockPnLTable stockPnl={result.stock_pnl ?? null} backtestId={result.id} />
            <TradeLogTable trades={result.trades ?? null} />
          </Space>
        )}
      </Space>
    </>
  );
}

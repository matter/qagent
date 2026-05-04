import { useState, useEffect, useCallback } from "react";
import {
  Button,
  Card,
  message,
  Modal,
  Space,
  Table,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import { ReloadOutlined, RollbackOutlined, PlayCircleOutlined } from "@ant-design/icons";
import { useNavigate } from "react-router-dom";
import {
  listBacktests,
  getBacktest,
  listStrategies,
} from "../../api";
import type { BacktestResultSummary, BacktestResultDetail, Market, Strategy } from "../../api";
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

const LEVEL_TAG: Record<string, { color: string; label: string }> = {
  excellent: { color: "success", label: "优秀" },
  good: { color: "processing", label: "良好" },
  average: { color: "warning", label: "一般" },
  poor: { color: "error", label: "较差" },
};

function numberValue(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function compliancePass(summary: Record<string, unknown> | null | undefined): boolean | undefined {
  const constraintPass = summary?.constraint_pass as boolean | undefined;
  if (constraintPass === false) return false;
  const compliance = summary?.portfolio_compliance as Record<string, unknown> | undefined;
  return compliance?.compliance_pass as boolean | undefined;
}

interface BacktestRow extends BacktestResultSummary {
  strategy_name?: string;
}

export interface BacktestRestoreConfig {
  strategyId: string;
  groupId: string;
  startDate: string;
  endDate: string;
  initialCapital: number;
  commission: number;
  slippage: number;
  maxPositions: number;
  benchmark: string;
  rebalanceFreq: string;
  rebalanceBuffer?: number;
  minHoldingDays?: number;
  reentryCooldownDays?: number;
}

interface BacktestHistoryProps {
  refreshKey?: number;
  onRestoreConfig?: (config: BacktestRestoreConfig) => void;
}

export default function BacktestHistory({ refreshKey, onRestoreConfig }: BacktestHistoryProps) {
  const [rows, setRows] = useState<BacktestRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [detailOpen, setDetailOpen] = useState(false);
  const [detail, setDetail] = useState<BacktestResultDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [messageApi, contextHolder] = message.useMessage();
  const navigate = useNavigate();

  const fetchAll = useCallback(async () => {
    setLoading(true);
    try {
      const [backtests, strategies] = await Promise.all([
        listBacktests(),
        listStrategies(),
      ]);
      const stratMap: Record<string, Strategy> = {};
      for (const s of strategies) {
        stratMap[s.id] = s;
      }
      const enriched: BacktestRow[] = backtests.map((bt) => ({
        ...bt,
        strategy_name: stratMap[bt.strategy_id]?.name ?? bt.strategy_id?.slice(0, 8),
      }));
      enriched.sort((a, b) => (b.created_at ?? "").localeCompare(a.created_at ?? ""));
      setRows(enriched);
    } catch {
      messageApi.error("加载回测历史失败");
    } finally {
      setLoading(false);
    }
  }, [messageApi]);

  useEffect(() => {
    fetchAll();
  }, [fetchAll, refreshKey]);

  const handleRowClick = async (record: BacktestRow) => {
    setDetailOpen(true);
    setDetailLoading(true);
    setDetail(null);
    try {
      const d = await getBacktest(record.id, record.market);
      setDetail(d);
    } catch {
      messageApi.error("加载回测详情失败");
      setDetailOpen(false);
    } finally {
      setDetailLoading(false);
    }
  };

  const handleRestore = (record: BacktestRow, e: React.MouseEvent) => {
    e.stopPropagation();
    if (!onRestoreConfig) return;
    const cfg = record.config as Record<string, unknown> | null;
    onRestoreConfig({
      strategyId: record.strategy_id,
      groupId: (cfg?.universe_group_id as string) ?? "",
      startDate: (cfg?.start_date as string) ?? "",
      endDate: (cfg?.end_date as string) ?? "",
      initialCapital: (cfg?.initial_capital as number) ?? 1000000,
      commission: (cfg?.commission_rate as number) ?? 0.001,
      slippage: (cfg?.slippage_rate as number) ?? 0.001,
      maxPositions: (cfg?.max_positions as number) ?? 50,
      benchmark: (cfg?.benchmark as string) ?? "SPY",
      rebalanceFreq: (cfg?.rebalance_freq as string) ?? (cfg?.rebalance_frequency as string) ?? "daily",
      rebalanceBuffer: (cfg?.rebalance_buffer as number) ?? 0,
      minHoldingDays: (cfg?.min_holding_days as number) ?? 0,
      reentryCooldownDays: (cfg?.reentry_cooldown_days as number) ?? 0,
    });
    messageApi.success("已还原回测配置");
  };

  const handleEnterPaperTrading = (record: BacktestRow, e: React.MouseEvent) => {
    e.stopPropagation();
    const cfg = record.config as Record<string, unknown> | null;
    const params = new URLSearchParams();
    params.set("strategy_id", record.strategy_id);
    params.set("market", record.market);
    if (cfg?.universe_group_id) params.set("group_id", cfg.universe_group_id as string);
    if (cfg?.initial_capital) params.set("initial_capital", String(cfg.initial_capital));
    if (cfg?.max_positions) params.set("max_positions", String(cfg.max_positions));
    if (cfg?.commission_rate) params.set("commission", String(cfg.commission_rate));
    if (cfg?.slippage_rate) params.set("slippage", String(cfg.slippage_rate));
    navigate(`/paper-trading?${params.toString()}`);
  };

  const columns = [
    {
      title: "Market",
      dataIndex: "market",
      key: "market",
      width: 80,
      render: (m: Market) => <Tag color={m === "CN" ? "red" : "blue"}>{m}</Tag>,
    },
    {
      title: "策略",
      dataIndex: "strategy_name",
      key: "strategy_name",
      sorter: (a: BacktestRow, b: BacktestRow) => (a.strategy_name ?? "").localeCompare(b.strategy_name ?? ""),
      render: (name: string | undefined) => name ?? "-",
    },
    {
      title: "年化收益",
      key: "annual_return",
      width: 110,
      sorter: (a: BacktestRow, b: BacktestRow) =>
        (numberValue(a.summary?.annual_return) ?? 0) - (numberValue(b.summary?.annual_return) ?? 0),
      render: (_: unknown, r: BacktestRow) => {
        const v = numberValue(r.summary?.annual_return);
        if (v === undefined || v === null) return "-";
        return (
          <Text style={{ color: v > 0 ? "#52c41a" : "#ff4d4f" }}>
            {(v * 100).toFixed(2)}%
          </Text>
        );
      },
    },
    {
      title: "Sharpe",
      key: "sharpe",
      width: 80,
      sorter: (a: BacktestRow, b: BacktestRow) =>
        ((numberValue(a.summary?.sharpe) ?? numberValue(a.summary?.sharpe_ratio) ?? 0)
          - (numberValue(b.summary?.sharpe) ?? numberValue(b.summary?.sharpe_ratio) ?? 0)),
      render: (_: unknown, r: BacktestRow) => {
        const v = numberValue(r.summary?.sharpe) ?? numberValue(r.summary?.sharpe_ratio);
        if (v === undefined || v === null) return "-";
        return (
          <Text style={{ color: v > 1 ? "#52c41a" : v > 0 ? "#1677ff" : "#ff4d4f" }}>
            {v.toFixed(3)}
          </Text>
        );
      },
    },
    {
      title: "最大回撤",
      key: "max_drawdown",
      width: 100,
      sorter: (a: BacktestRow, b: BacktestRow) =>
        (numberValue(a.summary?.max_drawdown) ?? 0) - (numberValue(b.summary?.max_drawdown) ?? 0),
      render: (_: unknown, r: BacktestRow) => {
        const v = numberValue(r.summary?.max_drawdown);
        if (v === undefined || v === null) return "-";
        return (
          <Text style={{ color: "#ff4d4f" }}>
            {(v * 100).toFixed(2)}%
          </Text>
        );
      },
    },
    {
      title: "合规",
      key: "compliance",
      width: 80,
      filters: [
        { text: "通过", value: "pass" },
        { text: "违规", value: "fail" },
      ],
      onFilter: (value: boolean | React.Key, record: BacktestRow) => {
        const pass = compliancePass(record.summary);
        if (value === "pass") return pass === true;
        if (value === "fail") return pass === false;
        return false;
      },
      render: (_: unknown, r: BacktestRow) => {
        const pass = compliancePass(r.summary);
        if (pass === undefined) return <Text type="secondary">-</Text>;
        return <Tag color={pass ? "success" : "error"}>{pass ? "通过" : "违规"}</Tag>;
      },
    },
    {
      title: "评级",
      dataIndex: "result_level",
      key: "result_level",
      width: 80,
      render: (level: string | null) => {
        if (!level) return <Text type="secondary">-</Text>;
        const cfg = LEVEL_TAG[level] ?? { color: "default", label: level };
        return <Tag color={cfg.color}>{cfg.label}</Tag>;
      },
    },
    {
      title: "日期",
      dataIndex: "created_at",
      key: "created_at",
      width: 160,
      sorter: (a: BacktestRow, b: BacktestRow) => (a.created_at ?? "").localeCompare(b.created_at ?? ""),
      defaultSortOrder: "descend" as const,
      render: (d: string | null) => d?.slice(0, 19) ?? "-",
    },
    {
      title: "操作",
      key: "actions",
      width: 100,
      render: (_: unknown, record: BacktestRow) => (
        <Space size="small">
          <Tooltip title="还原回测配置">
            <Button
              size="small"
              icon={<RollbackOutlined />}
              onClick={(e) => handleRestore(record, e)}
              disabled={!onRestoreConfig}
            />
          </Tooltip>
          <Tooltip title="进入模拟交易">
            <Button
              size="small"
              icon={<PlayCircleOutlined />}
              onClick={(e) => handleEnterPaperTrading(record, e)}
            />
          </Tooltip>
        </Space>
      ),
    },
  ];

  return (
    <>
      {contextHolder}
      <Card
        title="回测历史"
        extra={
          <Button icon={<ReloadOutlined />} size="small" onClick={fetchAll}>
            刷新
          </Button>
        }
      >
        <Table
          dataSource={rows}
          columns={columns}
          rowKey="id"
          loading={loading}
          size="small"
          pagination={{ pageSize: 20 }}
          scroll={{ x: 1000 }}
          onRow={(record) => ({
            onClick: () => handleRowClick(record),
            style: { cursor: "pointer" },
          })}
        />
      </Card>

      <Modal
        title="回测详情"
        open={detailOpen}
        onCancel={() => setDetailOpen(false)}
        footer={null}
        width={1000}
        destroyOnHidden
      >
        {detailLoading ? (
          <div style={{ textAlign: "center", padding: 48 }}>
            <Text type="secondary">加载中...</Text>
          </div>
        ) : detail ? (
          <Space orientation="vertical" style={{ width: "100%" }} size="middle">
            {detail.summary && <BacktestSummaryCards summary={detail.summary} />}
            <NavCurveChart navSeries={detail.nav_series} benchmarkNav={detail.benchmark_nav} />
            <DrawdownChart drawdownSeries={detail.drawdown_series} />
            <MonthlyReturnsHeatmap monthlyReturns={detail.monthly_returns} />
            <RebalanceDiagnosticsTable diagnostics={detail.rebalance_diagnostics} />
            <StockPnLTable stockPnl={detail.stock_pnl ?? null} backtestId={detail.id} />
            <TradeLogTable trades={detail.trades ?? null} />
          </Space>
        ) : null}
      </Modal>
    </>
  );
}

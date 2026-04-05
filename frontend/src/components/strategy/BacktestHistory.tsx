import { useState, useEffect, useCallback } from "react";
import {
  Button,
  Card,
  message,
  Modal,
  Space,
  Table,
  Tag,
  Typography,
} from "antd";
import { ReloadOutlined } from "@ant-design/icons";
import {
  listBacktests,
  getBacktest,
  listStrategies,
} from "../../api";
import type { BacktestResultSummary, BacktestResultDetail, Strategy } from "../../api";
import {
  BacktestSummaryCards,
  NavCurveChart,
  DrawdownChart,
  MonthlyReturnsHeatmap,
} from "./BacktestCharts";

const { Text } = Typography;

const LEVEL_TAG: Record<string, { color: string; label: string }> = {
  excellent: { color: "success", label: "优秀" },
  good: { color: "processing", label: "良好" },
  average: { color: "warning", label: "一般" },
  poor: { color: "error", label: "较差" },
};

interface BacktestRow extends BacktestResultSummary {
  strategy_name?: string;
}

interface BacktestHistoryProps {
  refreshKey?: number;
}

export default function BacktestHistory({ refreshKey }: BacktestHistoryProps) {
  const [rows, setRows] = useState<BacktestRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [detailOpen, setDetailOpen] = useState(false);
  const [detail, setDetail] = useState<BacktestResultDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [messageApi, contextHolder] = message.useMessage();

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
      const d = await getBacktest(record.id);
      setDetail(d);
    } catch {
      messageApi.error("加载回测详情失败");
      setDetailOpen(false);
    } finally {
      setDetailLoading(false);
    }
  };

  const columns = [
    {
      title: "策略",
      dataIndex: "strategy_name",
      key: "strategy_name",
      render: (name: string | undefined) => name ?? "-",
    },
    {
      title: "年化收益",
      key: "annual_return",
      width: 110,
      render: (_: unknown, r: BacktestRow) => {
        const v = r.summary?.annual_return;
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
      render: (_: unknown, r: BacktestRow) => {
        const v = r.summary?.sharpe ?? r.summary?.sharpe_ratio;
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
      render: (_: unknown, r: BacktestRow) => {
        const v = r.summary?.max_drawdown;
        if (v === undefined || v === null) return "-";
        return (
          <Text style={{ color: "#ff4d4f" }}>
            {(v * 100).toFixed(2)}%
          </Text>
        );
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
      render: (d: string | null) => d?.slice(0, 19) ?? "-",
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
        destroyOnClose
      >
        {detailLoading ? (
          <div style={{ textAlign: "center", padding: 48 }}>
            <Text type="secondary">加载中...</Text>
          </div>
        ) : detail ? (
          <Space direction="vertical" style={{ width: "100%" }} size="middle">
            {detail.summary && <BacktestSummaryCards summary={detail.summary} />}
            <NavCurveChart navSeries={detail.nav_series} benchmarkNav={detail.benchmark_nav} />
            <DrawdownChart drawdownSeries={detail.drawdown_series} />
            <MonthlyReturnsHeatmap monthlyReturns={detail.monthly_returns} />
          </Space>
        ) : null}
      </Modal>
    </>
  );
}

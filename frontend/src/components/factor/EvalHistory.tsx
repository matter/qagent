import { useState, useEffect, useCallback } from "react";
import {
  Button,
  Card,
  message,
  Modal,
  Space,
  Table,
  Typography,
} from "antd";
import { ReloadOutlined } from "@ant-design/icons";
import {
  listFactors,
  listEvaluations,
  getEvaluation,
} from "../../api";
import type { Factor, FactorEvalRecord, FactorEvalDetail } from "../../api";
import { EvalSummaryCards, ICSeriesChart, GroupReturnsChart } from "./EvalCharts";

const { Text } = Typography;

interface EvalRow extends FactorEvalRecord {
  factor_name?: string;
}

export default function EvalHistory() {
  const [rows, setRows] = useState<EvalRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [detailOpen, setDetailOpen] = useState(false);
  const [detail, setDetail] = useState<FactorEvalDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [messageApi, contextHolder] = message.useMessage();

  const fetchAll = useCallback(async () => {
    setLoading(true);
    try {
      const factors = await listFactors();
      const factorMap: Record<string, Factor> = {};
      for (const f of factors) {
        factorMap[f.id] = f;
      }

      const allEvals: EvalRow[] = [];
      await Promise.all(
        factors.map(async (f) => {
          try {
            const evals = await listEvaluations(f.id);
            for (const ev of evals) {
              allEvals.push({
                ...ev,
                factor_name: f.name,
              });
            }
          } catch {
            // skip
          }
        }),
      );

      // Sort by created_at desc
      allEvals.sort((a, b) => {
        const da = a.created_at ?? "";
        const db = b.created_at ?? "";
        return db.localeCompare(da);
      });

      setRows(allEvals);
    } catch {
      messageApi.error("加载评价历史失败");
    } finally {
      setLoading(false);
    }
  }, [messageApi]);

  useEffect(() => {
    fetchAll();
  }, [fetchAll]);

  const handleRowClick = async (record: EvalRow) => {
    setDetailOpen(true);
    setDetailLoading(true);
    setDetail(null);
    try {
      const d = await getEvaluation(record.id);
      setDetail(d);
    } catch {
      messageApi.error("加载评价详情失败");
      setDetailOpen(false);
    } finally {
      setDetailLoading(false);
    }
  };

  const columns = [
    {
      title: "因子",
      dataIndex: "factor_name",
      key: "factor_name",
      render: (name: string | undefined) => name ?? "-",
    },
    {
      title: "标签",
      dataIndex: "label_id",
      key: "label_id",
      width: 120,
      ellipsis: true,
    },
    {
      title: "IC Mean",
      key: "ic_mean",
      width: 100,
      render: (_: unknown, r: EvalRow) => {
        const v = r.summary?.ic_mean;
        if (v === undefined || v === null) return "-";
        return (
          <Text style={{ color: v > 0 ? "#52c41a" : "#ff4d4f" }}>
            {v.toFixed(4)}
          </Text>
        );
      },
    },
    {
      title: "IR",
      key: "ir",
      width: 80,
      render: (_: unknown, r: EvalRow) => {
        const v = r.summary?.ir;
        if (v === undefined || v === null) return "-";
        return (
          <Text style={{ color: v > 0.5 ? "#52c41a" : v > 0 ? "#1677ff" : "#ff4d4f" }}>
            {v.toFixed(3)}
          </Text>
        );
      },
    },
    {
      title: "胜率",
      key: "ic_win_rate",
      width: 90,
      render: (_: unknown, r: EvalRow) => {
        const v = r.summary?.ic_win_rate;
        if (v === undefined || v === null) return "-";
        return `${(v * 100).toFixed(1)}%`;
      },
    },
    {
      title: "多空收益",
      key: "ls_return",
      width: 100,
      render: (_: unknown, r: EvalRow) => {
        const v = r.summary?.long_short_annual_return;
        if (v === undefined || v === null) return "-";
        return (
          <Text style={{ color: v > 0 ? "#52c41a" : "#ff4d4f" }}>
            {(v * 100).toFixed(2)}%
          </Text>
        );
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
        title="评价历史"
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
        title="评价详情"
        open={detailOpen}
        onCancel={() => setDetailOpen(false)}
        footer={null}
        width={900}
        destroyOnClose
      >
        {detailLoading ? (
          <div style={{ textAlign: "center", padding: 48 }}>
            <Text type="secondary">加载中...</Text>
          </div>
        ) : detail ? (
          <Space direction="vertical" style={{ width: "100%" }} size="middle">
            <EvalSummaryCards summary={detail.summary} />
            <ICSeriesChart icSeries={detail.ic_series} />
            <GroupReturnsChart groupReturns={detail.group_returns} />
          </Space>
        ) : null}
      </Modal>
    </>
  );
}

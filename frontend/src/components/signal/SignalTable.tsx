import { Table, Tag, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { SignalDetail } from "../../api";

const { Text } = Typography;

function signalTag(signal: number) {
  if (signal === 1) return <Tag color="green">BUY</Tag>;
  if (signal === -1) return <Tag color="red">SELL</Tag>;
  return <Tag color="default">HOLD</Tag>;
}

const columns: ColumnsType<SignalDetail> = [
  {
    title: "Ticker",
    dataIndex: "ticker",
    key: "ticker",
    width: 100,
    sorter: (a, b) => a.ticker.localeCompare(b.ticker),
    render: (text: string) => <Text strong>{text}</Text>,
  },
  {
    title: "信号",
    dataIndex: "signal",
    key: "signal",
    width: 90,
    filters: [
      { text: "BUY", value: 1 },
      { text: "SELL", value: -1 },
      { text: "HOLD", value: 0 },
    ],
    onFilter: (value, record) => record.signal === value,
    render: (signal: number) => signalTag(signal),
  },
  {
    title: "目标权重 (%)",
    dataIndex: "target_weight",
    key: "target_weight",
    width: 120,
    sorter: (a, b) => a.target_weight - b.target_weight,
    render: (w: number) => `${(w * 100).toFixed(2)}%`,
    align: "right" as const,
  },
  {
    title: "强度",
    dataIndex: "strength",
    key: "strength",
    width: 100,
    sorter: (a, b) => a.strength - b.strength,
    defaultSortOrder: "descend" as const,
    render: (s: number) => {
      const color = s >= 0.7 ? "#52c41a" : s >= 0.4 ? "#1677ff" : "#999";
      return <Text style={{ color }}>{s.toFixed(4)}</Text>;
    },
    align: "right" as const,
  },
];

interface SignalTableProps {
  signals: SignalDetail[];
}

export default function SignalTable({ signals }: SignalTableProps) {
  const buyCount = signals.filter((s) => s.signal === 1).length;
  const sellCount = signals.filter((s) => s.signal === -1).length;
  const holdCount = signals.filter((s) => s.signal === 0).length;

  return (
    <Table
      dataSource={signals}
      columns={columns}
      rowKey="ticker"
      size="small"
      pagination={{ pageSize: 20, showSizeChanger: true, showTotal: (total) => `共 ${total} 条` }}
      title={() => (
        <span>
          信号列表:{" "}
          <Tag color="green">BUY {buyCount}</Tag>
          <Tag color="red">SELL {sellCount}</Tag>
          <Tag color="default">HOLD {holdCount}</Tag>
        </span>
      )}
    />
  );
}

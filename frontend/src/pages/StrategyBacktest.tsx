import { Typography, Card } from "antd";
import { FundOutlined } from "@ant-design/icons";

export default function StrategyBacktest() {
  return (
    <Card>
      <Typography.Title level={4}>
        <FundOutlined style={{ marginRight: 8 }} />
        策略回测
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        策略回测与绩效分析。此功能将在后续阶段实现。
      </Typography.Paragraph>
    </Card>
  );
}

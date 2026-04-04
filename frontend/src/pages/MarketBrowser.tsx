import { Typography, Card } from "antd";
import { LineChartOutlined } from "@ant-design/icons";

export default function MarketBrowser() {
  return (
    <Card>
      <Typography.Title level={4}>
        <LineChartOutlined style={{ marginRight: 8 }} />
        行情浏览
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        市场行情数据浏览与分析。此功能将在后续阶段实现。
      </Typography.Paragraph>
    </Card>
  );
}

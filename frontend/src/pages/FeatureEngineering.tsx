import { Typography, Card } from "antd";
import { ToolOutlined } from "@ant-design/icons";

export default function FeatureEngineering() {
  return (
    <Card>
      <Typography.Title level={4}>
        <ToolOutlined style={{ marginRight: 8 }} />
        特征工程
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        特征构建与选择。此功能将在后续阶段实现。
      </Typography.Paragraph>
    </Card>
  );
}

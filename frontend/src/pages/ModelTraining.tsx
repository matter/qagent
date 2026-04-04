import { Typography, Card } from "antd";
import { RocketOutlined } from "@ant-design/icons";

export default function ModelTraining() {
  return (
    <Card>
      <Typography.Title level={4}>
        <RocketOutlined style={{ marginRight: 8 }} />
        模型训练
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        机器学习模型训练与评估。此功能将在后续阶段实现。
      </Typography.Paragraph>
    </Card>
  );
}

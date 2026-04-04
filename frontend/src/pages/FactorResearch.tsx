import { Typography, Card } from "antd";
import { ExperimentOutlined } from "@ant-design/icons";

export default function FactorResearch() {
  return (
    <Card>
      <Typography.Title level={4}>
        <ExperimentOutlined style={{ marginRight: 8 }} />
        因子研究
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        因子定义、计算与分析。此功能将在后续阶段实现。
      </Typography.Paragraph>
    </Card>
  );
}

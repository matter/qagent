import { Typography, Card } from "antd";
import { ThunderboltOutlined } from "@ant-design/icons";

export default function SignalGeneration() {
  return (
    <Card>
      <Typography.Title level={4}>
        <ThunderboltOutlined style={{ marginRight: 8 }} />
        信号生成
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        交易信号生成与推送。此功能将在后续阶段实现。
      </Typography.Paragraph>
    </Card>
  );
}

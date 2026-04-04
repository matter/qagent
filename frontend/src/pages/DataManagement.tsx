import { Typography, Card } from "antd";
import { DatabaseOutlined } from "@ant-design/icons";

export default function DataManagement() {
  return (
    <Card>
      <Typography.Title level={4}>
        <DatabaseOutlined style={{ marginRight: 8 }} />
        数据管理
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        数据源配置、数据下载与管理。此功能将在后续阶段实现。
      </Typography.Paragraph>
    </Card>
  );
}

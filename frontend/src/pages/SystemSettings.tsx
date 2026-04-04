import { Typography, Card } from "antd";
import { SettingOutlined } from "@ant-design/icons";

export default function SystemSettings() {
  return (
    <Card>
      <Typography.Title level={4}>
        <SettingOutlined style={{ marginRight: 8 }} />
        系统设置
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        系统参数配置与管理。此功能将在后续阶段实现。
      </Typography.Paragraph>
    </Card>
  );
}

import { useState, useEffect, useCallback } from "react";
import {
  Card,
  Descriptions,
  Space,
  Spin,
  Tag,
  Typography,
  Button,
} from "antd";
import {
  InfoCircleOutlined,
  DatabaseOutlined,
  SettingOutlined,
  ApiOutlined,
  ReloadOutlined,
} from "@ant-design/icons";
import { getSystemInfo } from "../api";
import type { SystemInfo } from "../api";

const { Text, Link } = Typography;

export default function SystemSettings() {
  const [info, setInfo] = useState<SystemInfo | null>(null);
  const [loading, setLoading] = useState(false);

  const fetchInfo = useCallback(async () => {
    setLoading(true);
    try {
      const data = await getSystemInfo();
      setInfo(data);
    } catch {
      // noop
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchInfo();
  }, [fetchInfo]);

  if (loading && !info) {
    return (
      <div style={{ textAlign: "center", padding: 48 }}>
        <Spin size="large" />
      </div>
    );
  }

  return (
    <Space direction="vertical" style={{ width: "100%" }} size="middle">
      {/* System Info */}
      <Card
        title={
          <span>
            <InfoCircleOutlined style={{ marginRight: 8 }} />
            系统信息
          </span>
        }
        extra={
          <Button icon={<ReloadOutlined />} size="small" onClick={fetchInfo} loading={loading}>
            刷新
          </Button>
        }
      >
        {info ? (
          <Descriptions size="small" column={2} bordered>
            <Descriptions.Item label="版本">
              <Tag color="blue">v{info.version}</Tag>
            </Descriptions.Item>
            <Descriptions.Item label="Python 版本">
              <Text code>{info.python_version.split(" ")[0]}</Text>
            </Descriptions.Item>
            <Descriptions.Item label="DuckDB 路径" span={2}>
              <Text code>{info.db_path}</Text>
            </Descriptions.Item>
            <Descriptions.Item label="数据目录">
              <Text code>{info.data_dir}</Text>
            </Descriptions.Item>
            <Descriptions.Item label="模型目录">
              <Text code>{info.models_dir}</Text>
            </Descriptions.Item>
            <Descriptions.Item label="因子目录">
              <Text code>{info.factors_dir}</Text>
            </Descriptions.Item>
            <Descriptions.Item label="策略目录">
              <Text code>{info.strategies_dir}</Text>
            </Descriptions.Item>
          </Descriptions>
        ) : (
          <Text type="secondary">无法加载系统信息</Text>
        )}
      </Card>

      {/* Data Source Info */}
      <Card
        title={
          <span>
            <DatabaseOutlined style={{ marginRight: 8 }} />
            数据源信息
          </span>
        }
      >
        {info ? (
          <Descriptions size="small" column={1} bordered>
            <Descriptions.Item label="数据提供商">
              <Tag color="blue">{info.data_provider}</Tag>
            </Descriptions.Item>
            <Descriptions.Item label="市场日历">
              {info.market_calendar}
            </Descriptions.Item>
            <Descriptions.Item label="已知限制">
              <ul style={{ margin: 0, paddingLeft: 20 }}>
                <li>yfinance 提供免费的市场数据，可能存在延迟</li>
                <li>历史数据范围取决于数据提供商支持</li>
                <li>高频请求可能受到速率限制</li>
              </ul>
            </Descriptions.Item>
          </Descriptions>
        ) : (
          <Text type="secondary">无法加载数据源信息</Text>
        )}
      </Card>

      {/* Config Display */}
      <Card
        title={
          <span>
            <SettingOutlined style={{ marginRight: 8 }} />
            当前配置
          </span>
        }
      >
        {info ? (
          <pre
            style={{
              background: "rgba(0,0,0,0.3)",
              padding: 16,
              borderRadius: 6,
              color: "#d4d4d4",
              fontSize: 13,
              overflow: "auto",
              maxHeight: 400,
              margin: 0,
            }}
          >
            {JSON.stringify(info.config, null, 2)}
          </pre>
        ) : (
          <Text type="secondary">无法加载配置</Text>
        )}
      </Card>

      {/* API Docs Link */}
      <Card
        title={
          <span>
            <ApiOutlined style={{ marginRight: 8 }} />
            API 文档
          </span>
        }
      >
        <Space direction="vertical">
          <Text>FastAPI 自动生成的交互式 API 文档:</Text>
          <Link href="http://localhost:8000/docs" target="_blank">
            http://localhost:8000/docs
          </Link>
        </Space>
      </Card>
    </Space>
  );
}

import { useState } from "react";
import { Routes, Route, useNavigate, useLocation } from "react-router-dom";
import { ConfigProvider, Layout, Menu, theme, Typography } from "antd";
import {
  LineChartOutlined,
  DatabaseOutlined,
  ExperimentOutlined,
  ToolOutlined,
  RocketOutlined,
  FundOutlined,
  ThunderboltOutlined,
  PlayCircleOutlined,
  ScheduleOutlined,
  SettingOutlined,
} from "@ant-design/icons";
import type { MenuProps } from "antd";

import MarketBrowser from "./pages/MarketPage";
import DataManagement from "./pages/DataManagePage";
import FactorResearch from "./pages/FactorResearch";
import FeatureEngineering from "./pages/FeatureEngineering";
import ModelTraining from "./pages/ModelTraining";
import StrategyBacktest from "./pages/StrategyBacktest";
import SignalGeneration from "./pages/SignalGeneration";
import PaperTrading from "./pages/PaperTrading";
import SystemSettings from "./pages/SystemSettings";
import TaskManagement from "./pages/TaskManagement";
import MarketScopeSelector from "./components/MarketScopeSelector";

const { Sider, Content, Header } = Layout;
const { Title } = Typography;

const menuItems: MenuProps["items"] = [
  { key: "/market", icon: <LineChartOutlined />, label: "行情浏览" },
  { key: "/data", icon: <DatabaseOutlined />, label: "数据管理" },
  { key: "/factors", icon: <ExperimentOutlined />, label: "因子研究" },
  { key: "/features", icon: <ToolOutlined />, label: "特征工程" },
  { key: "/models", icon: <RocketOutlined />, label: "模型训练" },
  { key: "/backtest", icon: <FundOutlined />, label: "策略回测" },
  { key: "/signals", icon: <ThunderboltOutlined />, label: "信号生成" },
  { key: "/paper-trading", icon: <PlayCircleOutlined />, label: "模拟交易" },
  { key: "/tasks", icon: <ScheduleOutlined />, label: "任务管理" },
  { key: "/settings", icon: <SettingOutlined />, label: "系统设置" },
];

export default function App() {
  const [collapsed, setCollapsed] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const currentMenuItem = menuItems?.find(
    (i) => i && "key" in i && i.key === location.pathname,
  );

  const onMenuClick: MenuProps["onClick"] = ({ key }) => {
    navigate(key);
  };

  return (
    <ConfigProvider
      theme={{
        algorithm: theme.darkAlgorithm,
        token: {
          colorPrimary: "#1677ff",
          borderRadius: 6,
        },
      }}
    >
      <Layout style={{ minHeight: "100vh" }}>
        <Sider
          collapsible
          collapsed={collapsed}
          onCollapse={setCollapsed}
          style={{ borderRight: "1px solid rgba(255,255,255,0.08)" }}
        >
          <div
            style={{
              height: 48,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              margin: "8px 0",
            }}
          >
            <Title
              level={4}
              style={{ color: "#fff", margin: 0, whiteSpace: "nowrap" }}
            >
              {collapsed ? "Q" : "QAgent"}
            </Title>
          </div>
          <Menu
            theme="dark"
            mode="inline"
            selectedKeys={[location.pathname]}
            items={menuItems}
            onClick={onMenuClick}
          />
        </Sider>
        <Layout>
          <Header
            style={{
              padding: "0 24px",
              background: "transparent",
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              gap: 16,
            }}
          >
            <Title level={5} style={{ margin: 0, color: "rgba(255,255,255,0.85)" }}>
              {currentMenuItem
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                ? (currentMenuItem as any).label
                : "QAgent"}
            </Title>
            <MarketScopeSelector />
          </Header>
          <Content style={{ margin: 16, padding: 24, background: "rgba(255,255,255,0.04)", borderRadius: 8 }}>
            <Routes>
              <Route path="/" element={<MarketBrowser />} />
              <Route path="/market" element={<MarketBrowser />} />
              <Route path="/data" element={<DataManagement />} />
              <Route path="/factors" element={<FactorResearch />} />
              <Route path="/features" element={<FeatureEngineering />} />
              <Route path="/models" element={<ModelTraining />} />
              <Route path="/backtest" element={<StrategyBacktest />} />
              <Route path="/signals" element={<SignalGeneration />} />
              <Route path="/paper-trading" element={<PaperTrading />} />
              <Route path="/tasks" element={<TaskManagement />} />
              <Route path="/settings" element={<SystemSettings />} />
            </Routes>
          </Content>
        </Layout>
      </Layout>
    </ConfigProvider>
  );
}

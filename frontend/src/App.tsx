import { useEffect, useState } from "react";
import { Routes, Route, useNavigate, useLocation } from "react-router-dom";
import { Alert, Button, ConfigProvider, Layout, Menu, theme, Typography } from "antd";
import {
  DashboardOutlined,
  LineChartOutlined,
  DatabaseOutlined,
  ExperimentOutlined,
  ToolOutlined,
  RocketOutlined,
  ScheduleOutlined,
  SettingOutlined,
} from "@ant-design/icons";
import type { MenuProps } from "antd";

import MarketBrowser from "./pages/MarketPage";
import DataManagement from "./pages/DataManagePage";
import FactorResearch from "./pages/FactorResearch";
import FeatureEngineering from "./pages/FeatureEngineering";
import ModelTraining from "./pages/ModelTraining";
import SystemSettings from "./pages/SystemSettings";
import TaskManagement from "./pages/TaskManagement";
import ResearchWorkbench3 from "./pages/ResearchWorkbench3";
import MarketScopeSelector from "./components/MarketScopeSelector";
import { getActiveMarket, subscribeActiveMarket } from "./api/client";
import type { Market } from "./api";

const { Sider, Content, Header } = Layout;
const { Title } = Typography;

const menuItems: MenuProps["items"] = [
  { key: "/research", icon: <DashboardOutlined />, label: "研究工作台" },
  { key: "/market", icon: <LineChartOutlined />, label: "行情浏览" },
  { key: "/data", icon: <DatabaseOutlined />, label: "数据管理" },
  { key: "/factors", icon: <ExperimentOutlined />, label: "因子研究" },
  { key: "/features", icon: <ToolOutlined />, label: "特征工程" },
  { key: "/models", icon: <RocketOutlined />, label: "模型训练" },
  { key: "/tasks", icon: <ScheduleOutlined />, label: "任务管理" },
  { key: "/settings", icon: <SettingOutlined />, label: "系统设置" },
];

export default function App() {
  const [collapsed, setCollapsed] = useState(false);
  const [marketScope, setMarketScope] = useState<Market>(() => getActiveMarket());
  const navigate = useNavigate();
  const location = useLocation();
  const isResearchWorkbench = location.pathname === "/" || location.pathname === "/research";
  const selectedMenuKey = isResearchWorkbench ? "/research" : location.pathname;
  const currentMenuItem = menuItems?.find(
    (i) => i && "key" in i && i.key === selectedMenuKey,
  );

  const onMenuClick: MenuProps["onClick"] = ({ key }) => {
    navigate(key);
  };

  useEffect(() => subscribeActiveMarket(setMarketScope), []);

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
            selectedKeys={[selectedMenuKey]}
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
            {isResearchWorkbench ? null : <MarketScopeSelector />}
          </Header>
          <Content
            key={marketScope}
            style={{ margin: 16, padding: 24, background: "rgba(255,255,255,0.04)", borderRadius: 8 }}
          >
            <Routes>
              <Route path="/" element={<ResearchWorkbench3 />} />
              <Route path="/research" element={<ResearchWorkbench3 />} />
              <Route path="/market" element={<MarketBrowser />} />
              <Route path="/data" element={<DataManagement />} />
              <Route path="/factors" element={<FactorResearch />} />
              <Route path="/features" element={<FeatureEngineering />} />
              <Route path="/models" element={<ModelTraining />} />
              <Route path="/backtest" element={<LegacyRuntimeDisabledPage />} />
              <Route path="/signals" element={<LegacyRuntimeDisabledPage />} />
              <Route path="/paper-trading" element={<LegacyRuntimeDisabledPage />} />
              <Route path="/tasks" element={<TaskManagement />} />
              <Route path="/settings" element={<SystemSettings />} />
            </Routes>
          </Content>
        </Layout>
      </Layout>
    </ConfigProvider>
  );
}

function LegacyRuntimeDisabledPage() {
  const navigate = useNavigate();
  return (
    <Alert
      type="warning"
      showIcon
      message="旧运行入口已在 V3.2 禁用"
      description="策略回测、信号生成和模拟交易现在统一在 3.0 Research Workbench 中通过 StrategyGraph、production signal 和 paper session 完成。旧页面不再作为业务入口展示。"
      action={
        <Button type="primary" onClick={() => navigate("/research")}>
          打开研究工作台
        </Button>
      }
    />
  );
}

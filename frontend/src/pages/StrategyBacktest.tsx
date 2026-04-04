import { useState, useCallback } from "react";
import { Tabs } from "antd";
import {
  CodeOutlined,
  PlayCircleOutlined,
  AppstoreOutlined,
  HistoryOutlined,
} from "@ant-design/icons";
import StrategyEditorPanel from "../components/strategy/StrategyEditorPanel";
import BacktestRunnerPanel from "../components/strategy/BacktestRunnerPanel";
import StrategyList from "../components/strategy/StrategyList";
import BacktestHistory from "../components/strategy/BacktestHistory";

export default function StrategyBacktest() {
  const [activeTab, setActiveTab] = useState("editor");
  const [strategyRefreshKey, setStrategyRefreshKey] = useState(0);
  const [backtestRefreshKey, setBacktestRefreshKey] = useState(0);

  const handleStrategySaved = useCallback(() => {
    setStrategyRefreshKey((k) => k + 1);
  }, []);

  const handleBacktestComplete = useCallback(() => {
    setBacktestRefreshKey((k) => k + 1);
  }, []);

  const tabItems = [
    {
      key: "editor",
      label: (
        <span>
          <CodeOutlined />
          {" "}策略编辑器
        </span>
      ),
      children: <StrategyEditorPanel onStrategySaved={handleStrategySaved} />,
    },
    {
      key: "backtest",
      label: (
        <span>
          <PlayCircleOutlined />
          {" "}回测
        </span>
      ),
      children: <BacktestRunnerPanel onBacktestComplete={handleBacktestComplete} />,
    },
    {
      key: "strategies",
      label: (
        <span>
          <AppstoreOutlined />
          {" "}策略列表
        </span>
      ),
      children: <StrategyList refreshKey={strategyRefreshKey} />,
    },
    {
      key: "history",
      label: (
        <span>
          <HistoryOutlined />
          {" "}回测历史
        </span>
      ),
      children: <BacktestHistory refreshKey={backtestRefreshKey} />,
    },
  ];

  return (
    <Tabs
      activeKey={activeTab}
      onChange={setActiveTab}
      items={tabItems}
      style={{ minHeight: 600 }}
    />
  );
}

import { useState, useCallback } from "react";
import { Tabs } from "antd";
import { ThunderboltOutlined, HistoryOutlined } from "@ant-design/icons";
import SignalGeneratorPanel from "../components/signal/SignalGeneratorPanel";
import SignalHistory from "../components/signal/SignalHistory";
import type { SignalRestoreConfig } from "../components/signal/SignalHistory";

export default function SignalGeneration() {
  const [activeTab, setActiveTab] = useState("generate");
  const [historyRefreshKey, setHistoryRefreshKey] = useState(0);
  const [restoreConfig, setRestoreConfig] = useState<SignalRestoreConfig | null>(null);

  const handleSignalComplete = useCallback(() => {
    setHistoryRefreshKey((k) => k + 1);
  }, []);

  const handleRestoreConfig = useCallback((config: SignalRestoreConfig) => {
    setRestoreConfig(config);
    setActiveTab("generate");
  }, []);

  const tabItems = [
    {
      key: "generate",
      label: (
        <span>
          <ThunderboltOutlined />
          {" "}信号生成
        </span>
      ),
      children: <SignalGeneratorPanel onSignalComplete={handleSignalComplete} restoreConfig={restoreConfig} />,
    },
    {
      key: "history",
      label: (
        <span>
          <HistoryOutlined />
          {" "}信号历史
        </span>
      ),
      children: <SignalHistory refreshKey={historyRefreshKey} onRestoreConfig={handleRestoreConfig} />,
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

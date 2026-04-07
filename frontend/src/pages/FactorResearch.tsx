import { useState, useCallback } from "react";
import { Tabs } from "antd";
import {
  CodeOutlined,
  AppstoreOutlined,
  HistoryOutlined,
} from "@ant-design/icons";
import { getFactor } from "../api";
import type { Factor } from "../api";
import FactorEditor from "../components/factor/FactorEditor";
import FactorLibrary from "../components/factor/FactorLibrary";
import EvalHistory from "../components/factor/EvalHistory";
import type { EvalRestoreConfig } from "../components/factor/EvalHistory";

export default function FactorResearch() {
  const [activeTab, setActiveTab] = useState("editor");
  const [editingFactor, setEditingFactor] = useState<Factor | null>(null);
  const [libraryRefreshKey, setLibraryRefreshKey] = useState(0);
  const [evalConfig, setEvalConfig] = useState<EvalRestoreConfig | null>(null);

  const handleViewFactor = useCallback((factor: Factor) => {
    setEditingFactor(factor);
    setActiveTab("editor");
  }, []);

  const handleFactorSaved = useCallback(() => {
    setLibraryRefreshKey((k) => k + 1);
  }, []);

  const handleRestoreEvalConfig = useCallback(async (config: EvalRestoreConfig) => {
    try {
      const factor = await getFactor(config.factorId);
      setEditingFactor(factor);
      setEvalConfig(config);
      setActiveTab("editor");
    } catch {
      // If factor was deleted, still switch to editor with config
      setEditingFactor(null);
      setEvalConfig(config);
      setActiveTab("editor");
    }
  }, []);

  const tabItems = [
    {
      key: "editor",
      label: (
        <span>
          <CodeOutlined />
          {" "}因子编辑器
        </span>
      ),
      children: (
        <FactorEditor
          editingFactor={editingFactor}
          evalConfig={evalConfig}
          onFactorSaved={handleFactorSaved}
        />
      ),
    },
    {
      key: "library",
      label: (
        <span>
          <AppstoreOutlined />
          {" "}因子库
        </span>
      ),
      children: (
        <FactorLibrary
          onViewFactor={handleViewFactor}
          refreshKey={libraryRefreshKey}
        />
      ),
    },
    {
      key: "history",
      label: (
        <span>
          <HistoryOutlined />
          {" "}评价历史
        </span>
      ),
      children: <EvalHistory onRestoreConfig={handleRestoreEvalConfig} />,
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

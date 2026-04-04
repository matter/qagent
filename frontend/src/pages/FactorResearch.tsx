import { useState, useCallback } from "react";
import { Tabs } from "antd";
import {
  CodeOutlined,
  AppstoreOutlined,
  HistoryOutlined,
} from "@ant-design/icons";
import type { Factor } from "../api";
import FactorEditor from "../components/factor/FactorEditor";
import FactorLibrary from "../components/factor/FactorLibrary";
import EvalHistory from "../components/factor/EvalHistory";

export default function FactorResearch() {
  const [activeTab, setActiveTab] = useState("editor");
  const [editingFactor, setEditingFactor] = useState<Factor | null>(null);
  const [libraryRefreshKey, setLibraryRefreshKey] = useState(0);

  const handleViewFactor = useCallback((factor: Factor) => {
    setEditingFactor(factor);
    setActiveTab("editor");
  }, []);

  const handleFactorSaved = useCallback(() => {
    setLibraryRefreshKey((k) => k + 1);
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
      children: <EvalHistory />,
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

import { useState, useCallback } from "react";
import { Space } from "antd";
import FeatureSetCreator from "../components/feature/FeatureSetCreator";
import FeatureSetList from "../components/feature/FeatureSetList";
import type { FeatureSetRestoreConfig } from "../components/feature/FeatureSetList";

export default function FeatureEngineering() {
  const [listRefreshKey, setListRefreshKey] = useState(0);
  const [restoreConfig, setRestoreConfig] = useState<FeatureSetRestoreConfig | null>(null);

  const handleCreated = useCallback(() => {
    setListRefreshKey((k) => k + 1);
  }, []);

  const handleRestoreConfig = useCallback((config: FeatureSetRestoreConfig) => {
    setRestoreConfig(config);
  }, []);

  return (
    <Space orientation="vertical" style={{ width: "100%" }} size="middle">
      <FeatureSetCreator onCreated={handleCreated} restoreConfig={restoreConfig} />
      <FeatureSetList refreshKey={listRefreshKey} onRestoreConfig={handleRestoreConfig} />
    </Space>
  );
}

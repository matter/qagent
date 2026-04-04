import { useState, useCallback } from "react";
import { Space } from "antd";
import FeatureSetCreator from "../components/feature/FeatureSetCreator";
import FeatureSetList from "../components/feature/FeatureSetList";

export default function FeatureEngineering() {
  const [listRefreshKey, setListRefreshKey] = useState(0);

  const handleCreated = useCallback(() => {
    setListRefreshKey((k) => k + 1);
  }, []);

  return (
    <Space direction="vertical" style={{ width: "100%" }} size="middle">
      <FeatureSetCreator onCreated={handleCreated} />
      <FeatureSetList refreshKey={listRefreshKey} />
    </Space>
  );
}

import { useState, useCallback } from "react";
import { Tabs } from "antd";
import {
  SettingOutlined,
  AppstoreOutlined,
} from "@ant-design/icons";
import TrainConfigPanel from "../components/model/TrainConfigPanel";
import ModelList from "../components/model/ModelList";

export default function ModelTraining() {
  const [activeTab, setActiveTab] = useState("config");
  const [listRefreshKey, setListRefreshKey] = useState(0);

  const handleTrainComplete = useCallback(() => {
    setListRefreshKey((k) => k + 1);
  }, []);

  const tabItems = [
    {
      key: "config",
      label: (
        <span>
          <SettingOutlined />
          {" "}训练配置
        </span>
      ),
      children: <TrainConfigPanel onTrainComplete={handleTrainComplete} />,
    },
    {
      key: "models",
      label: (
        <span>
          <AppstoreOutlined />
          {" "}模型列表
        </span>
      ),
      children: <ModelList refreshKey={listRefreshKey} />,
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

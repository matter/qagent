import { useEffect, useState } from "react";
import { Segmented, Space, Tag, Tooltip, Typography } from "antd";
import {
  MARKET_STORAGE_KEY,
  getActiveMarket,
  setActiveMarket,
  subscribeActiveMarket,
} from "../api/client";
import type { Market } from "../api";

const { Text } = Typography;

const MARKET_OPTIONS: Array<{ label: string; value: Market }> = [
  { label: "US", value: "US" },
  { label: "CN", value: "CN" },
];

export default function MarketScopeSelector() {
  const [market, setMarket] = useState<Market>(() => getActiveMarket());

  useEffect(() => subscribeActiveMarket(setMarket), []);

  const handleChange = (value: string | number) => {
    const nextMarket: Market = value === "CN" ? "CN" : "US";
    setMarket(setActiveMarket(nextMarket));
  };

  return (
    <Space size={10} align="center" wrap={false}>
      <Text type="secondary" style={{ fontSize: 12 }}>
        Market
      </Text>
      <Tooltip title={`Saved in localStorage as ${MARKET_STORAGE_KEY}`}>
        <Segmented
          size="small"
          options={MARKET_OPTIONS}
          value={market}
          onChange={handleChange}
        />
      </Tooltip>
      <Tag
        color={market === "CN" ? "red" : "blue"}
        style={{ marginInlineEnd: 0, minWidth: 56, textAlign: "center" }}
      >
        {market === "CN" ? "A股" : "US"}
      </Tag>
    </Space>
  );
}

import axios, { type AxiosRequestConfig } from "axios";
import type { Market } from "./index";

const client = axios.create({
  baseURL: "/api",
  timeout: 30_000,
});

export const DEFAULT_MARKET: Market = "US";
export const MARKET_STORAGE_KEY = "qagent.market";

const MARKET_VALUES = new Set<Market>(["US", "CN"]);
const MARKET_SCOPED_PATHS = [
  "/stocks",
  "/data/status",
  "/data/update",
  "/data/quality",
  "/data/bars",
  "/groups",
  "/labels",
  "/factors",
  "/feature-sets",
  "/models",
  "/strategies",
  "/signals",
  "/paper-trading",
];

const MARKET_SCOPE_EXCLUSIONS = [
  "/data/update/progress",
  "/factors/templates",
  "/strategies/templates",
];

type MarketListener = (market: Market) => void;

const listeners = new Set<MarketListener>();

function canUseLocalStorage(): boolean {
  return typeof window !== "undefined" && typeof window.localStorage !== "undefined";
}

export function normalizeMarketScope(value: unknown): Market {
  if (typeof value !== "string") return DEFAULT_MARKET;
  const normalized = value.trim().toUpperCase();
  return MARKET_VALUES.has(normalized as Market) ? (normalized as Market) : DEFAULT_MARKET;
}

function readInitialMarket(): Market {
  if (!canUseLocalStorage()) return DEFAULT_MARKET;
  return normalizeMarketScope(window.localStorage.getItem(MARKET_STORAGE_KEY));
}

let activeMarket: Market = readInitialMarket();

export function getActiveMarket(): Market {
  return activeMarket;
}

export function setActiveMarket(market: Market): Market {
  const nextMarket = normalizeMarketScope(market);
  if (nextMarket === activeMarket) return activeMarket;

  activeMarket = nextMarket;
  if (canUseLocalStorage()) {
    window.localStorage.setItem(MARKET_STORAGE_KEY, nextMarket);
  }
  listeners.forEach((listener) => listener(nextMarket));
  return activeMarket;
}

export function subscribeActiveMarket(listener: MarketListener): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

function pathFromUrl(url?: string): string {
  if (!url) return "";
  try {
    return new URL(url, "http://qagent.local").pathname;
  } catch {
    return url.startsWith("/") ? url : `/${url}`;
  }
}

function isMarketScopedRequest(config: AxiosRequestConfig): boolean {
  const path = pathFromUrl(config.url);
  if (MARKET_SCOPE_EXCLUSIONS.some((prefix) => path.startsWith(prefix))) {
    return false;
  }
  return MARKET_SCOPED_PATHS.some((prefix) => path.startsWith(prefix));
}

function hasMarketValue(value: unknown): boolean {
  return value !== undefined && value !== null && value !== "";
}

function withMarketParam(params: unknown, market: Market): Record<string, unknown> {
  if (params && typeof params === "object" && !Array.isArray(params)) {
    const nextParams = { ...(params as Record<string, unknown>) };
    if (!hasMarketValue(nextParams.market)) nextParams.market = market;
    return nextParams;
  }
  return { market };
}

function withMarketBody(data: unknown, market: Market): unknown {
  const isFormData = typeof FormData !== "undefined" && data instanceof FormData;
  if (!data || typeof data !== "object" || Array.isArray(data) || isFormData) {
    return data;
  }

  const nextData = { ...(data as Record<string, unknown>) };
  if (!hasMarketValue(nextData.market)) nextData.market = market;
  return nextData;
}

client.interceptors.request.use((config) => {
  if (!isMarketScopedRequest(config)) return config;

  const market = getActiveMarket();
  const method = (config.method || "get").toLowerCase();

  if (method === "get" || method === "delete") {
    config.params = withMarketParam(config.params, market);
    return config;
  }

  if (config.data === undefined || config.data === null) {
    config.params = withMarketParam(config.params, market);
    return config;
  }

  config.data = withMarketBody(config.data, market);
  return config;
});

export default client;

import client from "./client";

// ---- Types ----

export interface StockSearchResult {
  ticker: string;
  name: string;
  exchange: string;
  sector: string;
  status: string;
}

export interface DailyBar {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  adj_factor: number;
}

export interface DataStatus {
  stock_count: number;
  tickers_with_bars: number;
  date_range: { min: string | null; max: string | null };
  total_bars: number;
  stale_tickers: number;
  latest_trading_day: string;
  last_update: {
    completed_at: string | null;
    status: string | null;
    type: string | null;
  };
}

export interface UpdateProgress {
  task_id?: string;
  status: string;
  started_at?: string | null;
  completed_at?: string | null;
  result?: Record<string, unknown> | null;
  error?: string | null;
  message?: string;
}

export interface StockGroup {
  id: string;
  name: string;
  description: string | null;
  group_type: string;
  filter_expr: string | null;
  created_at: string | null;
  updated_at: string | null;
  member_count: number;
  tickers?: string[];
}

// ---- Data API ----

export async function searchStocks(q: string, limit = 20): Promise<StockSearchResult[]> {
  const { data } = await client.get<StockSearchResult[]>("/stocks/search", {
    params: { q, limit },
  });
  return data;
}

export async function getDailyBars(
  ticker: string,
  start?: string,
  end?: string,
): Promise<DailyBar[]> {
  const { data } = await client.get<DailyBar[]>(`/stocks/${ticker}/daily`, {
    params: { start, end },
  });
  return data;
}

export async function getDataStatus(): Promise<DataStatus> {
  const { data } = await client.get<DataStatus>("/data/status");
  return data;
}

export async function triggerUpdate(mode: "incremental" | "full") {
  const { data } = await client.post("/data/update", { mode });
  return data;
}

export async function getUpdateProgress(): Promise<UpdateProgress> {
  const { data } = await client.get<UpdateProgress>("/data/update/progress");
  return data;
}

// ---- Groups API ----

export async function listGroups(): Promise<StockGroup[]> {
  const { data } = await client.get<StockGroup[]>("/groups");
  return data;
}

export async function getGroup(groupId: string): Promise<StockGroup> {
  const { data } = await client.get<StockGroup>(`/groups/${groupId}`);
  return data;
}

export async function createGroup(params: {
  name: string;
  description?: string;
  group_type?: string;
  tickers?: string[];
  filter_expr?: string;
}): Promise<StockGroup> {
  const { data } = await client.post<StockGroup>("/groups", params);
  return data;
}

export async function updateGroup(
  groupId: string,
  params: {
    name?: string;
    description?: string;
    tickers?: string[];
    filter_expr?: string;
  },
): Promise<StockGroup> {
  const { data } = await client.put<StockGroup>(`/groups/${groupId}`, params);
  return data;
}

export async function deleteGroup(groupId: string) {
  const { data } = await client.delete(`/groups/${groupId}`);
  return data;
}

export async function refreshGroup(groupId: string): Promise<StockGroup> {
  const { data } = await client.post<StockGroup>(`/groups/${groupId}/refresh`);
  return data;
}

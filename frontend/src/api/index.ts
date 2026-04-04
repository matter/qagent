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

// ---- Factor Types ----

export interface Factor {
  id: string;
  name: string;
  version: number;
  description: string | null;
  category: string;
  source_code: string;
  params: Record<string, unknown> | null;
  status: string;
  created_at: string;
  updated_at: string;
}

export interface FactorTemplate {
  name: string;
  source_code?: string;
}

export interface LabelDefinition {
  id: string;
  name: string;
  description: string | null;
  target_type: string;
  horizon: number;
  benchmark: string | null;
  status: string;
}

export interface FactorEvalSummary {
  ic_mean: number;
  ic_std: number;
  ir: number;
  ic_win_rate: number;
  long_short_annual_return: number;
  turnover: number;
  coverage: number;
}

export interface FactorEvalRecord {
  id: string;
  factor_id: string;
  label_id: string;
  universe_group_id: string;
  start_date: string | null;
  end_date: string | null;
  summary: FactorEvalSummary;
  created_at: string | null;
}

export interface FactorEvalDetail extends FactorEvalRecord {
  ic_series: Array<{ date: string; ic: number | null }>;
  group_returns: {
    dates: string[];
    groups: Record<string, number[]>;
  };
}

export interface TaskStatus {
  task_id: string;
  task_type: string;
  status: string;
  params: Record<string, unknown> | null;
  result: Record<string, unknown> | null;
  error: string | null;
  created_at: string | null;
  started_at: string | null;
  completed_at: string | null;
}

// ---- Factor API ----

export async function listFactors(category?: string, status?: string): Promise<Factor[]> {
  const { data } = await client.get<Factor[]>("/factors", {
    params: { category: category || undefined, status: status || undefined },
  });
  return data;
}

export async function getFactor(factorId: string): Promise<Factor> {
  const { data } = await client.get<Factor>(`/factors/${factorId}`);
  return data;
}

export async function createFactor(params: {
  name: string;
  source_code: string;
  description?: string;
  category?: string;
  params?: Record<string, unknown>;
}): Promise<Factor> {
  const { data } = await client.post<Factor>("/factors", params);
  return data;
}

export async function updateFactor(
  factorId: string,
  params: {
    source_code?: string;
    description?: string;
    category?: string;
    params?: Record<string, unknown>;
    status?: string;
  },
): Promise<Factor> {
  const { data } = await client.put<Factor>(`/factors/${factorId}`, params);
  return data;
}

export async function deleteFactor(factorId: string) {
  const { data } = await client.delete(`/factors/${factorId}`);
  return data;
}

export async function listTemplates(): Promise<FactorTemplate[]> {
  const { data } = await client.get<FactorTemplate[]>("/factors/templates");
  return data;
}

export async function getTemplate(name: string): Promise<FactorTemplate> {
  const { data } = await client.get<FactorTemplate>(`/factors/templates/${name}`);
  return data;
}

export async function computeFactor(
  factorId: string,
  body: { universe_group_id: string; start_date: string; end_date: string },
): Promise<{ task_id: string }> {
  const { data } = await client.post<{ task_id: string }>(`/factors/${factorId}/compute`, body);
  return data;
}

export async function evaluateFactor(
  factorId: string,
  body: { label_id: string; universe_group_id: string; start_date: string; end_date: string },
): Promise<{ task_id: string }> {
  const { data } = await client.post<{ task_id: string }>(`/factors/${factorId}/evaluate`, body);
  return data;
}

export async function listEvaluations(factorId: string): Promise<FactorEvalRecord[]> {
  const { data } = await client.get<FactorEvalRecord[]>(`/factors/${factorId}/evaluations`);
  return data;
}

export async function getEvaluation(evalId: string): Promise<FactorEvalDetail> {
  const { data } = await client.get<FactorEvalDetail>(`/factors/evaluations/${evalId}`);
  return data;
}

// ---- Label API ----

export async function listLabels(): Promise<LabelDefinition[]> {
  const { data } = await client.get<LabelDefinition[]>("/labels");
  return data;
}

// ---- Task API ----

export async function getTaskStatus(taskId: string): Promise<TaskStatus> {
  const { data } = await client.get<TaskStatus>(`/tasks/${taskId}`);
  return data;
}

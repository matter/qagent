import client from "./client";
export {
  DEFAULT_MARKET,
  MARKET_STORAGE_KEY,
  getActiveMarket,
  normalizeMarketScope,
  setActiveMarket,
  subscribeActiveMarket,
} from "./client";

// ---- Types ----

export type Market = "US" | "CN";
export type ExecutionModel = "next_open" | "planned_price" | "next_close";
export type PlannedPriceFallback = "cancel" | "next_close";

export interface StockSearchResult {
  market: Market;
  ticker: string;
  name: string;
  exchange: string;
  sector: string;
  status: string;
}

export interface DailyBar {
  market?: Market;
  ticker?: string;
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  adj_factor: number;
}

export interface IndexBar {
  market: Market;
  symbol: string;
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface GroupDailySnapshot {
  market: Market;
  group_id: string;
  date: string;
  total_tickers: number;
  tickers_with_bars: number;
  missing_count: number;
  missing_tickers: string[];
  items: Array<DailyBar & { has_bar: boolean }>;
}

export interface DataStatus {
  market?: Market;
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
  market: Market;
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

export async function searchStocks(q: string, limit = 20, market?: Market): Promise<StockSearchResult[]> {
  const { data } = await client.get<StockSearchResult[]>("/stocks/search", {
    params: { q, limit, market: market || undefined },
  });
  return data;
}

export async function getDailyBars(
  ticker: string,
  start?: string,
  end?: string,
  market?: Market,
): Promise<DailyBar[]> {
  const { data } = await client.get<DailyBar[]>(`/stocks/${ticker}/daily`, {
    params: { start, end, market: market || undefined },
  });
  return data;
}

export async function getIndexBars(
  symbol: string,
  start?: string,
  end?: string,
  market?: Market,
): Promise<IndexBar[]> {
  const { data } = await client.get<IndexBar[]>(`/data/index-bars/${symbol}`, {
    params: { start, end, market: market || undefined },
  });
  return data;
}

export async function getGroupDailySnapshot(
  groupId: string,
  targetDate: string,
  market?: Market,
  limit = 500,
): Promise<GroupDailySnapshot> {
  const { data } = await client.get<GroupDailySnapshot>(`/data/groups/${groupId}/daily-snapshot`, {
    params: { date: targetDate, market: market || undefined, limit },
  });
  return data;
}

export async function getDiagnosticDailyBars(
  tickers: string[],
  targetDate: string,
  market?: Market,
) {
  const { data } = await client.get("/diagnostics/daily-bars", {
    params: { tickers, date: targetDate, market: market || undefined },
  });
  return data;
}

export async function getDiagnosticFactorValues(
  factorId: string,
  tickers: string[],
  targetDate: string,
  market?: Market,
) {
  const { data } = await client.get("/diagnostics/factor-values", {
    params: { factor_id: factorId, tickers, date: targetDate, market: market || undefined },
  });
  return data;
}

export async function getDataStatus(market?: Market): Promise<DataStatus> {
  const { data } = await client.get<DataStatus>("/data/status", {
    params: { market: market || undefined },
  });
  return data;
}

export async function triggerUpdate(
  mode: "incremental" | "full",
  market?: Market,
  historyYears?: number,
  startDate?: string,
) {
  const { data } = await client.post("/data/update", {
    mode,
    market: market || undefined,
    history_years: historyYears,
    start_date: startDate || undefined,
  });
  return data;
}

export async function triggerMultiMarketUpdate(
  mode: "incremental" | "full",
  markets: Market[],
  historyYears?: number,
  startDate?: string,
) {
  const { data } = await client.post("/data/update/markets", {
    mode,
    markets,
    history_years: historyYears,
    start_date: startDate || undefined,
  });
  return data;
}

export async function refreshStockList(
  market?: Market,
): Promise<{ task_id: string; status: string; market?: Market }> {
  const { data } = await client.post("/data/refresh-stock-list", { market: market || undefined });
  return data;
}

export async function updateTickers(
  tickers: string[],
  market?: Market,
): Promise<{ task_id: string; status: string; market?: Market; tickers: number }> {
  const { data } = await client.post("/data/update/tickers", { tickers, market: market || undefined });
  return data;
}

export async function updateGroupData(
  groupId: string,
  market?: Market,
): Promise<{ task_id: string; status: string; market?: Market; tickers: number }> {
  const { data } = await client.post("/data/update/group", { group_id: groupId, market: market || undefined });
  return data;
}

export async function getUpdateProgress(): Promise<UpdateProgress> {
  const { data } = await client.get<UpdateProgress>("/data/update/progress");
  return data;
}

// ---- Groups API ----

export async function listGroups(market?: Market): Promise<StockGroup[]> {
  const { data } = await client.get<StockGroup[]>("/groups", {
    params: { market: market || undefined },
  });
  return data;
}

export async function getGroup(groupId: string, market?: Market): Promise<StockGroup> {
  const { data } = await client.get<StockGroup>(`/groups/${groupId}`, {
    params: { market: market || undefined },
  });
  return data;
}

export async function createGroup(params: {
  market?: Market;
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
    market?: Market;
    name?: string;
    description?: string;
    tickers?: string[];
    filter_expr?: string;
  },
): Promise<StockGroup> {
  const { data } = await client.put<StockGroup>(`/groups/${groupId}`, params);
  return data;
}

export async function deleteGroup(groupId: string, market?: Market) {
  const { data } = await client.delete(`/groups/${groupId}`, {
    params: { market: market || undefined },
  });
  return data;
}

export async function refreshGroup(groupId: string, market?: Market): Promise<StockGroup> {
  const { data } = await client.post<StockGroup>(
    `/groups/${groupId}/refresh`,
    undefined,
    { params: { market: market || undefined } },
  );
  return data;
}

export async function refreshIndexGroups(market?: Market): Promise<StockGroup[]> {
  const { data } = await client.post(
    "/groups/refresh-indices",
    undefined,
    { params: { market: market || undefined } },
  );
  return data;
}

// ---- Factor Types ----

export interface Factor {
  id: string;
  market: Market;
  name: string;
  version: number;
  description: string | null;
  category: string;
  source_code: string;
  params: Record<string, unknown> | null;
  status: string;
  created_at: string;
  updated_at: string;
  latest_ir?: number | null;
}

export interface FactorTemplate {
  name: string;
  source_code?: string;
}

export interface LabelDefinition {
  id: string;
  market: Market;
  name: string;
  description: string | null;
  target_type: string;
  horizon: number;
  benchmark: string | null;
  config: Record<string, unknown> | null;
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
  market: Market;
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
  result_summary: Record<string, unknown> | null;
  error: string | null;
  created_at: string | null;
  started_at: string | null;
  completed_at: string | null;
  source?: string;
  late_result_quarantined?: boolean;
  late_result_diagnostics?: Record<string, unknown>;
  authoritative_terminal?: boolean;
  requested_start_date?: string;
  requested_end_date?: string;
  effective_start_date?: string;
  effective_end_date?: string;
  date_adjustment?: Record<string, unknown>;
  interrupted?: boolean;
  retryable?: boolean;
  cancel_requested?: boolean;
  compute_may_continue?: boolean;
}

export interface MacroSeries {
  provider: string;
  series_id: string;
  title: string | null;
  frequency: string | null;
  units: string | null;
  seasonal_adjustment: string | null;
  source: string | null;
  source_url: string | null;
  metadata: Record<string, unknown>;
  created_at: string | null;
  updated_at: string | null;
}

export interface MacroObservation {
  provider: string;
  series_id: string;
  title: string | null;
  date: string;
  realtime_start: string;
  realtime_end: string;
  available_at: string | null;
  value: number | null;
  source_metadata: Record<string, unknown>;
}

export interface MacroUpdateTask {
  task_id: string;
  status: string;
  provider: string;
  series_ids: string[];
  poll_url: string;
}

export interface TaskResponse {
  task_id: string;
  status: string;
  task_type?: string;
  poll_url?: string;
  [key: string]: unknown;
}

export interface MacroObservationsResponse {
  provider: string;
  series_ids: string[];
  observations: MacroObservation[];
}

// ---- Factor API ----

export async function listFactors(category?: string, status?: string, market?: string): Promise<Factor[]> {
  const { data } = await client.get<Factor[]>("/factors", {
    params: { category: category || undefined, status: status || undefined, market: market || undefined },
  });
  return data;
}

export async function getFactor(factorId: string, market?: string): Promise<Factor> {
  const { data } = await client.get<Factor>(`/factors/${factorId}`, {
    params: { market: market || undefined },
  });
  return data;
}

export async function createFactor(params: {
  market?: string;
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
    market?: string;
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

export async function deleteFactor(factorId: string, market?: string) {
  const { data } = await client.delete(`/factors/${factorId}`, {
    params: { market: market || undefined },
  });
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
  body: { market?: string; universe_group_id: string; start_date: string; end_date: string },
): Promise<{ task_id: string; market?: string }> {
  const { data } = await client.post<{ task_id: string; market?: string }>(`/factors/${factorId}/compute`, body);
  return data;
}

export async function evaluateFactor(
  factorId: string,
  body: { market?: string; label_id: string; universe_group_id: string; start_date: string; end_date: string },
): Promise<{ task_id: string; market?: string }> {
  const { data } = await client.post<{ task_id: string; market?: string }>(`/factors/${factorId}/evaluate`, body);
  return data;
}

export async function evaluateFactorByBody(
  body: {
    factor_id: string;
    market?: string;
    label_id: string;
    universe_group_id: string;
    start_date: string;
    end_date: string;
  },
): Promise<{ task_id: string; factor_id: string; market?: string }> {
  const { data } = await client.post<{ task_id: string; factor_id: string; market?: string }>("/factors/evaluate", body);
  return data;
}

export async function listEvaluations(factorId: string, market?: string): Promise<FactorEvalRecord[]> {
  const { data } = await client.get<FactorEvalRecord[]>(`/factors/${factorId}/evaluations`, {
    params: { market: market || undefined },
  });
  return data;
}

export interface FactorEvalRecordWithName extends FactorEvalRecord {
  factor_name: string;
}

export async function listAllEvaluations(market?: string): Promise<FactorEvalRecordWithName[]> {
  const { data } = await client.get<FactorEvalRecordWithName[]>("/factors/evaluations", {
    params: { market: market || undefined },
  });
  return data;
}

export async function getEvaluation(evalId: string, market?: string): Promise<FactorEvalDetail> {
  const { data } = await client.get<FactorEvalDetail>(`/factors/evaluations/${evalId}`, {
    params: { market: market || undefined },
  });
  return data;
}

// ---- Label API ----

export async function listLabels(market?: Market): Promise<LabelDefinition[]> {
  const { data } = await client.get<LabelDefinition[]>("/labels", {
    params: { market: market || undefined },
  });
  return data;
}

// ---- Task API ----

export async function getTaskStatus(taskId: string): Promise<TaskStatus> {
  const { data } = await client.get<TaskStatus>(`/tasks/${taskId}`);
  return data;
}

export async function listTasks(params?: {
  task_type?: string;
  status?: string;
  source?: string;
  market?: string;
  limit?: number;
}): Promise<TaskStatus[]> {
  const { data } = await client.get<TaskStatus[]>("/tasks", { params });
  return data;
}

export async function cancelTask(taskId: string): Promise<{ task_id: string; status: string }> {
  const { data } = await client.post<{ task_id: string; status: string }>(`/tasks/${taskId}/cancel`, {});
  return data;
}

export async function bulkCancelTasks(params: {
  task_type?: string;
  status?: string;
  source?: string;
  market?: string;
}): Promise<{ status: string; cancelled_count: number; task_ids: string[] }> {
  const { data } = await client.post<{ status: string; cancelled_count: number; task_ids: string[] }>(
    "/tasks/bulk-cancel",
    params,
  );
  return data;
}

export interface TaskPauseRule {
  id: string;
  task_type: string | null;
  source: string | null;
  market: string | null;
  reason: string | null;
  active: boolean;
  created_at: string | null;
}

export async function listTaskPauseRules(activeOnly = true): Promise<TaskPauseRule[]> {
  const { data } = await client.get<TaskPauseRule[]>("/tasks/pause-rules", {
    params: { active_only: activeOnly },
  });
  return data;
}

export async function createTaskPauseRule(params: {
  task_type?: string;
  source?: string;
  market?: string;
  reason?: string;
}): Promise<TaskPauseRule> {
  const { data } = await client.post<TaskPauseRule>("/tasks/pause-rules", params);
  return data;
}

export async function deleteTaskPauseRule(ruleId: string): Promise<{ id: string; status: string }> {
  const { data } = await client.delete<{ id: string; status: string }>(`/tasks/pause-rules/${ruleId}`);
  return data;
}

// ---- Macro Data API ----

export async function updateFredSeries(params: {
  series_ids: string[];
  start_date?: string;
  end_date?: string;
}): Promise<MacroUpdateTask> {
  const { data } = await client.post<MacroUpdateTask>("/macro-data/fred/update", params);
  return data;
}

export async function listMacroSeries(provider = "fred", limit = 1000): Promise<MacroSeries[]> {
  const { data } = await client.get<{ provider: string; series: MacroSeries[] }>("/macro-data/series", {
    params: { provider, limit },
  });
  return data.series;
}

export async function queryMacroObservations(params: {
  series_ids: string[];
  start_date?: string;
  end_date?: string;
  as_of?: string;
  limit?: number;
}): Promise<MacroObservationsResponse> {
  const { data } = await client.get<MacroObservationsResponse>("/macro-data/observations", {
    params: {
      series_ids: params.series_ids.join(","),
      start_date: params.start_date,
      end_date: params.end_date,
      as_of: params.as_of,
      limit: params.limit,
    },
  });
  return data;
}

// ---- Feature Set Types ----

export interface FeatureSet {
  id: string;
  market: Market;
  name: string;
  description: string | null;
  factor_refs: Array<{ factor_id: string; factor_name: string; version: number }>;
  preprocessing: Record<string, string> | null;
  status: string;
  created_at: string;
}

export interface CorrelationMatrix {
  factor_names: string[];
  matrix: number[][];
}

// ---- Feature Set API ----

export async function listFeatureSets(market?: string): Promise<FeatureSet[]> {
  const { data } = await client.get<FeatureSet[]>("/feature-sets", {
    params: { market: market || undefined },
  });
  return data;
}

export async function getFeatureSet(fsId: string, market?: string): Promise<FeatureSet> {
  const { data } = await client.get<FeatureSet>(`/feature-sets/${fsId}`, {
    params: { market: market || undefined },
  });
  return data;
}

export async function createFeatureSet(params: {
  market?: string;
  name: string;
  description?: string;
  factor_refs: Array<{ factor_id: string; factor_name: string; version: number }>;
  preprocessing?: Record<string, string>;
}): Promise<FeatureSet> {
  const { data } = await client.post<FeatureSet>("/feature-sets", params);
  return data;
}

export async function deleteFeatureSet(fsId: string, market?: string) {
  const { data } = await client.delete(`/feature-sets/${fsId}`, {
    params: { market: market || undefined },
  });
  return data;
}

export async function getCorrelationMatrix(
  fsId: string,
  body: { market?: string; universe_group_id: string; start_date: string; end_date: string },
): Promise<CorrelationMatrix> {
  const { data } = await client.post<CorrelationMatrix>(`/feature-sets/${fsId}/correlation`, body);
  return data;
}

// ---- Model Types ----

export interface Model {
  id: string;
  market: Market;
  name: string;
  feature_set_id: string;
  label_id: string;
  model_type: string;
  model_params: Record<string, unknown> | null;
  train_config: Record<string, unknown> | null;
  eval_metrics: Record<string, unknown> | null;
  status: string;
  created_at: string;
}

// ---- Model API ----

export async function trainModel(params: {
  market?: string;
  name: string;
  feature_set_id: string;
  label_id: string;
  model_type?: string;
  model_params?: Record<string, unknown>;
  train_config?: Record<string, unknown>;
  universe_group_id: string;
  objective_type?: "regression" | "classification" | "ranking" | "pairwise" | "listwise";
  ranking_config?: Record<string, unknown>;
}): Promise<{ task_id: string; market?: string }> {
  const { data } = await client.post<{ task_id: string; market?: string }>("/models/train", params);
  return data;
}

export async function listModels(
  market?: string,
  options: { limit?: number; offset?: number } = {},
): Promise<Model[]> {
  const { data } = await client.get<Model[]>("/models", {
    params: {
      market: market || undefined,
      limit: options.limit,
      offset: options.offset,
    },
  });
  return data;
}

export async function getModel(modelId: string, market?: string): Promise<Model> {
  const { data } = await client.get<Model>(`/models/${modelId}`, {
    params: { market: market || undefined },
  });
  return data;
}

export async function deleteModel(modelId: string, market?: string) {
  const { data } = await client.delete(`/models/${modelId}`, {
    params: { market: market || undefined },
  });
  return data;
}

// ---- Strategy Types ----

export interface StrategySummary {
  id: string;
  market: Market;
  name: string;
  version: number;
  description: string | null;
  required_factors: string[] | null;
  required_models: string[] | null;
  position_sizing: string;
  constraint_config?: Record<string, unknown> | null;
  default_backtest_config?: BacktestRunConfig | null;
  default_paper_config?: Record<string, unknown> | null;
  status: string;
  created_at: string;
}

export interface Strategy extends StrategySummary {
  source_code: string;
}

export interface StrategyTemplate {
  name: string;
  source_code?: string;
}

export interface BacktestResultSummary {
  id: string;
  market: Market;
  strategy_id: string;
  config: BacktestRunConfig | null;
  summary: BacktestSummary | null;
  result_level: string | null;
  trade_count: number | null;
  created_at: string;
}

export interface BacktestResultDetail extends BacktestResultSummary {
  nav_series: Array<{ date: string; value: number }> | null;
  benchmark_nav: Array<{ date: string; value: number }> | null;
  drawdown_series: Array<{ date: string; value: number }> | null;
  monthly_returns: Record<string, Record<string, number>> | null;
  trades: TradeRecord[] | null;
  stock_pnl: StockPnL[] | null;
  rebalance_diagnostics?: RebalanceDiagnostic[] | null;
  rebalance_diagnostics_count?: number;
}

export interface BacktestDebugReplay {
  backtest_id: string;
  market: Market;
  manifest: Record<string, unknown>;
  items: Array<Record<string, unknown>>;
}

export interface RebalanceDiagnostic {
  date?: string;
  market_state?: string;
  lane_counts?: Record<string, number>;
  blocked_buy_limit_up?: number | string[] | null;
  kept_due_limit_down?: number | string[] | null;
  [key: string]: unknown;
}

export interface BacktestRunConfig {
  start_date?: string;
  end_date?: string;
  warmup_start_date?: string | null;
  evaluation_start_date?: string | null;
  initial_entry_policy?: string;
  initial_capital?: number;
  commission_rate?: number;
  slippage_rate?: number;
  max_positions?: number;
  benchmark?: string;
  rebalance_freq?: string;
  rebalance_frequency?: string;
  rebalance_buffer?: number;
  min_holding_days?: number;
  reentry_cooldown_days?: number;
  normalize_target_weights?: boolean;
  execution_model?: ExecutionModel;
  planned_price_buffer_bps?: number;
  planned_price_fallback?: PlannedPriceFallback;
  universe_group_id?: string;
  constraint_config?: Record<string, unknown>;
  [key: string]: unknown;
}

export interface BacktestSummary {
  annual_return?: number;
  sharpe?: number;
  sharpe_ratio?: number;
  max_drawdown?: number;
  constraint_pass?: boolean;
  portfolio_compliance?: Record<string, unknown>;
  planned_price_execution?: Record<string, unknown>;
  planned_price_inputs?: Record<string, unknown>;
  fill_diagnostics?: Record<string, unknown>;
  [key: string]: unknown;
}

export interface TradeRecord {
  date: string;
  ticker: string;
  action: "buy" | "sell";
  shares: number;
  price: number;
  cost: number;
}

export interface StockPnL {
  ticker: string;
  buy_count: number;
  sell_count: number;
  total_buy_value: number;
  total_sell_value: number;
  realized_pnl: number;
  pnl_pct: number;
  win_count: number;
  loss_count: number;
}

export interface StockChartData {
  market?: string;
  ticker: string;
  daily_bars: Array<{ date: string; open: number; high: number; low: number; close: number; volume: number }>;
  trades: Array<{ date: string; action: string; shares: number; price: number; cost: number }>;
}

// ---- Strategy API ----

export async function createStrategy(params: {
  market?: string;
  name: string;
  source_code: string;
  description?: string;
  position_sizing?: string;
  constraint_config?: Record<string, unknown>;
}): Promise<Strategy> {
  const { data } = await client.post<Strategy>("/strategies", params);
  return data;
}

export async function listStrategies(
  market?: string,
  params?: { limit?: number; offset?: number },
): Promise<StrategySummary[]> {
  const { data } = await client.get<StrategySummary[]>("/strategies", {
    params: {
      market: market || undefined,
      limit: params?.limit,
      offset: params?.offset,
    },
  });
  return data;
}

export async function getStrategy(strategyId: string, market?: string): Promise<Strategy> {
  const { data } = await client.get<Strategy>(`/strategies/${strategyId}`, {
    params: { market: market || undefined },
  });
  return data;
}

export async function updateStrategy(
  strategyId: string,
  params: {
    market?: string;
    source_code?: string;
    description?: string;
    position_sizing?: string;
    constraint_config?: Record<string, unknown>;
    status?: string;
  },
): Promise<Strategy> {
  const { data } = await client.put<Strategy>(`/strategies/${strategyId}`, params);
  return data;
}

export async function deleteStrategy(strategyId: string, market?: string) {
  const { data } = await client.delete(`/strategies/${strategyId}`, {
    params: { market: market || undefined },
  });
  return data;
}

export async function listStrategyTemplates(): Promise<StrategyTemplate[]> {
  const { data } = await client.get<StrategyTemplate[]>("/strategies/templates");
  return data;
}

export async function getStrategyTemplate(name: string): Promise<StrategyTemplate> {
  const { data } = await client.get<StrategyTemplate>(`/strategies/templates/${name}`);
  return data;
}

export async function runBacktest(
  strategyId: string,
  body: { market?: string; config: BacktestRunConfig; universe_group_id: string },
): Promise<{ task_id: string; market?: string }> {
  const { data } = await client.post<{ task_id: string; market?: string }>(`/strategies/${strategyId}/backtest`, body);
  return data;
}

export async function listBacktests(
  strategyId?: string,
  market?: string,
  params?: { limit?: number; offset?: number },
): Promise<BacktestResultSummary[]> {
  const { data } = await client.get<BacktestResultSummary[]>("/strategies/backtests", {
    params: {
      strategy_id: strategyId || undefined,
      market: market || undefined,
      limit: params?.limit,
      offset: params?.offset,
    },
  });
  return data;
}

export async function getBacktest(backtestId: string, market?: string): Promise<BacktestResultDetail> {
  const { data } = await client.get<BacktestResultDetail>(`/strategies/backtests/${backtestId}`, {
    params: { market: market || undefined },
  });
  return data;
}

export async function getBacktestDebugReplay(
  backtestId: string,
  market?: string,
  params?: { date?: string; ticker?: string },
): Promise<BacktestDebugReplay> {
  const { data } = await client.get<BacktestDebugReplay>(`/strategies/backtests/${backtestId}/debug-replay`, {
    params: {
      market: market || undefined,
      date: params?.date || undefined,
      ticker: params?.ticker || undefined,
    },
  });
  return data;
}

export async function cleanupBacktestDebugReplay(ttlHours = 24) {
  const { data } = await client.delete("/strategies/backtests/debug-replay/expired", {
    params: { ttl_hours: ttlHours },
  });
  return data;
}

export async function deleteBacktest(backtestId: string, market?: string) {
  const { data } = await client.delete(`/strategies/backtests/${backtestId}`, {
    params: { market: market || undefined },
  });
  return data;
}

export async function getBacktestStockChart(backtestId: string, ticker: string, market?: string): Promise<StockChartData> {
  const { data } = await client.get<StockChartData>(`/strategies/backtests/${backtestId}/stock/${ticker}`, {
    params: { market: market || undefined },
  });
  return data;
}

// ---- Signal Types ----

export interface SignalDetail {
  market?: string;
  ticker: string;
  signal: number;  // 1=buy, -1=sell, 0=hold
  target_weight: number;
  strength: number;
}

export interface SignalRun {
  id: string;
  market: Market;
  strategy_id: string;
  strategy_version: number;
  target_date: string;
  universe_group_id: string;
  result_level: string;  // exploratory / formal
  dependency_snapshot: Record<string, unknown> | null;
  signal_count: number;
  created_at: string;
  signals?: SignalDetail[];
  warnings?: string[];
}

// ---- Signal API ----

export async function generateSignals(body: {
  market?: string;
  strategy_id: string;
  target_date: string;
  universe_group_id: string;
  constraint_config?: Record<string, unknown>;
}): Promise<{ task_id: string; market?: string }> {
  const { data } = await client.post<{ task_id: string; market?: string }>("/signals/generate", body);
  return data;
}

export async function listSignalRuns(strategyId?: string, market?: string): Promise<SignalRun[]> {
  const { data } = await client.get<SignalRun[]>("/signals", {
    params: { strategy_id: strategyId || undefined, market: market || undefined },
  });
  return data;
}

export async function getSignalRun(runId: string, market?: string): Promise<SignalRun> {
  const { data } = await client.get<SignalRun>(`/signals/${runId}`, {
    params: { market: market || undefined },
  });
  return data;
}

export async function exportSignals(runId: string, format: "csv" | "json", market?: string): Promise<void> {
  const response = await client.get(`/signals/${runId}/export`, {
    params: { format, market: market || undefined },
    responseType: "blob",
  });
  const blob = new Blob([response.data as BlobPart]);
  const disposition = response.headers["content-disposition"] as string | undefined;
  let filename = `signals_${runId}.${format}`;
  if (disposition) {
    const match = disposition.match(/filename="?([^"]+)"?/);
    if (match) filename = match[1];
  }
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// ---- System Info Types ----

export interface SystemInfo {
  version: string;
  python_version: string;
  db_path: string;
  data_dir: string;
  models_dir: string;
  factors_dir: string;
  strategies_dir: string;
  data_provider: string;
  server_host: string;
  server_port: number;
  market_calendar: string;
  config: Record<string, unknown>;
}

// ---- System API ----

export async function getSystemInfo(): Promise<SystemInfo> {
  const { data } = await client.get<SystemInfo>("/system/info");
  return data;
}

// ---- QAgent 3.0 Research Kernel Types ----

export interface ResearchProject3 {
  id: string;
  name: string;
  market_profile_id: string;
  default_universe_id?: string | null;
  data_policy_id?: string | null;
  trading_rule_set_id?: string | null;
  cost_model_id?: string | null;
  benchmark_policy_id?: string | null;
  artifact_policy_id?: string | null;
  metadata: Record<string, unknown>;
  created_at: string | null;
  updated_at: string | null;
}

export interface ResearchRun3 {
  id: string;
  project_id: string;
  market_profile_id: string;
  run_type: string;
  status: string;
  lifecycle_stage: string;
  retention_class: string;
  params: Record<string, unknown>;
  input_refs: Array<Record<string, unknown>>;
  output_refs: Array<Record<string, unknown>>;
  metrics_summary: Record<string, unknown>;
  qa_summary: Record<string, unknown>;
  warnings: Array<Record<string, unknown>>;
  error_message: string | null;
  created_by: string;
  created_at: string | null;
  started_at: string | null;
  completed_at: string | null;
  updated_at: string | null;
}

export interface ResearchArtifact3 {
  id: string;
  run_id: string;
  project_id: string;
  artifact_type: string;
  uri: string;
  format: string;
  schema_version: string;
  byte_size: number;
  content_hash: string;
  lifecycle_stage: string;
  retention_class: string;
  cleanup_after: string | null;
  rebuildable: boolean;
  metadata: Record<string, unknown>;
  created_at: string | null;
}

export interface ResearchLineage3 {
  run_id: string;
  edges: LineageEdge3[];
}

export interface CleanupPreview3 {
  mode: string;
  filters: Record<string, unknown>;
  summary: {
    matched_count: number;
    candidate_count: number;
    protected_count: number;
    candidate_bytes: number;
    protected_bytes: number;
  };
  candidates: ResearchArtifact3[];
  protected: Array<{ artifact: ResearchArtifact3; reasons: string[] }>;
  warnings: string[];
}

export interface LineageEdge3 {
  id: string;
  from_type: string;
  from_id: string;
  to_type: string;
  to_id: string;
  relation: string;
  metadata: Record<string, unknown>;
  created_at: string | null;
}

export interface MarketProfile3 {
  id: string;
  market_code: string;
  asset_class: string;
  name: string;
  currency: string;
  timezone: string;
  status: string;
  data_policy?: Record<string, unknown>;
  trading_rule_set?: Record<string, unknown>;
  cost_model?: Record<string, unknown>;
  benchmark_policy?: Record<string, unknown>;
  metadata: Record<string, unknown>;
}

export interface ProjectDataStatus3 {
  project_id: string;
  market_profile_id: string;
  market: Market;
  provider: string;
  latest_trading_day: string;
  coverage: {
    asset_count: number;
    active_asset_count: number;
    tickers_with_bars: number;
    total_bars: number;
    date_range: { min: string | null; max: string | null };
    stale_assets: number;
  };
  last_update: {
    completed_at: string | null;
    status: string | null;
    type: string | null;
  };
  latest_snapshot: Record<string, unknown> | null;
  semantics: Record<string, unknown>;
}

export interface AgentPlaybook3 {
  id: string;
  name: string;
  category: string;
  description: string;
  steps: Array<Record<string, unknown>>;
  optimization_targets: Array<Record<string, unknown>>;
  required_assets: Array<Record<string, unknown>>;
  status: string;
  metadata: Record<string, unknown>;
  created_at: string | null;
  updated_at: string | null;
}

export interface AgentResearchPlan3 {
  id: string;
  project_id: string;
  market_profile_id: string;
  hypothesis: string;
  playbook_id: string | null;
  search_space: Record<string, unknown>;
  budget: Record<string, unknown>;
  stop_conditions: Record<string, unknown>;
  status: string;
  created_by: string;
  metadata: Record<string, unknown>;
  budget_state?: Record<string, unknown>;
  created_at: string | null;
  updated_at: string | null;
}

export interface QaReport3 {
  id: string;
  project_id: string;
  market_profile_id: string;
  source_type: string;
  source_id: string;
  status: string;
  blocking: boolean;
  findings: Array<Record<string, unknown>>;
  metrics: Record<string, unknown>;
  artifact_refs: Array<Record<string, unknown>>;
  created_at: string | null;
}

export interface PromotionRecord3 {
  id: string;
  project_id: string;
  source_type: string;
  source_id: string;
  target_type: string;
  target_id: string;
  decision: string;
  policy_snapshot: Record<string, unknown>;
  qa_summary: Record<string, unknown>;
  approved_by: string | null;
  rationale: string | null;
  created_at: string | null;
}

export interface Universe3 {
  id: string;
  project_id: string;
  market_profile_id: string;
  name: string;
  description: string | null;
  universe_type: string;
  source_ref: Record<string, unknown>;
  filter_expr: string | null;
  lifecycle_stage: string;
  status: string;
  metadata: Record<string, unknown>;
  created_at: string | null;
  updated_at: string | null;
}

export interface Dataset3 {
  id: string;
  project_id: string;
  market_profile_id: string;
  name: string;
  description: string | null;
  universe_id: string;
  feature_pipeline_id: string;
  label_spec_id: string;
  legacy_label_id: string | null;
  start_date: string;
  end_date: string;
  split_policy: Record<string, unknown>;
  lifecycle_stage: string;
  retention_class: string;
  status: string;
  materialized_run_id: string | null;
  dataset_artifact_id: string | null;
  profile_artifact_id: string | null;
  row_count: number;
  feature_count: number;
  label_count: number;
  qa_summary: Record<string, unknown>;
  metadata: Record<string, unknown>;
  created_at: string | null;
  updated_at: string | null;
}

export interface FactorSpec3 {
  id: string;
  project_id: string;
  market_profile_id: string;
  name: string;
  description: string | null;
  version: number;
  source_type: string;
  compute_mode: string;
  lifecycle_stage: string;
  status: string;
  semantic_tags: string[];
  created_at: string | null;
  updated_at: string | null;
}

export interface FactorRun3 {
  id: string;
  run_id: string;
  project_id: string;
  market_profile_id: string;
  factor_spec_id: string;
  factor_spec_version: number;
  universe_id: string;
  start_date: string | null;
  end_date: string | null;
  mode: string;
  status: string;
  params: Record<string, unknown>;
  data_snapshot_id: string | null;
  data_policy: Record<string, unknown>;
  output_artifact_id: string | null;
  profile: Record<string, unknown>;
  qa_summary: Record<string, unknown>;
  created_at: string | null;
  completed_at: string | null;
}

export interface PortfolioRun3 {
  id: string;
  run_id: string;
  project_id: string;
  market_profile_id: string;
  decision_date: string | null;
  portfolio_construction_spec_id: string;
  risk_control_spec_id: string | null;
  rebalance_policy_spec_id: string | null;
  execution_policy_spec_id: string;
  state_policy_spec_id: string;
  input_artifact_id: string;
  target_artifact_id: string;
  trace_artifact_id: string;
  order_intent_artifact_id: string;
  profile: Record<string, unknown>;
  status: string;
  lifecycle_stage: string;
  created_at: string | null;
  completed_at: string | null;
}

export interface ModelExperiment3 {
  id: string;
  run_id: string;
  project_id: string;
  market_profile_id: string;
  dataset_id: string;
  name: string;
  model_type: string;
  objective: string;
  metrics: Record<string, unknown>;
  qa_summary: Record<string, unknown>;
  status: string;
  lifecycle_stage: string;
  created_at: string | null;
  completed_at: string | null;
}

export interface StrategyGraph3 {
  id: string;
  project_id: string;
  market_profile_id: string;
  name: string;
  description: string | null;
  graph_type: string;
  version: number;
  graph_config: Record<string, unknown>;
  dependency_refs: Array<Record<string, unknown>>;
  lifecycle_stage: string;
  status: string;
  metadata: Record<string, unknown>;
  created_at: string | null;
  updated_at: string | null;
}

export interface ProviderCapability3 {
  provider: string;
  dataset: string;
  market_profile_id: string;
  capability: string;
  quality_level: string;
  pit_supported: boolean;
  license_scope: string;
  availability: string;
  as_of_date: string | null;
  available_at: string | null;
  metadata: Record<string, unknown>;
  created_at: string | null;
  updated_at: string | null;
}

export interface PublicationGate3 {
  gate: string;
  status: string;
  reason: string;
}

export interface DataQualityContract3 {
  market_profile_id: string | null;
  capabilities: ProviderCapability3[];
  summary: Record<string, unknown>;
  policy: Record<string, unknown>;
  publication_gates: PublicationGate3[];
}

export interface BacktestRun3 {
  id: string;
  run_id: string;
  project_id: string;
  market_profile_id: string;
  strategy_graph_id: string;
  start_date: string | null;
  end_date: string | null;
  config: Record<string, unknown>;
  summary: Record<string, unknown>;
  status: string;
  lifecycle_stage: string;
  created_at: string | null;
  completed_at: string | null;
}

export interface ProductionSignalRun3 {
  id: string;
  run_id: string;
  project_id: string;
  market_profile_id: string;
  strategy_graph_id: string;
  strategy_signal_id: string;
  decision_date: string | null;
  portfolio_run_id: string;
  target_artifact_id: string;
  order_intent_artifact_id: string;
  qa_report_id: string | null;
  status: string;
  lifecycle_stage: string;
  approved_by: string;
  profile: Record<string, unknown>;
  created_at: string | null;
  completed_at: string | null;
}

export interface PaperSession3 {
  id: string;
  project_id: string;
  market_profile_id: string;
  strategy_graph_id: string;
  name: string;
  status: string;
  start_date: string | null;
  current_date: string | null;
  initial_capital: number;
  current_nav: number;
  current_weights: Record<string, number>;
  config: Record<string, unknown>;
  created_at: string | null;
  updated_at: string | null;
}

// ---- QAgent 3.0 Research Kernel API ----

export async function getBootstrapProject3(): Promise<ResearchProject3> {
  const { data } = await client.get<ResearchProject3>("/research/projects/bootstrap");
  return data;
}

export async function listResearchRuns3(params?: {
  project_id?: string;
  run_type?: string;
  status?: string;
  lifecycle_stage?: string;
  created_by?: string;
  limit?: number;
}): Promise<ResearchRun3[]> {
  const { data } = await client.get<ResearchRun3[]>("/research/runs", { params });
  return data;
}

export async function listResearchArtifacts3(params?: {
  project_id?: string;
  run_id?: string;
  artifact_type?: string;
  lifecycle_stage?: string;
  retention_class?: string;
  limit?: number;
}): Promise<ResearchArtifact3[]> {
  const { data } = await client.get<ResearchArtifact3[]>("/research/artifacts", { params });
  return data;
}

export async function getResearchLineage3(runId: string): Promise<ResearchLineage3> {
  const { data } = await client.get<ResearchLineage3>(`/research/lineage/${runId}`);
  return data;
}

export async function getResearchRun3(runId: string): Promise<ResearchRun3> {
  const { data } = await client.get<ResearchRun3>(`/research/runs/${runId}`);
  return data;
}

export async function getResearchArtifact3(artifactId: string): Promise<ResearchArtifact3> {
  const { data } = await client.get<ResearchArtifact3>(`/research/artifacts/${artifactId}`);
  return data;
}

export async function previewArtifactCleanup3(body: {
  project_id?: string;
  run_id?: string;
  artifact_ids?: string[];
  lifecycle_stage?: string;
  retention_class?: string;
  artifact_type?: string;
  include_published?: boolean;
  limit?: number;
}): Promise<CleanupPreview3> {
  const { data } = await client.post<CleanupPreview3>("/research/artifacts/cleanup-preview", body);
  return data;
}

export async function archiveResearchArtifact3(
  artifactId: string,
  body?: { retention_class?: string; archive_reason?: string },
): Promise<ResearchArtifact3> {
  const { data } = await client.post<ResearchArtifact3>(`/research/artifacts/${artifactId}/archive`, body || {});
  return data;
}

export async function listPromotionRecords3(params?: {
  project_id?: string;
  source_type?: string;
  source_id?: string;
  target_type?: string;
  target_id?: string;
  decision?: string;
  limit?: number;
}): Promise<PromotionRecord3[]> {
  const { data } = await client.get<PromotionRecord3[]>("/research/promotions", { params });
  return data;
}

export async function getPromotionRecord3(promotionId: string): Promise<PromotionRecord3> {
  const { data } = await client.get<PromotionRecord3>(`/research/promotions/${promotionId}`);
  return data;
}

export async function getProjectDataStatus3(projectId: string): Promise<ProjectDataStatus3> {
  const { data } = await client.get<ProjectDataStatus3>(`/market-data/projects/${projectId}/status`);
  return data;
}

export async function listProviderCapabilities3(params?: {
  provider?: string;
  market_profile_id?: string;
  dataset?: string;
}): Promise<ProviderCapability3[]> {
  const { data } = await client.get<ProviderCapability3[]>("/market-data/provider-capabilities", { params });
  return data;
}

export async function getDataQualityContract3(params?: {
  market_profile_id?: string;
}): Promise<DataQualityContract3> {
  const { data } = await client.get<DataQualityContract3>("/market-data/quality-contract", { params });
  return data;
}

export async function listAgentPlaybooks3(): Promise<AgentPlaybook3[]> {
  const { data } = await client.get<AgentPlaybook3[]>("/research/agent/playbooks");
  return data;
}

export async function listAgentResearchPlans3(params?: {
  project_id?: string;
  status?: string;
  limit?: number;
}): Promise<AgentResearchPlan3[]> {
  const { data } = await client.get<AgentResearchPlan3[]>("/research/agent/plans", { params });
  return data;
}

export async function listQaReports3(params?: {
  source_type?: string;
  source_id?: string;
  status?: string;
  limit?: number;
}): Promise<QaReport3[]> {
  const { data } = await client.get<QaReport3[]>("/research/agent/qa", { params });
  return data;
}

export async function getQaReport3(qaReportId: string): Promise<QaReport3> {
  const { data } = await client.get<QaReport3>(`/research/agent/qa/${qaReportId}`);
  return data;
}

export async function evaluatePromotion3(body: {
  source_type: string;
  source_id: string;
  qa_report_id: string;
  metrics?: Record<string, unknown>;
  policy_id?: string;
  approved_by?: string;
  rationale?: string;
}): Promise<PromotionRecord3> {
  const { data } = await client.post<PromotionRecord3>("/research/agent/promotion", body);
  return data;
}

export async function listUniverses3(params?: {
  project_id?: string;
  market_profile_id?: string;
  status?: string;
  limit?: number;
}): Promise<Universe3[]> {
  const { data } = await client.get<Universe3[]>("/research-assets/universes", { params });
  return data;
}

export async function listDatasets3(params?: {
  project_id?: string;
  market_profile_id?: string;
  universe_id?: string;
  status?: string;
  limit?: number;
}): Promise<Dataset3[]> {
  const { data } = await client.get<Dataset3[]>("/research-assets/datasets", { params });
  return data;
}

export async function listFactorSpecs3(params?: {
  project_id?: string;
  market_profile_id?: string;
  status?: string;
}): Promise<FactorSpec3[]> {
  const { data } = await client.get<FactorSpec3[]>("/research-assets/factor-specs", { params });
  return data;
}

export async function getFactorSpec3(factorSpecId: string): Promise<FactorSpec3> {
  const { data } = await client.get<FactorSpec3>(`/research-assets/factor-specs/${factorSpecId}`);
  return data;
}

export async function listFactorRuns3(params?: {
  factor_spec_id?: string;
  universe_id?: string;
  mode?: string;
}): Promise<FactorRun3[]> {
  const { data } = await client.get<FactorRun3[]>("/research-assets/factor-runs", { params });
  return data;
}

export async function getFactorRun3(factorRunId: string): Promise<FactorRun3> {
  const { data } = await client.get<FactorRun3>(`/research-assets/factor-runs/${factorRunId}`);
  return data;
}

export async function listModelExperiments3(params?: {
  dataset_id?: string;
}): Promise<ModelExperiment3[]> {
  const { data } = await client.get<ModelExperiment3[]>("/research-assets/model-experiments", { params });
  return data;
}

export async function getModelExperiment3(experimentId: string): Promise<ModelExperiment3> {
  const { data } = await client.get<ModelExperiment3>(`/research-assets/model-experiments/${experimentId}`);
  return data;
}

export async function listStrategyGraphs3(params?: {
  project_id?: string;
  status?: string;
}): Promise<StrategyGraph3[]> {
  const { data } = await client.get<StrategyGraph3[]>("/research-assets/strategy-graphs", { params });
  return data;
}

export async function getStrategyGraph3(strategyGraphId: string): Promise<StrategyGraph3> {
  const { data } = await client.get<StrategyGraph3>(`/research-assets/strategy-graphs/${strategyGraphId}`);
  return data;
}

export async function backtestStrategyGraph3(
  strategyGraphId: string,
  body: {
    start_date: string;
    end_date: string;
    alpha_frames_by_date?: Record<string, Array<Record<string, unknown>>>;
    legacy_signal_frames_by_date?: Record<string, Array<Record<string, unknown>>>;
    initial_capital?: number;
    lifecycle_stage?: string;
    price_field?: string;
  },
): Promise<TaskResponse> {
  const { data } = await client.post<TaskResponse>(`/research-assets/strategy-graphs/${strategyGraphId}/backtest`, body);
  return data;
}

export async function listStrategyGraphBacktests3(
  strategyGraphId: string,
  params?: { limit?: number },
): Promise<BacktestRun3[]> {
  const { data } = await client.get<BacktestRun3[]>(
    `/research-assets/strategy-graphs/${strategyGraphId}/backtests`,
    { params },
  );
  return data;
}

export async function getStrategyGraphBacktest3(backtestRunId: string): Promise<BacktestRun3> {
  const { data } = await client.get<BacktestRun3>(`/research-assets/backtests/${backtestRunId}`);
  return data;
}

export async function listProductionSignalRuns3(params?: {
  strategy_graph_id?: string;
  limit?: number;
}): Promise<ProductionSignalRun3[]> {
  const { data } = await client.get<ProductionSignalRun3[]>("/research-assets/production-signals", { params });
  return data;
}

export async function getProductionSignalRun3(signalRunId: string): Promise<ProductionSignalRun3> {
  const { data } = await client.get<ProductionSignalRun3>(`/research-assets/production-signals/${signalRunId}`);
  return data;
}

export async function listPaperSessions3(params?: {
  status?: string;
  limit?: number;
}): Promise<PaperSession3[]> {
  const { data } = await client.get<PaperSession3[]>("/research-assets/paper-sessions", { params });
  return data;
}

export async function getPaperSession3(sessionId: string): Promise<PaperSession3> {
  const { data } = await client.get<PaperSession3>(`/research-assets/paper-sessions/${sessionId}`);
  return data;
}

export async function listPortfolioRuns3(params?: {
  limit?: number;
}): Promise<PortfolioRun3[]> {
  const { data } = await client.get<PortfolioRun3[]>("/research-assets/portfolio-runs", { params });
  return data;
}

export async function getPortfolioRun3(portfolioRunId: string): Promise<PortfolioRun3> {
  const { data } = await client.get<PortfolioRun3>(`/research-assets/portfolio-runs/${portfolioRunId}`);
  return data;
}

export async function getUniverse3(universeId: string): Promise<Universe3> {
  const { data } = await client.get<Universe3>(`/research-assets/universes/${universeId}`);
  return data;
}

export async function getDataset3(datasetId: string): Promise<Dataset3> {
  const { data } = await client.get<Dataset3>(`/research-assets/datasets/${datasetId}`);
  return data;
}

// ---- Paper Trading Types ----

export interface PaperTradingSession {
  id: string;
  market: Market;
  name: string;
  strategy_id: string;
  universe_group_id: string;
  config: Record<string, unknown> | null;
  status: string;
  start_date: string | null;
  current_date: string | null;
  initial_capital: number;
  current_nav: number | null;
  total_trades: number;
  created_at: string | null;
  updated_at: string | null;
  strategy_name: string | null;
}

export interface PaperDailyRecord {
  date: string;
  nav: number;
  cash: number;
  position_count?: number;
  trade_count?: number;
}

export interface PaperPosition {
  ticker: string;
  shares: number;
  avg_price: number;
  latest_price?: number | null;
  market_value?: number | null;
  unrealized_pnl?: number | null;
  weight?: number | null;
  date: string;
}

export interface PaperTrade {
  date: string;
  ticker: string;
  action: string;
  shares: number;
  price: number;
  cost: number;
  trade_reason?: string;
  position_state?: string;
  holding_days?: number;
}

export interface PaperAdvanceResult {
  task_id: string;
  status: string;
  market?: string;
}

export interface PaperBacktestComparisonDay {
  date: string;
  paper_nav: number | null;
  paper_cash: number | null;
  paper_position_count: number;
  paper_positions: string[];
  paper_trades: PaperTrade[];
  backtest_nav?: number | null;
  backtest_signal_date?: string | null;
  backtest_rebalance?: Record<string, unknown> | null;
  backtest_target_positions?: string[];
  backtest_trades: PaperTrade[];
  paper_trade_count: number;
  backtest_trade_count: number;
  missing_in_paper: string[];
  extra_in_paper: string[];
}

export interface PaperBacktestComparison {
  session_id: string;
  market?: string;
  backtest_id: string;
  summary: {
    paper_total_trades: number;
    backtest_total_trades: number;
    trade_delta: number;
    dates_compared: number;
    dates_with_trade_differences: number;
    paper_final_nav?: number | null;
    backtest_final_nav?: number | null;
    final_nav_delta?: number | null;
  };
  daily: PaperBacktestComparisonDay[];
}

// ---- Paper Trading API ----

export async function listPaperSessions(market?: string): Promise<PaperTradingSession[]> {
  const { data } = await client.get<PaperTradingSession[]>("/paper-trading/sessions", {
    params: { market: market || undefined },
  });
  return data;
}

export async function createPaperSession(body: {
  market?: string;
  strategy_id: string;
  universe_group_id: string;
  start_date: string;
  name?: string;
  config?: Record<string, unknown>;
}): Promise<PaperTradingSession> {
  const { data } = await client.post<PaperTradingSession>("/paper-trading/sessions", body);
  return data;
}

export async function deletePaperSession(sessionId: string, market?: string) {
  const { data } = await client.delete(`/paper-trading/sessions/${sessionId}`, {
    params: { market: market || undefined },
  });
  return data;
}

export async function pausePaperSession(sessionId: string, market?: string): Promise<PaperTradingSession> {
  const { data } = await client.post<PaperTradingSession>(
    `/paper-trading/sessions/${sessionId}/pause`,
    undefined,
    { params: { market: market || undefined } },
  );
  return data;
}

export async function resumePaperSession(sessionId: string, market?: string): Promise<PaperTradingSession> {
  const { data } = await client.post<PaperTradingSession>(
    `/paper-trading/sessions/${sessionId}/resume`,
    undefined,
    { params: { market: market || undefined } },
  );
  return data;
}

export async function advancePaperSession(
  sessionId: string,
  targetDate?: string,
  steps?: number,
  market?: string,
): Promise<PaperAdvanceResult> {
  const body: Record<string, unknown> = {};
  if (targetDate) body.target_date = targetDate;
  if (steps && steps > 0) body.steps = steps;
  if (market) body.market = market;
  const { data } = await client.post(
    `/paper-trading/sessions/${sessionId}/advance`,
    body,
  );
  return data;
}

export async function getPaperDailySeries(sessionId: string, market?: string): Promise<PaperDailyRecord[]> {
  const { data } = await client.get<PaperDailyRecord[]>(`/paper-trading/sessions/${sessionId}/daily`, {
    params: { market: market || undefined },
  });
  return data;
}

export async function getPaperPositions(sessionId: string, date?: string, market?: string): Promise<PaperPosition[]> {
  const { data } = await client.get<PaperPosition[]>(`/paper-trading/sessions/${sessionId}/positions`, {
    params: { date, market: market || undefined },
  });
  return data;
}

export async function comparePaperWithBacktest(
  sessionId: string,
  backtestId: string,
  market?: string,
): Promise<PaperBacktestComparison> {
  const { data } = await client.get<PaperBacktestComparison>(
    `/paper-trading/sessions/${sessionId}/compare-backtest/${backtestId}`,
    { params: { market: market || undefined } },
  );
  return data;
}

export async function getPaperTrades(sessionId: string, limit = 200, market?: string): Promise<PaperTrade[]> {
  const { data } = await client.get<PaperTrade[]>(`/paper-trading/sessions/${sessionId}/trades`, {
    params: { limit, market: market || undefined },
  });
  return data;
}

export async function getPaperSummary(sessionId: string, market?: string): Promise<PaperTradingSession & {
  total_return: number;
  max_drawdown: number;
  trading_days: number;
  latest_nav?: number;
}> {
  const { data } = await client.get(`/paper-trading/sessions/${sessionId}/summary`, {
    params: { market: market || undefined },
  });
  return data;
}

export interface PaperActionPlan {
  ticker: string;
  action: string;
  current_shares: number;
  target_weight: number;
}

export interface PaperSignalsResult {
  market?: string;
  signals: Array<{ ticker: string; signal: number; target_weight: number; strength: number }>;
  action_plan: PaperActionPlan[];
  target_date: string | null;
  error?: string;
}

export async function getPaperLatestSignals(sessionId: string, market?: string): Promise<PaperSignalsResult | { task_id: string; status: string; market?: string }> {
  const { data } = await client.get(`/paper-trading/sessions/${sessionId}/signals`, {
    params: { market: market || undefined },
  });
  return data;
}

export async function getPaperStockChart(sessionId: string, ticker: string, market?: string): Promise<StockChartData> {
  const { data } = await client.get<StockChartData>(`/paper-trading/sessions/${sessionId}/stock/${ticker}`, {
    params: { market: market || undefined },
  });
  return data;
}

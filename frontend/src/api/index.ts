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

export async function updateTickers(tickers: string[]): Promise<{ task_id: string; status: string; tickers: number }> {
  const { data } = await client.post("/data/update/tickers", { tickers });
  return data;
}

export async function updateGroupData(groupId: string): Promise<{ task_id: string; status: string; tickers: number }> {
  const { data } = await client.post("/data/update/group", { group_id: groupId });
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

export async function refreshIndexGroups() {
  const { data } = await client.post('/groups/refresh-indices');
  return data;
}

// ---- Factor Types ----

export interface Factor {
  id: string;
  market: string;
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
  market: string;
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
  market: string;
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

export async function listLabels(): Promise<LabelDefinition[]> {
  const { data } = await client.get<LabelDefinition[]>("/labels");
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
  limit?: number;
}): Promise<TaskStatus[]> {
  const { data } = await client.get<TaskStatus[]>("/tasks", { params });
  return data;
}

export async function cancelTask(taskId: string): Promise<{ task_id: string; status: string }> {
  const { data } = await client.post<{ task_id: string; status: string }>(`/tasks/${taskId}/cancel`, {});
  return data;
}

// ---- Feature Set Types ----

export interface FeatureSet {
  id: string;
  market: string;
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
  market: string;
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

export async function listModels(market?: string): Promise<Model[]> {
  const { data } = await client.get<Model[]>("/models", {
    params: { market: market || undefined },
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

export interface Strategy {
  id: string;
  market: string;
  name: string;
  version: number;
  description: string | null;
  source_code: string;
  required_factors: string[] | null;
  required_models: string[] | null;
  position_sizing: string;
  status: string;
  created_at: string;
}

export interface StrategyTemplate {
  name: string;
  source_code?: string;
}

export interface BacktestResultSummary {
  id: string;
  market: string;
  strategy_id: string;
  config: Record<string, unknown> | null;
  summary: Record<string, number> | null;
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
}): Promise<Strategy> {
  const { data } = await client.post<Strategy>("/strategies", params);
  return data;
}

export async function listStrategies(market?: string): Promise<Strategy[]> {
  const { data } = await client.get<Strategy[]>("/strategies", {
    params: { market: market || undefined },
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
  body: { market?: string; config: Record<string, unknown>; universe_group_id: string },
): Promise<{ task_id: string; market?: string }> {
  const { data } = await client.post<{ task_id: string; market?: string }>(`/strategies/${strategyId}/backtest`, body);
  return data;
}

export async function listBacktests(strategyId?: string, market?: string): Promise<BacktestResultSummary[]> {
  const { data } = await client.get<BacktestResultSummary[]>("/strategies/backtests", {
    params: { strategy_id: strategyId || undefined, market: market || undefined },
  });
  return data;
}

export async function getBacktest(backtestId: string, market?: string): Promise<BacktestResultDetail> {
  const { data } = await client.get<BacktestResultDetail>(`/strategies/backtests/${backtestId}`, {
    params: { market: market || undefined },
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
  ticker: string;
  signal: number;  // 1=buy, -1=sell, 0=hold
  target_weight: number;
  strength: number;
}

export interface SignalRun {
  id: string;
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
  strategy_id: string;
  target_date: string;
  universe_group_id: string;
}): Promise<{ task_id: string }> {
  const { data } = await client.post<{ task_id: string }>("/signals/generate", body);
  return data;
}

export async function listSignalRuns(strategyId?: string): Promise<SignalRun[]> {
  const { data } = await client.get<SignalRun[]>("/signals", {
    params: { strategy_id: strategyId || undefined },
  });
  return data;
}

export async function getSignalRun(runId: string): Promise<SignalRun> {
  const { data } = await client.get<SignalRun>(`/signals/${runId}`);
  return data;
}

export async function exportSignals(runId: string, format: "csv" | "json"): Promise<void> {
  const response = await client.get(`/signals/${runId}/export`, {
    params: { format },
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

// ---- Paper Trading Types ----

export interface PaperTradingSession {
  id: string;
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

export async function listPaperSessions(): Promise<PaperTradingSession[]> {
  const { data } = await client.get<PaperTradingSession[]>("/paper-trading/sessions");
  return data;
}

export async function createPaperSession(body: {
  strategy_id: string;
  universe_group_id: string;
  start_date: string;
  name?: string;
  config?: Record<string, unknown>;
}): Promise<PaperTradingSession> {
  const { data } = await client.post<PaperTradingSession>("/paper-trading/sessions", body);
  return data;
}

export async function deletePaperSession(sessionId: string) {
  const { data } = await client.delete(`/paper-trading/sessions/${sessionId}`);
  return data;
}

export async function pausePaperSession(sessionId: string): Promise<PaperTradingSession> {
  const { data } = await client.post<PaperTradingSession>(`/paper-trading/sessions/${sessionId}/pause`);
  return data;
}

export async function resumePaperSession(sessionId: string): Promise<PaperTradingSession> {
  const { data } = await client.post<PaperTradingSession>(`/paper-trading/sessions/${sessionId}/resume`);
  return data;
}

export async function advancePaperSession(
  sessionId: string,
  targetDate?: string,
  steps?: number,
): Promise<{ task_id: string; status: string }> {
  const body: Record<string, unknown> = {};
  if (targetDate) body.target_date = targetDate;
  if (steps && steps > 0) body.steps = steps;
  const { data } = await client.post(
    `/paper-trading/sessions/${sessionId}/advance`,
    body,
  );
  return data;
}

export async function getPaperDailySeries(sessionId: string): Promise<PaperDailyRecord[]> {
  const { data } = await client.get<PaperDailyRecord[]>(`/paper-trading/sessions/${sessionId}/daily`);
  return data;
}

export async function getPaperPositions(sessionId: string, date?: string): Promise<PaperPosition[]> {
  const { data } = await client.get<PaperPosition[]>(`/paper-trading/sessions/${sessionId}/positions`, {
    params: { date },
  });
  return data;
}

export async function comparePaperWithBacktest(
  sessionId: string,
  backtestId: string,
): Promise<PaperBacktestComparison> {
  const { data } = await client.get<PaperBacktestComparison>(
    `/paper-trading/sessions/${sessionId}/compare-backtest/${backtestId}`,
  );
  return data;
}

export async function getPaperTrades(sessionId: string, limit = 200): Promise<PaperTrade[]> {
  const { data } = await client.get<PaperTrade[]>(`/paper-trading/sessions/${sessionId}/trades`, {
    params: { limit },
  });
  return data;
}

export async function getPaperSummary(sessionId: string): Promise<PaperTradingSession & {
  total_return: number;
  max_drawdown: number;
  trading_days: number;
  latest_nav?: number;
}> {
  const { data } = await client.get(`/paper-trading/sessions/${sessionId}/summary`);
  return data;
}

export interface PaperActionPlan {
  ticker: string;
  action: string;
  current_shares: number;
  target_weight: number;
}

export interface PaperSignalsResult {
  signals: Array<{ ticker: string; signal: number; target_weight: number; strength: number }>;
  action_plan: PaperActionPlan[];
  target_date: string | null;
  error?: string;
}

export async function getPaperLatestSignals(sessionId: string): Promise<PaperSignalsResult | { task_id: string; status: string }> {
  const { data } = await client.get(`/paper-trading/sessions/${sessionId}/signals`);
  return data;
}

export async function getPaperStockChart(sessionId: string, ticker: string): Promise<StockChartData> {
  const { data } = await client.get<StockChartData>(`/paper-trading/sessions/${sessionId}/stock/${ticker}`);
  return data;
}

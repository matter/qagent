"""Paper trading service -- forward-testing strategies with real market data.

Provides session management and day-by-day simulation that reuses the
existing signal generation pipeline but executes trades incrementally,
building up an out-of-sample track record over time.
"""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timedelta

import pandas as pd

from backend.db import get_connection
from backend.logger import get_logger
from backend.services.calendar_service import get_latest_trading_day, get_trading_days, offset_trading_days
from backend.services.group_service import GroupService
from backend.services.signal_service import SignalService
from backend.services.strategy_service import StrategyService
from backend.services.factor_engine import FactorEngine
from backend.services.feature_service import FeatureService
from backend.services.model_service import ModelService
from backend.strategies.base import StrategyContext

log = get_logger(__name__)


class PaperTradingService:
    """Manage paper trading sessions and day-by-day advancement."""

    def __init__(self) -> None:
        self._signal_service = SignalService()
        self._strategy_service = StrategyService()
        self._group_service = GroupService()
        self._factor_engine = FactorEngine()
        self._feature_service = FeatureService()
        self._model_service = ModelService()

    # ------------------------------------------------------------------
    # Session CRUD
    # ------------------------------------------------------------------

    def create_session(
        self,
        strategy_id: str,
        universe_group_id: str,
        start_date: str,
        name: str | None = None,
        config: dict | None = None,
    ) -> dict:
        """Create a new paper trading session."""
        strategy = self._strategy_service.get_strategy(strategy_id)
        tickers = self._group_service.get_group_tickers(universe_group_id)
        if not tickers:
            raise ValueError(f"Universe group '{universe_group_id}' has no members")

        config = config or {}
        initial_capital = config.get("initial_capital", 1_000_000.0)

        session_id = uuid.uuid4().hex[:12]
        if not name:
            name = f"{strategy['name']} 模拟 {start_date}"

        conn = get_connection()
        now = datetime.utcnow()
        conn.execute(
            """INSERT INTO paper_trading_sessions
               (id, name, strategy_id, universe_group_id, config,
                status, start_date, current_date, initial_capital,
                current_nav, total_trades, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, 'active', ?, NULL, ?, ?, 0, ?, ?)""",
            [
                session_id, name, strategy_id, universe_group_id,
                json.dumps(config, default=str),
                start_date, initial_capital, initial_capital, now, now,
            ],
        )

        log.info(
            "paper_trading.created",
            session_id=session_id,
            strategy=strategy["name"],
            start_date=start_date,
        )
        return self.get_session(session_id)

    def list_sessions(self) -> list[dict]:
        """List all paper trading sessions."""
        conn = get_connection()
        rows = conn.execute(
            """SELECT s.id, s.name, s.strategy_id, s.universe_group_id,
                      s.config, s.status, s.start_date, s.current_date,
                      s.initial_capital, s.current_nav, s.total_trades,
                      s.created_at, s.updated_at,
                      st.name AS strategy_name
               FROM paper_trading_sessions s
               LEFT JOIN strategies st ON s.strategy_id = st.id
               ORDER BY s.created_at DESC"""
        ).fetchall()
        return [self._session_row_to_dict(r) for r in rows]

    def get_session(self, session_id: str) -> dict:
        """Get a single session with full detail."""
        conn = get_connection()
        row = conn.execute(
            """SELECT s.id, s.name, s.strategy_id, s.universe_group_id,
                      s.config, s.status, s.start_date, s.current_date,
                      s.initial_capital, s.current_nav, s.total_trades,
                      s.created_at, s.updated_at,
                      st.name AS strategy_name
               FROM paper_trading_sessions s
               LEFT JOIN strategies st ON s.strategy_id = st.id
               WHERE s.id = ?""",
            [session_id],
        ).fetchone()
        if not row:
            raise ValueError(f"Session {session_id} not found")
        return self._session_row_to_dict(row)

    def delete_session(self, session_id: str) -> None:
        """Delete a session and all its daily records."""
        conn = get_connection()
        conn.execute("DELETE FROM paper_trading_daily WHERE session_id = ?", [session_id])
        conn.execute("DELETE FROM paper_trading_sessions WHERE id = ?", [session_id])
        log.info("paper_trading.deleted", session_id=session_id)

    def pause_session(self, session_id: str) -> dict:
        """Pause an active session."""
        conn = get_connection()
        conn.execute(
            "UPDATE paper_trading_sessions SET status = 'paused', updated_at = ? WHERE id = ?",
            [datetime.utcnow(), session_id],
        )
        return self.get_session(session_id)

    def resume_session(self, session_id: str) -> dict:
        """Resume a paused session."""
        conn = get_connection()
        conn.execute(
            "UPDATE paper_trading_sessions SET status = 'active', updated_at = ? WHERE id = ?",
            [datetime.utcnow(), session_id],
        )
        return self.get_session(session_id)

    @staticmethod
    def _session_row_to_dict(row) -> dict:
        """Convert a DuckDB row from the paper_trading_sessions query to a dict.

        Expected column order (matching list_sessions / get_session SQL):
          0: id, 1: name, 2: strategy_id, 3: universe_group_id,
          4: config, 5: status, 6: start_date, 7: current_date,
          8: initial_capital, 9: current_nav, 10: total_trades,
          11: created_at, 12: updated_at, 13: strategy_name
        """
        config_raw = row[4]
        if isinstance(config_raw, str):
            try:
                config_raw = json.loads(config_raw)
            except (json.JSONDecodeError, TypeError):
                config_raw = {}
        return {
            "id": row[0],
            "name": row[1],
            "strategy_id": row[2],
            "universe_group_id": row[3],
            "config": config_raw or {},
            "status": row[5],
            "start_date": str(row[6]) if row[6] else None,
            "current_date": str(row[7]) if row[7] else None,
            "initial_capital": float(row[8]) if row[8] is not None else 1_000_000.0,
            "current_nav": float(row[9]) if row[9] is not None else None,
            "total_trades": int(row[10]) if row[10] is not None else 0,
            "created_at": str(row[11]) if row[11] else None,
            "updated_at": str(row[12]) if row[12] else None,
            "strategy_name": row[13] if len(row) > 13 else None,
        }

    # ------------------------------------------------------------------
    # Day-by-day advancement
    # ------------------------------------------------------------------

    @staticmethod
    def _coerce_date(value) -> date | None:
        if value is None:
            return None
        if isinstance(value, date):
            return value
        return date.fromisoformat(str(value))

    def _resolve_advance_trading_days(
        self,
        *,
        session: dict,
        target_date: str | None,
        steps: int,
    ) -> list[date]:
        """Resolve the unfiltered trading-day window for session advancement."""
        session_start = self._coerce_date(session.get("start_date"))
        if session_start is None:
            raise ValueError("Session is missing start_date")

        current = self._coerce_date(session.get("current_date"))
        start_from = current + timedelta(days=1) if current else session_start

        if steps > 0:
            generous_end = start_from + timedelta(days=steps * 3 + 30)
            all_days = get_trading_days(start_from, generous_end)
            return all_days[:steps] if all_days else []

        end = date.fromisoformat(target_date) if target_date else get_latest_trading_day()
        if start_from > end:
            if current is None:
                raise ValueError(
                    f"target_date {end} is before session start_date {session_start}"
                )
            return []

        return get_trading_days(start_from, end)

    def advance(
        self, session_id: str, target_date: str | None = None, steps: int = 0
    ) -> dict:
        """Advance a session forward.

        Args:
            session_id: Session to advance.
            target_date: Advance up to this date (inclusive).
            steps: If >0, advance exactly this many trading days (overrides target_date).
                   0 means advance to target_date or latest trading day.

        Processes each unprocessed trading day sequentially:
          1. Fresh sessions record the first trading day as a no-trade baseline
          2. Generate signals using data up to T-1 (previous trading day)
          3. Execute trades at T's open price
          4. Value portfolio at T's close price
          5. Record daily snapshot

        Returns summary of the advancement.
        """
        # Use batch-optimized version for multiple days
        session = self.get_session(session_id)
        if session["status"] != "active":
            raise ValueError(f"Session is {session['status']}, not active")

        # Determine which days to process
        trading_days = self._resolve_advance_trading_days(
            session=session,
            target_date=target_date,
            steps=steps,
        )

        if not trading_days:
            return {
                "session_id": session_id,
                "days_processed": 0,
                "message": "No trading days in range",
            }

        conn = get_connection()
        latest_bar_row = conn.execute("SELECT MAX(date) FROM daily_bars").fetchone()
        if not latest_bar_row or not latest_bar_row[0]:
            raise ValueError("本地无行情数据，请先更新数据")
        latest_bar_date = (
            latest_bar_row[0]
            if isinstance(latest_bar_row[0], date)
            else date.fromisoformat(str(latest_bar_row[0]))
        )

        days_with_data = [d for d in trading_days if d <= latest_bar_date]
        days_without = [d for d in trading_days if d > latest_bar_date]

        if not days_with_data:
            first_needed = trading_days[0]
            raise ValueError(
                f"无法推进：本地数据截止到 {latest_bar_date}，"
                f"下一交易日 {first_needed} 尚无数据。"
                f"请先更新行情数据。"
            )

        trading_days = days_with_data

        processed = set()
        rows = conn.execute(
            "SELECT date FROM paper_trading_daily WHERE session_id = ?",
            [session_id],
        ).fetchall()
        for r in rows:
            processed.add(r[0] if isinstance(r[0], date) else date.fromisoformat(str(r[0])))
        trading_days = [d for d in trading_days if d not in processed]

        if not trading_days:
            if not session.get("current_date"):
                return {
                    "session_id": session_id,
                    "days_processed": 0,
                    "current_date": None,
                    "message": "No unprocessed trading days",
                }
            return {
                "session_id": session_id,
                "days_processed": 0,
                "current_date": session.get("current_date"),
                "message": "Already up to date",
            }

        config = session.get("config") or {}
        capital = session["initial_capital"]
        commission_rate = config.get("commission_rate", 0.001)
        slippage_rate = config.get("slippage_rate", 0.001)
        max_positions = config.get("max_positions", 50)
        cost_rate = commission_rate + slippage_rate

        # Execution constraints (aligned with backtest engine)
        rebalance_buffer = config.get("rebalance_buffer", 0.0)
        min_holding_days = config.get("min_holding_days", 0)
        reentry_cooldown_days = config.get("reentry_cooldown_days", 0)

        # Position sizing (read from strategy definition, aligned with backtest)
        strategy_def = self._strategy_service.get_strategy(session["strategy_id"])
        position_sizing = strategy_def.get("position_sizing", "equal_weight")
        max_position_pct = config.get("max_position_pct", 0.10)

        # Warn if strategy has custom weight logic under equal_weight sizing
        weight_warnings = StrategyService._validate_weight_effectiveness(
            strategy_def.get("source_code", ""), position_sizing
        )
        for w in weight_warnings:
            log.warning("paper_trading.weight_ineffective", detail=w)

        tickers = self._group_service.get_group_tickers(session["universe_group_id"])

        record_initial_baseline = session.get("current_date") is None
        execution_days = trading_days[1:] if record_initial_baseline else trading_days

        # Pre-load shared resources once for days that can actually execute.
        # A fresh session's first day is only a baseline snapshot so paper
        # trading matches the backtest engine's T+1 first-trade semantics.
        batch_ctx = None
        if execution_days:
            batch_ctx = self._prepare_signal_context(
                strategy_id=session["strategy_id"],
                signal_dates=[self._prev_trading_day(d) for d in execution_days],
                universe_group_id=session["universe_group_id"],
                tickers=tickers,
            )

        # Load price cache for trade execution (separate from signal generation)
        price_cache = self._preload_prices(tickers, trading_days[0], trading_days[-1], conn)

        positions, cash = self._load_latest_state(session_id, capital)
        days_processed = 0
        total_new_trades = 0
        baseline_days = 0
        signal_errors = 0
        daily_snapshots: list[tuple] = []

        # Track holding days and exit dates for execution constraints
        ticker_holding_days: dict[str, int] = {}
        ticker_exit_day: dict[str, int] = {}  # ticker -> day_idx when last sold

        # Initialize holding_days from existing positions
        if positions:
            existing_holding = self._calculate_holding_days(
                session_id, list(positions.keys()),
                trading_days[0] if trading_days else date.today(), conn,
            )
            ticker_holding_days.update(existing_holding)

        log.info(
            "paper_trading.advance_start",
            session_id=session_id,
            days_to_process=len(trading_days),
            range=f"{trading_days[0]}~{trading_days[-1]}",
            execution_constraints=f"buffer={rebalance_buffer}, min_hold={min_holding_days}, cooldown={reentry_cooldown_days}",
            position_sizing=position_sizing,
        )

        for idx, trade_date in enumerate(trading_days):
            if record_initial_baseline and idx == 0:
                nav = self._value_portfolio_cached(positions, cash, trade_date, price_cache)
                daily_snapshots.append((
                    session_id, trade_date, nav, cash,
                    json.dumps(positions, default=str),
                    json.dumps([], default=str),
                ))
                days_processed += 1
                baseline_days += 1
                continue

            signal_date = self._prev_trading_day(trade_date)

            # --- Day-by-day signal generation with real-time portfolio state ---
            # Build portfolio state from current (evolving) positions
            portfolio_state = self._build_portfolio_state_from_memory(
                positions, cash, trade_date, price_cache,
                ticker_holding_days,
            )

            # Generate signals for this single day using pre-loaded context
            signals = self._generate_signal_single_day(
                batch_ctx, signal_date, portfolio_state,
            )

            if not signals:
                signal_errors += 1

            # Apply position sizing (aligned with backtest_service._apply_position_sizing)
            target_weights = self._apply_position_sizing_from_signals(
                signals, position_sizing, max_positions, max_position_pct,
            )

            # Apply execution constraints (aligned with backtest_engine)
            if rebalance_buffer > 0 or min_holding_days > 0 or reentry_cooldown_days > 0:
                # Calculate current weights from positions
                current_weights = portfolio_state.get("current_weights", {})
                effective_targets = dict(target_weights)

                all_involved = set(current_weights.keys()) | set(target_weights.keys())
                for ticker in all_involved:
                    old_w = current_weights.get(ticker, 0.0)
                    new_w = target_weights.get(ticker, 0.0)

                    # Buffer: skip trade if weight change is below threshold
                    if rebalance_buffer > 0 and abs(new_w - old_w) < rebalance_buffer:
                        effective_targets[ticker] = old_w
                        continue

                    # Min holding days: prevent selling before N days
                    if min_holding_days > 0 and old_w > 0 and new_w < old_w:
                        days_held = ticker_holding_days.get(ticker, 0)
                        if days_held < min_holding_days:
                            effective_targets[ticker] = old_w
                            continue

                    # Re-entry cooldown: prevent buying back after recent sell
                    if reentry_cooldown_days > 0 and old_w == 0 and new_w > 0:
                        exit_idx = ticker_exit_day.get(ticker)
                        if exit_idx is not None and (idx - exit_idx) < reentry_cooldown_days:
                            effective_targets.pop(ticker, None)
                            continue

                # Re-normalize if constraints altered the weights
                eff_sum = sum(w for w in effective_targets.values() if w > 0)
                if eff_sum > 0:
                    target_weights = {
                        t: w / eff_sum for t, w in effective_targets.items() if w > 0
                    }
                else:
                    target_weights = {}

            day_trades = self._execute_trades_cached(
                positions, cash, target_weights,
                trade_date, cost_rate, price_cache,
            )

            # Track exits and holding days for execution constraints
            old_positions = set(positions.keys())
            cash = day_trades["cash_after"]
            positions = day_trades["positions_after"]
            new_positions = set(positions.keys())

            # Update holding days: increment for held, reset for new entries
            for ticker in new_positions:
                if ticker in old_positions:
                    ticker_holding_days[ticker] = ticker_holding_days.get(ticker, 0) + 1
                else:
                    ticker_holding_days[ticker] = 1

            # Track exits for cooldown
            for ticker in old_positions - new_positions:
                ticker_exit_day[ticker] = idx
                ticker_holding_days.pop(ticker, None)

            total_new_trades += len(day_trades["trades"])
            nav = self._value_portfolio_cached(positions, cash, trade_date, price_cache)

            daily_snapshots.append((
                session_id, trade_date, nav, cash,
                json.dumps(positions, default=str),
                json.dumps(day_trades["trades"], default=str),
            ))
            days_processed += 1

        if daily_snapshots:
            conn.executemany(
                """INSERT OR REPLACE INTO paper_trading_daily
                   (session_id, date, nav, cash, positions_json, trades_json)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                daily_snapshots,
            )
            last_date = trading_days[-1] if trading_days else None
            last_nav = daily_snapshots[-1][2] if daily_snapshots else None
            conn.execute(
                """UPDATE paper_trading_sessions
                   SET current_date = ?, current_nav = ?,
                       total_trades = total_trades + ?,
                       updated_at = ?
                   WHERE id = ?""",
                [last_date, last_nav, total_new_trades,
                 datetime.utcnow(), session_id],
            )

        log.info(
            "paper_trading.advance_done",
            session_id=session_id,
            days=days_processed,
            trades=total_new_trades,
            signal_errors=signal_errors,
        )
        result = {
            "session_id": session_id,
            "days_processed": days_processed,
            "new_trades": total_new_trades,
            "current_date": str(trading_days[-1]) if trading_days else None,
            "baseline_days": baseline_days,
            "execution_rule": (
                "T+1 open; a fresh session records its first trading day as "
                "a no-trade baseline and starts executing on the next trading day"
            ),
        }
        if days_without:
            result["message"] = (
                f"已推进 {days_processed} 天。"
                f"另有 {len(days_without)} 天因本地数据不足被跳过"
                f"（数据截止 {latest_bar_date}）。"
            )
        return result

    # ------------------------------------------------------------------
    # Latest signals / T+1 action plan
    # ------------------------------------------------------------------

    def get_cached_signals(self, session_id: str) -> dict | None:
        """Return cached signal result for current T+1 date, or None."""
        session = self.get_session(session_id)
        current = session.get("current_date")
        if not current:
            return None
        if isinstance(current, str):
            current_d = date.fromisoformat(current)
        else:
            current_d = current
        next_days = get_trading_days(current_d + timedelta(days=1), current_d + timedelta(days=10))
        if not next_days:
            return None
        signal_date = next_days[0]
        conn = get_connection()
        row = conn.execute(
            "SELECT result_json FROM paper_trading_signal_cache "
            "WHERE session_id = ? AND signal_date = ?",
            [session_id, str(signal_date)],
        ).fetchone()
        if row:
            return json.loads(row[0])
        return None

    def _save_signal_cache(self, session_id: str, signal_date: date, result: dict) -> None:
        """Persist signal result to cache."""
        conn = get_connection()
        conn.execute(
            "INSERT OR REPLACE INTO paper_trading_signal_cache "
            "(session_id, signal_date, result_json) VALUES (?, ?, ?)",
            [session_id, str(signal_date), json.dumps(result, default=str)],
        )

    def get_latest_signals(self, session_id: str) -> dict:
        """Generate signals for the next trading day (T+1 action plan).

        Uses data up to session's current_date (T) to generate signals
        for T+1.  This mirrors what advance() would do on the next step.
        """
        session = self.get_session(session_id)
        current = session.get("current_date")
        if not current:
            return {"signals": [], "action_plan": [], "target_date": None}

        # Next trading day after current_date
        if isinstance(current, str):
            current_d = date.fromisoformat(current)
        else:
            current_d = current
        next_days = get_trading_days(current_d + timedelta(days=1), current_d + timedelta(days=10))
        if not next_days:
            return {"signals": [], "action_plan": [], "target_date": None}
        target_date = next_days[0]

        # Check signal cache first
        cached = self.get_cached_signals(session_id)
        if cached:
            log.info("paper_trading.signals_cache_hit", session_id=session_id, target_date=str(target_date))
            return cached

        # Load current portfolio state for stateful strategies
        positions, cash = self._load_latest_state(session_id, session["initial_capital"])
        portfolio_state = self._build_portfolio_state(session_id, positions, cash, current_d)

        # Lightweight signal generation: no validation, no persistence, just signals
        try:
            signals = self._generate_signals_lightweight(
                strategy_id=session["strategy_id"],
                signal_date=current_d,
                universe_group_id=session["universe_group_id"],
                tickers_override=None,
                portfolio_state=portfolio_state,
            )
        except Exception as exc:
            log.warning("paper_trading.lightweight_signal_fallback", error=str(exc))
            # Fallback to full SignalService pipeline
            try:
                signal_result = self._signal_service.generate_signals(
                    strategy_id=session["strategy_id"],
                    target_date=str(current_d),
                    universe_group_id=session["universe_group_id"],
                )
                signals = signal_result.get("signals", [])
            except Exception as exc2:
                log.warning("paper_trading.signal_preview_error", error=str(exc2))
                return {"signals": [], "action_plan": [], "target_date": str(target_date), "error": str(exc2)}

        # Load current positions
        positions, cash = self._load_latest_state(session_id, session["initial_capital"])
        config = session.get("config") or {}
        max_positions = config.get("max_positions", 50)

        # Build target weights
        target_weights: dict[str, float] = {}
        for sig in signals:
            ticker = sig.get("ticker", "")
            weight = sig.get("target_weight", 0.0)
            if weight and weight > 0:
                target_weights[ticker] = float(weight)

        if len(target_weights) > max_positions:
            sorted_tw = sorted(target_weights.items(), key=lambda x: x[1], reverse=True)
            target_weights = dict(sorted_tw[:max_positions])

        # Apply normalization aligned with advance() execution logic
        normalize_weights = config.get("normalize_weights", True)
        if normalize_weights:
            w_sum = sum(target_weights.values())
            if w_sum > 0:
                target_weights = {t: w / w_sum for t, w in target_weights.items()}
        else:
            # Absolute weight mode: apply cash_reserve and max_position_pct constraints
            cash_reserve_pct = config.get("cash_reserve_pct", 0.0)
            max_position_pct = config.get("max_position_pct", 1.0)

            for ticker in list(target_weights.keys()):
                if target_weights[ticker] > max_position_pct:
                    target_weights[ticker] = max_position_pct

            max_total = 1.0 - cash_reserve_pct
            w_sum = sum(target_weights.values())
            if w_sum > max_total and w_sum > 0:
                scale = max_total / w_sum
                target_weights = {t: w * scale for t, w in target_weights.items()}

        # Build action plan: compare current positions vs target
        action_plan = []
        all_tickers = set(positions.keys()) | set(target_weights.keys())
        for ticker in sorted(all_tickers):
            current_shares = positions.get(ticker, {}).get("shares", 0.0)
            target_w = target_weights.get(ticker, 0.0)
            action = "hold"
            if current_shares > 0 and target_w == 0:
                action = "sell"
            elif current_shares == 0 and target_w > 0:
                action = "buy"
            elif current_shares > 0 and target_w > 0:
                action = "hold"
            action_plan.append({
                "ticker": ticker,
                "action": action,
                "current_shares": round(current_shares, 2),
                "target_weight": round(target_w, 4),
            })

        result = {
            "signals": signals,
            "action_plan": action_plan,
            "target_date": str(target_date),
        }

        # Cache for future requests
        try:
            self._save_signal_cache(session_id, target_date, result)
            log.info("paper_trading.signals_cached", session_id=session_id, target_date=str(target_date))
        except Exception as exc:
            log.warning("paper_trading.signal_cache_save_error", error=str(exc))

        return result

    def _generate_signals_lightweight(
        self,
        strategy_id: str,
        signal_date: date,
        universe_group_id: str,
        tickers_override: list[str] | None = None,
        portfolio_state: dict | None = None,
    ) -> list[dict]:
        """Generate signals for a single date without validation or persistence.

        Optimized: bulk-loads all factor cache data (strategy + model features)
        in a single DB query, then passes pre-computed features to the model.

        Args:
            portfolio_state: Optional dict with current_weights, holding_days,
                           avg_entry_price, unrealized_pnl for stateful strategies.

        Returns list of signal dicts with ticker, signal, target_weight, strength.
        """
        strategy_def = self._strategy_service.get_strategy(strategy_id)
        strategy_instance = self._load_strategy_instance(strategy_def)

        tickers = tickers_override if tickers_override else self._group_service.get_group_tickers(universe_group_id)
        if not tickers:
            return []

        start_lookback = signal_date - timedelta(days=250)
        start_str = str(start_lookback)
        end_str = str(signal_date)

        # Load OHLCV data with lookback
        prices_close, prices_open, prices_high, prices_low, prices_volume = self._load_prices(
            tickers, start_str, end_str
        )
        if prices_close.empty:
            return []

        # --- Bulk load ALL factor data (strategy + model features) ---
        required_factors = strategy_def.get("required_factors", [])
        required_models = strategy_def.get("required_models", [])

        strategy_factor_map = self._resolve_factor_ids(required_factors) if required_factors else {}
        all_factor_ids = set(strategy_factor_map.values())

        # Resolve model feature sets
        model_fs_map: dict[str, tuple[str, dict[str, str]]] = {}
        for model_id in required_models:
            try:
                model_record = self._model_service.get_model(model_id)
                fs_id = model_record["feature_set_id"]
                fs = self._feature_service.get_feature_set(fs_id)
                fs_id_to_name: dict[str, str] = {}
                for ref in fs["factor_refs"]:
                    fid = ref["factor_id"]
                    fname = ref.get("factor_name", fid)
                    fs_id_to_name[fid] = fname
                    all_factor_ids.add(fid)
                model_fs_map[model_id] = (fs_id, fs_id_to_name)
            except Exception as exc:
                log.warning("paper_trading.lightweight_model_fs_failed", model_id=model_id, error=str(exc))

        # Bulk load all cached factor values in ONE query
        cached_by_id = self._factor_engine.load_cached_factors_bulk(
            list(all_factor_ids), tickers, start_str, end_str
        )

        # Build strategy factor_data
        factor_data: dict[str, pd.DataFrame] = {}
        for factor_name, factor_id in strategy_factor_map.items():
            if factor_id in cached_by_id and not cached_by_id[factor_id].empty:
                factor_data[factor_name] = cached_by_id[factor_id]
            else:
                try:
                    df = self._factor_engine.compute_factor(factor_id, tickers, start_str, end_str)
                    if not df.empty:
                        factor_data[factor_name] = df
                except Exception as exc:
                    log.warning("paper_trading.lightweight_factor_failed", factor_name=factor_name, error=str(exc))

        # Build model predictions using pre-computed features
        # Parallelize model predictions to speed up computation
        from concurrent.futures import ThreadPoolExecutor, as_completed

        model_predictions: dict[str, pd.Series] = {}

        def _predict_single_model(model_id: str) -> tuple[str, pd.Series | None]:
            """Predict for a single model, returns (model_id, predictions)."""
            if model_id not in model_fs_map:
                return (model_id, None)

            fs_id, fs_id_to_name = model_fs_map[model_id]
            fs = self._feature_service.get_feature_set(fs_id)
            preprocessing = fs["preprocessing"]

            feature_data_local: dict[str, pd.DataFrame] = {}
            for fid, fname in fs_id_to_name.items():
                if fid in cached_by_id and not cached_by_id[fid].empty:
                    feature_data_local[fname] = cached_by_id[fid]
                else:
                    try:
                        df = self._factor_engine.compute_factor(fid, tickers, start_str, end_str)
                        if not df.empty:
                            feature_data_local[fname] = df
                    except Exception as exc:
                        log.warning("paper_trading.lightweight_model_factor_failed", factor=fname, error=str(exc))

            # Apply preprocessing
            processed: dict[str, pd.DataFrame] = {}
            for fname, df in feature_data_local.items():
                processed[fname] = self._feature_service._apply_preprocessing(df, preprocessing)

            try:
                preds = self._model_service.predict_with_features(
                    model_id=model_id,
                    feature_data=processed,
                    tickers=tickers,
                    date=end_str,
                )
                return (model_id, preds if not preds.empty else None)
            except Exception as exc:
                log.warning("paper_trading.lightweight_model_failed", model_id=model_id, error=str(exc))
                return (model_id, None)

        # Execute model predictions in parallel (max 4 workers to avoid overwhelming system)
        if required_models:
            with ThreadPoolExecutor(max_workers=min(4, max(1, len(required_models)))) as executor:
                futures = {executor.submit(_predict_single_model, mid): mid for mid in required_models}
                for future in as_completed(futures):
                    model_id, preds = future.result()
                    if preds is not None:
                        model_predictions[model_id] = preds

        # Build context and generate signals
        trade_ts = pd.Timestamp(signal_date)
        prices_multi = self._build_prices_multi(
            prices_close, prices_open, prices_high, prices_low, prices_volume, tickers
        )

        # Include portfolio state for stateful strategies
        context_kwargs = {
            "prices": prices_multi.loc[:trade_ts],
            "factor_values": factor_data,
            "model_predictions": model_predictions,
            "current_date": trade_ts,
        }
        if portfolio_state:
            context_kwargs.update(portfolio_state)

        context = StrategyContext(**context_kwargs)

        raw_signals = strategy_instance.generate_signals(context)
        if raw_signals.empty:
            return []

        signal_list = []
        for ticker in raw_signals.index:
            row = raw_signals.loc[ticker]
            signal_list.append({
                "ticker": str(ticker),
                "signal": int(row.get("signal", 0)),
                "target_weight": float(row.get("weight", 0.0)),
                "strength": float(row.get("strength", 0.0)),
            })
        return signal_list

    # ------------------------------------------------------------------
    # Stock trade chart data
    # ------------------------------------------------------------------

    def get_stock_chart(self, session_id: str, ticker: str) -> dict:
        """Return daily bars and trade markers for a stock within a paper session."""
        session = self.get_session(session_id)
        conn = get_connection()

        start = session["start_date"]
        end = session.get("current_date") or start

        # Fetch daily bars
        bars = conn.execute(
            """SELECT date, open, high, low, close, volume
               FROM daily_bars
               WHERE ticker = ? AND date >= ? AND date <= ?
               ORDER BY date""",
            [ticker, start, end],
        ).fetchall()

        daily_bars = [
            {
                "date": str(r[0]),
                "open": r[1],
                "high": r[2],
                "low": r[3],
                "close": r[4],
                "volume": r[5],
            }
            for r in bars
        ]

        # Collect trades for this ticker from daily snapshots
        trade_rows = conn.execute(
            """SELECT date, trades_json FROM paper_trading_daily
               WHERE session_id = ? ORDER BY date""",
            [session_id],
        ).fetchall()

        ticker_trades = []
        for row in trade_rows:
            trades = json.loads(row[1]) if isinstance(row[1], str) else (row[1] or [])
            for t in trades:
                if t.get("ticker") == ticker:
                    ticker_trades.append({
                        "date": str(row[0]),
                        "action": t["action"],
                        "shares": t["shares"],
                        "price": t["price"],
                        "cost": t.get("cost", 0),
                    })

        return {
            "ticker": ticker,
            "daily_bars": daily_bars,
            "trades": ticker_trades,
        }

    # ------------------------------------------------------------------
    # Query daily data
    # ------------------------------------------------------------------

    def get_daily_series(self, session_id: str) -> list[dict]:
        """Return daily NAV series for charting."""
        conn = get_connection()
        rows = conn.execute(
            """SELECT date, nav, cash FROM paper_trading_daily
               WHERE session_id = ? ORDER BY date""",
            [session_id],
        ).fetchall()
        return [
            {"date": str(r[0]), "nav": r[1], "cash": r[2]}
            for r in rows
        ]

    def get_positions(self, session_id: str) -> list[dict]:
        """Return current positions for the latest date with market values."""
        conn = get_connection()
        row = conn.execute(
            """SELECT positions_json, date, nav FROM paper_trading_daily
               WHERE session_id = ? ORDER BY date DESC LIMIT 1""",
            [session_id],
        ).fetchone()
        if not row or not row[0]:
            return []
        positions = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        pos_date = str(row[1])
        nav = float(row[2]) if row[2] else 0.0

        # Fetch latest prices for all position tickers
        pos_tickers = list(positions.keys())
        latest_prices: dict[str, float] = {}
        if pos_tickers:
            placeholders = ",".join("?" for _ in pos_tickers)
            price_rows = conn.execute(
                f"""SELECT ticker, close FROM daily_bars
                    WHERE ticker IN ({placeholders}) AND date <= ?
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) = 1""",
                [*pos_tickers, pos_date],
            ).fetchall()
            for t, c in price_rows:
                latest_prices[t] = float(c)

        result = []
        for ticker, pos in positions.items():
            shares = pos.get("shares", 0)
            avg_price = pos.get("avg_price", 0)
            price = latest_prices.get(ticker)
            market_value = round(shares * price, 2) if price is not None else None
            unrealized_pnl = round((price - avg_price) * shares, 2) if price is not None else None
            weight = round(market_value / nav, 6) if market_value is not None and nav > 0 else None
            result.append({
                "ticker": ticker,
                "shares": shares,
                "avg_price": avg_price,
                "latest_price": price,
                "market_value": market_value,
                "unrealized_pnl": unrealized_pnl,
                "weight": weight,
                "date": pos_date,
            })
        return result

    def get_trades(self, session_id: str, limit: int = 200) -> list[dict]:
        """Return recent trades across all days."""
        conn = get_connection()
        rows = conn.execute(
            """SELECT date, trades_json FROM paper_trading_daily
               WHERE session_id = ? ORDER BY date DESC""",
            [session_id],
        ).fetchall()
        all_trades: list[dict] = []
        for row in rows:
            trades = json.loads(row[1]) if isinstance(row[1], str) else (row[1] or [])
            for t in trades:
                t["date"] = str(row[0])
            all_trades.extend(trades)
            if len(all_trades) >= limit:
                break
        return all_trades[:limit]

    def get_summary(self, session_id: str) -> dict:
        """Compute summary metrics for a session."""
        session = self.get_session(session_id)
        series = self.get_daily_series(session_id)

        if len(series) < 2:
            return {
                **session,
                "total_return": 0.0,
                "max_drawdown": 0.0,
                "trading_days": len(series),
            }

        navs = [s["nav"] for s in series]
        initial = session["initial_capital"]
        total_return = (navs[-1] / initial - 1) if initial > 0 else 0

        # Max drawdown
        peak = navs[0]
        max_dd = 0.0
        for v in navs:
            if v > peak:
                peak = v
            dd = (peak - v) / peak if peak > 0 else 0
            if dd > max_dd:
                max_dd = dd

        return {
            **session,
            "total_return": round(total_return, 6),
            "max_drawdown": round(max_dd, 6),
            "trading_days": len(series),
            "latest_nav": navs[-1],
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _prev_trading_day(d: date) -> date:
        """Return the trading day immediately before *d*."""
        return offset_trading_days(d, -1)

    def _prepare_signal_context(
        self,
        strategy_id: str,
        signal_dates: list[date],
        universe_group_id: str,
        tickers: list[str],
    ) -> dict:
        """Pre-load strategy, factors, models, and prices for signal generation.

        Loads everything once so that day-by-day signal generation only needs
        to build the StrategyContext per day without re-loading from DB.

        Returns a dict with all pre-loaded resources.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        strategy_def = self._strategy_service.get_strategy(strategy_id)
        strategy_instance = self._load_strategy_instance(strategy_def)

        required_factors = strategy_def.get("required_factors", [])
        required_models = strategy_def.get("required_models", [])

        start_lookback = min(signal_dates) - timedelta(days=250)
        end_date = max(signal_dates)
        start_str = str(start_lookback)
        end_str = str(end_date)

        # Bulk load ALL factor data (strategy + model features)
        strategy_factor_map = self._resolve_factor_ids(required_factors) if required_factors else {}
        all_factor_ids = set(strategy_factor_map.values())

        model_fs_map: dict[str, tuple[str, dict[str, str]]] = {}
        for model_id in required_models:
            try:
                model_record = self._model_service.get_model(model_id)
                fs_id = model_record["feature_set_id"]
                fs = self._feature_service.get_feature_set(fs_id)
                fs_id_to_name: dict[str, str] = {}
                for ref in fs["factor_refs"]:
                    fid = ref["factor_id"]
                    fname = ref.get("factor_name", fid)
                    fs_id_to_name[fid] = fname
                    all_factor_ids.add(fid)
                model_fs_map[model_id] = (fs_id, fs_id_to_name)
            except Exception as exc:
                log.warning("paper_trading.prepare_model_fs_failed", model_id=model_id, error=str(exc))

        cached_by_id = self._factor_engine.load_cached_factors_bulk(
            list(all_factor_ids), tickers, start_str, end_str
        )

        # Build strategy factor_data
        factor_data: dict[str, pd.DataFrame] = {}
        for factor_name, factor_id in strategy_factor_map.items():
            if factor_id in cached_by_id and not cached_by_id[factor_id].empty:
                factor_data[factor_name] = cached_by_id[factor_id]
            else:
                try:
                    df = self._factor_engine.compute_factor(factor_id, tickers, start_str, end_str)
                    if not df.empty:
                        factor_data[factor_name] = df
                except Exception as exc:
                    log.warning("paper_trading.prepare_factor_failed", factor_name=factor_name, error=str(exc))

        # Build model predictions for all dates
        model_predictions: dict[str, dict[date, pd.Series]] = {}

        def _predict_model_batch(model_id: str) -> tuple[str, dict[date, pd.Series]]:
            result_preds: dict[date, pd.Series] = {}
            if model_id not in model_fs_map:
                return (model_id, result_preds)

            fs_id, fs_id_to_name = model_fs_map[model_id]
            fs = self._feature_service.get_feature_set(fs_id)
            preprocessing = fs["preprocessing"]

            feature_data_local: dict[str, pd.DataFrame] = {}
            for fid, fname in fs_id_to_name.items():
                if fid in cached_by_id and not cached_by_id[fid].empty:
                    feature_data_local[fname] = cached_by_id[fid]
                else:
                    try:
                        df = self._factor_engine.compute_factor(fid, tickers, start_str, end_str)
                        if not df.empty:
                            feature_data_local[fname] = df
                    except Exception as exc:
                        log.warning("paper_trading.prepare_model_factor_failed", factor=fname, error=str(exc))

            processed_features: dict[str, pd.DataFrame] = {}
            for fname, df in feature_data_local.items():
                processed_features[fname] = self._feature_service._apply_preprocessing(df, preprocessing)

            for d in signal_dates:
                try:
                    preds = self._model_service.predict_with_features(
                        model_id=model_id,
                        feature_data=processed_features,
                        tickers=tickers,
                        date=str(d),
                    )
                    if not preds.empty:
                        result_preds[d] = preds
                except Exception as exc:
                    log.warning("paper_trading.prepare_model_failed", model_id=model_id, date=str(d), error=str(exc))

            return (model_id, result_preds)

        if required_models:
            with ThreadPoolExecutor(max_workers=min(4, max(1, len(required_models)))) as executor:
                futures = {executor.submit(_predict_model_batch, mid): mid for mid in required_models}
                for future in as_completed(futures):
                    model_id, preds_by_date = future.result()
                    model_predictions[model_id] = preds_by_date

        # -- Runtime check: warn if required models produced no predictions --
        if required_models and not model_predictions:
            log.error(
                "paper_trading.no_model_predictions",
                detail=(
                    f"策略声明了 required_models={required_models} "
                    f"但没有任何模型产出预测。策略可能退化为 0 trades。"
                ),
            )
        elif required_models:
            empty_models = [m for m, preds in model_predictions.items() if not preds]
            if empty_models:
                log.warning(
                    "paper_trading.partial_model_predictions",
                    empty_models=empty_models,
                    signal_dates=[str(d) for d in signal_dates],
                )

        # Pre-load OHLCV data
        prices_close, prices_open, prices_high, prices_low, prices_volume = self._load_prices(
            tickers, start_str, end_str
        )
        prices_multi = self._build_prices_multi(
            prices_close, prices_open, prices_high, prices_low, prices_volume, tickers
        )

        return {
            "strategy_instance": strategy_instance,
            "factor_data": factor_data,
            "model_predictions": model_predictions,
            "prices_multi": prices_multi,
            "tickers": tickers,
        }

    def _generate_signal_single_day(
        self,
        batch_ctx: dict,
        signal_date: date,
        portfolio_state: dict | None = None,
    ) -> list[dict]:
        """Generate signals for a single day using pre-loaded context.

        Unlike _generate_signals_batch which pre-generates all days,
        this generates one day at a time so stateful strategies see
        the correct evolving portfolio state.
        """
        strategy_instance = batch_ctx["strategy_instance"]
        factor_data = batch_ctx["factor_data"]
        model_predictions = batch_ctx["model_predictions"]
        prices_multi = batch_ctx["prices_multi"]

        trade_ts = pd.Timestamp(signal_date)

        # Filter factor data up to this date
        date_factor_data = {}
        for name, df in factor_data.items():
            if not df.empty:
                date_factor_data[name] = df[df.index <= trade_ts]

        # Filter model predictions for this date
        date_model_preds = {}
        for model_id, preds_by_date in model_predictions.items():
            if signal_date in preds_by_date:
                date_model_preds[model_id] = preds_by_date[signal_date]

        # Build context with portfolio state
        context_kwargs = {
            "prices": prices_multi.loc[:trade_ts],
            "factor_values": date_factor_data,
            "model_predictions": date_model_preds,
            "current_date": trade_ts,
        }

        if portfolio_state:
            context_kwargs.update(portfolio_state)

        context = StrategyContext(**context_kwargs)

        try:
            raw_signals = strategy_instance.generate_signals(context)
            if raw_signals.empty:
                return []

            signal_list = []
            for ticker in raw_signals.index:
                row = raw_signals.loc[ticker]
                signal_list.append({
                    "ticker": str(ticker),
                    "signal": int(row.get("signal", 0)),
                    "target_weight": float(row.get("weight", 0.0)),
                    "strength": float(row.get("strength", 0.0)),
                })
            return signal_list
        except Exception as exc:
            log.warning("paper_trading.signal_single_day_failed", date=str(signal_date), error=str(exc))
            return []

    def _build_portfolio_state_from_memory(
        self,
        positions: dict[str, dict],
        cash: float,
        trade_date: date,
        price_cache: dict[date, dict[str, tuple[float, float]]],
        ticker_holding_days: dict[str, int],
    ) -> dict:
        """Build portfolio state from in-memory positions (no DB query).

        Unlike _build_portfolio_state which queries paper_trading_daily,
        this builds state from the evolving in-memory positions dict,
        ensuring stateful strategies see correct day-by-day state.
        """
        if not positions:
            return {
                "current_weights": {},
                "holding_days": {},
                "avg_entry_price": {},
                "unrealized_pnl": {},
            }

        # Use price_cache for current prices
        day_prices = price_cache.get(trade_date, {})
        prev_day = self._prev_trading_day(trade_date)
        prev_prices = price_cache.get(prev_day, {})
        # Prefer previous day close (most recent settlement), fall back to trade_date
        best_prices = prev_prices if prev_prices else day_prices

        portfolio_value = cash
        current_prices: dict[str, float] = {}
        for ticker, pos in positions.items():
            prices = best_prices.get(ticker)
            if prices:
                current_prices[ticker] = prices[1]  # close price
            else:
                current_prices[ticker] = pos.get("avg_price", 0)
            portfolio_value += pos["shares"] * current_prices[ticker]

        current_weights = {}
        avg_entry_price = {}
        unrealized_pnl = {}

        for ticker, pos in positions.items():
            shares = pos["shares"]
            entry_price = pos.get("avg_price", 0)
            cur_price = current_prices.get(ticker, entry_price)

            position_value = shares * cur_price
            current_weights[ticker] = position_value / portfolio_value if portfolio_value > 0 else 0
            avg_entry_price[ticker] = entry_price
            unrealized_pnl[ticker] = (cur_price / entry_price - 1) if entry_price > 0 else 0

        return {
            "current_weights": current_weights,
            "holding_days": dict(ticker_holding_days),
            "avg_entry_price": avg_entry_price,
            "unrealized_pnl": unrealized_pnl,
        }

    @staticmethod
    def _apply_position_sizing_from_signals(
        signals: list[dict],
        method: str,
        max_positions: int,
        max_position_pct: float = 0.10,
    ) -> dict[str, float]:
        """Apply position sizing to signal list, aligned with backtest_service._apply_position_sizing.

        Args:
            signals: List of signal dicts with ticker, signal, target_weight, strength.
            method: One of 'equal_weight', 'signal_weight', 'max_position'.
            max_positions: Maximum number of positions.
            max_position_pct: Maximum weight per position.

        Returns:
            Dict mapping ticker -> target weight (summing to ~1.0).
        """
        # Filter to buy signals only
        buys = [s for s in signals if s.get("signal") == 1 and s.get("target_weight", 0) > 0]
        if not buys:
            return {}

        # Sort by strength descending, take top N
        buys.sort(key=lambda x: x.get("strength", 0), reverse=True)
        if len(buys) > max_positions:
            buys = buys[:max_positions]

        if method == "equal_weight":
            n = len(buys)
            return {s["ticker"]: 1.0 / n for s in buys}

        elif method == "signal_weight":
            total_strength = sum(s.get("strength", 0) for s in buys)
            if total_strength > 0:
                return {s["ticker"]: s.get("strength", 0) / total_strength for s in buys}
            else:
                n = len(buys)
                return {s["ticker"]: 1.0 / n for s in buys}

        elif method == "max_position":
            total_strength = sum(s.get("strength", 0) for s in buys)
            if total_strength > 0:
                raw_weights = {s["ticker"]: s.get("strength", 0) / total_strength for s in buys}
            else:
                n = len(buys)
                raw_weights = {s["ticker"]: 1.0 / n for s in buys}

            # Iterative capping
            capped = {}
            uncapped = dict(raw_weights)
            for _ in range(10):  # max iterations
                excess = 0.0
                newly_capped = {}
                for t, w in list(uncapped.items()):
                    if w > max_position_pct:
                        excess += w - max_position_pct
                        newly_capped[t] = max_position_pct
                        del uncapped[t]
                if not newly_capped:
                    break
                capped.update(newly_capped)
                if uncapped and excess > 0:
                    uncapped_sum = sum(uncapped.values())
                    if uncapped_sum > 0:
                        for t in uncapped:
                            uncapped[t] += excess * (uncapped[t] / uncapped_sum)
            capped.update(uncapped)
            return capped

        else:
            # Default to equal weight
            n = len(buys)
            return {s["ticker"]: 1.0 / n for s in buys}

    def _generate_signals_batch(
        self,
        strategy_id: str,
        signal_dates: list[date],
        universe_group_id: str,
        tickers: list[str],
        session_id: str | None = None,
    ) -> dict[int, list[dict]]:
        """Generate signals for multiple dates in one batch.

        Loads strategy, factors, and models once, then generates signals
        for each date without persisting to DB or re-validating dependencies.

        Optimized: uses bulk cache loading for all factors (strategy + model
        feature set) in a single DB query, then passes pre-computed features
        directly to the model — avoiding 264 individual factor queries.

        Args:
            session_id: Optional paper trading session ID for portfolio state.

        Returns: dict[date_index, list[signal_dict]]
        """
        if not signal_dates:
            return {}

        # Load strategy definition and instantiate once
        strategy_def = self._strategy_service.get_strategy(strategy_id)
        strategy_instance = self._load_strategy_instance(strategy_def)

        required_factors = strategy_def.get("required_factors", [])
        required_models = strategy_def.get("required_models", [])

        start_lookback = min(signal_dates) - timedelta(days=250)
        end_date = max(signal_dates)
        start_str = str(start_lookback)
        end_str = str(end_date)

        # --- Bulk load ALL factor data (strategy + model features) ---
        # 1. Collect all factor IDs we need
        strategy_factor_map = self._resolve_factor_ids(required_factors) if required_factors else {}
        all_factor_ids = set(strategy_factor_map.values())

        # Resolve model feature sets and collect their factor IDs
        model_fs_map: dict[str, tuple[str, dict[str, str]]] = {}  # model_id -> (fs_id, {factor_name: factor_id})
        for model_id in required_models:
            try:
                model_record = self._model_service.get_model(model_id)
                fs_id = model_record["feature_set_id"]
                fs = self._feature_service.get_feature_set(fs_id)
                fs_id_to_name: dict[str, str] = {}
                for ref in fs["factor_refs"]:
                    fid = ref["factor_id"]
                    fname = ref.get("factor_name", fid)
                    fs_id_to_name[fid] = fname
                    all_factor_ids.add(fid)
                model_fs_map[model_id] = (fs_id, fs_id_to_name)
            except Exception as exc:
                log.warning("paper_trading.batch_model_fs_failed", model_id=model_id, error=str(exc))

        # 2. Bulk load all cached factor values in ONE query
        all_factor_ids_list = list(all_factor_ids)
        cached_by_id = self._factor_engine.load_cached_factors_bulk(
            all_factor_ids_list, tickers, start_str, end_str
        )

        # 3. Build strategy factor_data (name -> DataFrame)
        factor_data: dict[str, pd.DataFrame] = {}
        missing_strategy_factors: list[tuple[str, str]] = []
        for factor_name, factor_id in strategy_factor_map.items():
            if factor_id in cached_by_id and not cached_by_id[factor_id].empty:
                factor_data[factor_name] = cached_by_id[factor_id]
            else:
                missing_strategy_factors.append((factor_name, factor_id))

        # Fallback: compute any uncached strategy factors individually
        for factor_name, factor_id in missing_strategy_factors:
            try:
                df = self._factor_engine.compute_factor(factor_id, tickers, start_str, end_str)
                if not df.empty:
                    factor_data[factor_name] = df
            except Exception as exc:
                log.warning("paper_trading.batch_factor_failed", factor_name=factor_name, error=str(exc))

        # 4. Build model feature data and generate predictions
        # Parallelize model predictions to speed up computation
        from concurrent.futures import ThreadPoolExecutor, as_completed

        model_predictions: dict[str, dict[date, pd.Series]] = {}

        def _predict_model_batch(model_id: str) -> tuple[str, dict[date, pd.Series]]:
            """Predict for a single model across all dates, returns (model_id, {date: predictions})."""
            result: dict[date, pd.Series] = {}
            if model_id not in model_fs_map:
                return (model_id, result)

            fs_id, fs_id_to_name = model_fs_map[model_id]
            fs = self._feature_service.get_feature_set(fs_id)
            preprocessing = fs["preprocessing"]

            # Build feature_data from bulk cache (factor_name -> DataFrame)
            feature_data_local: dict[str, pd.DataFrame] = {}
            missing_model_factors: list[tuple[str, str]] = []
            for fid, fname in fs_id_to_name.items():
                if fid in cached_by_id and not cached_by_id[fid].empty:
                    feature_data_local[fname] = cached_by_id[fid]
                else:
                    missing_model_factors.append((fname, fid))

            # Fallback for uncached model factors
            for fname, fid in missing_model_factors:
                try:
                    df = self._factor_engine.compute_factor(fid, tickers, start_str, end_str)
                    if not df.empty:
                        feature_data_local[fname] = df
                except Exception as exc:
                    log.warning("paper_trading.batch_model_factor_failed", factor=fname, error=str(exc))

            # Apply preprocessing
            processed_features: dict[str, pd.DataFrame] = {}
            for fname, df in feature_data_local.items():
                processed_features[fname] = self._feature_service._apply_preprocessing(df, preprocessing)

            # Generate predictions per date using pre-computed features
            for d in signal_dates:
                try:
                    preds = self._model_service.predict_with_features(
                        model_id=model_id,
                        feature_data=processed_features,
                        tickers=tickers,
                        date=str(d),
                    )
                    if not preds.empty:
                        result[d] = preds
                except Exception as exc:
                    log.warning("paper_trading.batch_model_failed", model_id=model_id, date=str(d), error=str(exc))

            return (model_id, result)

        # Execute model predictions in parallel (max 4 workers)
        if required_models:
            with ThreadPoolExecutor(max_workers=min(4, max(1, len(required_models)))) as executor:
                futures = {executor.submit(_predict_model_batch, mid): mid for mid in required_models}
                for future in as_completed(futures):
                    model_id, preds_by_date = future.result()
                    model_predictions[model_id] = preds_by_date

        # Pre-load OHLCV data for the entire range
        prices_close, prices_open, prices_high, prices_low, prices_volume = self._load_prices(
            tickers, start_str, end_str
        )

        # Generate signals per date
        results: dict[int, list[dict]] = {}
        prices_multi = self._build_prices_multi(
            prices_close, prices_open, prices_high, prices_low, prices_volume, tickers
        )

        # Load portfolio state if session_id provided
        conn = get_connection()
        positions_by_date: dict[date, dict] = {}
        cash_by_date: dict[date, float] = {}

        if session_id:
            # Load historical positions for each signal date
            for idx, target_date in enumerate(signal_dates):
                # Get positions as of the day before signal generation
                row = conn.execute(
                    """SELECT positions_json, cash FROM paper_trading_daily
                       WHERE session_id = ? AND date <= ?
                       ORDER BY date DESC LIMIT 1""",
                    [session_id, target_date],
                ).fetchone()
                if row:
                    positions_by_date[target_date] = json.loads(row[0]) if isinstance(row[0], str) else (row[0] or {})
                    cash_by_date[target_date] = row[1]
                else:
                    positions_by_date[target_date] = {}
                    cash_by_date[target_date] = 0.0

        for idx, target_date in enumerate(signal_dates):
            trade_ts = pd.Timestamp(target_date)
            # Filter factor data up to this date
            date_factor_data = {}
            for name, df in factor_data.items():
                if not df.empty:
                    date_factor_data[name] = df[df.index <= trade_ts]
            # Filter model predictions for this date
            date_model_preds = {}
            for model_id, preds_by_date in model_predictions.items():
                if target_date in preds_by_date:
                    date_model_preds[model_id] = preds_by_date[target_date]

            # Build context with portfolio state if available
            context_kwargs = {
                "prices": prices_multi.loc[:trade_ts],
                "factor_values": date_factor_data,
                "model_predictions": date_model_preds,
                "current_date": trade_ts,
            }

            if session_id and target_date in positions_by_date:
                portfolio_state = self._build_portfolio_state(
                    session_id,
                    positions_by_date[target_date],
                    cash_by_date[target_date],
                    target_date,
                )
                context_kwargs.update(portfolio_state)

            context = StrategyContext(**context_kwargs)

            try:
                raw_signals = strategy_instance.generate_signals(context)
                if raw_signals.empty:
                    results[idx] = []
                    continue

                signal_list = []
                for ticker in raw_signals.index:
                    row = raw_signals.loc[ticker]
                    signal_list.append({
                        "ticker": str(ticker),
                        "signal": int(row.get("signal", 0)),
                        "target_weight": float(row.get("weight", 0.0)),
                        "strength": float(row.get("strength", 0.0)),
                    })
                results[idx] = signal_list
            except Exception as exc:
                log.warning("paper_trading.batch_signal_failed", date=str(target_date), error=str(exc))
                results[idx] = []

        return results

    def _load_strategy_instance(self, strategy_def: dict):
        """Load and return strategy instance from definition."""
        from backend.strategies.loader import load_strategy_from_code
        return load_strategy_from_code(strategy_def["source_code"])

    def _resolve_factor_ids(self, factor_names: list[str]) -> dict[str, str]:
        """Resolve factor names to factor IDs (latest version) in a single query."""
        if not factor_names:
            return {}
        conn = get_connection()
        placeholders = ",".join("?" for _ in factor_names)
        rows = conn.execute(
            f"""SELECT name, id, version FROM factors
                WHERE name IN ({placeholders})
                ORDER BY version DESC""",
            factor_names,
        ).fetchall()
        result: dict[str, str] = {}
        for name, fid, _version in rows:
            if name not in result:
                result[name] = fid
        return result

    @staticmethod
    def _load_prices(
        tickers: list[str],
        start_date: str,
        end_date: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Load OHLCV price DataFrames from daily_bars."""
        conn = get_connection()
        placeholders = ",".join(f"'{t}'" for t in tickers)
        query = f"""
            SELECT ticker, date, open, high, low, close, volume
            FROM daily_bars
            WHERE ticker IN ({placeholders})
              AND date >= ? AND date <= ?
            ORDER BY date
        """
        df = conn.execute(query, [start_date, end_date]).fetchdf()
        if df.empty:
            empty = pd.DataFrame()
            return empty, empty, empty, empty, empty

        df["date"] = pd.to_datetime(df["date"])
        close_pivot = df.pivot(index="date", columns="ticker", values="close")
        open_pivot = df.pivot(index="date", columns="ticker", values="open")
        high_pivot = df.pivot(index="date", columns="ticker", values="high")
        low_pivot = df.pivot(index="date", columns="ticker", values="low")
        volume_pivot = df.pivot(index="date", columns="ticker", values="volume")
        return close_pivot, open_pivot, high_pivot, low_pivot, volume_pivot

    @staticmethod
    def _build_prices_multi(
        prices_close: pd.DataFrame,
        prices_open: pd.DataFrame,
        prices_high: pd.DataFrame,
        prices_low: pd.DataFrame,
        prices_volume: pd.DataFrame,
        tickers: list[str],
    ) -> pd.DataFrame:
        """Build a MultiIndex-column DataFrame with (field, ticker) columns."""
        frames = {}
        field_dfs = [
            ("close", prices_close),
            ("open", prices_open),
            ("high", prices_high),
            ("low", prices_low),
            ("volume", prices_volume),
        ]
        for field_name, df in field_dfs:
            for ticker in tickers:
                if ticker in df.columns:
                    frames[(field_name, ticker)] = df[ticker]

        if not frames:
            return pd.DataFrame()

        result = pd.DataFrame(frames)
        result.columns = pd.MultiIndex.from_tuples(
            result.columns, names=["field", "ticker"]
        )
        return result

    @staticmethod
    def _preload_prices(
        tickers: list[str],
        start: date,
        end: date,
        conn,
    ) -> dict[date, dict[str, tuple[float, float]]]:
        """Pre-load open and close prices for all tickers in a date range.

        Returns: {date: {ticker: (open, close)}}
        """
        if not tickers:
            return {}
        placeholders = ",".join(f"'{t}'" for t in tickers)
        rows = conn.execute(
            f"""SELECT date, ticker, open, close FROM daily_bars
                WHERE date >= ? AND date <= ? AND ticker IN ({placeholders})""",
            [start, end],
        ).fetchall()

        cache: dict[date, dict[str, tuple[float, float]]] = {}
        for r in rows:
            d = r[0] if isinstance(r[0], date) else date.fromisoformat(str(r[0]))
            if d not in cache:
                cache[d] = {}
            if r[2] and r[3]:
                cache[d][r[1]] = (float(r[2]), float(r[3]))
        return cache

    def _load_latest_state(
        self, session_id: str, initial_capital: float
    ) -> tuple[dict[str, dict], float]:
        """Load positions and cash from the last recorded day."""
        conn = get_connection()
        row = conn.execute(
            """SELECT positions_json, cash FROM paper_trading_daily
               WHERE session_id = ? ORDER BY date DESC LIMIT 1""",
            [session_id],
        ).fetchone()
        if row:
            positions = json.loads(row[0]) if isinstance(row[0], str) else (row[0] or {})
            cash = row[1]
        else:
            positions = {}
            cash = initial_capital
        return positions, cash

    @staticmethod
    def _execute_trades_cached(
        positions: dict[str, dict],
        cash: float,
        target_weights: dict[str, float],
        trade_date: date,
        cost_rate: float,
        price_cache: dict[date, dict[str, tuple[float, float]]],
    ) -> dict:
        """Simulate trade execution using cached prices."""
        day_prices = price_cache.get(trade_date, {})
        all_tickers = set(positions.keys()) | set(target_weights.keys())

        # Calculate current portfolio value at open
        portfolio_value = cash
        for ticker, pos in positions.items():
            prices = day_prices.get(ticker)
            if prices:
                portfolio_value += pos["shares"] * prices[0]  # open price

        if portfolio_value <= 0:
            return {"trades": [], "positions_after": positions, "cash_after": cash}

        trades: list[dict] = []
        new_positions = dict(positions)

        for ticker in all_tickers:
            old_shares = positions.get(ticker, {}).get("shares", 0.0)
            target_w = target_weights.get(ticker, 0.0)

            prices = day_prices.get(ticker)
            if not prices:
                continue
            price = prices[0]  # open price
            if not price or price <= 0:
                continue

            target_value = target_w * portfolio_value
            target_shares = target_value / price
            share_change = target_shares - old_shares

            if abs(share_change * price) < 1.0:
                continue

            trade_value = abs(share_change * price)
            trade_cost = trade_value * cost_rate
            cash -= trade_cost

            if share_change > 0:
                cash -= share_change * price
                old_value = old_shares * positions.get(ticker, {}).get("avg_price", price)
                new_value = old_value + share_change * price
                new_shares = old_shares + share_change
                avg_price = new_value / new_shares if new_shares > 0 else price
                new_positions[ticker] = {"shares": new_shares, "avg_price": round(avg_price, 4)}
                trades.append({
                    "ticker": ticker,
                    "action": "buy",
                    "shares": round(share_change, 4),
                    "price": round(price, 4),
                    "cost": round(trade_cost, 4),
                })
            else:
                cash += abs(share_change) * price
                remaining = old_shares + share_change
                if remaining > 0.01:
                    new_positions[ticker] = {
                        "shares": remaining,
                        "avg_price": positions.get(ticker, {}).get("avg_price", price),
                    }
                else:
                    new_positions.pop(ticker, None)
                trades.append({
                    "ticker": ticker,
                    "action": "sell",
                    "shares": round(abs(share_change), 4),
                    "price": round(price, 4),
                    "cost": round(trade_cost, 4),
                })

        return {
            "trades": trades,
            "positions_after": new_positions,
            "cash_after": round(cash, 2),
        }

    @staticmethod
    def _value_portfolio_cached(
        positions: dict[str, dict],
        cash: float,
        trade_date: date,
        price_cache: dict[date, dict[str, tuple[float, float]]],
    ) -> float:
        """Value portfolio at close prices using cached data."""
        if not positions:
            return cash

        day_prices = price_cache.get(trade_date, {})
        total = cash
        for ticker, pos in positions.items():
            prices = day_prices.get(ticker)
            if prices:
                total += pos["shares"] * prices[1]  # close price
            else:
                total += pos["shares"] * pos.get("avg_price", 0)
        return round(total, 2)

    def _build_portfolio_state(
        self,
        session_id: str,
        positions: dict[str, dict],
        cash: float,
        current_date: date,
    ) -> dict:
        """Build portfolio state dict for StrategyContext.

        Computes current_weights, holding_days, avg_entry_price, unrealized_pnl
        from positions and historical trades.

        Returns dict with keys: current_weights, holding_days, avg_entry_price, unrealized_pnl
        """
        if not positions:
            return {
                "current_weights": {},
                "holding_days": {},
                "avg_entry_price": {},
                "unrealized_pnl": {},
            }

        conn = get_connection()

        # Get latest close prices for current_date
        tickers = list(positions.keys())
        placeholders = ",".join(f"'{t}'" for t in tickers)
        price_rows = conn.execute(
            f"""SELECT ticker, close FROM daily_bars
                WHERE date = ? AND ticker IN ({placeholders})""",
            [current_date],
        ).fetchall()
        current_prices = {r[0]: float(r[1]) for r in price_rows if r[1]}

        # Calculate portfolio value
        portfolio_value = cash
        for ticker, pos in positions.items():
            price = current_prices.get(ticker, pos.get("avg_price", 0))
            portfolio_value += pos["shares"] * price

        # Build state dicts
        current_weights = {}
        avg_entry_price = {}
        unrealized_pnl = {}

        for ticker, pos in positions.items():
            shares = pos["shares"]
            entry_price = pos.get("avg_price", 0)
            current_price = current_prices.get(ticker, entry_price)

            position_value = shares * current_price
            current_weights[ticker] = position_value / portfolio_value if portfolio_value > 0 else 0
            avg_entry_price[ticker] = entry_price
            unrealized_pnl[ticker] = (current_price / entry_price - 1) if entry_price > 0 else 0

        # Calculate holding_days from trade history
        holding_days = self._calculate_holding_days(session_id, tickers, current_date, conn)

        return {
            "current_weights": current_weights,
            "holding_days": holding_days,
            "avg_entry_price": avg_entry_price,
            "unrealized_pnl": unrealized_pnl,
        }

    def _calculate_holding_days(
        self,
        session_id: str,
        tickers: list[str],
        current_date: date,
        conn,
    ) -> dict[str, int]:
        """Calculate holding days for each ticker from trade history.

        Returns dict[ticker -> days_held]
        """
        # Get all daily snapshots up to current_date
        rows = conn.execute(
            """SELECT date, trades_json FROM paper_trading_daily
               WHERE session_id = ? AND date <= ?
               ORDER BY date""",
            [session_id, current_date],
        ).fetchall()

        # Track first buy date for each ticker
        first_buy: dict[str, date] = {}
        last_sell: dict[str, date] = {}

        for row in rows:
            trade_date = row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0]))
            trades = json.loads(row[1]) if isinstance(row[1], str) else (row[1] or [])

            for t in trades:
                ticker = t.get("ticker")
                action = t.get("action")

                if action == "buy":
                    if ticker not in first_buy or ticker in last_sell:
                        # New position or re-entry after sell
                        first_buy[ticker] = trade_date
                        if ticker in last_sell:
                            del last_sell[ticker]
                elif action == "sell":
                    # Check if fully closed
                    last_sell[ticker] = trade_date

        # Calculate holding days
        holding_days = {}
        trading_days_list = get_trading_days(
            min(first_buy.values()) if first_buy else current_date,
            current_date
        )

        for ticker in tickers:
            if ticker in first_buy and ticker not in last_sell:
                # Count trading days from first buy to current
                entry_date = first_buy[ticker]
                days_held = len([d for d in trading_days_list if d >= entry_date and d <= current_date])
                holding_days[ticker] = max(1, days_held)

        return holding_days
        config_raw = row[4]
        if isinstance(config_raw, str):
            try:
                config_raw = json.loads(config_raw)
            except (json.JSONDecodeError, TypeError):
                config_raw = {}

        return {
            "id": row[0],
            "name": row[1],
            "strategy_id": row[2],
            "universe_group_id": row[3],
            "config": config_raw,
            "status": row[5],
            "start_date": str(row[6]) if row[6] else None,
            "current_date": str(row[7]) if row[7] else None,
            "initial_capital": row[8],
            "current_nav": row[9],
            "total_trades": row[10],
            "created_at": str(row[11]) if row[11] else None,
            "updated_at": str(row[12]) if row[12] else None,
            "strategy_name": row[13] if len(row) > 13 else None,
        }

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

log = get_logger(__name__)


class PaperTradingService:
    """Manage paper trading sessions and day-by-day advancement."""

    def __init__(self) -> None:
        self._signal_service = SignalService()
        self._strategy_service = StrategyService()
        self._group_service = GroupService()

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

    # ------------------------------------------------------------------
    # Day-by-day advancement
    # ------------------------------------------------------------------

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
          1. Generate signals using data up to T-1 (previous trading day)
          2. Execute trades at T's open price
          3. Value portfolio at T's close price
          4. Record daily snapshot

        Returns summary of the advancement.
        """
        session = self.get_session(session_id)
        if session["status"] != "active":
            raise ValueError(f"Session is {session['status']}, not active")

        # Determine which days to process
        current = session["current_date"]
        if current:
            if isinstance(current, str):
                current = date.fromisoformat(current)
            start_from = current + timedelta(days=1)
        else:
            start_from = date.fromisoformat(session["start_date"])

        if steps > 0:
            # Step mode: get enough trading days from start_from
            # Fetch a generous range then slice
            generous_end = start_from + timedelta(days=steps * 3 + 30)
            all_days = get_trading_days(start_from, generous_end)
            trading_days = all_days[:steps] if all_days else []
        else:
            if target_date:
                end = date.fromisoformat(target_date)
            else:
                end = get_latest_trading_day()
            if start_from > end:
                return {
                    "session_id": session_id,
                    "days_processed": 0,
                    "message": "Already up to date",
                }
            trading_days = get_trading_days(start_from, end)

        if not trading_days:
            return {
                "session_id": session_id,
                "days_processed": 0,
                "message": "No trading days in range",
            }

        conn = get_connection()

        # --- Data availability check ---
        # To advance to trade_date T, we need T's daily bars (open for
        # execution, close for valuation).  Query the latest bar date
        # and reject any trading days beyond it.
        latest_bar_row = conn.execute("SELECT MAX(date) FROM daily_bars").fetchone()
        if not latest_bar_row or not latest_bar_row[0]:
            raise ValueError("本地无行情数据，请先更新数据")
        latest_bar_date = (
            latest_bar_row[0]
            if isinstance(latest_bar_row[0], date)
            else date.fromisoformat(str(latest_bar_row[0]))
        )

        # Filter trading_days to those with data available
        days_with_data = [d for d in trading_days if d <= latest_bar_date]
        days_without = [d for d in trading_days if d > latest_bar_date]

        if not days_with_data:
            # None of the requested days have data
            first_needed = trading_days[0]
            raise ValueError(
                f"无法推进：本地数据截止到 {latest_bar_date}，"
                f"下一交易日 {first_needed} 尚无数据。"
                f"请先更新行情数据。"
            )

        # If step mode requested specific count but data covers fewer days,
        # only process what we have but warn in the response
        trading_days = days_with_data

        # Pre-load already processed dates to skip them
        processed = set()
        rows = conn.execute(
            "SELECT date FROM paper_trading_daily WHERE session_id = ?",
            [session_id],
        ).fetchall()
        for r in rows:
            processed.add(r[0] if isinstance(r[0], date) else date.fromisoformat(str(r[0])))
        trading_days = [d for d in trading_days if d not in processed]

        if not trading_days:
            return {
                "session_id": session_id,
                "days_processed": 0,
                "message": "Already up to date",
            }

        # Load config
        config = session.get("config") or {}
        capital = session["initial_capital"]
        commission_rate = config.get("commission_rate", 0.001)
        slippage_rate = config.get("slippage_rate", 0.001)
        max_positions = config.get("max_positions", 50)
        cost_rate = commission_rate + slippage_rate

        # Pre-load all prices for the entire date range
        tickers = self._group_service.get_group_tickers(session["universe_group_id"])
        price_cache = self._preload_prices(tickers, trading_days[0], trading_days[-1], conn)

        # Load last known portfolio state
        positions, cash = self._load_latest_state(session_id, capital)

        days_processed = 0
        total_new_trades = 0
        signal_errors = 0
        daily_snapshots: list[tuple] = []

        log.info(
            "paper_trading.advance_start",
            session_id=session_id,
            days_to_process=len(trading_days),
            range=f"{trading_days[0]}~{trading_days[-1]}",
        )

        for trade_date in trading_days:
            # Signal generation uses data up to T-1 (the previous trading
            # day).  Decisions are made at end of T-1 and executed at T's
            # open -- the standard T+1 model.
            signal_date = self._prev_trading_day(trade_date)

            # 1. Generate signals based on T-1 data
            try:
                signal_result = self._signal_service.generate_signals(
                    strategy_id=session["strategy_id"],
                    target_date=str(signal_date),
                    universe_group_id=session["universe_group_id"],
                )
                signals = signal_result.get("signals", [])
            except Exception:
                signal_errors += 1
                signals = []

            # 2. Build target weights from signals
            target_weights: dict[str, float] = {}
            for sig in signals:
                ticker = sig.get("ticker", "")
                weight = sig.get("target_weight", 0.0)
                if weight and weight > 0:
                    target_weights[ticker] = float(weight)

            # Enforce max positions
            if len(target_weights) > max_positions:
                sorted_tw = sorted(
                    target_weights.items(), key=lambda x: x[1], reverse=True
                )
                target_weights = dict(sorted_tw[:max_positions])

            # Normalize
            w_sum = sum(target_weights.values())
            if w_sum > 0:
                target_weights = {t: w / w_sum for t, w in target_weights.items()}

            # 3. Execute trades at this day's open price
            day_trades = self._execute_trades_cached(
                positions, cash, target_weights,
                trade_date, cost_rate, price_cache,
            )
            cash = day_trades["cash_after"]
            positions = day_trades["positions_after"]
            total_new_trades += len(day_trades["trades"])

            # 4. Value portfolio at close
            nav = self._value_portfolio_cached(positions, cash, trade_date, price_cache)

            # 5. Collect snapshot for batch insert
            daily_snapshots.append((
                session_id, trade_date, nav, cash,
                json.dumps(positions, default=str),
                json.dumps(day_trades["trades"], default=str),
            ))

            days_processed += 1

        # Batch insert all daily snapshots
        if daily_snapshots:
            conn.executemany(
                """INSERT OR REPLACE INTO paper_trading_daily
                   (session_id, date, nav, cash, positions_json, trades_json)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                daily_snapshots,
            )
            # Update session once
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

        # Generate signals using data through current_date (T),
        # producing the action plan for T+1.
        try:
            signal_result = self._signal_service.generate_signals(
                strategy_id=session["strategy_id"],
                target_date=str(current_d),
                universe_group_id=session["universe_group_id"],
            )
            signals = signal_result.get("signals", [])
        except Exception as exc:
            log.warning("paper_trading.signal_preview_error", error=str(exc))
            return {"signals": [], "action_plan": [], "target_date": str(target_date), "error": str(exc)}

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

        w_sum = sum(target_weights.values())
        if w_sum > 0:
            target_weights = {t: w / w_sum for t, w in target_weights.items()}

        # Build action plan: compare current positions vs target
        action_plan = []
        all_tickers = set(positions.keys()) | set(target_weights.keys())
        for ticker in sorted(all_tickers):
            current_shares = positions.get(ticker, {}).get("shares", 0.0)
            target_w = target_weights.get(ticker, 0.0)
            current_w = 0.0  # approximate
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

        return {
            "signals": signals,
            "action_plan": action_plan,
            "target_date": str(target_date),
        }

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
        """Return current positions for the latest date."""
        conn = get_connection()
        row = conn.execute(
            """SELECT positions_json, date FROM paper_trading_daily
               WHERE session_id = ? ORDER BY date DESC LIMIT 1""",
            [session_id],
        ).fetchone()
        if not row or not row[0]:
            return []
        positions = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        result = []
        for ticker, pos in positions.items():
            result.append({
                "ticker": ticker,
                "shares": pos.get("shares", 0),
                "avg_price": pos.get("avg_price", 0),
                "date": str(row[1]),
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

    @staticmethod
    def _session_row_to_dict(row) -> dict:
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

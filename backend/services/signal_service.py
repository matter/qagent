"""Signal generation service -- full pipeline from strategy to trade signals.

Orchestrates:
  strategy -> dependency validation -> factors -> model predictions -> signals -> persist
"""

from __future__ import annotations

import json
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime

import numpy as np
import pandas as pd

from backend.db import get_connection
from backend.logger import get_logger
from backend.services.factor_engine import FactorEngine
from backend.services.feature_service import FeatureService
from backend.services.group_service import GroupService
from backend.services.model_service import ModelService
from backend.services.strategy_service import StrategyService
from backend.strategies.base import StrategyContext
from backend.strategies.loader import load_strategy_from_code

log = get_logger(__name__)


class SignalService:
    """Generate, persist, and query trading signals."""

    def __init__(self) -> None:
        self._strategy_service = StrategyService()
        self._factor_engine = FactorEngine()
        self._feature_service = FeatureService()
        self._model_service = ModelService()
        self._group_service = GroupService()

    # ------------------------------------------------------------------
    # Signal generation
    # ------------------------------------------------------------------

    def generate_signals(
        self,
        strategy_id: str,
        target_date: str,
        universe_group_id: str,
    ) -> dict:
        """Full signal generation pipeline.

        Steps
        -----
        1.  Load strategy definition and instantiate.
        2.  Validate dependency chain (per requirements section 9.3).
        3.  Resolve universe tickers from group.
        4.  Load OHLCV data up to target_date.
        5.  Compute required factor values.
        6.  Run model predictions (if strategy uses models).
        7.  Build StrategyContext for target_date.
        8.  Call strategy.generate_signals(context).
        9.  Determine result_level based on dependency validation.
        10. Build dependency_snapshot.
        11. Save to signal_runs + signal_details tables.
        12. Return signal list with metadata.
        """
        # ---- 1. Load strategy ----
        strategy_def = self._strategy_service.get_strategy(strategy_id)
        strategy_instance = load_strategy_from_code(strategy_def["source_code"])

        log.info(
            "signal_service.start",
            strategy=strategy_def["name"],
            version=strategy_def["version"],
            target_date=target_date,
        )

        # ---- 2. Validate dependency chain ----
        validation = self._validate_dependency_chain(strategy_def, target_date, universe_group_id)

        # If pipeline is blocked (a critical step cannot proceed), raise
        if validation["blocked"]:
            raise ValueError(
                f"Signal generation blocked: {'; '.join(validation['errors'])}"
            )

        # ---- 3. Resolve universe tickers ----
        tickers = self._group_service.get_group_tickers(universe_group_id)
        if not tickers:
            raise ValueError(f"Universe group '{universe_group_id}' has no members")

        # ---- 4. Load OHLCV data up to target_date ----
        # Use a lookback window for factor warm-up (120 trading days ~ 6 months)
        conn = get_connection()
        lookback_query = """
            SELECT MIN(date) FROM (
                SELECT DISTINCT date FROM daily_bars
                WHERE date <= ?
                ORDER BY date DESC
                LIMIT 250
            )
        """
        row = conn.execute(lookback_query, [target_date]).fetchone()
        start_date = str(row[0]) if row and row[0] else target_date

        prices_close, prices_open, prices_high, prices_low, prices_volume = (
            self._load_prices(tickers, start_date, target_date)
        )
        if prices_close.empty:
            raise ValueError("No price data available for the given tickers and date range")

        # ---- 5. Bulk-load all factor data (strategy + model features) ----
        required_factors = strategy_def.get("required_factors", [])
        required_models = strategy_def.get("required_models", [])

        # Resolve strategy factor IDs
        strategy_factor_map = self._resolve_factor_ids(required_factors) if required_factors else {}
        all_factor_ids = set(strategy_factor_map.values())

        # Resolve model feature sets and collect all factor IDs
        model_fs_map: dict[str, tuple[str, dict[str, str], dict]] = {}
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
                model_fs_map[model_id] = (fs_id, fs_id_to_name, fs["preprocessing"])
            except Exception as exc:
                log.warning("signal_service.model_fs_failed", model_id=model_id, error=str(exc))

        # Bulk load all cached factor values in ONE query
        cached_by_id = self._factor_engine.load_cached_factors_bulk(
            list(all_factor_ids), tickers, start_date, target_date
        )

        # Build strategy factor_data from cache, fallback to compute
        factor_data: dict[str, pd.DataFrame] = {}
        for factor_name, factor_id in strategy_factor_map.items():
            if factor_id in cached_by_id and not cached_by_id[factor_id].empty:
                factor_data[factor_name] = cached_by_id[factor_id]
            else:
                try:
                    df = self._factor_engine.compute_factor(
                        factor_id, tickers, start_date, target_date
                    )
                    if not df.empty:
                        factor_data[factor_name] = df
                except Exception as exc:
                    log.warning(
                        "signal_service.factor_failed",
                        factor_name=factor_name,
                        error=str(exc),
                    )

        # ---- 6. Run model predictions using pre-computed features ----
        model_predictions: dict[str, pd.Series] = {}

        for model_id in required_models:
            if model_id not in model_fs_map:
                continue
            try:
                fs_id, fs_id_to_name, preprocessing = model_fs_map[model_id]

                # Assemble feature data from bulk cache
                feature_data_local: dict[str, pd.DataFrame] = {}
                for fid, fname in fs_id_to_name.items():
                    if fid in cached_by_id and not cached_by_id[fid].empty:
                        feature_data_local[fname] = cached_by_id[fid]
                    else:
                        try:
                            df = self._factor_engine.compute_factor(fid, tickers, start_date, target_date)
                            if not df.empty:
                                feature_data_local[fname] = df
                        except Exception:
                            pass

                # Apply preprocessing
                processed: dict[str, pd.DataFrame] = {}
                for fname, df in feature_data_local.items():
                    processed[fname] = self._feature_service._apply_preprocessing(df, preprocessing)

                preds = self._model_service.predict_with_features(
                    model_id=model_id,
                    feature_data=processed,
                    tickers=tickers,
                    date=target_date,
                )
                if not preds.empty:
                    model_predictions[model_id] = preds
            except Exception as exc:
                log.warning(
                    "signal_service.model_predict_failed",
                    model_id=model_id,
                    error=str(exc),
                )

        # ---- 7. Build StrategyContext ----
        prices_multi = self._build_prices_multi(
            prices_close, prices_open, prices_high, prices_low, prices_volume, tickers,
        )
        trade_ts = pd.Timestamp(target_date)

        context = StrategyContext(
            prices=prices_multi.loc[:trade_ts],
            factor_values=factor_data,
            model_predictions=model_predictions,
            current_date=trade_ts,
        )

        # ---- 8. Generate signals ----
        try:
            raw_signals = strategy_instance.generate_signals(context)
        except Exception as exc:
            raise ValueError(
                f"Strategy signal generation failed: {exc}"
            ) from exc

        # ---- 9. Determine result_level ----
        result_level = self._determine_result_level(validation)

        # ---- 10. Build dependency_snapshot ----
        dependency_snapshot = self._build_dependency_snapshot(
            strategy_def, required_factors, required_models, target_date, validation
        )

        # ---- 11. Save to DB ----
        run_id = uuid.uuid4().hex[:12]
        signal_records = self._save_signal_run(
            run_id=run_id,
            strategy_id=strategy_id,
            strategy_version=strategy_def.get("version", 1),
            target_date=target_date,
            universe_group_id=universe_group_id,
            result_level=result_level,
            dependency_snapshot=dependency_snapshot,
            raw_signals=raw_signals,
        )

        # ---- 12. Return results ----
        result = {
            "run_id": run_id,
            "strategy_id": strategy_id,
            "strategy_name": strategy_def["name"],
            "strategy_version": strategy_def.get("version", 1),
            "target_date": target_date,
            "universe_group_id": universe_group_id,
            "result_level": result_level,
            "signal_count": len(signal_records),
            "warnings": validation["warnings"],
            "signals": signal_records,
            "dependency_snapshot": dependency_snapshot,
        }

        log.info(
            "signal_service.done",
            run_id=run_id,
            signal_count=len(signal_records),
            result_level=result_level,
        )
        return result

    # ------------------------------------------------------------------
    # Diagnostic snapshot (lightweight, no DB persistence)
    # ------------------------------------------------------------------

    def diagnose_signals(
        self,
        strategy_id: str,
        target_date: str,
        universe_group_id: str,
        max_tickers: int = 0,
        focus_tickers: list[str] | None = None,
    ) -> dict:
        """Lightweight signal diagnosis for a given date.

        Runs the same pipeline as generate_signals but:
        - Does NOT save to DB
        - Returns intermediate diagnostics: model scores, factor values,
          candidate pool, final signals, and elimination reasons.

        Args:
            max_tickers: If > 0, randomly sample the universe to this size.
            focus_tickers: If provided, always include these tickers in the
                universe (even when sampling) and append a per-ticker
                diagnostic snapshot to the result.
            max_tickers: If > 0, randomly sample the universe to this size
                to keep computation fast on large pools.
        """
        # ---- 1. Load strategy ----
        strategy_def = self._strategy_service.get_strategy(strategy_id)
        strategy_instance = load_strategy_from_code(strategy_def["source_code"])

        # ---- 2. Resolve universe tickers ----
        tickers = self._group_service.get_group_tickers(universe_group_id)
        if not tickers:
            raise ValueError(f"Universe group '{universe_group_id}' has no members")

        full_universe_size = len(tickers)
        sampled = False
        if max_tickers > 0 and len(tickers) > max_tickers:
            import random
            sample_set = set(random.sample(tickers, max_tickers))
            # Always include focus_tickers in the universe
            if focus_tickers:
                sample_set.update(t for t in focus_tickers if t in set(tickers))
            tickers = sorted(sample_set)
            sampled = True
        elif focus_tickers:
            # Ensure focus_tickers are in the universe even without sampling
            ticker_set = set(tickers)
            for t in focus_tickers:
                if t not in ticker_set:
                    focus_tickers = [ft for ft in focus_tickers if ft in ticker_set]

        # ---- 3. Load OHLCV data up to target_date ----
        conn = get_connection()
        lookback_query = """
            SELECT MIN(date) FROM (
                SELECT DISTINCT date FROM daily_bars
                WHERE date <= ?
                ORDER BY date DESC
                LIMIT 250
            )
        """
        row = conn.execute(lookback_query, [target_date]).fetchone()
        start_date = str(row[0]) if row and row[0] else target_date

        prices_close, prices_open, prices_high, prices_low, prices_volume = (
            self._load_prices(tickers, start_date, target_date)
        )
        if prices_close.empty:
            raise ValueError("No price data available for the given tickers and date range")

        # Track tickers with price data on target_date
        trade_ts = pd.Timestamp(target_date)
        has_price = set()
        if trade_ts in prices_close.index:
            row_data = prices_close.loc[trade_ts]
            has_price = {t for t in tickers if t in row_data.index and pd.notna(row_data[t])}

        # ---- 4. Bulk-load all factor data (strategy + model features) ----
        required_factors = strategy_def.get("required_factors", [])
        required_models = strategy_def.get("required_models", [])

        strategy_factor_map = self._resolve_factor_ids(required_factors) if required_factors else {}
        all_factor_ids = set(strategy_factor_map.values())

        model_fs_map: dict[str, tuple[str, dict[str, str], dict]] = {}
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
                model_fs_map[model_id] = (fs_id, fs_id_to_name, fs["preprocessing"])
            except Exception as exc:
                log.warning("signal_service.diagnose_model_fs_failed", model_id=model_id, error=str(exc))

        cached_by_id = self._factor_engine.load_cached_factors_bulk(
            list(all_factor_ids), tickers, start_date, target_date
        )

        factor_data: dict[str, pd.DataFrame] = {}
        factor_snapshot: dict[str, dict] = {}

        for factor_name, factor_id in strategy_factor_map.items():
            try:
                if factor_id in cached_by_id and not cached_by_id[factor_id].empty:
                    df = cached_by_id[factor_id]
                else:
                    df = self._factor_engine.compute_factor(
                        factor_id, tickers, start_date, target_date
                    )
                if not df.empty:
                    factor_data[factor_name] = df
                    if trade_ts in df.index:
                        vals = df.loc[trade_ts].dropna()
                        factor_snapshot[factor_name] = {
                            "coverage": len(vals),
                            "mean": round(float(vals.mean()), 6) if len(vals) > 0 else None,
                            "std": round(float(vals.std()), 6) if len(vals) > 1 else None,
                            "top5": {t: round(float(v), 6) for t, v in vals.nlargest(5).items()},
                            "bottom5": {t: round(float(v), 6) for t, v in vals.nsmallest(5).items()},
                        }
            except Exception as exc:
                factor_snapshot[factor_name] = {"error": str(exc)}

        # ---- 5. Run model predictions using pre-computed features ----
        model_predictions: dict[str, pd.Series] = {}
        model_snapshot: dict[str, dict] = {}
        per_model_timeout = max(30, 600 // max(len(required_models), 1))

        for model_id in required_models:
            if model_id not in model_fs_map:
                model_snapshot[model_id] = {"error": "feature set not resolved"}
                continue
            try:
                fs_id, fs_id_to_name, preprocessing = model_fs_map[model_id]

                feature_data_local: dict[str, pd.DataFrame] = {}
                for fid, fname in fs_id_to_name.items():
                    if fid in cached_by_id and not cached_by_id[fid].empty:
                        feature_data_local[fname] = cached_by_id[fid]
                    else:
                        try:
                            df = self._factor_engine.compute_factor(fid, tickers, start_date, target_date)
                            if not df.empty:
                                feature_data_local[fname] = df
                        except Exception:
                            pass

                processed: dict[str, pd.DataFrame] = {}
                for fname, df in feature_data_local.items():
                    processed[fname] = self._feature_service._apply_preprocessing(df, preprocessing)

                with ThreadPoolExecutor(max_workers=1) as _pool:
                    fut = _pool.submit(
                        self._model_service.predict_with_features,
                        model_id=model_id,
                        feature_data=processed,
                        tickers=tickers,
                        date=target_date,
                    )
                    try:
                        preds = fut.result(timeout=per_model_timeout)
                    except FuturesTimeoutError:
                        model_snapshot[model_id] = {
                            "error": f"predict timed out ({per_model_timeout}s)"
                        }
                        log.warning(
                            "diagnose.model_timeout",
                            model_id=model_id,
                            timeout=per_model_timeout,
                        )
                        continue

                if not preds.empty:
                    model_predictions[model_id] = preds
                    model_snapshot[model_id] = {
                        "coverage": len(preds),
                        "mean": round(float(preds.mean()), 6),
                        "std": round(float(preds.std()), 6) if len(preds) > 1 else None,
                        "top10": {t: round(float(v), 6) for t, v in preds.nlargest(10).items()},
                        "bottom5": {t: round(float(v), 6) for t, v in preds.nsmallest(5).items()},
                    }
                else:
                    model_snapshot[model_id] = {"coverage": 0, "error": "empty predictions"}
            except Exception as exc:
                model_snapshot[model_id] = {"error": str(exc)}

        # ---- 6. Build StrategyContext & generate signals ----
        prices_multi = self._build_prices_multi(
            prices_close, prices_open, prices_high, prices_low, prices_volume, tickers,
        )

        context = StrategyContext(
            prices=prices_multi.loc[:trade_ts],
            factor_values=factor_data,
            model_predictions=model_predictions,
            current_date=trade_ts,
        )

        try:
            raw_signals = strategy_instance.generate_signals(context)
        except Exception as exc:
            raise ValueError(f"Strategy signal generation failed: {exc}") from exc

        # ---- 7. Build diagnostic output ----
        # Extract strategy-populated diagnostics (candidate_pool, gates, etc.)
        strategy_diagnostics = dict(context.diagnostics) if context.diagnostics else {}

        # Handle both dict and DataFrame signal formats
        signal_tickers: set[str] = set()
        signals_list: list[dict] = []

        if isinstance(raw_signals, dict):
            signal_tickers = set(raw_signals.keys())
            for ticker, data in sorted(
                raw_signals.items(),
                key=lambda x: -(x[1].get("target_weight", 0) if isinstance(x[1], dict) else 0),
            ):
                if isinstance(data, dict):
                    signals_list.append({
                        "ticker": ticker,
                        "signal": data.get("signal", "buy"),
                        "target_weight": round(data.get("target_weight", 0), 6),
                        "strength": round(data.get("strength", 0), 6),
                    })
        elif isinstance(raw_signals, pd.DataFrame) and not raw_signals.empty:
            signal_tickers = set(raw_signals.index)
            for ticker in raw_signals.index:
                row = raw_signals.loc[ticker]
                signals_list.append({
                    "ticker": str(ticker),
                    "signal": int(row.get("signal", 1)) if "signal" in row else 1,
                    "target_weight": round(float(row.get("weight", 0)), 6) if "weight" in row else 0,
                    "strength": round(float(row.get("strength", 0)), 6) if "strength" in row else 0,
                })
            signals_list.sort(key=lambda x: -x["target_weight"])

        eliminated = sorted(has_price - signal_tickers)

        # Build per-ticker elimination reasons
        no_price = sorted(set(tickers) - has_price)
        no_model_coverage: set[str] = set()
        if required_models and model_predictions:
            all_model_tickers = set()
            for preds in model_predictions.values():
                all_model_tickers.update(preds.index)
            no_model_coverage = has_price - all_model_tickers - signal_tickers

        strategy_filtered = sorted(
            has_price - signal_tickers - no_model_coverage
        )

        elimination_reasons: dict[str, list[str]] = {}
        if no_price:
            elimination_reasons["no_price_data"] = no_price[:30]
        if no_model_coverage:
            elimination_reasons["no_model_coverage"] = sorted(no_model_coverage)[:30]
        if strategy_filtered:
            elimination_reasons["strategy_filtered"] = strategy_filtered[:30]

        # Extract candidate_pool and gates from strategy_diagnostics if available
        candidate_pool = strategy_diagnostics.get(
            "candidate_pool",
            sorted(has_price)[:100],
        )
        gates = strategy_diagnostics.get("gates", {})

        # Add model quantiles to model_snapshot
        for mid, snap in model_snapshot.items():
            if mid in model_predictions and "error" not in snap:
                preds = model_predictions[mid]
                for ticker in list(signal_tickers)[:20]:
                    if ticker in preds.index:
                        rank = (preds < preds[ticker]).sum() / max(len(preds) - 1, 1)
                        snap.setdefault("quantiles", {})[ticker] = round(float(rank), 4)

        # Build per-ticker focus snapshot for targeted diagnosis
        focus_snapshot: list[dict] = []
        if focus_tickers:
            candidate_set = set(candidate_pool) if isinstance(candidate_pool, (list, set)) else set()
            for ft in focus_tickers:
                snap: dict = {"ticker": ft}

                # Price availability
                snap["has_price"] = ft in has_price

                # Elimination reason
                if ft in signal_tickers:
                    snap["status"] = "in_signal"
                elif ft not in has_price:
                    snap["status"] = "no_price_data"
                elif ft in no_model_coverage:
                    snap["status"] = "no_model_coverage"
                elif ft in candidate_set:
                    snap["status"] = "in_candidate_but_filtered"
                else:
                    snap["status"] = "strategy_filtered"

                # In candidate pool?
                snap["in_candidate_pool"] = ft in candidate_set

                # Signal weight if selected
                for sig in signals_list:
                    if sig["ticker"] == ft:
                        snap["signal"] = sig
                        break

                # Model scores + percentile rank in full pool
                snap["model_scores"] = {}
                for mid, preds in model_predictions.items():
                    if ft in preds.index:
                        score = float(preds[ft])
                        rank_pct = float((preds <= score).sum()) / max(len(preds), 1)
                        snap["model_scores"][mid] = {
                            "score": round(score, 6),
                            "percentile": round(rank_pct, 4),
                            "rank": int((preds > score).sum()) + 1,
                            "total": len(preds),
                        }
                    else:
                        snap["model_scores"][mid] = {"error": "no_coverage"}

                # Factor values + cross-sectional rank
                snap["factor_values"] = {}
                for fname, df in factor_data.items():
                    if trade_ts in df.index and ft in df.columns:
                        val = df.loc[trade_ts, ft]
                        if pd.notna(val):
                            row_vals = df.loc[trade_ts].dropna()
                            rank_pct = float((row_vals <= val).sum()) / max(len(row_vals), 1)
                            snap["factor_values"][fname] = {
                                "value": round(float(val), 6),
                                "percentile": round(rank_pct, 4),
                                "rank": int((row_vals > val).sum()) + 1,
                                "total": len(row_vals),
                            }
                        else:
                            snap["factor_values"][fname] = {"error": "nan"}
                    else:
                        snap["factor_values"][fname] = {"error": "no_data"}

                # Gates detail from strategy diagnostics
                per_ticker_gates = gates.get("per_ticker", {}) if isinstance(gates, dict) else {}
                if ft in per_ticker_gates:
                    snap["gates"] = per_ticker_gates[ft]

                focus_snapshot.append(snap)

        return {
            "strategy_id": strategy_id,
            "strategy_name": strategy_def["name"],
            "target_date": target_date,
            "universe_group_id": universe_group_id,
            "universe_size": full_universe_size,
            "sampled": sampled,
            "tickers_processed": len(tickers),
            "has_price_count": len(has_price),
            "signal_count": len(signals_list),
            "eliminated_count": len(eliminated),
            "signals": signals_list,
            "candidate_pool": candidate_pool if isinstance(candidate_pool, list) else sorted(candidate_pool)[:100],
            "elimination_reasons": elimination_reasons,
            "eliminated_tickers": eliminated[:50],
            "gates": gates,
            "model_diagnostics": model_snapshot,
            "factor_diagnostics": factor_snapshot,
            "strategy_diagnostics": {
                k: v for k, v in strategy_diagnostics.items()
                if k not in ("candidate_pool", "gates")
            },
            "focus_ticker_snapshots": focus_snapshot if focus_snapshot else None,
        }


    def list_signal_runs(
        self, strategy_id: str | None = None, limit: int = 50
    ) -> list[dict]:
        """List signal runs, optionally filtered by strategy_id."""
        conn = get_connection()
        if strategy_id:
            rows = conn.execute(
                """SELECT id, strategy_id, strategy_version, target_date,
                          universe_group_id, result_level, signal_count, created_at
                   FROM signal_runs
                   WHERE strategy_id = ?
                   ORDER BY created_at DESC
                   LIMIT ?""",
                [strategy_id, limit],
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT id, strategy_id, strategy_version, target_date,
                          universe_group_id, result_level, signal_count, created_at
                   FROM signal_runs
                   ORDER BY created_at DESC
                   LIMIT ?""",
                [limit],
            ).fetchall()

        return [
            {
                "id": r[0],
                "strategy_id": r[1],
                "strategy_version": r[2],
                "target_date": str(r[3]) if r[3] else None,
                "universe_group_id": r[4],
                "result_level": r[5],
                "signal_count": r[6],
                "created_at": str(r[7]) if r[7] else None,
            }
            for r in rows
        ]

    def get_signal_run(self, run_id: str) -> dict:
        """Return a signal run with its detail entries."""
        conn = get_connection()
        row = conn.execute(
            """SELECT id, strategy_id, strategy_version, target_date,
                      universe_group_id, result_level, dependency_snapshot,
                      signal_count, created_at
               FROM signal_runs
               WHERE id = ?""",
            [run_id],
        ).fetchone()

        if row is None:
            raise ValueError(f"Signal run {run_id} not found")

        details = self.get_signal_details(run_id)

        return {
            "id": row[0],
            "strategy_id": row[1],
            "strategy_version": row[2],
            "target_date": str(row[3]) if row[3] else None,
            "universe_group_id": row[4],
            "result_level": row[5],
            "dependency_snapshot": _parse_json(row[6]),
            "signal_count": row[7],
            "created_at": str(row[8]) if row[8] else None,
            "signals": details,
        }

    def get_signal_details(self, run_id: str) -> list[dict]:
        """Return signal detail entries for a run."""
        conn = get_connection()
        rows = conn.execute(
            """SELECT run_id, ticker, signal, target_weight, strength
               FROM signal_details
               WHERE run_id = ?
               ORDER BY strength DESC""",
            [run_id],
        ).fetchall()

        return [
            {
                "ticker": r[1],
                "signal": r[2],
                "target_weight": r[3],
                "strength": r[4],
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Dependency chain validation (per requirements section 9.3)
    # ------------------------------------------------------------------

    def _validate_dependency_chain(
        self, strategy_def: dict, target_date: str, universe_group_id: str | None = None
    ) -> dict:
        """Validate the full dependency chain before signal generation.

        Returns a dict with:
            blocked: bool - if True, signal generation should be aborted
            errors: list[str] - critical errors that block generation
            warnings: list[str] - non-blocking issues
            strategy_status: str
            factor_statuses: dict[name -> status]
            model_statuses: dict[id -> status]
            data_fresh: bool
        """
        errors: list[str] = []
        warnings: list[str] = []
        blocked = False

        # -- Strategy status check --
        strategy_status = strategy_def.get("status", "draft")

        if strategy_status == "draft":
            warnings.append(
                f"Strategy '{strategy_def['name']}' is in draft status; "
                "signals will be marked as exploratory"
            )
        elif strategy_status == "archived":
            warnings.append(
                f"Strategy '{strategy_def['name']}' is archived; "
                "signals will be marked as exploratory"
            )

        # -- Factor status checks --
        conn = get_connection()
        required_factors = strategy_def.get("required_factors", [])
        factor_statuses: dict[str, str] = {}

        for factor_name in required_factors:
            row = conn.execute(
                """SELECT id, status FROM factors
                   WHERE name = ?
                   ORDER BY version DESC
                   LIMIT 1""",
                [factor_name],
            ).fetchone()
            if row is None:
                errors.append(f"Required factor '{factor_name}' not found")
                blocked = True
            else:
                factor_statuses[factor_name] = row[1]
                if row[1] in ("draft",):
                    warnings.append(
                        f"Factor '{factor_name}' is in draft status"
                    )
                elif row[1] == "archived":
                    warnings.append(
                        f"Factor '{factor_name}' is archived"
                    )

        # -- Model status checks --
        required_models = strategy_def.get("required_models", [])
        model_statuses: dict[str, str] = {}

        for model_id in required_models:
            row = conn.execute(
                "SELECT id, status FROM models WHERE id = ?",
                [model_id],
            ).fetchone()
            if row is None:
                errors.append(f"Required model '{model_id}' not found")
                blocked = True
            else:
                model_statuses[model_id] = row[1]
                if row[1] in ("draft",):
                    warnings.append(
                        f"Model '{model_id}' is in draft status"
                    )

        # -- Data freshness check --
        data_fresh = True
        latest_bar = conn.execute(
            "SELECT MAX(date) FROM daily_bars"
        ).fetchone()
        if latest_bar and latest_bar[0]:
            latest_date = str(latest_bar[0])
            if latest_date < target_date:
                data_fresh = False
                warnings.append(
                    f"Data is stale: latest bar date is {latest_date}, "
                    f"target date is {target_date}. Signals based on stale data."
                )
        else:
            data_fresh = False
            warnings.append("No price data available in database")

        # -- Universe coverage check --
        # Verify that the target_date has sufficient data across the universe
        universe_coverage = 1.0
        if universe_group_id:
            from backend.services.group_service import GroupService
            group_svc = GroupService()
            try:
                universe_tickers = group_svc.get_group_tickers(universe_group_id)
                if universe_tickers:
                    placeholders = ",".join(f"'{t}'" for t in universe_tickers)
                    covered = conn.execute(
                        f"SELECT COUNT(DISTINCT ticker) FROM daily_bars "
                        f"WHERE ticker IN ({placeholders}) AND date = ?",
                        [target_date],
                    ).fetchone()[0]
                    universe_coverage = covered / len(universe_tickers)
                    if universe_coverage < 0.95:
                        warnings.append(
                            f"Universe coverage is low: {covered}/{len(universe_tickers)} "
                            f"({universe_coverage:.0%}) tickers have data for {target_date}. "
                            f"Signals may be biased toward the available subset."
                        )
                    if universe_coverage < 0.5:
                        errors.append(
                            f"Universe coverage too low: only {universe_coverage:.0%} "
                            f"of tickers have data for {target_date}. "
                            f"Wait for data update to complete."
                        )
                        blocked = True
            except Exception:
                pass  # group lookup failure should not block validation

        return {
            "blocked": blocked,
            "errors": errors,
            "warnings": warnings,
            "strategy_status": strategy_status,
            "factor_statuses": factor_statuses,
            "model_statuses": model_statuses,
            "data_fresh": data_fresh,
            "universe_coverage": universe_coverage,
        }

    def _determine_result_level(self, validation: dict) -> str:
        """Determine the result_level based on dependency validation.

        Rules (from requirements section 2.5 and 9.3):
        - If any dependency is draft or archived -> exploratory
        - If data is stale -> exploratory + warning
        - If strategy is not published -> exploratory
        - If all published + fresh data -> formal
          (but still exploratory with yfinance data source, per section 2.5)
        """
        # Check strategy status
        if validation["strategy_status"] not in ("validated", "published"):
            return "exploratory"

        # Check factor statuses
        for status in validation["factor_statuses"].values():
            if status not in ("validated", "published", "active"):
                return "exploratory"

        # Check model statuses
        for status in validation["model_statuses"].values():
            if status not in ("validated", "published", "trained"):
                return "exploratory"

        # Check data freshness
        if not validation["data_fresh"]:
            return "exploratory"

        # Even if all checks pass, yfinance data means exploratory
        # (per requirements section 2.5: "first phase uses yfinance,
        #  all results default to exploratory")
        return "exploratory"

    def _build_dependency_snapshot(
        self,
        strategy_def: dict,
        required_factors: list[str],
        required_models: list[str],
        target_date: str,
        validation: dict,
    ) -> dict:
        """Build a snapshot of all dependencies used for this signal run."""
        conn = get_connection()

        # Factor versions
        factor_snapshots = []
        for factor_name in required_factors:
            row = conn.execute(
                """SELECT id, version, status FROM factors
                   WHERE name = ?
                   ORDER BY version DESC
                   LIMIT 1""",
                [factor_name],
            ).fetchone()
            if row:
                factor_snapshots.append({
                    "name": factor_name,
                    "id": row[0],
                    "version": row[1],
                    "status": row[2],
                })

        # Model snapshots
        model_snapshots = []
        for model_id in required_models:
            row = conn.execute(
                "SELECT id, name, status FROM models WHERE id = ?",
                [model_id],
            ).fetchone()
            if row:
                model_snapshots.append({
                    "id": row[0],
                    "name": row[1],
                    "status": row[2],
                })

        # Data status
        latest_bar = conn.execute("SELECT MAX(date) FROM daily_bars").fetchone()
        data_status = {
            "latest_bar_date": str(latest_bar[0]) if latest_bar and latest_bar[0] else None,
            "target_date": target_date,
            "data_fresh": validation["data_fresh"],
        }

        return {
            "strategy": {
                "id": strategy_def["id"],
                "name": strategy_def["name"],
                "version": strategy_def.get("version", 1),
                "status": strategy_def.get("status", "draft"),
            },
            "factors": factor_snapshots,
            "models": model_snapshots,
            "data_status": data_status,
            "warnings": validation["warnings"],
        }

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _save_signal_run(
        self,
        run_id: str,
        strategy_id: str,
        strategy_version: int,
        target_date: str,
        universe_group_id: str,
        result_level: str,
        dependency_snapshot: dict,
        raw_signals: pd.DataFrame,
    ) -> list[dict]:
        """Persist signal run and detail records. Returns list of signal dicts."""
        conn = get_connection()
        now = datetime.utcnow()

        # Build signal records from raw_signals DataFrame
        # raw_signals has index=ticker, columns=[signal, weight, strength]
        signal_records: list[dict] = []

        if not raw_signals.empty:
            for ticker in raw_signals.index:
                row = raw_signals.loc[ticker]
                sig = int(row.get("signal", 0))
                weight = float(row.get("weight", 0.0))
                strength = float(row.get("strength", 0.0))
                signal_records.append({
                    "ticker": str(ticker),
                    "signal": sig,
                    "target_weight": weight,
                    "strength": strength,
                })

        signal_count = len(signal_records)

        # Insert signal_runs
        conn.execute(
            """INSERT INTO signal_runs
               (id, strategy_id, strategy_version, target_date,
                universe_group_id, result_level, dependency_snapshot,
                signal_count, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                run_id,
                strategy_id,
                strategy_version,
                target_date,
                universe_group_id,
                result_level,
                json.dumps(dependency_snapshot, default=str),
                signal_count,
                now,
            ],
        )

        # Insert signal_details
        for rec in signal_records:
            conn.execute(
                """INSERT INTO signal_details
                   (run_id, ticker, signal, target_weight, strength)
                   VALUES (?, ?, ?, ?, ?)""",
                [
                    run_id,
                    rec["ticker"],
                    rec["signal"],
                    rec["target_weight"],
                    rec["strength"],
                ],
            )

        log.info(
            "signal_service.saved",
            run_id=run_id,
            signal_count=signal_count,
        )
        return signal_records

    # ------------------------------------------------------------------
    # Internal helpers (reused from BacktestService pattern)
    # ------------------------------------------------------------------

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
        for name in factor_names:
            if name not in result:
                log.warning("signal_service.factor_not_found", name=name)
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


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _parse_json(raw) -> dict | list:
    """Safely parse a JSON column value."""
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return {}
    return raw if raw else {}

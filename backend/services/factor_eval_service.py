"""Factor evaluation engine – compute IC, group returns, turnover, and coverage."""

from __future__ import annotations

import copy
import json
import uuid
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from backend.db import get_connection
from backend.logger import get_logger
from backend.services.factor_engine import FactorEngine
from backend.services.factor_service import FactorService
from backend.services.group_service import GroupService
from backend.services.label_service import LabelService
from backend.services.market_context import normalize_market
from backend.time_utils import utc_now_naive

log = get_logger(__name__)

# Minimum number of stocks on a date to compute cross-sectional metrics.
_MIN_STOCKS_FOR_IC = 10


class FactorEvalService:
    """Evaluate factor predictive power against a label across a universe."""

    def __init__(self) -> None:
        self._factor_engine = FactorEngine()
        self._factor_service = FactorService()
        self._label_service = LabelService()
        self._group_service = GroupService()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def evaluate_factor(
        self,
        factor_id: str,
        label_id: str,
        universe_group_id: str,
        start_date: str,
        end_date: str,
        market: str | None = None,
        stage_domain_write: Any | None = None,
    ) -> dict:
        """Run a full factor evaluation and persist the results.

        Steps:
            1. Resolve universe tickers
            2. Compute factor values
            3. Compute label values
            4. Align factor + label
            5. Calculate IC, group returns, turnover, coverage
            6. Persist to factor_eval_results table

        Returns:
            Full evaluation result dict.
        """
        resolved_market = normalize_market(market)
        log.info(
            "factor_eval.start",
            market=resolved_market,
            factor_id=factor_id,
            label_id=label_id,
            universe=universe_group_id,
            start=start_date,
            end=end_date,
        )

        # --- 1. Resolve universe ---
        tickers = self._group_service.get_group_tickers(universe_group_id, market=resolved_market)
        if not tickers:
            raise ValueError(
                f"Universe group '{universe_group_id}' has no members"
            )
        group_info = self._group_service.get_group(universe_group_id, market=resolved_market)
        universe_name = group_info.get("name", universe_group_id)

        # --- 2. Compute factor values ---
        factor_df = self._factor_engine.compute_factor(
            factor_id, tickers, start_date, end_date, market=resolved_market
        )
        if factor_df.empty:
            tickers_with_data = 0
            log.warning(
                "factor_eval.no_factor_data",
                factor_id=factor_id,
                universe_size=len(tickers),
                tickers_with_data=tickers_with_data,
                start=start_date,
                end=end_date,
            )
            raise ValueError(
                f"Factor computation produced no data. "
                f"Universe had {len(tickers)} tickers, date range {start_date} to {end_date}. "
                f"Check that daily_bars has data for this period."
            )
        else:
            tickers_with_data = factor_df.notna().any().sum()
            log.info(
                "factor_eval.factor_data",
                universe_size=len(tickers),
                tickers_with_data=int(tickers_with_data),
                dates=len(factor_df),
            )

        factor_def = self._factor_service.get_factor(factor_id, market=resolved_market)

        # --- 3. Compute label values ---
        label_long = self._label_service.compute_label_values_cached(
            label_id, tickers, start_date, end_date, market=resolved_market
        )
        if label_long.empty:
            raise ValueError(
                f"Label computation produced no data. "
                f"Universe had {len(tickers)} tickers, date range {start_date} to {end_date}."
            )

        label_def = self._label_service.get_label(label_id, market=resolved_market)

        # Pivot label to wide format (date x ticker)
        label_df = label_long.pivot(index="date", columns="ticker", values="label_value")
        label_df.index = pd.to_datetime(label_df.index)

        # --- 4. Align factor and label ---
        common_dates = factor_df.index.intersection(label_df.index)
        common_tickers = list(set(factor_df.columns) & set(label_df.columns))

        if len(common_dates) == 0 or len(common_tickers) == 0:
            raise ValueError(
                f"No overlapping dates/tickers between factor and label data. "
                f"Factor dates: {len(factor_df.index)}, label dates: {len(label_df.index)}, "
                f"factor tickers: {len(factor_df.columns)}, label tickers: {len(label_df.columns)}."
            )

        factor_aligned = factor_df.loc[common_dates, common_tickers].sort_index()
        label_aligned = label_df.loc[common_dates, common_tickers].sort_index()

        log.info(
            "factor_eval.aligned",
            dates=len(common_dates),
            tickers=len(common_tickers),
            universe_total=len(tickers),
        )

        # --- 5. Calculate metrics ---
        ic_series = self._compute_ic_series(factor_aligned, label_aligned)
        group_returns = self._compute_group_returns(factor_aligned, label_aligned)
        turnover = self._compute_turnover(factor_aligned)
        coverage = self._compute_coverage(factor_aligned)

        # --- Summary stats ---
        ic_values = [item["ic"] for item in ic_series if item["ic"] is not None]
        if ic_values:
            ic_mean = float(np.mean(ic_values))
            ic_std = float(np.std(ic_values, ddof=1)) if len(ic_values) > 1 else 0.0
            ir = ic_mean / ic_std if ic_std > 0 else 0.0
            ic_win_rate = float(np.mean([1.0 if v > 0 else 0.0 for v in ic_values]))
        else:
            ic_mean = ic_std = ir = ic_win_rate = 0.0

        # Long-short annual return
        ls_returns = group_returns["groups"].get("long_short", [])
        if len(ls_returns) > 1:
            total_ls_return = ls_returns[-1]
            n_periods = len(ls_returns)
            # Approximate annualization (assuming ~252 trading days)
            years = n_periods / 252.0
            if years > 0 and total_ls_return > -1:
                long_short_annual = (1 + total_ls_return) ** (1 / years) - 1
            else:
                long_short_annual = 0.0
        else:
            long_short_annual = 0.0

        summary = {
            "ic_mean": round(ic_mean, 6),
            "ic_std": round(ic_std, 6),
            "ir": round(ir, 4),
            "ic_win_rate": round(ic_win_rate, 4),
            "long_short_annual_return": round(float(long_short_annual), 6),
            "turnover": round(float(turnover), 4),
            "coverage": round(float(coverage), 4),
        }

        # --- Build result dict ---
        result = {
            "factor_id": factor_id,
            "market": resolved_market,
            "factor_name": factor_def["name"],
            "label_id": label_id,
            "label_name": label_def["name"],
            "universe": universe_name,
            "date_range": {"start": start_date, "end": end_date},
            "summary": summary,
            "ic_series": ic_series,
            "group_returns": group_returns,
        }

        # --- 6. Persist ---
        eval_id = uuid.uuid4().hex[:12]
        result["id"] = eval_id
        save_payload = {
            "eval_id": eval_id,
            "factor_id": factor_id,
            "label_id": label_id,
            "universe_group_id": universe_group_id,
            "start_date": start_date,
            "end_date": end_date,
            "summary": copy.deepcopy(summary),
            "ic_series": copy.deepcopy(ic_series),
            "group_returns": copy.deepcopy(group_returns),
            "market": resolved_market,
        }
        if callable(stage_domain_write):
            stage_domain_write(
                "factor_eval_results",
                {
                    "id": eval_id,
                    "market": resolved_market,
                    "factor_id": factor_id,
                    "label_id": label_id,
                },
                commit=lambda conn=None, payload=save_payload: self._save_result(
                    **payload,
                    conn=conn,
                ),
            )
        else:
            self._save_result(**save_payload)

        log.info("factor_eval.done", eval_id=eval_id, ic_mean=summary["ic_mean"])
        return result

    def list_evaluations(self, factor_id: str, market: str | None = None) -> list[dict]:
        """List all evaluation results for a factor."""
        resolved_market = normalize_market(market)
        conn = get_connection()
        rows = conn.execute(
            """SELECT id, market, factor_id, label_id, universe_group_id,
                      start_date, end_date, summary, created_at
               FROM factor_eval_results
               WHERE market = ? AND factor_id = ?
               ORDER BY created_at DESC""",
            [resolved_market, factor_id],
        ).fetchall()

        results = []
        for r in rows:
            summary_raw = r[7]
            if isinstance(summary_raw, str):
                try:
                    summary_parsed = json.loads(summary_raw)
                except (json.JSONDecodeError, TypeError):
                    summary_parsed = {}
            else:
                summary_parsed = summary_raw if summary_raw else {}

            results.append({
                "id": r[0],
                "market": r[1],
                "factor_id": r[2],
                "label_id": r[3],
                "universe_group_id": r[4],
                "start_date": str(r[5]) if r[5] else None,
                "end_date": str(r[6]) if r[6] else None,
                "summary": summary_parsed,
                "created_at": str(r[8]) if r[8] else None,
            })
        return results

    def list_all_evaluations(self, market: str | None = None) -> list[dict]:
        """List all evaluation results across all factors with factor names.

        Uses a single JOIN query instead of N per-factor queries.
        """
        resolved_market = normalize_market(market)
        conn = get_connection()
        rows = conn.execute(
            """SELECT e.id, e.market, e.factor_id, f.name AS factor_name,
                      e.label_id, e.universe_group_id,
                      e.start_date, e.end_date, e.summary, e.created_at
               FROM factor_eval_results e
               JOIN factors f ON f.market = e.market AND f.id = e.factor_id
               WHERE e.market = ?
               ORDER BY e.created_at DESC""",
            [resolved_market],
        ).fetchall()

        results = []
        for r in rows:
            summary_raw = r[8]
            if isinstance(summary_raw, str):
                try:
                    summary_parsed = json.loads(summary_raw)
                except (json.JSONDecodeError, TypeError):
                    summary_parsed = {}
            else:
                summary_parsed = summary_raw if summary_raw else {}

            results.append({
                "id": r[0],
                "market": r[1],
                "factor_id": r[2],
                "factor_name": r[3],
                "label_id": r[4],
                "universe_group_id": r[5],
                "start_date": str(r[6]) if r[6] else None,
                "end_date": str(r[7]) if r[7] else None,
                "summary": summary_parsed,
                "created_at": str(r[9]) if r[9] else None,
            })
        return results

    def get_evaluation(self, eval_id: str, market: str | None = None) -> dict:
        """Get a specific evaluation result with full detail."""
        resolved_market = normalize_market(market)
        conn = get_connection()
        row = conn.execute(
            """SELECT id, market, factor_id, label_id, universe_group_id,
                      start_date, end_date, summary, ic_series,
                      group_returns, created_at
               FROM factor_eval_results
               WHERE id = ? AND market = ?""",
            [eval_id, resolved_market],
        ).fetchone()

        if row is None:
            raise ValueError(f"Evaluation {eval_id} not found")

        def _parse_json(raw):
            if isinstance(raw, str):
                try:
                    return json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    return {}
            return raw if raw else {}

        return {
            "id": row[0],
            "market": row[1],
            "factor_id": row[2],
            "label_id": row[3],
            "universe_group_id": row[4],
            "start_date": str(row[5]) if row[5] else None,
            "end_date": str(row[6]) if row[6] else None,
            "summary": _parse_json(row[7]),
            "ic_series": _parse_json(row[8]),
            "group_returns": _parse_json(row[9]),
            "created_at": str(row[10]) if row[10] else None,
        }

    # ------------------------------------------------------------------
    # IC computation
    # ------------------------------------------------------------------

    def _compute_ic_series(
        self,
        factor_df: pd.DataFrame,
        label_df: pd.DataFrame,
    ) -> list[dict]:
        """Compute cross-sectional Spearman rank IC for each date.

        For each date, correlate factor values across stocks with label values.
        """
        ic_list: list[dict] = []

        for dt in factor_df.index:
            fvals = factor_df.loc[dt].dropna()
            lvals = label_df.loc[dt].dropna()

            # Intersect tickers with valid data on this date
            common = fvals.index.intersection(lvals.index)
            if len(common) < _MIN_STOCKS_FOR_IC:
                ic_list.append({
                    "date": str(dt.date()) if hasattr(dt, "date") else str(dt),
                    "ic": None,
                })
                continue

            f = fvals.loc[common].values
            l = lvals.loc[common].values

            # Skip if constant arrays (correlation undefined)
            if np.std(f) == 0 or np.std(l) == 0:
                ic_list.append({
                    "date": str(dt.date()) if hasattr(dt, "date") else str(dt),
                    "ic": None,
                })
                continue

            corr, _ = spearmanr(f, l)
            ic_list.append({
                "date": str(dt.date()) if hasattr(dt, "date") else str(dt),
                "ic": round(float(corr), 6) if not np.isnan(corr) else None,
            })

        return ic_list

    # ------------------------------------------------------------------
    # Group returns computation
    # ------------------------------------------------------------------

    def _compute_group_returns(
        self,
        factor_df: pd.DataFrame,
        label_df: pd.DataFrame,
        n_groups: int = 5,
    ) -> dict:
        """Compute quantile group returns.

        For each date, sort stocks into n_groups quantiles by factor value,
        then compute equal-weighted forward return for each group.
        Returns cumulative return series per group plus long-short (G5 - G1).
        """
        dates = sorted(factor_df.index)
        group_daily_returns: dict[str, list[float]] = {
            f"G{i+1}": [] for i in range(n_groups)
        }
        group_daily_returns["long_short"] = []
        valid_dates: list[str] = []

        for dt in dates:
            fvals = factor_df.loc[dt].dropna()
            lvals = label_df.loc[dt].dropna()

            common = fvals.index.intersection(lvals.index)
            if len(common) < n_groups * 2:
                # Not enough stocks to form meaningful groups
                continue

            fv = fvals.loc[common]
            lv = lvals.loc[common]

            # Assign quantile groups (1-based)
            try:
                groups = pd.qcut(fv, n_groups, labels=False, duplicates="drop") + 1
            except ValueError:
                # qcut may fail if too many duplicates
                continue

            # If qcut dropped groups due to duplicates, skip this date
            unique_groups = groups.dropna().unique()
            if len(unique_groups) < n_groups:
                continue

            valid_dates.append(
                str(dt.date()) if hasattr(dt, "date") else str(dt)
            )

            for g in range(1, n_groups + 1):
                mask = groups == g
                group_tickers = mask[mask].index
                if len(group_tickers) > 0:
                    mean_ret = float(lv.loc[group_tickers].mean())
                else:
                    mean_ret = 0.0
                group_daily_returns[f"G{g}"].append(mean_ret)

            # Long-short: G_n (top) minus G1 (bottom)
            g_top = group_daily_returns[f"G{n_groups}"][-1]
            g_bottom = group_daily_returns["G1"][-1]
            group_daily_returns["long_short"].append(g_top - g_bottom)

        # Convert daily returns to cumulative returns
        group_cumulative: dict[str, list[float]] = {}
        for group_name, daily_rets in group_daily_returns.items():
            if not daily_rets:
                group_cumulative[group_name] = []
                continue
            cum = []
            cumval = 0.0
            for r in daily_rets:
                cumval = (1 + cumval) * (1 + r) - 1
                cum.append(round(cumval, 6))
            group_cumulative[group_name] = cum

        return {
            "dates": valid_dates,
            "groups": group_cumulative,
        }

    # ------------------------------------------------------------------
    # Turnover computation
    # ------------------------------------------------------------------

    def _compute_turnover(self, factor_df: pd.DataFrame) -> float:
        """Compute average factor turnover.

        Turnover is measured as 1 - Spearman rank correlation of factor values
        between consecutive dates. Returns the mean turnover.
        """
        dates = sorted(factor_df.index)
        if len(dates) < 2:
            return 0.0

        turnover_values: list[float] = []
        for i in range(1, len(dates)):
            prev = factor_df.loc[dates[i - 1]].dropna()
            curr = factor_df.loc[dates[i]].dropna()
            common = prev.index.intersection(curr.index)

            if len(common) < _MIN_STOCKS_FOR_IC:
                continue

            p = prev.loc[common].values
            c = curr.loc[common].values

            if np.std(p) == 0 or np.std(c) == 0:
                continue

            corr, _ = spearmanr(p, c)
            if not np.isnan(corr):
                turnover_values.append(1.0 - corr)

        return float(np.mean(turnover_values)) if turnover_values else 0.0

    # ------------------------------------------------------------------
    # Coverage computation
    # ------------------------------------------------------------------

    def _compute_coverage(self, factor_df: pd.DataFrame) -> float:
        """Compute factor coverage: % of (date, ticker) pairs with non-NaN values."""
        total = factor_df.size
        if total == 0:
            return 0.0
        valid = factor_df.notna().sum().sum()
        return float(valid / total)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _save_result(
        self,
        eval_id: str,
        factor_id: str,
        label_id: str,
        universe_group_id: str,
        start_date: str,
        end_date: str,
        summary: dict,
        ic_series: list,
        group_returns: dict,
        market: str | None = None,
        conn: Any | None = None,
    ) -> None:
        """Persist evaluation result to the factor_eval_results table."""
        resolved_market = normalize_market(market)
        conn = conn or get_connection()
        conn.execute(
            """INSERT INTO factor_eval_results
               (id, market, factor_id, label_id, universe_group_id,
                start_date, end_date, summary, ic_series, group_returns, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                eval_id,
                resolved_market,
                factor_id,
                label_id,
                universe_group_id,
                start_date,
                end_date,
                json.dumps(summary),
                json.dumps(ic_series),
                json.dumps(group_returns),
                utc_now_naive(),
            ],
        )
        log.info("factor_eval.saved", eval_id=eval_id)

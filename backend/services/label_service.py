"""Label definition service – create, manage, and compute prediction targets."""

from __future__ import annotations

import json
import uuid
from datetime import datetime

import numpy as np
import pandas as pd

from backend.db import get_connection
from backend.logger import get_logger
from backend.services.market_context import (
    get_default_benchmark,
    infer_ticker_market,
    normalize_market,
    normalize_ticker,
)
from backend.time_utils import utc_now_naive

log = get_logger(__name__)

# ---- Target type taxonomy ----
# Regression types  → model trains with LGBMRegressor
# Classification types → model trains with LGBMClassifier

_REGRESSION_TARGET_TYPES = {"return", "rank", "excess_return", "path_return", "composite", "trend_continuation"}
_CLASSIFICATION_TARGET_TYPES = {"binary", "top_quantile", "bottom_quantile", "large_move", "excess_binary", "path_quality", "triple_barrier"}

_VALID_TARGET_TYPES = _REGRESSION_TARGET_TYPES | _CLASSIFICATION_TARGET_TYPES

_PRESET_LABELS = [
    # ---- Regression presets ----
    {
        "id": "preset_fwd_return_1d",
        "name": "fwd_return_1d",
        "description": "1-day forward return (short-term reversal)",
        "target_type": "return",
        "horizon": 1,
        "benchmark": None,
        "config": None,
    },
    {
        "id": "preset_fwd_return_5d",
        "name": "fwd_return_5d",
        "description": "5-day forward return",
        "target_type": "return",
        "horizon": 5,
        "benchmark": None,
        "config": None,
    },
    {
        "id": "preset_fwd_return_10d",
        "name": "fwd_return_10d",
        "description": "10-day forward return (medium-term)",
        "target_type": "return",
        "horizon": 10,
        "benchmark": None,
        "config": None,
    },
    {
        "id": "preset_fwd_return_20d",
        "name": "fwd_return_20d",
        "description": "20-day forward return",
        "target_type": "return",
        "horizon": 20,
        "benchmark": None,
        "config": None,
    },
    {
        "id": "preset_fwd_return_60d",
        "name": "fwd_return_60d",
        "description": "60-day forward return (quarterly momentum)",
        "target_type": "return",
        "horizon": 60,
        "benchmark": None,
        "config": None,
    },
    {
        "id": "preset_fwd_rank_5d",
        "name": "fwd_rank_5d",
        "description": "Cross-sectional rank of 5-day forward return (0~1)",
        "target_type": "rank",
        "horizon": 5,
        "benchmark": None,
        "config": None,
    },
    {
        "id": "preset_fwd_rank_10d",
        "name": "fwd_rank_10d",
        "description": "Cross-sectional rank of 10-day forward return (0~1)",
        "target_type": "rank",
        "horizon": 10,
        "benchmark": None,
        "config": None,
    },
    {
        "id": "preset_fwd_rank_20d",
        "name": "fwd_rank_20d",
        "description": "Cross-sectional rank of 20-day forward return (0~1)",
        "target_type": "rank",
        "horizon": 20,
        "benchmark": None,
        "config": None,
    },
    {
        "id": "preset_fwd_excess_5d",
        "name": "fwd_excess_5d",
        "description": "5-day forward excess return vs SPY",
        "target_type": "excess_return",
        "horizon": 5,
        "benchmark": "SPY",
        "config": None,
    },
    {
        "id": "preset_fwd_excess_10d",
        "name": "fwd_excess_10d",
        "description": "10-day forward excess return vs SPY",
        "target_type": "excess_return",
        "horizon": 10,
        "benchmark": "SPY",
        "config": None,
    },
    {
        "id": "preset_fwd_excess_20d",
        "name": "fwd_excess_20d",
        "description": "20-day forward excess return vs SPY",
        "target_type": "excess_return",
        "horizon": 20,
        "benchmark": "SPY",
        "config": None,
    },
    {
        "id": "preset_fwd_return_5d_vol_adj",
        "name": "fwd_return_5d_vol_adj",
        "description": "5-day forward return / 20-day realized volatility",
        "target_type": "return",
        "horizon": 5,
        "benchmark": None,
        "config": {"vol_adjust": True, "vol_window": 20},
    },

    # ---- Classification presets ----
    # binary: simple up/down direction
    {
        "id": "preset_fwd_binary_5d",
        "name": "fwd_binary_5d",
        "description": "5-day forward direction (1=up, 0=down)",
        "target_type": "binary",
        "horizon": 5,
        "benchmark": None,
        "config": None,
    },
    {
        "id": "preset_fwd_binary_20d",
        "name": "fwd_binary_20d",
        "description": "20-day forward direction (1=up, 0=down)",
        "target_type": "binary",
        "horizon": 20,
        "benchmark": None,
        "config": None,
    },
    # top_quantile: top 20% by forward return → 1, else 0
    {
        "id": "preset_top_q20_5d",
        "name": "top_quantile_20pct_5d",
        "description": "Top 20% by 5-day forward return (1=top, 0=rest)",
        "target_type": "top_quantile",
        "horizon": 5,
        "benchmark": None,
        "config": {"quantile": 0.2},
    },
    {
        "id": "preset_top_q20_20d",
        "name": "top_quantile_20pct_20d",
        "description": "Top 20% by 20-day forward return (1=top, 0=rest)",
        "target_type": "top_quantile",
        "horizon": 20,
        "benchmark": None,
        "config": {"quantile": 0.2},
    },
    {
        "id": "preset_top_q10_10d",
        "name": "top_quantile_10pct_10d",
        "description": "Top 10% by 10-day forward return (1=top, 0=rest)",
        "target_type": "top_quantile",
        "horizon": 10,
        "benchmark": None,
        "config": {"quantile": 0.1},
    },
    # bottom_quantile: bottom 20% by forward return → 1, else 0  (short-selling / risk avoidance)
    {
        "id": "preset_bottom_q20_5d",
        "name": "bottom_quantile_20pct_5d",
        "description": "Bottom 20% by 5-day forward return (1=bottom, 0=rest)",
        "target_type": "bottom_quantile",
        "horizon": 5,
        "benchmark": None,
        "config": {"quantile": 0.2},
    },
    # large_move: absolute forward return > threshold → 1
    {
        "id": "preset_large_move_5d",
        "name": "large_move_5pct_5d",
        "description": "5-day |return| > 5% (1=large move, 0=quiet)",
        "target_type": "large_move",
        "horizon": 5,
        "benchmark": None,
        "config": {"threshold": 0.05},
    },
    {
        "id": "preset_large_move_20d",
        "name": "large_move_10pct_20d",
        "description": "20-day |return| > 10% (1=large move, 0=quiet)",
        "target_type": "large_move",
        "horizon": 20,
        "benchmark": None,
        "config": {"threshold": 0.10},
    },
    # excess_binary: beat the benchmark → 1, else 0
    {
        "id": "preset_excess_binary_5d",
        "name": "excess_binary_5d",
        "description": "5-day return beats SPY (1=outperform, 0=underperform)",
        "target_type": "excess_binary",
        "horizon": 5,
        "benchmark": "SPY",
        "config": None,
    },
    {
        "id": "preset_excess_binary_20d",
        "name": "excess_binary_20d",
        "description": "20-day return beats SPY (1=outperform, 0=underperform)",
        "target_type": "excess_binary",
        "horizon": 20,
        "benchmark": "SPY",
        "config": None,
    },
    # ---- Path-aware presets ----
    {
        "id": "preset_path_return_10d",
        "name": "path_return_10d",
        "description": "10-day forward return penalized by max drawdown and shock ratio",
        "target_type": "path_return",
        "horizon": 10,
        "benchmark": None,
        "config": {
            "drawdown_penalty": 1.0,
            "shock_penalty": 0.5,
        },
    },
    {
        "id": "preset_path_return_20d",
        "name": "path_return_20d",
        "description": "20-day forward return penalized by max drawdown and shock ratio",
        "target_type": "path_return",
        "horizon": 20,
        "benchmark": None,
        "config": {
            "drawdown_penalty": 1.0,
            "shock_penalty": 0.5,
        },
    },
    {
        "id": "preset_path_quality_10d",
        "name": "path_quality_10d",
        "description": "10-day path quality: up >5%, max DD <8%, shock ratio <0.3",
        "target_type": "path_quality",
        "horizon": 10,
        "benchmark": None,
        "config": {
            "min_return": 0.05,
            "max_drawdown": 0.08,
            "max_shock_ratio": 0.3,
        },
    },
    {
        "id": "preset_path_quality_20d",
        "name": "path_quality_20d",
        "description": "20-day path quality: up >8%, max DD <12%, shock ratio <0.3",
        "target_type": "path_quality",
        "horizon": 20,
        "benchmark": None,
        "config": {
            "min_return": 0.08,
            "max_drawdown": 0.12,
            "max_shock_ratio": 0.3,
        },
    },
]


class LabelService:
    """CRUD and computation for prediction-target label definitions."""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ensure_presets(self, market: str | None = None) -> None:
        """Create or update preset label definitions."""
        resolved_market = normalize_market(market)
        conn = get_connection()
        for raw_preset in _PRESET_LABELS:
            preset = dict(raw_preset)
            if resolved_market != "US":
                preset["id"] = f"{resolved_market.lower()}_{preset['id']}"
                preset["name"] = f"{resolved_market.lower()}_{preset['name']}"
                if preset.get("benchmark"):
                    preset["benchmark"] = get_default_benchmark(resolved_market)
            config_json = json.dumps(preset["config"]) if preset.get("config") else None
            row = conn.execute(
                "SELECT id, config FROM label_definitions WHERE id = ? AND market = ?",
                [preset["id"], resolved_market],
            ).fetchone()
            if row is None:
                conn.execute(
                    """INSERT INTO label_definitions
                       (id, market, name, description, target_type, horizon, benchmark, config, status)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active')""",
                    [
                        preset["id"],
                        resolved_market,
                        preset["name"],
                        preset["description"],
                        preset["target_type"],
                        preset["horizon"],
                        preset["benchmark"],
                        config_json,
                    ],
                )
                log.info("label.preset_created", market=resolved_market, name=preset["name"])
            else:
                existing_config = row[1]
                if existing_config != config_json:
                    conn.execute(
                        """UPDATE label_definitions
                           SET description = ?, target_type = ?, horizon = ?,
                               benchmark = ?, config = ?
                           WHERE id = ? AND market = ?""",
                        [
                            preset["description"],
                            preset["target_type"],
                            preset["horizon"],
                            preset["benchmark"],
                            config_json,
                            preset["id"],
                            resolved_market,
                        ],
                    )
                    log.info("label.preset_updated", market=resolved_market, name=preset["name"])

    def create_label(
        self,
        name: str,
        description: str | None = None,
        target_type: str = "return",
        horizon: int = 5,
        benchmark: str | None = None,
        config: dict | None = None,
        market: str | None = None,
    ) -> dict:
        """Create a new label definition."""
        resolved_market = normalize_market(market)
        benchmark = self._normalize_and_validate_benchmark(benchmark, resolved_market)
        if target_type not in _VALID_TARGET_TYPES:
            raise ValueError(
                f"target_type must be one of {_VALID_TARGET_TYPES}, got '{target_type}'"
            )
        if target_type in ("excess_return", "excess_binary") and not benchmark:
            raise ValueError(f"benchmark is required for {target_type} target_type")
        if target_type == "composite":
            if horizon < 0:
                raise ValueError("horizon must be >= 0 for composite labels")
        elif horizon < 1:
            raise ValueError("horizon must be >= 1")
        self._validate_config(target_type, config)

        conn = get_connection()
        label_id = uuid.uuid4().hex[:12]
        now = utc_now_naive()
        config_json = json.dumps(config) if config else None

        conn.execute(
            """INSERT INTO label_definitions
               (id, market, name, description, target_type, horizon, benchmark, config, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?)""",
            [label_id, resolved_market, name, description, target_type, horizon, benchmark, config_json, now, now],
        )
        log.info("label.created", id=label_id, market=resolved_market, name=name)
        return self.get_label(label_id, market=resolved_market)

    def update_label(
        self,
        label_id: str,
        name: str | None = None,
        description: str | None = None,
        target_type: str | None = None,
        horizon: int | None = None,
        benchmark: str | None = None,
        config: dict | None = None,
        status: str | None = None,
        market: str | None = None,
    ) -> dict:
        """Update an existing label definition."""
        conn = get_connection()
        existing = self._fetch_row(label_id, market)
        if existing is None:
            raise ValueError(f"Label {label_id} not found")

        resolved_market = existing["market"]
        effective_target_type = target_type or existing["target_type"]
        effective_benchmark = benchmark if benchmark is not None else existing.get("benchmark")

        if target_type is not None and target_type not in _VALID_TARGET_TYPES:
            raise ValueError(
                f"target_type must be one of {_VALID_TARGET_TYPES}, got '{target_type}'"
            )
        effective_benchmark = self._normalize_and_validate_benchmark(
            effective_benchmark, resolved_market
        )
        if effective_target_type in ("excess_return", "excess_binary") and not effective_benchmark:
            raise ValueError(f"benchmark is required for {effective_target_type} target_type")

        now = utc_now_naive()
        sets: list[str] = ["updated_at = ?"]
        params: list = [now]

        for col, val in [
            ("name", name),
            ("description", description),
            ("target_type", target_type),
            ("horizon", horizon),
            ("benchmark", effective_benchmark if benchmark is not None else None),
            ("status", status),
        ]:
            if val is not None:
                sets.append(f"{col} = ?")
                params.append(val)

        if config is not None:
            sets.append("config = ?")
            params.append(json.dumps(config))

        params.append(label_id)
        params.append(resolved_market)
        conn.execute(
            f"UPDATE label_definitions SET {', '.join(sets)} WHERE id = ? AND market = ?", params
        )
        log.info("label.updated", id=label_id, market=resolved_market)
        return self.get_label(label_id, market=resolved_market)

    def delete_label(self, label_id: str, market: str | None = None) -> None:
        """Delete a label definition."""
        conn = get_connection()
        existing = self._fetch_row(label_id, market)
        if existing is None:
            raise ValueError(f"Label {label_id} not found")

        conn.execute(
            "DELETE FROM label_definitions WHERE id = ? AND market = ?",
            [label_id, existing["market"]],
        )
        log.info("label.deleted", id=label_id, market=existing["market"])

    def get_label(self, label_id: str, market: str | None = None) -> dict:
        """Return a single label definition."""
        row = self._fetch_row(label_id, market)
        if row is None:
            raise ValueError(f"Label {label_id} not found")
        return row

    def list_labels(self, market: str | None = None) -> list[dict]:
        """List all label definitions."""
        resolved_market = normalize_market(market)
        conn = get_connection()
        rows = conn.execute(
            """SELECT id, market, name, description, target_type, horizon,
                      benchmark, config, status, created_at, updated_at
               FROM label_definitions
               WHERE market = ?
               ORDER BY created_at""",
            [resolved_market],
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    @staticmethod
    def get_task_type(target_type: str) -> str:
        """Return 'classification' or 'regression' for a given target_type."""
        if target_type in _CLASSIFICATION_TARGET_TYPES:
            return "classification"
        return "regression"

    def compute_label_values(
        self,
        label_id: str,
        tickers: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        market: str | None = None,
    ) -> pd.DataFrame:
        """Compute label values for given tickers and date range.

        Returns a DataFrame with columns: ticker, date, label_value.
        """
        label = self.get_label(label_id, market=market)
        resolved_market = label["market"]
        tickers = [normalize_ticker(t, resolved_market) for t in tickers if str(t).strip()]
        target_type = label["target_type"]
        horizon = label["horizon"]
        benchmark = label.get("benchmark")
        config = label.get("config") or {}

        # ---- Composite label: combine multiple sub-labels ----
        if target_type == "composite":
            return self._compute_composite_label(
                config, tickers, start_date, end_date, market=resolved_market
            )

        conn = get_connection()

        # Build query for daily bars
        # Fetch extra horizon days beyond end_date so shift(-horizon) doesn't produce NaN at the tail
        if not tickers:
            return pd.DataFrame(columns=["ticker", "date", "label_value"])
        placeholders = ",".join("?" for _ in tickers)
        where_parts = [f"market = ? AND ticker IN ({placeholders})"]
        params: list = [resolved_market, *tickers]
        if start_date:
            where_parts.append("date >= ?")
            params.append(start_date)
        if end_date:
            # Extend data fetch by horizon * 2 calendar days to ensure enough trading days
            from datetime import timedelta
            data_end = (pd.Timestamp(end_date) + timedelta(days=horizon * 2 + 10)).strftime("%Y-%m-%d")
            where_parts.append("date <= ?")
            params.append(data_end)

        where_clause = " AND ".join(where_parts)
        # For path-aware types, load OHLC; otherwise only close
        if target_type in ("path_return", "path_quality", "trend_continuation", "triple_barrier"):
            bars_df = conn.execute(
                f"SELECT ticker, date, open, high, low, close FROM daily_bars WHERE {where_clause} ORDER BY ticker, date",
                params,
            ).fetchdf()
        else:
            bars_df = conn.execute(
                f"SELECT ticker, date, close FROM daily_bars WHERE {where_clause} ORDER BY ticker, date",
                params,
            ).fetchdf()

        if bars_df.empty:
            return pd.DataFrame(columns=["ticker", "date", "label_value"])

        # Compute forward returns per ticker
        result_frames: list[pd.DataFrame] = []
        for ticker, grp in bars_df.groupby("ticker"):
            grp = grp.sort_values("date").reset_index(drop=True)
            fwd_close = grp["close"].shift(-horizon)
            fwd_return = (fwd_close - grp["close"]) / grp["close"]

            sub = pd.DataFrame({
                "ticker": ticker,
                "date": grp["date"],
                "fwd_return": fwd_return,
            })
            result_frames.append(sub)

        if not result_frames:
            return pd.DataFrame(columns=["ticker", "date", "label_value"])

        combined = pd.concat(result_frames, ignore_index=True)

        # ---- Dispatch by target_type ----
        if target_type == "return":
            vol_adjust = config.get("vol_adjust", False)
            if vol_adjust:
                vol_window = config.get("vol_window", 20)
                # Compute realized volatility per ticker and divide fwd_return by it
                for ticker, grp_idx in combined.groupby("ticker").groups.items():
                    sub = combined.loc[grp_idx].sort_values("date")
                    # Need daily returns to compute realized vol; re-derive from bars
                    ticker_bars = bars_df[bars_df["ticker"] == ticker].sort_values("date")
                    daily_ret = ticker_bars["close"].pct_change()
                    realized_vol = daily_ret.rolling(vol_window).std()
                    # Align realized_vol with combined index via date
                    vol_map = dict(zip(ticker_bars["date"], realized_vol))
                    vol_series = sub["date"].map(vol_map)
                    # Avoid division by zero: replace 0/NaN vol with NaN
                    vol_series = vol_series.replace(0, np.nan)
                    combined.loc[grp_idx, "label_value"] = (
                        combined.loc[grp_idx, "fwd_return"] / vol_series.values
                    )
            else:
                combined["label_value"] = combined["fwd_return"]

        elif target_type == "binary":
            combined["label_value"] = (combined["fwd_return"] > 0).astype(float)
            combined.loc[combined["fwd_return"].isna(), "label_value"] = np.nan

        elif target_type == "rank":
            combined["label_value"] = combined.groupby("date")["fwd_return"].rank(pct=True)

        elif target_type == "excess_return":
            bench_return_map = self._load_benchmark_returns(
                benchmark, horizon, start_date, end_date, conn, market=resolved_market
            )
            combined["bench_return"] = combined["date"].map(bench_return_map)
            combined["label_value"] = combined["fwd_return"] - combined["bench_return"]
            combined.drop(columns=["bench_return"], inplace=True)

        elif target_type == "top_quantile":
            q = config.get("quantile", 0.2)
            threshold = combined.groupby("date")["fwd_return"].transform(
                lambda x: x.quantile(1 - q)
            )
            combined["label_value"] = (combined["fwd_return"] >= threshold).astype(float)
            combined.loc[combined["fwd_return"].isna(), "label_value"] = np.nan

        elif target_type == "bottom_quantile":
            q = config.get("quantile", 0.2)
            threshold = combined.groupby("date")["fwd_return"].transform(
                lambda x: x.quantile(q)
            )
            combined["label_value"] = (combined["fwd_return"] <= threshold).astype(float)
            combined.loc[combined["fwd_return"].isna(), "label_value"] = np.nan

        elif target_type == "large_move":
            t = config.get("threshold", 0.05)
            combined["label_value"] = (combined["fwd_return"].abs() > t).astype(float)
            combined.loc[combined["fwd_return"].isna(), "label_value"] = np.nan

        elif target_type == "excess_binary":
            bench_return_map = self._load_benchmark_returns(
                benchmark, horizon, start_date, end_date, conn, market=resolved_market
            )
            combined["bench_return"] = combined["date"].map(bench_return_map)
            excess = combined["fwd_return"] - combined["bench_return"]
            combined["label_value"] = (excess > 0).astype(float)
            combined.loc[excess.isna(), "label_value"] = np.nan
            combined.drop(columns=["bench_return"], inplace=True)

        elif target_type in ("path_return", "path_quality"):
            # Compute future-path statistics from daily close data
            path_stats = self._compute_path_stats(bars_df, horizon)
            # path_stats: DataFrame[ticker, date, fwd_return, max_drawdown,
            #   max_single_day, shock_ratio, gap_dependency, close_location]
            combined = combined.merge(
                path_stats, on=["ticker", "date"], how="left", suffixes=("", "_path"),
            )

            if target_type == "path_return":
                # Penalized forward return via generic penalties map:
                # label = fwd_return - sum(penalty_i * stat_i)
                # Legacy keys drawdown_penalty/shock_penalty still supported
                penalties = config.get("penalties", {})
                if not penalties:
                    # Fallback to legacy config keys
                    dd_pen = config.get("drawdown_penalty", 0.0)
                    shock_pen = config.get("shock_penalty", 0.0)
                    if dd_pen:
                        penalties["max_drawdown"] = dd_pen
                    if shock_pen:
                        penalties["shock_ratio"] = shock_pen
                label_val = combined["fwd_return"].copy()
                for stat_name, penalty in penalties.items():
                    if stat_name in combined.columns and penalty != 0:
                        label_val = label_val - penalty * combined[stat_name]
                combined["label_value"] = label_val
            else:  # path_quality
                # Binary: 1 if all constraints are met
                min_ret = config.get("min_return", 0.05)
                max_dd = config.get("max_drawdown", 0.10)
                max_shock = config.get("max_shock_ratio", 0.3)
                max_single = config.get("max_single_day_return")
                max_gap = config.get("max_gap_dependency")
                min_cl = config.get("min_close_location")
                quality = (
                    (combined["fwd_return"] >= min_ret)
                    & (combined["max_drawdown"] <= max_dd)
                    & (combined["shock_ratio"] <= max_shock)
                )
                if max_single is not None:
                    quality = quality & (combined["max_single_day"] <= max_single)
                if max_gap is not None:
                    quality = quality & (combined["gap_dependency"] <= max_gap)
                if min_cl is not None:
                    quality = quality & (combined["close_location"] >= min_cl)
                combined["label_value"] = quality.astype(float)
                combined.loc[combined["fwd_return"].isna(), "label_value"] = np.nan

            # Drop intermediate path columns
            for col in ["max_drawdown", "max_single_day", "shock_ratio",
                        "gap_dependency", "close_location"]:
                if col in combined.columns:
                    combined.drop(columns=[col], inplace=True)
            # Drop duplicate fwd_return_path if present
            if "fwd_return_path" in combined.columns:
                combined.drop(columns=["fwd_return_path"], inplace=True)

        elif target_type == "trend_continuation":
            # Regression: fwd_return scaled by path persistence.
            # persistence = fraction of days where cumulative return keeps
            # advancing (no new drawdown from running peak).
            # label = fwd_return * persistence^exponent
            # High for slow steady uptrends, low for erratic/climax moves.
            path_stats = self._compute_path_stats(bars_df, horizon)
            tc_stats = self._compute_trend_continuation_stats(bars_df, horizon)
            combined = combined.merge(
                path_stats, on=["ticker", "date"], how="left", suffixes=("", "_path"),
            )
            combined = combined.merge(
                tc_stats, on=["ticker", "date"], how="left",
            )
            exponent = config.get("persistence_exponent", 1.0)
            dd_penalty = config.get("drawdown_penalty", 0.0)
            shock_penalty = config.get("shock_penalty", 0.0)
            label_val = combined["fwd_return"] * (combined["persistence"] ** exponent)
            if dd_penalty:
                label_val = label_val - dd_penalty * combined["max_drawdown"]
            if shock_penalty:
                label_val = label_val - shock_penalty * combined["shock_ratio"]
            combined["label_value"] = label_val
            for col in ["persistence", "max_drawdown", "max_single_day",
                        "shock_ratio", "gap_dependency", "close_location",
                        "fwd_return_path"]:
                if col in combined.columns:
                    combined.drop(columns=[col], inplace=True)

        elif target_type == "triple_barrier":
            # Classification: 1 if profit target hit before stop loss within
            # horizon, 0 otherwise.  Natural trap filter — climax/gap moves
            # that reverse quickly get labelled 0.
            tb_df = self._compute_triple_barrier(bars_df, horizon, config)
            combined = combined.merge(
                tb_df, on=["ticker", "date"], how="left",
            )
            combined["label_value"] = combined["tb_label"].astype(float)
            combined.loc[combined["tb_label"].isna(), "label_value"] = np.nan
            if "tb_label" in combined.columns:
                combined.drop(columns=["tb_label"], inplace=True)
            if "tb_duration" in combined.columns:
                combined.drop(columns=["tb_duration"], inplace=True)

        else:
            raise ValueError(f"Unsupported target_type: {target_type}")

        combined.drop(columns=["fwd_return"], inplace=True)

        # Trim labels back to the originally requested end_date
        # (we fetched extra bars to compute forward returns at the tail)
        if end_date:
            combined = combined[combined["date"] <= pd.Timestamp(end_date)]

        return combined

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_benchmark_returns(
        self,
        benchmark: str | None,
        horizon: int,
        start_date: str | None,
        end_date: str | None,
        conn,
        market: str | None = None,
    ) -> dict:
        """Load benchmark forward returns as a date -> float mapping."""
        if not benchmark:
            raise ValueError("benchmark is required for excess_return / excess_binary")
        resolved_market = normalize_market(market)
        benchmark = self._normalize_and_validate_benchmark(benchmark, resolved_market)

        bench_where = ["market = ?", "symbol = ?"]
        bench_params: list = [resolved_market, benchmark]
        if start_date:
            bench_where.append("date >= ?")
            bench_params.append(start_date)
        if end_date:
            bench_where.append("date <= ?")
            bench_params.append(end_date)

        bench_df = conn.execute(
            f"SELECT date, close FROM index_bars WHERE {' AND '.join(bench_where)} ORDER BY date",
            bench_params,
        ).fetchdf()

        if bench_df.empty:
            log.warning("label.no_benchmark_data", market=resolved_market, benchmark=benchmark)
            raise ValueError(
                f"Benchmark data missing for {benchmark} in market {resolved_market}; "
                f"update index_bars for the requested date range before computing excess-return labels"
            )

        bench_df = bench_df.sort_values("date").reset_index(drop=True)
        bench_fwd = bench_df["close"].shift(-horizon)
        bench_return = (bench_fwd - bench_df["close"]) / bench_df["close"]
        return dict(zip(bench_df["date"], bench_return))

    @staticmethod
    def _compute_path_stats(
        bars_df: pd.DataFrame, horizon: int,
    ) -> pd.DataFrame:
        """Compute future-path statistics for each (ticker, date).

        Expects bars_df with columns: ticker, date, open, high, low, close.

        For each observation day, look at the next ``horizon`` trading days
        and compute:
        - ``fwd_return``: endpoint return (close[t+h] / close[t] - 1)
        - ``max_drawdown``: worst peak-to-trough in the forward window
        - ``max_single_day``: largest absolute single-day return
        - ``shock_ratio``: fraction of days with |daily_return| > 2 std of window
        - ``gap_dependency``: abs sum of overnight gaps / abs total return
        - ``close_location``: mean of (close - low) / (high - low) in window

        Returns a DataFrame with columns:
            ticker, date, fwd_return, max_drawdown, max_single_day,
            shock_ratio, gap_dependency, close_location
        """
        has_ohlc = all(c in bars_df.columns for c in ("open", "high", "low"))
        records = []
        for ticker, grp in bars_df.groupby("ticker"):
            grp = grp.sort_values("date").reset_index(drop=True)
            closes = grp["close"].values
            dates = grp["date"].values
            opens = grp["open"].values if has_ohlc else None
            highs = grp["high"].values if has_ohlc else None
            lows = grp["low"].values if has_ohlc else None
            n = len(closes)

            for i in range(n - horizon):
                window = closes[i: i + horizon + 1]  # t to t+h inclusive
                entry_price = window[0]
                if entry_price == 0 or np.isnan(entry_price):
                    continue

                fwd_return = (window[-1] - entry_price) / entry_price

                # Daily returns within the forward window
                daily_rets = np.diff(window) / window[:-1]

                # Max drawdown
                cumulative = np.cumprod(1 + daily_rets)
                running_max = np.maximum.accumulate(cumulative)
                drawdowns = (running_max - cumulative) / running_max
                max_dd = float(np.nanmax(drawdowns)) if len(drawdowns) > 0 else 0.0

                # Max single-day absolute return
                max_single = float(np.nanmax(np.abs(daily_rets))) if len(daily_rets) > 0 else 0.0

                # Shock ratio: days with |ret| > 2*std
                if len(daily_rets) > 1:
                    std = np.nanstd(daily_rets)
                    if std > 0:
                        shock_days = np.sum(np.abs(daily_rets) > 2 * std)
                        shock_ratio = float(shock_days) / len(daily_rets)
                    else:
                        shock_ratio = 0.0
                else:
                    shock_ratio = 0.0

                # Gap dependency: overnight gap contribution
                if has_ohlc and abs(fwd_return) > 1e-10:
                    # gap[j] = open[i+j+1] - close[i+j] for j in 0..horizon-1
                    gap_sum = 0.0
                    for j in range(horizon):
                        idx = i + j
                        if idx + 1 < n:
                            gap_sum += abs(opens[idx + 1] - closes[idx])
                    total_abs_move = abs(window[-1] - entry_price)
                    gap_dep = min(gap_sum / (total_abs_move + 1e-10), 1.0) if total_abs_move > 1e-10 else 0.0
                else:
                    gap_dep = 0.0

                # Close location: mean of (close - low) / (high - low)
                if has_ohlc:
                    cl_vals = []
                    for j in range(1, horizon + 1):
                        idx = i + j
                        if idx < n:
                            h = highs[idx]
                            l = lows[idx]
                            c = closes[idx]
                            rng = h - l
                            if rng > 1e-10:
                                cl_vals.append((c - l) / rng)
                    close_loc = float(np.mean(cl_vals)) if cl_vals else 0.5
                else:
                    close_loc = 0.5

                records.append({
                    "ticker": ticker,
                    "date": dates[i],
                    "fwd_return": fwd_return,
                    "max_drawdown": max_dd,
                    "max_single_day": max_single,
                    "shock_ratio": shock_ratio,
                    "gap_dependency": gap_dep,
                    "close_location": close_loc,
                })

        if not records:
            return pd.DataFrame(columns=[
                "ticker", "date", "fwd_return", "max_drawdown",
                "max_single_day", "shock_ratio", "gap_dependency", "close_location",
            ])

        return pd.DataFrame(records)

    @staticmethod
    def _compute_trend_continuation_stats(
        bars_df: pd.DataFrame, horizon: int,
    ) -> pd.DataFrame:
        """Compute trend persistence for each (ticker, date).

        persistence = fraction of days in the forward window where the
        cumulative return stays at or above the running peak drawdown
        threshold.  A perfectly steady uptrend scores 1.0; an erratic
        or climax-reversal path scores much lower.
        """
        records = []
        for ticker, grp in bars_df.groupby("ticker"):
            grp = grp.sort_values("date").reset_index(drop=True)
            closes = grp["close"].values
            dates = grp["date"].values
            n = len(closes)

            for i in range(n - horizon):
                entry = closes[i]
                if entry == 0 or np.isnan(entry):
                    continue

                window = closes[i + 1: i + horizon + 1]
                cum_ret = window / entry
                running_max = np.maximum.accumulate(cum_ret)
                # A day "persists" if it hasn't drawn down >1% from peak
                persist_days = int(np.sum((running_max - cum_ret) / running_max < 0.01))
                persistence = persist_days / max(len(window), 1)

                records.append({
                    "ticker": ticker,
                    "date": dates[i],
                    "persistence": persistence,
                })

        if not records:
            return pd.DataFrame(columns=["ticker", "date", "persistence"])
        return pd.DataFrame(records)

    @staticmethod
    def _compute_triple_barrier(
        bars_df: pd.DataFrame, horizon: int, config: dict,
    ) -> pd.DataFrame:
        """Compute triple-barrier label for each (ticker, date).

        Three barriers:
        - Upper: entry_price * (1 + take_profit)  → label = 1
        - Lower: entry_price * (1 - stop_loss)    → label = 0
        - Time:  horizon days expire               → label = 1 if fwd_return > 0, else 0

        Uses intraday high/low when available for realistic barrier touches.

        Returns DataFrame with columns: ticker, date, tb_label, tb_duration.
        """
        take_profit = config.get("take_profit", 0.10)
        stop_loss = config.get("stop_loss", 0.05)

        has_hl = all(c in bars_df.columns for c in ("high", "low"))
        records = []

        for ticker, grp in bars_df.groupby("ticker"):
            grp = grp.sort_values("date").reset_index(drop=True)
            closes = grp["close"].values
            highs = grp["high"].values if has_hl else closes
            lows = grp["low"].values if has_hl else closes
            dates = grp["date"].values
            n = len(closes)

            for i in range(n - horizon):
                entry = closes[i]
                if entry == 0 or np.isnan(entry):
                    continue

                upper = entry * (1.0 + take_profit)
                lower = entry * (1.0 - stop_loss)
                label = None
                duration = horizon

                for j in range(1, horizon + 1):
                    idx = i + j
                    if idx >= n:
                        break
                    # Check barriers using intraday extremes
                    if highs[idx] >= upper:
                        label = 1
                        duration = j
                        break
                    if lows[idx] <= lower:
                        label = 0
                        duration = j
                        break

                # Time barrier: horizon expired without hitting either
                if label is None:
                    end_idx = min(i + horizon, n - 1)
                    fwd_ret = (closes[end_idx] - entry) / entry
                    label = 1 if fwd_ret > 0 else 0

                records.append({
                    "ticker": ticker,
                    "date": dates[i],
                    "tb_label": label,
                    "tb_duration": duration,
                })

        if not records:
            return pd.DataFrame(columns=["ticker", "date", "tb_label", "tb_duration"])
        return pd.DataFrame(records)

    def _compute_composite_label(
        self,
        config: dict,
        tickers: list[str],
        start_date: str | None,
        end_date: str | None,
        market: str | None = None,
    ) -> pd.DataFrame:
        """Compute a composite label as weighted sum of sub-labels.

        Config format::

            {
                "components": [
                    {"label_id": "preset_fwd_rank_10d", "weight": 0.6},
                    {"label_id": "preset_fwd_rank_20d", "weight": 0.4},
                ],
                "normalize": true   // optional, rank-normalize final value
            }
        """
        components = config.get("components", [])
        if not components:
            raise ValueError("composite label requires 'components' in config")

        merged: pd.DataFrame | None = None

        for i, comp in enumerate(components):
            sub_label_id = comp.get("label_id")
            weight = comp.get("weight", 1.0)
            if not sub_label_id:
                raise ValueError(f"component {i} missing 'label_id'")

            sub_df = self.compute_label_values(
                sub_label_id, tickers, start_date, end_date, market=market
            )
            if sub_df.empty:
                continue

            sub_df = sub_df.rename(columns={"label_value": f"sub_{i}"})
            sub_df[f"sub_{i}"] = sub_df[f"sub_{i}"] * weight

            if merged is None:
                merged = sub_df
            else:
                merged = merged.merge(sub_df, on=["ticker", "date"], how="outer")

        if merged is None or merged.empty:
            return pd.DataFrame(columns=["ticker", "date", "label_value"])

        # Sum all weighted sub-labels
        sub_cols = [c for c in merged.columns if c.startswith("sub_")]
        merged["label_value"] = merged[sub_cols].sum(axis=1, min_count=1)
        merged.drop(columns=sub_cols, inplace=True)

        # Optional rank normalization
        if config.get("normalize"):
            merged["label_value"] = merged.groupby("date")["label_value"].rank(pct=True)

        return merged

    @staticmethod
    def _validate_config(target_type: str, config: dict | None) -> None:
        """Validate config dict for target types that require extra parameters."""
        if target_type in ("top_quantile", "bottom_quantile"):
            if config:
                q = config.get("quantile")
                if q is not None and (q <= 0 or q >= 1):
                    raise ValueError("quantile must be between 0 and 1 (exclusive)")
        elif target_type == "large_move":
            if config:
                t = config.get("threshold")
                if t is not None and t <= 0:
                    raise ValueError("threshold must be positive")
        elif target_type == "path_return":
            if config:
                dd = config.get("drawdown_penalty")
                if dd is not None and dd < 0:
                    raise ValueError("drawdown_penalty must be >= 0")
        elif target_type == "path_quality":
            if config:
                max_dd = config.get("max_drawdown")
                if max_dd is not None and max_dd <= 0:
                    raise ValueError("max_drawdown must be positive")
        elif target_type == "composite":
            if not config or not config.get("components"):
                raise ValueError("composite label requires 'components' list in config")
            for i, comp in enumerate(config["components"]):
                if not comp.get("label_id"):
                    raise ValueError(f"component {i} missing 'label_id'")
        elif target_type == "trend_continuation":
            if config:
                exp = config.get("persistence_exponent")
                if exp is not None and exp < 0:
                    raise ValueError("persistence_exponent must be >= 0")
        elif target_type == "triple_barrier":
            if config:
                tp = config.get("take_profit")
                sl = config.get("stop_loss")
                if tp is not None and tp <= 0:
                    raise ValueError("take_profit must be positive")
                if sl is not None and sl <= 0:
                    raise ValueError("stop_loss must be positive")

    def _fetch_row(self, label_id: str, market: str | None = None) -> dict | None:
        resolved_market = normalize_market(market)
        conn = get_connection()
        row = conn.execute(
            """SELECT id, market, name, description, target_type, horizon,
                      benchmark, config, status, created_at, updated_at
               FROM label_definitions WHERE id = ? AND market = ?""",
            [label_id, resolved_market],
        ).fetchone()
        if row is None:
            return None
        return self._row_to_dict(row)

    def _normalize_and_validate_benchmark(self, benchmark: str | None, market: str) -> str | None:
        if benchmark is None:
            return None
        normalized = normalize_ticker(benchmark, market)
        hinted_market = infer_ticker_market(normalized)
        if hinted_market is not None and hinted_market != market:
            raise ValueError(f"benchmark {benchmark} is not valid for market {market}")
        return normalized

    @staticmethod
    def _row_to_dict(row) -> dict:
        config_raw = row[7]
        if isinstance(config_raw, str):
            try:
                config_parsed = json.loads(config_raw)
            except (json.JSONDecodeError, TypeError):
                config_parsed = None
        else:
            config_parsed = config_raw

        return {
            "id": row[0],
            "market": row[1],
            "name": row[2],
            "description": row[3],
            "target_type": row[4],
            "horizon": row[5],
            "benchmark": row[6],
            "config": config_parsed,
            "status": row[8],
            "created_at": str(row[9]) if row[9] else None,
            "updated_at": str(row[10]) if row[10] else None,
        }

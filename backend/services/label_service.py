"""Label definition service – create, manage, and compute prediction targets."""

from __future__ import annotations

import json
import uuid
from datetime import datetime

import numpy as np
import pandas as pd

from backend.db import get_connection
from backend.logger import get_logger

log = get_logger(__name__)

# ---- Target type taxonomy ----
# Regression types  → model trains with LGBMRegressor
# Classification types → model trains with LGBMClassifier

_REGRESSION_TARGET_TYPES = {"return", "rank", "excess_return"}
_CLASSIFICATION_TARGET_TYPES = {"binary", "top_quantile", "bottom_quantile", "large_move", "excess_binary"}

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
        "config": None,
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
]


class LabelService:
    """CRUD and computation for prediction-target label definitions."""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ensure_presets(self) -> None:
        """Create preset label definitions if they do not already exist."""
        conn = get_connection()
        for preset in _PRESET_LABELS:
            row = conn.execute(
                "SELECT id FROM label_definitions WHERE id = ?", [preset["id"]]
            ).fetchone()
            if row is None:
                config_json = json.dumps(preset["config"]) if preset.get("config") else None
                conn.execute(
                    """INSERT INTO label_definitions
                       (id, name, description, target_type, horizon, benchmark, config, status)
                       VALUES (?, ?, ?, ?, ?, ?, ?, 'active')""",
                    [
                        preset["id"],
                        preset["name"],
                        preset["description"],
                        preset["target_type"],
                        preset["horizon"],
                        preset["benchmark"],
                        config_json,
                    ],
                )
                log.info("label.preset_created", name=preset["name"])

    def create_label(
        self,
        name: str,
        description: str | None = None,
        target_type: str = "return",
        horizon: int = 5,
        benchmark: str | None = None,
        config: dict | None = None,
    ) -> dict:
        """Create a new label definition."""
        if target_type not in _VALID_TARGET_TYPES:
            raise ValueError(
                f"target_type must be one of {_VALID_TARGET_TYPES}, got '{target_type}'"
            )
        if target_type in ("excess_return", "excess_binary") and not benchmark:
            raise ValueError(f"benchmark is required for {target_type} target_type")
        if horizon < 1:
            raise ValueError("horizon must be >= 1")
        self._validate_config(target_type, config)

        conn = get_connection()
        label_id = uuid.uuid4().hex[:12]
        now = datetime.utcnow()
        config_json = json.dumps(config) if config else None

        conn.execute(
            """INSERT INTO label_definitions
               (id, name, description, target_type, horizon, benchmark, config, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?)""",
            [label_id, name, description, target_type, horizon, benchmark, config_json, now, now],
        )
        log.info("label.created", id=label_id, name=name)
        return self.get_label(label_id)

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
    ) -> dict:
        """Update an existing label definition."""
        conn = get_connection()
        existing = self._fetch_row(label_id)
        if existing is None:
            raise ValueError(f"Label {label_id} not found")

        if target_type is not None and target_type not in _VALID_TARGET_TYPES:
            raise ValueError(
                f"target_type must be one of {_VALID_TARGET_TYPES}, got '{target_type}'"
            )

        now = datetime.utcnow()
        sets: list[str] = ["updated_at = ?"]
        params: list = [now]

        for col, val in [
            ("name", name),
            ("description", description),
            ("target_type", target_type),
            ("horizon", horizon),
            ("benchmark", benchmark),
            ("status", status),
        ]:
            if val is not None:
                sets.append(f"{col} = ?")
                params.append(val)

        if config is not None:
            sets.append("config = ?")
            params.append(json.dumps(config))

        params.append(label_id)
        conn.execute(
            f"UPDATE label_definitions SET {', '.join(sets)} WHERE id = ?", params
        )
        log.info("label.updated", id=label_id)
        return self.get_label(label_id)

    def delete_label(self, label_id: str) -> None:
        """Delete a label definition."""
        conn = get_connection()
        existing = self._fetch_row(label_id)
        if existing is None:
            raise ValueError(f"Label {label_id} not found")

        conn.execute("DELETE FROM label_definitions WHERE id = ?", [label_id])
        log.info("label.deleted", id=label_id)

    def get_label(self, label_id: str) -> dict:
        """Return a single label definition."""
        row = self._fetch_row(label_id)
        if row is None:
            raise ValueError(f"Label {label_id} not found")
        return row

    def list_labels(self) -> list[dict]:
        """List all label definitions."""
        conn = get_connection()
        rows = conn.execute(
            """SELECT id, name, description, target_type, horizon,
                      benchmark, config, status, created_at, updated_at
               FROM label_definitions
               ORDER BY created_at"""
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
    ) -> pd.DataFrame:
        """Compute label values for given tickers and date range.

        Returns a DataFrame with columns: ticker, date, label_value.
        """
        label = self.get_label(label_id)
        target_type = label["target_type"]
        horizon = label["horizon"]
        benchmark = label.get("benchmark")
        config = label.get("config") or {}

        conn = get_connection()

        # Build query for daily bars
        where_parts = ["ticker IN (" + ",".join(f"'{t}'" for t in tickers) + ")"]
        params: list = []
        if start_date:
            where_parts.append("date >= ?")
            params.append(start_date)
        if end_date:
            where_parts.append("date <= ?")
            params.append(end_date)

        where_clause = " AND ".join(where_parts)
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
            combined["label_value"] = combined["fwd_return"]

        elif target_type == "binary":
            combined["label_value"] = (combined["fwd_return"] > 0).astype(float)
            combined.loc[combined["fwd_return"].isna(), "label_value"] = np.nan

        elif target_type == "rank":
            combined["label_value"] = combined.groupby("date")["fwd_return"].rank(pct=True)

        elif target_type == "excess_return":
            bench_return_map = self._load_benchmark_returns(
                benchmark, horizon, start_date, end_date, conn
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
                benchmark, horizon, start_date, end_date, conn
            )
            combined["bench_return"] = combined["date"].map(bench_return_map)
            excess = combined["fwd_return"] - combined["bench_return"]
            combined["label_value"] = (excess > 0).astype(float)
            combined.loc[excess.isna(), "label_value"] = np.nan
            combined.drop(columns=["bench_return"], inplace=True)

        else:
            raise ValueError(f"Unsupported target_type: {target_type}")

        combined.drop(columns=["fwd_return"], inplace=True)
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
    ) -> dict:
        """Load benchmark forward returns as a date -> float mapping."""
        if not benchmark:
            raise ValueError("benchmark is required for excess_return / excess_binary")

        bench_where = ["symbol = ?"]
        bench_params: list = [benchmark]
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
            log.warning("label.no_benchmark_data", benchmark=benchmark)
            return {}

        bench_df = bench_df.sort_values("date").reset_index(drop=True)
        bench_fwd = bench_df["close"].shift(-horizon)
        bench_return = (bench_fwd - bench_df["close"]) / bench_df["close"]
        return dict(zip(bench_df["date"], bench_return))

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

    def _fetch_row(self, label_id: str) -> dict | None:
        conn = get_connection()
        row = conn.execute(
            """SELECT id, name, description, target_type, horizon,
                      benchmark, config, status, created_at, updated_at
               FROM label_definitions WHERE id = ?""",
            [label_id],
        ).fetchone()
        if row is None:
            return None
        return self._row_to_dict(row)

    @staticmethod
    def _row_to_dict(row) -> dict:
        config_raw = row[6]
        if isinstance(config_raw, str):
            try:
                config_parsed = json.loads(config_raw)
            except (json.JSONDecodeError, TypeError):
                config_parsed = None
        else:
            config_parsed = config_raw

        return {
            "id": row[0],
            "name": row[1],
            "description": row[2],
            "target_type": row[3],
            "horizon": row[4],
            "benchmark": row[5],
            "config": config_parsed,
            "status": row[7],
            "created_at": str(row[8]) if row[8] else None,
            "updated_at": str(row[9]) if row[9] else None,
        }

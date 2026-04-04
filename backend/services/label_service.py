"""Label definition service – create, manage, and compute prediction targets."""

from __future__ import annotations

import uuid
from datetime import datetime

import numpy as np
import pandas as pd

from backend.db import get_connection
from backend.logger import get_logger

log = get_logger(__name__)

_VALID_TARGET_TYPES = {"return", "rank", "binary", "excess_return"}

_PRESET_LABELS = [
    {
        "id": "preset_fwd_return_5d",
        "name": "fwd_return_5d",
        "description": "5-day forward return",
        "target_type": "return",
        "horizon": 5,
        "benchmark": None,
    },
    {
        "id": "preset_fwd_return_20d",
        "name": "fwd_return_20d",
        "description": "20-day forward return",
        "target_type": "return",
        "horizon": 20,
        "benchmark": None,
    },
    {
        "id": "preset_fwd_rank_5d",
        "name": "fwd_rank_5d",
        "description": "Cross-sectional rank of 5-day forward return (0~1)",
        "target_type": "rank",
        "horizon": 5,
        "benchmark": None,
    },
    {
        "id": "preset_fwd_excess_5d",
        "name": "fwd_excess_5d",
        "description": "5-day forward excess return vs SPY",
        "target_type": "excess_return",
        "horizon": 5,
        "benchmark": "SPY",
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
                conn.execute(
                    """INSERT INTO label_definitions
                       (id, name, description, target_type, horizon, benchmark, status)
                       VALUES (?, ?, ?, ?, ?, ?, 'active')""",
                    [
                        preset["id"],
                        preset["name"],
                        preset["description"],
                        preset["target_type"],
                        preset["horizon"],
                        preset["benchmark"],
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
    ) -> dict:
        """Create a new label definition."""
        if target_type not in _VALID_TARGET_TYPES:
            raise ValueError(
                f"target_type must be one of {_VALID_TARGET_TYPES}, got '{target_type}'"
            )
        if target_type == "excess_return" and not benchmark:
            raise ValueError("benchmark is required for excess_return target_type")
        if horizon < 1:
            raise ValueError("horizon must be >= 1")

        conn = get_connection()
        label_id = uuid.uuid4().hex[:12]
        now = datetime.utcnow()

        conn.execute(
            """INSERT INTO label_definitions
               (id, name, description, target_type, horizon, benchmark, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, 'draft', ?, ?)""",
            [label_id, name, description, target_type, horizon, benchmark, now, now],
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
                      benchmark, status, created_at, updated_at
               FROM label_definitions
               ORDER BY created_at"""
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

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

        if target_type == "return":
            combined["label_value"] = combined["fwd_return"]

        elif target_type == "binary":
            combined["label_value"] = (combined["fwd_return"] > 0).astype(float)
            combined.loc[combined["fwd_return"].isna(), "label_value"] = np.nan

        elif target_type == "rank":
            combined["label_value"] = combined.groupby("date")["fwd_return"].rank(pct=True)

        elif target_type == "excess_return":
            if not benchmark:
                raise ValueError("benchmark is required for excess_return")
            # Load benchmark returns
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
                combined["label_value"] = np.nan
            else:
                bench_df = bench_df.sort_values("date").reset_index(drop=True)
                bench_fwd = bench_df["close"].shift(-horizon)
                bench_return = (bench_fwd - bench_df["close"]) / bench_df["close"]
                bench_map = dict(zip(bench_df["date"], bench_return))
                combined["bench_return"] = combined["date"].map(bench_map)
                combined["label_value"] = combined["fwd_return"] - combined["bench_return"]
                combined.drop(columns=["bench_return"], inplace=True)

        combined.drop(columns=["fwd_return"], inplace=True)
        return combined

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_row(self, label_id: str) -> dict | None:
        conn = get_connection()
        row = conn.execute(
            """SELECT id, name, description, target_type, horizon,
                      benchmark, status, created_at, updated_at
               FROM label_definitions WHERE id = ?""",
            [label_id],
        ).fetchone()
        if row is None:
            return None
        return self._row_to_dict(row)

    @staticmethod
    def _row_to_dict(row) -> dict:
        return {
            "id": row[0],
            "name": row[1],
            "description": row[2],
            "target_type": row[3],
            "horizon": row[4],
            "benchmark": row[5],
            "status": row[6],
            "created_at": str(row[7]) if row[7] else None,
            "updated_at": str(row[8]) if row[8] else None,
        }

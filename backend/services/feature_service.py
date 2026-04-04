"""Feature set management and preprocessing pipeline."""

from __future__ import annotations

import json
import uuid
from datetime import datetime

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from backend.db import get_connection
from backend.logger import get_logger
from backend.services.factor_engine import FactorEngine

log = get_logger(__name__)

_VALID_MISSING = {"forward_fill", "cross_sectional_mean", "drop"}
_VALID_OUTLIER = {"mad", "winsorize", None}
_VALID_NORMALIZE = {"zscore", "rank", None}
_VALID_NEUTRALIZE = {None}

_DEFAULT_PREPROCESSING = {
    "missing": "forward_fill",
    "outlier": "mad",
    "normalize": "rank",
    "neutralize": None,
}


class FeatureService:
    """CRUD and computation for feature sets (collections of factors with preprocessing)."""

    def __init__(self) -> None:
        self._factor_engine = FactorEngine()

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def create_feature_set(
        self,
        name: str,
        description: str | None = None,
        factor_refs: list[dict] | None = None,
        preprocessing: dict | None = None,
    ) -> dict:
        """Create a new feature set."""
        if not name or not name.strip():
            raise ValueError("name must not be empty")
        if not factor_refs:
            raise ValueError("factor_refs must contain at least one factor reference")

        preprocessing = self._validate_preprocessing(preprocessing)

        conn = get_connection()
        fs_id = uuid.uuid4().hex[:12]
        now = datetime.utcnow()

        conn.execute(
            """INSERT INTO feature_sets
               (id, name, description, factor_refs, preprocessing, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, 'draft', ?, ?)""",
            [
                fs_id,
                name.strip(),
                description,
                json.dumps(factor_refs),
                json.dumps(preprocessing),
                now,
                now,
            ],
        )
        log.info("feature_set.created", id=fs_id, name=name)
        return self.get_feature_set(fs_id)

    def update_feature_set(
        self,
        fs_id: str,
        name: str | None = None,
        description: str | None = None,
        factor_refs: list[dict] | None = None,
        preprocessing: dict | None = None,
        status: str | None = None,
    ) -> dict:
        """Update an existing feature set."""
        conn = get_connection()
        existing = self._fetch_row(fs_id)
        if existing is None:
            raise ValueError(f"Feature set {fs_id} not found")

        now = datetime.utcnow()
        sets: list[str] = ["updated_at = ?"]
        params: list = [now]

        if name is not None:
            sets.append("name = ?")
            params.append(name.strip())
        if description is not None:
            sets.append("description = ?")
            params.append(description)
        if factor_refs is not None:
            if not factor_refs:
                raise ValueError("factor_refs must contain at least one factor reference")
            sets.append("factor_refs = ?")
            params.append(json.dumps(factor_refs))
        if preprocessing is not None:
            preprocessing = self._validate_preprocessing(preprocessing)
            sets.append("preprocessing = ?")
            params.append(json.dumps(preprocessing))
        if status is not None:
            sets.append("status = ?")
            params.append(status)

        params.append(fs_id)
        conn.execute(
            f"UPDATE feature_sets SET {', '.join(sets)} WHERE id = ?", params
        )
        log.info("feature_set.updated", id=fs_id)
        return self.get_feature_set(fs_id)

    def delete_feature_set(self, fs_id: str) -> None:
        """Delete a feature set."""
        conn = get_connection()
        existing = self._fetch_row(fs_id)
        if existing is None:
            raise ValueError(f"Feature set {fs_id} not found")

        conn.execute("DELETE FROM feature_sets WHERE id = ?", [fs_id])
        log.info("feature_set.deleted", id=fs_id)

    def get_feature_set(self, fs_id: str) -> dict:
        """Return a single feature set."""
        row = self._fetch_row(fs_id)
        if row is None:
            raise ValueError(f"Feature set {fs_id} not found")
        return row

    def list_feature_sets(self) -> list[dict]:
        """List all feature sets."""
        conn = get_connection()
        rows = conn.execute(
            """SELECT id, name, description, factor_refs, preprocessing,
                      status, created_at, updated_at
               FROM feature_sets
               ORDER BY created_at"""
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Compute features (factor values + preprocessing pipeline)
    # ------------------------------------------------------------------

    def compute_features(
        self,
        fs_id: str,
        tickers: list[str],
        start_date: str,
        end_date: str,
    ) -> dict[str, pd.DataFrame]:
        """Compute preprocessed feature values for a feature set.

        Returns:
            dict mapping factor_name -> DataFrame(dates x tickers).
        """
        fs = self.get_feature_set(fs_id)
        factor_refs = fs["factor_refs"]
        preprocessing = fs["preprocessing"]

        log.info(
            "feature_set.compute.start",
            fs_id=fs_id,
            factors=len(factor_refs),
            tickers=len(tickers),
        )

        # 1. Compute raw factor values
        raw_data: dict[str, pd.DataFrame] = {}
        for ref in factor_refs:
            factor_id = ref["factor_id"]
            factor_name = ref.get("factor_name", factor_id)
            try:
                df = self._factor_engine.compute_factor(
                    factor_id, tickers, start_date, end_date
                )
                if not df.empty:
                    raw_data[factor_name] = df
                else:
                    log.warning(
                        "feature_set.compute.empty_factor",
                        factor_id=factor_id,
                        factor_name=factor_name,
                    )
            except Exception as exc:
                log.warning(
                    "feature_set.compute.factor_failed",
                    factor_id=factor_id,
                    error=str(exc),
                )

        if not raw_data:
            raise ValueError("No factor data could be computed for this feature set")

        # 2. Apply preprocessing pipeline to each factor independently
        result: dict[str, pd.DataFrame] = {}
        for factor_name, df in raw_data.items():
            processed = self._apply_preprocessing(df, preprocessing)
            result[factor_name] = processed

        log.info(
            "feature_set.compute.done",
            fs_id=fs_id,
            factors_computed=len(result),
        )
        return result

    # ------------------------------------------------------------------
    # Correlation matrix
    # ------------------------------------------------------------------

    def compute_correlation_matrix(
        self,
        fs_id: str,
        tickers: list[str],
        start_date: str,
        end_date: str,
    ) -> dict:
        """Compute pairwise correlation between all factors in the set.

        Returns:
            {factor_names: [...], matrix: [[...]]}
        """
        feature_data = self.compute_features(fs_id, tickers, start_date, end_date)

        factor_names = sorted(feature_data.keys())
        n = len(factor_names)

        if n < 2:
            return {
                "factor_names": factor_names,
                "matrix": [[1.0]] if n == 1 else [],
            }

        # Stack all factors into a single panel: for each date, flatten all
        # factor-ticker values into a long vector, then compute correlations
        # across dates.  More precisely: for each date, collect the
        # cross-sectional vector of each factor, then compute Spearman
        # rank-correlation between pairs of factors averaged over dates.

        # Collect cross-sectional vectors per factor per date
        all_dates = set()
        for df in feature_data.values():
            all_dates.update(df.index.tolist())
        common_dates = sorted(all_dates)

        # Build a matrix: rows = dates, columns = factors, values = average
        # cross-sectional rank for that factor on that date.
        # Actually we want pairwise factor correlation computed cross-sectionally:
        # for each date, correlate factor_i values across tickers with factor_j values.
        # Then average across dates.

        corr_sums = np.zeros((n, n))
        corr_counts = np.zeros((n, n))

        for dt in common_dates:
            vectors: dict[int, pd.Series] = {}
            for i, fname in enumerate(factor_names):
                df = feature_data[fname]
                if dt in df.index:
                    vectors[i] = df.loc[dt].dropna()

            for i in range(n):
                for j in range(i, n):
                    if i not in vectors or j not in vectors:
                        continue
                    vi = vectors[i]
                    vj = vectors[j]
                    common_tickers = vi.index.intersection(vj.index)
                    if len(common_tickers) < 10:
                        continue
                    a = vi.loc[common_tickers].values
                    b = vj.loc[common_tickers].values
                    if np.std(a) == 0 or np.std(b) == 0:
                        continue
                    corr, _ = spearmanr(a, b)
                    if not np.isnan(corr):
                        corr_sums[i, j] += corr
                        corr_sums[j, i] += corr
                        corr_counts[i, j] += 1
                        corr_counts[j, i] += 1

        # Build result matrix
        matrix: list[list[float | None]] = []
        for i in range(n):
            row: list[float | None] = []
            for j in range(n):
                if i == j:
                    row.append(1.0)
                elif corr_counts[i, j] > 0:
                    row.append(round(float(corr_sums[i, j] / corr_counts[i, j]), 6))
                else:
                    row.append(None)
            matrix.append(row)

        return {
            "factor_names": factor_names,
            "matrix": matrix,
        }

    # ------------------------------------------------------------------
    # Preprocessing pipeline
    # ------------------------------------------------------------------

    def _apply_preprocessing(
        self, df: pd.DataFrame, preprocessing: dict
    ) -> pd.DataFrame:
        """Apply the preprocessing pipeline to a factor DataFrame (dates x tickers).

        All cross-sectional operations are computed PER DATE (row-wise).
        """
        result = df.copy()

        # Step 1: Missing value handling
        missing_method = preprocessing.get("missing", "forward_fill")
        result = self._handle_missing(result, missing_method)

        # Step 2: Outlier removal
        outlier_method = preprocessing.get("outlier")
        if outlier_method:
            result = self._handle_outliers(result, outlier_method)

        # Step 3: Normalization (cross-sectional, per date)
        normalize_method = preprocessing.get("normalize")
        if normalize_method:
            result = self._handle_normalize(result, normalize_method)

        # Step 4: Neutralization (skip for now)
        # neutralize_method = preprocessing.get("neutralize")

        return result

    @staticmethod
    def _handle_missing(df: pd.DataFrame, method: str) -> pd.DataFrame:
        """Handle missing values."""
        if method == "forward_fill":
            # Forward fill per ticker (column-wise), then backward fill
            # for any remaining NaNs at the start
            return df.ffill().bfill()
        elif method == "cross_sectional_mean":
            # Replace NaN with the cross-sectional mean for that date (row mean)
            row_means = df.mean(axis=1)
            return df.apply(lambda col: col.fillna(row_means))
        elif method == "drop":
            # Drop rows (dates) that have any NaN
            return df.dropna(how="any")
        else:
            return df

    @staticmethod
    def _handle_outliers(df: pd.DataFrame, method: str) -> pd.DataFrame:
        """Handle outliers cross-sectionally (per date/row)."""
        if method == "mad":
            # 3x MAD clipping, computed per date (row)
            median = df.median(axis=1)
            mad = df.sub(median, axis=0).abs().median(axis=1)
            # MAD-based bounds: median +/- 3 * 1.4826 * MAD
            # 1.4826 is the consistency constant for normal distribution
            scale = 3.0 * 1.4826 * mad
            lower = median - scale
            upper = median + scale
            return df.clip(lower=lower, upper=upper, axis=0)
        elif method == "winsorize":
            # Winsorize at 1st and 99th percentile per date (row)
            def _winsorize_row(row: pd.Series) -> pd.Series:
                valid = row.dropna()
                if len(valid) < 5:
                    return row
                p01 = valid.quantile(0.01)
                p99 = valid.quantile(0.99)
                return row.clip(lower=p01, upper=p99)

            return df.apply(_winsorize_row, axis=1)
        else:
            return df

    @staticmethod
    def _handle_normalize(df: pd.DataFrame, method: str) -> pd.DataFrame:
        """Normalize cross-sectionally (per date/row)."""
        if method == "zscore":
            # Z-score per date: (x - mean) / std across tickers
            row_mean = df.mean(axis=1)
            row_std = df.std(axis=1)
            # Avoid division by zero
            row_std = row_std.replace(0, np.nan)
            return df.sub(row_mean, axis=0).div(row_std, axis=0)
        elif method == "rank":
            # Cross-sectional rank per date, scaled to 0~1
            def _rank_row(row: pd.Series) -> pd.Series:
                return row.rank(pct=True)

            return df.apply(_rank_row, axis=1)
        else:
            return df

    # ------------------------------------------------------------------
    # Validation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_preprocessing(preprocessing: dict | None) -> dict:
        """Validate and fill defaults for preprocessing config."""
        if preprocessing is None:
            return dict(_DEFAULT_PREPROCESSING)

        result = dict(_DEFAULT_PREPROCESSING)
        result.update(preprocessing)

        missing = result.get("missing")
        if missing not in _VALID_MISSING:
            raise ValueError(
                f"preprocessing.missing must be one of {_VALID_MISSING}, got '{missing}'"
            )

        outlier = result.get("outlier")
        if outlier not in _VALID_OUTLIER:
            raise ValueError(
                f"preprocessing.outlier must be one of {_VALID_OUTLIER}, got '{outlier}'"
            )

        normalize = result.get("normalize")
        if normalize not in _VALID_NORMALIZE:
            raise ValueError(
                f"preprocessing.normalize must be one of {_VALID_NORMALIZE}, got '{normalize}'"
            )

        neutralize = result.get("neutralize")
        if neutralize not in _VALID_NEUTRALIZE:
            raise ValueError(
                f"preprocessing.neutralize must be one of {_VALID_NEUTRALIZE}, got '{neutralize}'"
            )

        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_row(self, fs_id: str) -> dict | None:
        conn = get_connection()
        row = conn.execute(
            """SELECT id, name, description, factor_refs, preprocessing,
                      status, created_at, updated_at
               FROM feature_sets WHERE id = ?""",
            [fs_id],
        ).fetchone()
        if row is None:
            return None
        return self._row_to_dict(row)

    @staticmethod
    def _row_to_dict(row) -> dict:
        def _parse_json(raw):
            if isinstance(raw, str):
                try:
                    return json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    return {}
            return raw if raw else {}

        return {
            "id": row[0],
            "name": row[1],
            "description": row[2],
            "factor_refs": _parse_json(row[3]),
            "preprocessing": _parse_json(row[4]),
            "status": row[5],
            "created_at": str(row[6]) if row[6] else None,
            "updated_at": str(row[7]) if row[7] else None,
        }

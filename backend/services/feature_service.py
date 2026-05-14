"""Feature set management and preprocessing pipeline."""

from __future__ import annotations

import json
import uuid

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from backend.db import get_connection
from backend.logger import get_logger
from backend.services.factor_engine import FactorEngine
from backend.services.market_context import normalize_market, normalize_ticker
from backend.services.research_cache_service import ResearchCacheService
from backend.time_utils import utc_now_naive

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

# Map frontend shorthand values to internal canonical values.
_PREPROCESSING_ALIASES = {
    "missing": {
        "ffill": "forward_fill",
        "forward_fill": "forward_fill",
        "cross_sectional_mean": "cross_sectional_mean",
        "median": "cross_sectional_mean",
        "drop": "drop",
        "zero": "drop",  # closest available
    },
    "outlier": {
        "winsorize": "winsorize",
        "clip": "winsorize",
        "mad": "mad",
        "zscore": "mad",
        "none": None,
    },
    "normalize": {
        "zscore": "zscore",
        "minmax": "zscore",  # closest available
        "rank": "rank",
        "none": None,
    },
    "neutralize": {
        "industry": None,   # not yet implemented; accept silently
        "market": None,
        "both": None,
        "none": None,
    },
}


class FeatureService:
    """CRUD and computation for feature sets (collections of factors with preprocessing)."""

    def __init__(
        self,
        factor_engine: FactorEngine | None = None,
        cache_service: ResearchCacheService | None = None,
    ) -> None:
        self._factor_engine = factor_engine or FactorEngine()
        self._cache_service = cache_service or ResearchCacheService()

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def create_feature_set(
        self,
        name: str,
        description: str | None = None,
        factor_refs: list[dict] | None = None,
        preprocessing: dict | None = None,
        market: str | None = None,
    ) -> dict:
        """Create a new feature set."""
        resolved_market = normalize_market(market)
        if not name or not name.strip():
            raise ValueError("name must not be empty")
        if not factor_refs:
            raise ValueError("factor_refs must contain at least one factor reference")
        self._validate_factor_refs(factor_refs, resolved_market)

        preprocessing = self._validate_preprocessing(preprocessing)

        conn = get_connection()
        fs_id = uuid.uuid4().hex[:12]
        now = utc_now_naive()

        conn.execute(
            """INSERT INTO feature_sets
               (id, market, name, description, factor_refs, preprocessing, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, 'draft', ?, ?)""",
            [
                fs_id,
                resolved_market,
                name.strip(),
                description,
                json.dumps(factor_refs),
                json.dumps(preprocessing),
                now,
                now,
            ],
        )
        log.info("feature_set.created", id=fs_id, market=resolved_market, name=name)
        return self.get_feature_set(fs_id, market=resolved_market)

    def update_feature_set(
        self,
        fs_id: str,
        name: str | None = None,
        description: str | None = None,
        factor_refs: list[dict] | None = None,
        preprocessing: dict | None = None,
        status: str | None = None,
        market: str | None = None,
    ) -> dict:
        """Update an existing feature set."""
        conn = get_connection()
        existing = self._fetch_row(fs_id, market)
        if existing is None:
            raise ValueError(f"Feature set {fs_id} not found")
        resolved_market = existing["market"]

        now = utc_now_naive()
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
            self._validate_factor_refs(factor_refs, resolved_market)
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
        params.append(resolved_market)
        conn.execute(
            f"UPDATE feature_sets SET {', '.join(sets)} WHERE id = ? AND market = ?", params
        )
        log.info("feature_set.updated", id=fs_id, market=resolved_market)
        return self.get_feature_set(fs_id, market=resolved_market)

    def delete_feature_set(self, fs_id: str, market: str | None = None) -> None:
        """Delete a feature set."""
        conn = get_connection()
        existing = self._fetch_row(fs_id, market)
        if existing is None:
            raise ValueError(f"Feature set {fs_id} not found")

        conn.execute("DELETE FROM feature_sets WHERE id = ? AND market = ?", [fs_id, existing["market"]])
        log.info("feature_set.deleted", id=fs_id, market=existing["market"])

    def get_feature_set(self, fs_id: str, market: str | None = None) -> dict:
        """Return a single feature set."""
        row = self._fetch_row(fs_id, market)
        if row is None:
            raise ValueError(f"Feature set {fs_id} not found")
        return row

    def list_feature_sets(self, market: str | None = None) -> list[dict]:
        """List all feature sets."""
        resolved_market = normalize_market(market)
        conn = get_connection()
        rows = conn.execute(
            """SELECT id, market, name, description, factor_refs, preprocessing,
                      status, created_at, updated_at
               FROM feature_sets
               WHERE market = ?
               ORDER BY created_at""",
            [resolved_market],
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
        market: str | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Compute preprocessed feature values for a feature set.

        Returns:
            dict mapping factor_name -> DataFrame(dates x tickers).
        """
        fs = self.get_feature_set(fs_id, market=market)
        resolved_market = fs["market"]
        tickers = [normalize_ticker(t, resolved_market) for t in tickers]
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
                    factor_id, tickers, start_date, end_date, market=resolved_market
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

    def compute_features_from_cache(
        self,
        fs_id: str,
        tickers: list[str],
        start_date: str,
        end_date: str,
        market: str | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Load preprocessed features using bulk cache read (single DB query).

        Falls back to the standard per-factor path for any factors missing
        from the cache.  Much faster when most factors are already cached.
        """
        fs = self.get_feature_set(fs_id, market=market)
        resolved_market = fs["market"]
        tickers = [normalize_ticker(t, resolved_market) for t in tickers]
        factor_refs = fs["factor_refs"]
        preprocessing = fs["preprocessing"]

        # Build factor_id -> factor_name mapping
        id_to_name: dict[str, str] = {}
        for ref in factor_refs:
            fid = ref["factor_id"]
            fname = ref.get("factor_name", fid)
            id_to_name[fid] = fname

        factor_ids = list(id_to_name.keys())

        log.info(
            "feature_set.compute_from_cache.start",
            fs_id=fs_id,
            factors=len(factor_ids),
            tickers=len(tickers),
        )

        hot_cache = self._cache_service.load_feature_matrix(
            market=resolved_market,
            feature_set_id=fs_id,
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            factor_refs=factor_refs,
            preprocessing=preprocessing,
        )
        if hot_cache is not None:
            log.info(
                "feature_set.compute_from_cache.hot_hit",
                fs_id=fs_id,
                cache_key=hot_cache["record"]["cache_key"],
            )
            return hot_cache["feature_data"]

        # Bulk load all cached factor values in one query
        cached = self._factor_engine.load_cached_factors_bulk(
            factor_ids, tickers, start_date, end_date, market=resolved_market
        )

        # Map factor_id results to factor_name keys
        raw_data: dict[str, pd.DataFrame] = {}
        missing_refs: list[dict] = []
        for ref in factor_refs:
            fid = ref["factor_id"]
            fname = ref.get("factor_name", fid)
            cached_df = cached.get(fid)
            if cached_df is not None and not cached_df.empty and self._factor_cache_covers_request(
                cached_df,
                tickers,
                start_date,
                end_date,
            ):
                raw_data[fname] = cached_df
            else:
                missing_refs.append(ref)

        # Fall back to per-factor computation for uncached factors
        if missing_refs:
            log.info(
                "feature_set.compute_from_cache.fallback",
                missing=len(missing_refs),
            )
            for ref in missing_refs:
                fid = ref["factor_id"]
                fname = ref.get("factor_name", fid)
                try:
                    df = self._factor_engine.compute_factor(
                        fid, tickers, start_date, end_date, market=resolved_market
                    )
                    if not df.empty:
                        raw_data[fname] = df
                except Exception as exc:
                    log.warning(
                        "feature_set.compute_from_cache.factor_failed",
                        factor_id=fid,
                        error=str(exc),
                    )

        if not raw_data:
            raise ValueError("No factor data could be loaded for this feature set")

        # Apply preprocessing
        result: dict[str, pd.DataFrame] = {}
        for factor_name, df in raw_data.items():
            result[factor_name] = self._apply_preprocessing(df, preprocessing)

        try:
            self._cache_service.store_feature_matrix(
                market=resolved_market,
                feature_set_id=fs_id,
                tickers=tickers,
                start_date=start_date,
                end_date=end_date,
                factor_refs=factor_refs,
                preprocessing=preprocessing,
                feature_data=result,
            )
        except Exception as exc:
            log.warning(
                "feature_set.compute_from_cache.hot_store_failed",
                fs_id=fs_id,
                error=str(exc),
            )

        log.info(
            "feature_set.compute_from_cache.done",
            fs_id=fs_id,
            factors_computed=len(result),
        )
        return result

    @staticmethod
    def _factor_cache_covers_request(
        df: pd.DataFrame,
        tickers: list[str],
        start_date: str,
        end_date: str,
    ) -> bool:
        """Return whether a cached factor frame covers the requested panel."""
        if df.empty:
            return False
        missing_tickers = set(tickers).difference(set(df.columns.astype(str)))
        if missing_tickers:
            return False
        index = pd.to_datetime(df.index)
        start = pd.Timestamp(start_date)
        end = pd.Timestamp(end_date)
        if index.min() > start or index.max() < end:
            return False
        in_range = df.loc[(index >= start) & (index <= end), tickers]
        if in_range.empty:
            return False
        return True

    # ------------------------------------------------------------------
    # Correlation matrix
    # ------------------------------------------------------------------

    def compute_correlation_matrix(
        self,
        fs_id: str,
        tickers: list[str],
        start_date: str,
        end_date: str,
        market: str | None = None,
    ) -> dict:
        """Compute pairwise correlation between all factors in the set.

        Returns:
            {factor_names: [...], matrix: [[...]]}
        """
        feature_data = self.compute_features_from_cache(fs_id, tickers, start_date, end_date, market=market)

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
            # Forward fill per ticker (column-wise) only.
            # Backward filling would leak future information into earlier dates
            # and make features depend on the requested end_date.
            return df.ffill()
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
            # Winsorize at 1st and 99th percentile per date (row) — vectorized
            lower = df.quantile(0.01, axis=1)
            upper = df.quantile(0.99, axis=1)
            return df.clip(lower=lower, upper=upper, axis=0)
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
            # Cross-sectional rank per date, scaled to 0~1 — vectorized
            return df.rank(axis=1, pct=True)
        else:
            return df

    # ------------------------------------------------------------------
    # Validation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_preprocessing(preprocessing: dict | None) -> dict:
        """Validate and fill defaults for preprocessing config.

        Accepts both canonical values and frontend shorthand aliases.
        """
        if preprocessing is None:
            return dict(_DEFAULT_PREPROCESSING)

        result = dict(_DEFAULT_PREPROCESSING)
        result.update(preprocessing)

        # Resolve aliases for each key
        for key in ("missing", "outlier", "normalize", "neutralize"):
            val = result.get(key)
            if val is None:
                continue
            alias_map = _PREPROCESSING_ALIASES.get(key, {})
            if isinstance(val, str):
                canonical = alias_map.get(val.lower())
                if canonical is not None or val.lower() in alias_map:
                    result[key] = alias_map.get(val.lower())
                # else leave as-is for validation below

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

    def _fetch_row(self, fs_id: str, market: str | None = None) -> dict | None:
        resolved_market = normalize_market(market)
        conn = get_connection()
        row = conn.execute(
            """SELECT id, market, name, description, factor_refs, preprocessing,
                      status, created_at, updated_at
               FROM feature_sets WHERE id = ? AND market = ?""",
            [fs_id, resolved_market],
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
            "market": row[1],
            "name": row[2],
            "description": row[3],
            "factor_refs": _parse_json(row[4]),
            "preprocessing": _parse_json(row[5]),
            "status": row[6],
            "created_at": str(row[7]) if row[7] else None,
            "updated_at": str(row[8]) if row[8] else None,
        }

    def _validate_factor_refs(self, factor_refs: list[dict], market: str) -> None:
        conn = get_connection()
        for ref in factor_refs:
            factor_id = ref.get("factor_id")
            if not factor_id:
                raise ValueError("factor_refs entries must include factor_id")
            row = conn.execute(
                "SELECT market FROM factors WHERE id = ?",
                [factor_id],
            ).fetchone()
            if row is None:
                raise ValueError(f"Factor {factor_id} not found")
            if row[0] != market:
                raise ValueError(
                    f"Factor {factor_id} belongs to market {row[0]}, not {market}"
                )

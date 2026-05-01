"""Utilities for date-grouped cross-sectional ranking datasets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class RankingDataset:
    X: pd.DataFrame
    y: pd.Series
    raw_y: pd.Series
    group_sizes: list[int]
    dates: list[str]
    dropped_groups: int = 0


def build_date_groups(
    X: pd.DataFrame,
    y: pd.Series,
    *,
    min_group_size: int = 5,
    label_gain: str = "ordinal",
) -> RankingDataset:
    """Build LightGBM ranking groups from a MultiIndex(date, ticker) dataset.

    LightGBM's ranker requires integer relevance labels. For continuous return
    labels, we preserve the within-day ordering by converting each date's raw
    labels to ordinal gains where larger raw labels receive larger gains.
    """
    if not isinstance(X.index, pd.MultiIndex) or "date" not in X.index.names:
        raise ValueError("Ranking datasets require X indexed by MultiIndex(date, ticker)")
    if not isinstance(y.index, pd.MultiIndex) or "date" not in y.index.names:
        raise ValueError("Ranking datasets require y indexed by MultiIndex(date, ticker)")

    min_group_size = max(2, int(min_group_size))
    common_idx = X.index.intersection(y.index)
    aligned = X.loc[common_idx].copy()
    raw_y = y.loc[common_idx].copy()

    mask = aligned.notna().all(axis=1) & raw_y.notna()
    aligned = aligned.loc[mask]
    raw_y = raw_y.loc[mask]
    aligned = _normalize_date_index(aligned)
    raw_y = _normalize_date_index(raw_y)

    if aligned.empty:
        return RankingDataset(
            X=pd.DataFrame(columns=X.columns),
            y=pd.Series(dtype=int, name="label_gain"),
            raw_y=pd.Series(dtype=float, name="raw_label"),
            group_sizes=[],
            dates=[],
        )

    frame = aligned.copy()
    frame["_raw_label"] = raw_y.astype(float)
    frame = frame.sort_index(level=["date", "ticker"])

    frames: list[pd.DataFrame] = []
    gains: list[pd.Series] = []
    raw_parts: list[pd.Series] = []
    group_sizes: list[int] = []
    dates: list[str] = []
    dropped = 0

    for dt, group in frame.groupby(level="date", sort=True):
        if len(group) < min_group_size:
            dropped += 1
            continue
        raw = group["_raw_label"].astype(float)
        gain = _to_gain_labels(raw, label_gain=label_gain)
        features = group.drop(columns=["_raw_label"])
        frames.append(features)
        gains.append(gain)
        raw_parts.append(raw)
        group_sizes.append(len(group))
        dates.append(str(pd.Timestamp(dt).date()))

    if not frames:
        return RankingDataset(
            X=pd.DataFrame(columns=X.columns),
            y=pd.Series(dtype=int, name="label_gain"),
            raw_y=pd.Series(dtype=float, name="raw_label"),
            group_sizes=[],
            dates=[],
            dropped_groups=dropped,
        )

    X_rank = pd.concat(frames).sort_index(level=["date", "ticker"])
    y_rank = pd.concat(gains).loc[X_rank.index].astype(int)
    y_rank.name = "label_gain"
    raw_rank = pd.concat(raw_parts).loc[X_rank.index].astype(float)
    raw_rank.name = "raw_label"

    return RankingDataset(
        X=X_rank,
        y=y_rank,
        raw_y=raw_rank,
        group_sizes=group_sizes,
        dates=dates,
        dropped_groups=dropped,
    )


def compute_ranking_metrics(
    raw_y: pd.Series,
    preds: pd.Series,
    *,
    eval_at: list[int] | tuple[int, ...] = (5, 10, 20),
    pairwise_sample_limit: int = 2000,
) -> dict[str, Any]:
    """Compute cross-sectional ranking metrics on date-grouped predictions."""
    if raw_y.empty or preds.empty:
        return {}

    common_idx = raw_y.index.intersection(preds.index)
    raw_y = raw_y.loc[common_idx].astype(float)
    preds = preds.loc[common_idx].astype(float)
    if raw_y.empty:
        return {}

    metrics: dict[str, Any] = {}
    eval_points = sorted({int(k) for k in eval_at if int(k) > 0})
    ndcg_values = {k: [] for k in eval_points}
    top_label_values = {k: [] for k in eval_points}
    rank_ics: list[float] = []
    pairwise_correct = 0
    pairwise_total = 0

    for _, y_day in raw_y.groupby(level="date", sort=True):
        p_day = preds.loc[y_day.index]
        if len(y_day) < 2:
            continue

        y_values = y_day.to_numpy(dtype=float)
        p_values = p_day.to_numpy(dtype=float)

        for k in eval_points:
            kk = min(k, len(y_day))
            if kk <= 0:
                continue
            order = np.argsort(-p_values)[:kk]
            top_label_values[k].append(float(np.mean(y_values[order])))
            ndcg_values[k].append(_ndcg_at_k(y_values, p_values, kk))

        ic = pd.Series(y_values).corr(pd.Series(p_values), method="spearman")
        if ic is not None and not np.isnan(ic):
            rank_ics.append(float(ic))

        correct, total = _pairwise_accuracy_counts(
            y_values,
            p_values,
            limit=max(1, pairwise_sample_limit - pairwise_total),
        )
        pairwise_correct += correct
        pairwise_total += total
        if pairwise_total >= pairwise_sample_limit:
            pairwise_total = pairwise_sample_limit

    for k, values in ndcg_values.items():
        if values:
            metrics[f"ndcg@{k}"] = round(float(np.mean(values)), 6)
            metrics[f"top_{k}_mean_label"] = round(float(np.mean(top_label_values[k])), 6)

    if rank_ics:
        metrics["rank_ic_mean"] = round(float(np.mean(rank_ics)), 6)
        metrics["rank_ic_std"] = round(float(np.std(rank_ics)), 6) if len(rank_ics) > 1 else 0.0
    if pairwise_total > 0:
        metrics["pairwise_accuracy_sampled"] = round(float(pairwise_correct / pairwise_total), 6)
        metrics["pairwise_pairs_sampled"] = pairwise_total

    return metrics


def _to_gain_labels(raw: pd.Series, *, label_gain: str) -> pd.Series:
    if label_gain == "identity" and _is_dense_non_negative_integer_like(raw):
        gain = raw.astype(int)
    else:
        gain = raw.rank(method="first", ascending=True).astype(int) - 1
    gain.name = "label_gain"
    return gain


def _normalize_date_index(obj):
    if not isinstance(obj.index, pd.MultiIndex) or "date" not in obj.index.names:
        return obj
    arrays = []
    for name in obj.index.names:
        values = obj.index.get_level_values(name)
        if name == "date":
            arrays.append(pd.to_datetime(values))
        else:
            arrays.append(values)
    result = obj.copy()
    result.index = pd.MultiIndex.from_arrays(arrays, names=obj.index.names)
    return result


def _is_non_negative_integer_like(values: pd.Series) -> bool:
    arr = values.to_numpy(dtype=float)
    return bool(np.all(arr >= 0) and np.all(np.isclose(arr, np.round(arr))))


def _is_dense_non_negative_integer_like(values: pd.Series) -> bool:
    if not _is_non_negative_integer_like(values):
        return False
    labels = sorted({int(v) for v in values})
    if not labels:
        return False
    return labels == list(range(max(labels) + 1))


def _ndcg_at_k(y_true: np.ndarray, y_score: np.ndarray, k: int) -> float:
    gains = _gain_values(y_true)
    predicted_order = np.argsort(-y_score)[:k]
    ideal_order = np.argsort(-y_true)[:k]
    dcg = _dcg(gains[predicted_order])
    idcg = _dcg(gains[ideal_order])
    return float(dcg / idcg) if idcg > 0 else 0.0


def _gain_values(y_true: np.ndarray) -> np.ndarray:
    shifted = y_true - np.nanmin(y_true)
    return np.power(2.0, shifted) - 1.0


def _dcg(gains: np.ndarray) -> float:
    discounts = np.log2(np.arange(2, len(gains) + 2))
    return float(np.sum(gains / discounts))


def _pairwise_accuracy_counts(
    y_true: np.ndarray,
    y_score: np.ndarray,
    *,
    limit: int,
) -> tuple[int, int]:
    correct = 0
    total = 0
    n = len(y_true)
    for i in range(n):
        for j in range(i + 1, n):
            if total >= limit:
                return correct, total
            label_diff = y_true[i] - y_true[j]
            if label_diff == 0:
                continue
            pred_diff = y_score[i] - y_score[j]
            if pred_diff == 0:
                continue
            total += 1
            if (label_diff > 0 and pred_diff > 0) or (label_diff < 0 and pred_diff < 0):
                correct += 1
    return correct, total

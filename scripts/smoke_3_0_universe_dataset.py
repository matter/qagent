#!/usr/bin/env python3
"""Smoke test for QAgent 3.0 Universe + Dataset engine."""

from __future__ import annotations

from backend.db import init_db
from backend.services.dataset_service import DatasetService
from backend.services.factor_service import FactorService
from backend.services.feature_service import FeatureService
from backend.services.label_service import LabelService
from backend.services.universe_service import UniverseService


FACTOR_SOURCE = '''
from backend.factors.base import FactorBase
import pandas as pd

class DatasetSmokeMomentum(FactorBase):
    name = "DatasetSmokeMomentum"
    description = "5-day close momentum for M4 smoke"

    def compute(self, data):
        return data["close"].pct_change(5)
'''


def main() -> int:
    init_db()

    factor_id = _ensure_factor()
    feature_set_id = _ensure_feature_set(factor_id)
    label_id = _ensure_label()

    universe = UniverseService().create_from_legacy_group(
        legacy_group_id="test20",
        market="US",
        project_id="bootstrap_us",
        name="M4 Smoke test20 universe",
    )
    materialized_universe = UniverseService().materialize_universe(
        universe["id"],
        start_date="2025-01-02",
        end_date="2025-03-31",
    )
    assert materialized_universe["materialization"]["asset_count"] >= 10

    dataset = DatasetService().create_dataset(
        name="M4 Smoke Dataset",
        universe_id=universe["id"],
        feature_set_id=feature_set_id,
        label_id=label_id,
        start_date="2025-01-02",
        end_date="2025-03-31",
        split_policy={
            "train": {"start": "2025-01-02", "end": "2025-02-14"},
            "valid": {"start": "2025-02-18", "end": "2025-03-07"},
            "test": {"start": "2025-03-10", "end": "2025-03-31"},
            "purge_gap": 5,
        },
    )
    result = DatasetService().materialize_dataset(dataset["id"])
    profile = DatasetService().profile_dataset(dataset["id"])
    sample = DatasetService().sample_dataset(dataset["id"], limit=5)
    query = DatasetService().query_dataset(
        dataset["id"],
        start_date="2025-03-03",
        end_date="2025-03-07",
        columns=["DatasetSmokeMomentum", "label"],
        limit=10,
    )

    assert result["artifact"]["artifact_type"] == "dataset_panel"
    assert profile["coverage"]["row_count"] > 0
    assert profile["feature_count"] == 1
    assert len(sample["rows"]) == 5
    assert query["count"] > 0

    print(
        {
            "universe_id": universe["id"],
            "dataset_id": dataset["id"],
            "rows": profile["coverage"]["row_count"],
            "features": profile["feature_count"],
            "run_id": result["run"]["id"],
            "artifact_id": result["artifact"]["id"],
        }
    )
    return 0


def _ensure_factor() -> str:
    svc = FactorService()
    existing = [item for item in svc.list_factors(market="US") if item["name"] == "DatasetSmokeMomentum"]
    if existing:
        return existing[0]["id"]
    return svc.create_factor(
        name="DatasetSmokeMomentum",
        source_code=FACTOR_SOURCE,
        description="M4 smoke factor",
        category="custom",
        market="US",
    )["id"]


def _ensure_feature_set(factor_id: str) -> str:
    svc = FeatureService()
    existing = [item for item in svc.list_feature_sets(market="US") if item["name"] == "M4 Smoke Feature Set"]
    if existing:
        return existing[0]["id"]
    return svc.create_feature_set(
        name="M4 Smoke Feature Set",
        description="M4 dataset smoke feature set",
        factor_refs=[{"factor_id": factor_id, "factor_name": "DatasetSmokeMomentum", "version": 1}],
        preprocessing={"missing": "forward_fill", "outlier": None, "normalize": "rank", "neutralize": None},
        market="US",
    )["id"]


def _ensure_label() -> str:
    svc = LabelService()
    labels = svc.list_labels(market="US")
    for label in labels:
        if label["name"] == "fwd_return_5d":
            return label["id"]
    svc.ensure_presets(market="US")
    return next(label["id"] for label in svc.list_labels(market="US") if label["name"] == "fwd_return_5d")


if __name__ == "__main__":
    raise SystemExit(main())

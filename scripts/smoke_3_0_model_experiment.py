#!/usr/bin/env python3
"""Smoke test for QAgent 3.0 Model Experiment + Package."""

from __future__ import annotations

from backend.db import init_db
from backend.services.dataset_service import DatasetService
from backend.services.factor_service import FactorService
from backend.services.feature_service import FeatureService
from backend.services.label_service import LabelService
from backend.services.model_experiment_3_service import ModelExperiment3Service
from backend.services.universe_service import UniverseService


FACTOR_SOURCE = '''
from backend.factors.base import FactorBase
import pandas as pd

class Model3SmokeMomentum(FactorBase):
    name = "Model3SmokeMomentum"
    description = "5-day close momentum for M6 smoke"

    def compute(self, data):
        return data["close"].pct_change(5)
'''


def main() -> int:
    init_db()

    factor = _ensure_factor()
    feature_set = _ensure_feature_set(factor["id"])
    label_id = _ensure_label()
    universe = UniverseService().create_from_legacy_group(
        legacy_group_id="test20",
        market="US",
        project_id="bootstrap_us",
        name="M6 Smoke test20 universe",
    )
    dataset = DatasetService().create_dataset(
        name="M6 Smoke Dataset",
        universe_id=universe["id"],
        feature_set_id=feature_set["id"],
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
    materialized = DatasetService().materialize_dataset(dataset["id"])
    svc = ModelExperiment3Service()
    trained = svc.train_experiment(
        name="M6 Smoke Model Experiment",
        dataset_id=materialized["dataset"]["id"],
        model_params={"n_estimators": 12, "max_depth": 3, "random_state": 42},
        random_seed=42,
    )
    promoted = svc.promote_experiment(
        trained["experiment"]["id"],
        package_name="M6 Smoke Model Package",
        approved_by="smoke",
        rationale="M6 smoke promotion",
    )
    predicted = svc.predict_panel(
        model_package_id=promoted["package"]["id"],
        dataset_id=materialized["dataset"]["id"],
    )

    assert trained["experiment"]["status"] == "completed"
    assert trained["model_artifact"]["artifact_type"] == "model_file"
    assert trained["prediction_artifact"]["artifact_type"] == "model_predictions"
    assert promoted["promotion_record"]["decision"] == "promoted"
    assert predicted["profile"]["row_count"] > 0

    print(
        {
            "dataset_id": materialized["dataset"]["id"],
            "experiment_id": trained["experiment"]["id"],
            "package_id": promoted["package"]["id"],
            "prediction_rows": predicted["profile"]["row_count"],
            "model_artifact_id": trained["model_artifact"]["id"],
        }
    )
    return 0


def _ensure_factor() -> dict:
    svc = FactorService()
    for factor in svc.list_factors(market="US"):
        if factor["name"] == "Model3SmokeMomentum":
            return factor
    return svc.create_factor(
        name="Model3SmokeMomentum",
        source_code=FACTOR_SOURCE,
        description="M6 smoke factor",
        category="custom",
        market="US",
    )


def _ensure_feature_set(factor_id: str) -> dict:
    svc = FeatureService()
    for feature_set in svc.list_feature_sets(market="US"):
        if feature_set["name"] == "M6 Smoke Feature Set":
            return feature_set
    return svc.create_feature_set(
        name="M6 Smoke Feature Set",
        description="M6 model smoke feature set",
        factor_refs=[{"factor_id": factor_id, "factor_name": "Model3SmokeMomentum", "version": 1}],
        preprocessing={"missing": "forward_fill", "outlier": None, "normalize": None, "neutralize": None},
        market="US",
    )


def _ensure_label() -> str:
    svc = LabelService()
    svc.ensure_presets(market="US")
    for label in svc.list_labels(market="US"):
        if label["name"] == "fwd_return_5d":
            return label["id"]
    raise RuntimeError("Preset label fwd_return_5d was not seeded")


if __name__ == "__main__":
    raise SystemExit(main())

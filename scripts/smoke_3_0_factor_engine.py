#!/usr/bin/env python3
"""Smoke test for QAgent 3.0 Factor Engine."""

from __future__ import annotations

from backend.db import init_db
from backend.services.factor_engine_3_service import FactorEngine3Service
from backend.services.factor_service import FactorService
from backend.services.label_service import LabelService
from backend.services.universe_service import UniverseService


FACTOR_SOURCE = '''
from backend.factors.base import FactorBase
import pandas as pd

class Factor3SmokeMomentum(FactorBase):
    name = "Factor3SmokeMomentum"
    description = "5-day close momentum for M5 smoke"
    params = {"window": 5}
    category = "momentum"

    def compute(self, data):
        return data["close"].pct_change(5)
'''


def main() -> int:
    init_db()

    legacy_factor = _ensure_legacy_factor()
    label_id = _ensure_label()
    universe = UniverseService().create_from_legacy_group(
        legacy_group_id="test20",
        market="US",
        project_id="bootstrap_us",
        name="M5 Smoke test20 universe",
    )
    UniverseService().materialize_universe(
        universe["id"],
        start_date="2025-01-02",
        end_date="2025-03-31",
    )

    service = FactorEngine3Service()
    spec = service.create_spec_from_legacy_factor(
        legacy_factor_id=legacy_factor["id"],
        market="US",
        project_id="bootstrap_us",
        name="M5 Smoke FactorSpec",
    )
    preview = service.preview_factor(
        factor_spec_id=spec["id"],
        universe_id=universe["id"],
        start_date="2025-01-02",
        end_date="2025-03-31",
    )
    materialized = service.materialize_factor(
        factor_spec_id=spec["id"],
        universe_id=universe["id"],
        start_date="2025-01-02",
        end_date="2025-03-31",
    )
    evaluated = service.evaluate_factor_run(
        factor_run_id=materialized["factor_run"]["id"],
        label_id=label_id,
    )
    sample = service.sample_factor_run(materialized["factor_run"]["id"], limit=5)

    assert spec["source_type"] == "legacy_factor"
    assert preview["artifact"]["artifact_type"] == "factor_preview"
    assert materialized["artifact"]["artifact_type"] == "factor_values"
    assert materialized["profile"]["coverage"]["row_count"] > 0
    assert evaluated["evaluation_artifact"]["artifact_type"] == "factor_evaluation"
    assert "ic_mean" in evaluated["metrics"]
    assert len(sample["rows"]) == 5

    print(
        {
            "factor_spec_id": spec["id"],
            "factor_run_id": materialized["factor_run"]["id"],
            "rows": materialized["profile"]["coverage"]["row_count"],
            "artifact_id": materialized["artifact"]["id"],
            "evaluation_artifact_id": evaluated["evaluation_artifact"]["id"],
        }
    )
    return 0


def _ensure_legacy_factor() -> dict:
    svc = FactorService()
    for factor in svc.list_factors(market="US"):
        if factor["name"] == "Factor3SmokeMomentum":
            return factor
    return svc.create_factor(
        name="Factor3SmokeMomentum",
        source_code=FACTOR_SOURCE,
        description="M5 smoke legacy factor",
        category="custom",
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

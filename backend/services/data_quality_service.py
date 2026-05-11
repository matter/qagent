"""Data quality and provider capability contracts for QAgent 3.0."""

from __future__ import annotations

import json
from typing import Any

from backend.db import get_connection


class DataQualityService:
    """Expose explicit data-source capability and quality metadata."""

    def list_provider_capabilities(
        self,
        *,
        provider: str | None = None,
        market_profile_id: str | None = None,
        dataset: str | None = None,
    ) -> list[dict[str, Any]]:
        query = """SELECT provider, dataset, market_profile_id, capability,
                          quality_level, pit_supported, license_scope,
                          availability, as_of_date, available_at, metadata,
                          created_at, updated_at
                   FROM provider_capabilities
                   WHERE 1 = 1"""
        params: list[Any] = []
        if provider:
            query += " AND provider = ?"
            params.append(provider.strip().lower())
        if market_profile_id:
            query += " AND market_profile_id = ?"
            params.append(market_profile_id)
        if dataset:
            query += " AND dataset = ?"
            params.append(dataset)
        query += " ORDER BY provider, market_profile_id, dataset, capability"
        rows = get_connection().execute(query, params).fetchall()
        return [self._capability_row(row) for row in rows]

    def get_data_quality_contract(self, *, market_profile_id: str | None = None) -> dict:
        rows = self.list_provider_capabilities(market_profile_id=market_profile_id)
        publication_gates = self._publication_gates(rows, market_profile_id=market_profile_id)
        return {
            "market_profile_id": market_profile_id,
            "capabilities": rows,
            "summary": {
                "provider_count": len({row["provider"] for row in rows}),
                "dataset_count": len({(row["provider"], row["dataset"]) for row in rows}),
                "pit_supported_count": sum(1 for row in rows if row["pit_supported"]),
                "highest_quality_level": self._highest_quality_level(rows),
                "publication_grade": all(item["status"] == "pass" for item in publication_gates),
            },
            "policy": {
                "free_sources_are_not_assumed_pit": True,
                "missing_capability_blocks_validated_or_published_research": True,
                "research_grade_warning": (
                    "Exploratory/free sources may support local research, but they are not "
                    "publication-grade until PIT, survivorship, and corporate-action gates pass."
                ),
            },
            "publication_gates": publication_gates,
        }

    @staticmethod
    def _capability_row(row: tuple) -> dict[str, Any]:
        return {
            "provider": row[0],
            "dataset": row[1],
            "market_profile_id": row[2],
            "capability": row[3],
            "quality_level": row[4],
            "pit_supported": bool(row[5]),
            "license_scope": row[6],
            "availability": row[7],
            "as_of_date": str(row[8]) if row[8] is not None else None,
            "available_at": str(row[9]) if row[9] is not None else None,
            "metadata": _json(row[10], {}),
            "created_at": str(row[11]) if row[11] is not None else None,
            "updated_at": str(row[12]) if row[12] is not None else None,
        }

    @staticmethod
    def _highest_quality_level(rows: list[dict[str, Any]]) -> str | None:
        if not rows:
            return None
        order = {"unknown": 0, "exploratory": 1, "research_grade": 2, "validated": 3}
        return max(
            (str(row.get("quality_level") or "unknown") for row in rows),
            key=lambda item: order.get(item, -1),
        )

    @staticmethod
    def _publication_gates(
        rows: list[dict[str, Any]],
        *,
        market_profile_id: str | None,
    ) -> list[dict[str, Any]]:
        if not rows:
            return [
                {
                    "gate": "provider_capability",
                    "status": "blocked",
                    "reason": "No provider capability metadata is registered for this scope.",
                }
            ]

        pit_supported = any(row["pit_supported"] for row in rows)
        gates = [
            {
                "gate": "pit_data",
                "status": "pass" if pit_supported else "blocked",
                "reason": (
                    "At least one provider capability is PIT-supported."
                    if pit_supported
                    else "Registered free providers are explicitly non-PIT; promotion evidence must not treat them as strict historical replay."
                ),
            }
        ]

        is_equity_profile = market_profile_id in {"US_EQ", "CN_A"} or any(
            row["market_profile_id"] in {"US_EQ", "CN_A"} for row in rows
        )
        if is_equity_profile:
            gates.extend(
                [
                    {
                        "gate": "survivorship_safe_universe",
                        "status": "blocked",
                        "reason": "Current free stock lists do not include dated delistings and historical index/universe membership.",
                    },
                    {
                        "gate": "corporate_actions",
                        "status": "blocked",
                        "reason": "Corporate-action table exists, but free providers have not populated split/dividend/symbol-change history as dated facts.",
                    },
                ]
            )
        else:
            gates.extend(
                [
                    {
                        "gate": "survivorship_safe_universe",
                        "status": "not_applicable",
                        "reason": "Not an equity universe scope.",
                    },
                    {
                        "gate": "corporate_actions",
                        "status": "not_applicable",
                        "reason": "Not an equity price-adjustment scope.",
                    },
                ]
            )
        return gates


def _json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default

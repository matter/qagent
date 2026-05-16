"""QAgent V3.2 old-architecture separation inventory service.

The V3.2 migration is intentionally not a compatibility layer.  This service
only builds a read-only manifest that classifies old tables as sources for
re-entry, import, rebuild, archive, or delete work under the 3.0 architecture.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections import OrderedDict
from contextlib import contextmanager
from datetime import timezone
from pathlib import Path
from typing import Any

import duckdb

from backend.config import settings
from backend.db import get_connection
from backend.time_utils import utc_now_naive


_SOURCE_TABLES: "OrderedDict[str, dict[str, Any]]" = OrderedDict(
    [
        ("factors", {"action": "re_enter", "new_table": "factor_specs", "id_column": "id"}),
        ("stocks", {"action": "import", "new_table": "assets", "id_columns": ["market", "ticker"]}),
        ("daily_bars", {"action": "import", "new_table": "daily_bars", "id_columns": ["market", "ticker", "date"]}),
        ("stock_groups", {"action": "import", "new_table": "universes", "id_column": "id"}),
        (
            "stock_group_members",
            {"action": "import", "new_table": "universe_memberships", "id_columns": ["group_id", "market", "ticker"]},
        ),
        ("label_definitions", {"action": "re_enter", "new_table": "label_specs", "id_column": "id"}),
        ("feature_sets", {"action": "rebuild", "new_table": "feature_pipelines", "id_column": "id"}),
        ("models", {"action": "rebuild", "new_table": "model_packages", "id_column": "id"}),
        ("strategies", {"action": "rebuild", "new_table": "strategy_graphs", "id_column": "id"}),
        ("paper_trading_sessions", {"action": "rebuild", "new_table": "paper_sessions", "id_column": "id"}),
        ("paper_trading_daily", {"action": "archive", "new_table": "artifacts", "id_columns": ["session_id", "market", "date"]}),
        (
            "paper_trading_signal_cache",
            {"action": "archive", "new_table": "artifacts", "id_columns": ["session_id", "market", "signal_date"]},
        ),
        ("backtest_results", {"action": "archive", "new_table": "artifacts", "id_column": "id"}),
        ("signal_runs", {"action": "archive", "new_table": "artifacts", "id_column": "id"}),
        ("signal_details", {"action": "archive", "new_table": "artifacts", "id_columns": ["run_id", "market", "ticker"]}),
        ("factor_values_cache", {"action": "delete_after_rebuild", "new_table": "factor_values", "id_columns": ["market", "factor_id", "ticker", "date"]}),
        ("factor_eval_results", {"action": "archive", "new_table": "artifacts", "id_column": "id"}),
    ]
)

_ORDER_BY = {
    "factors": "market, name, version, id",
    "stocks": "market, ticker",
    "daily_bars": "market, ticker, date",
    "stock_groups": "market, name, id",
    "stock_group_members": "group_id, market, ticker",
    "label_definitions": "market, name, id",
    "feature_sets": "market, name, id",
    "models": "market, name, id",
    "strategies": "market, name, version, id",
    "paper_trading_sessions": "market, status, id",
    "paper_trading_daily": "session_id, market, date",
    "paper_trading_signal_cache": "session_id, market, signal_date",
    "backtest_results": "market, created_at, id",
    "signal_runs": "market, target_date, id",
    "signal_details": "run_id, market, ticker",
    "factor_values_cache": "market, factor_id, ticker, date",
    "factor_eval_results": "market, factor_id, created_at, id",
}

_DEPENDENCY_COLUMNS = {
    "feature_sets": ["factor_refs"],
    "models": ["feature_set_id", "label_id"],
    "strategies": ["required_factors", "required_models"],
    "paper_trading_sessions": ["strategy_id", "universe_group_id"],
    "paper_trading_daily": ["session_id"],
    "paper_trading_signal_cache": ["session_id"],
    "backtest_results": ["strategy_id"],
    "signal_runs": ["strategy_id", "universe_group_id"],
    "signal_details": ["run_id"],
    "factor_values_cache": ["factor_id"],
    "factor_eval_results": ["factor_id", "label_id", "universe_group_id"],
}


class Migration32Service:
    """Build V3.2 dry-run inventory manifests without mutating domain state."""

    def build_dry_run_manifest(self, db_path: Path | None = None) -> dict[str, Any]:
        with self._connection(db_path) as conn:
            assets: dict[str, dict[str, Any]] = OrderedDict()
            asset_map: dict[str, dict[str, Any]] = OrderedDict()
            warnings: list[str] = []
            source_row_count = 0

            for table, spec in _SOURCE_TABLES.items():
                table_info = self._table_manifest(conn, table, spec)
                assets[table] = table_info
                source_row_count += int(table_info["row_count"])
                if table_info["exists"]:
                    asset_map.update(self._asset_rows(conn, table, spec))
                elif spec["action"] in {"re_enter", "import", "rebuild"}:
                    warnings.append(f"{table}: source table does not exist")

            manifest_id = self._manifest_id(assets)
            generated_at = utc_now_naive().replace(tzinfo=timezone.utc).isoformat()
            return {
                "manifest_id": manifest_id,
                "version": "3.2",
                "mode": "dry-run",
                "would_write": False,
                "generated_at": generated_at,
                "database": str(db_path or settings.db_path),
                "policy": {
                    "old_runtime_compatibility": False,
                    "old_tables_are_runtime_sources": False,
                    "required_preservation_method": "re_enter_import_or_rebuild_in_3_0",
                },
                "assets": assets,
                "asset_map": asset_map,
                "cleanup_policy": {
                    "requires_backup": True,
                    "requires_human_approval": True,
                    "delete_only_after_validation": True,
                },
                "warnings": warnings,
                "summary": {
                    "source_tables_checked": len(_SOURCE_TABLES),
                    "source_row_count": source_row_count,
                    "asset_map_count": len(asset_map),
                    "required_rebuild_tables": [
                        table for table, info in assets.items() if info["action"] in {"re_enter", "import", "rebuild"}
                    ],
                },
            }

    def write_manifest_files(self, manifest: dict[str, Any], out_dir: Path | None = None) -> tuple[Path, Path]:
        report_dir = out_dir or (settings.project_root / "docs" / "v3.2" / "reports")
        report_dir.mkdir(parents=True, exist_ok=True)
        manifest_id = str(manifest["manifest_id"])
        json_path = report_dir / f"3.2-migration-dry-run-{manifest_id}.json"
        md_path = report_dir / f"3.2-migration-dry-run-{manifest_id}.md"
        json_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        md_path.write_text(self.render_manifest_markdown(manifest), encoding="utf-8")
        return json_path, md_path

    def apply_basic_assets(self, db_path: Path | None = None) -> dict[str, Any]:
        """Import/re-enter the first safe V3.2 asset slice into 3.0 tables.

        This method intentionally does not delete or mutate old source tables.
        It only creates idempotent 3.0 records for assets, factor specs, and
        universes so later milestones can rebuild datasets/models/strategies
        without calling old runtime paths.
        """
        with self._connection(db_path) as conn:
            manifest = self.build_dry_run_manifest(db_path)
            asset_result = self._import_assets(conn)
            factor_result = self._reenter_factor_specs(conn)
            universe_result = self._rebuild_universes(conn)
            return {
                "mode": "apply_basic_assets",
                "manifest_id": manifest["manifest_id"],
                "would_delete_old_tables": False,
                "assets": asset_result,
                "factor_specs": factor_result,
                "universes": universe_result,
            }

    def apply_market_data_snapshots(self, db_path: Path | None = None) -> dict[str, Any]:
        """Register legacy daily bars as auditable 3.0 data snapshots.

        The current 3.0 runtime still reads the market-aware ``daily_bars``
        table directly. V3.2 therefore treats k-line migration as a metadata
        promotion step: keep the source rows untouched, ensure assets exist,
        and persist per-market coverage/quality snapshots for validation and
        future asset-id physical migration.
        """
        with self._connection(db_path) as conn:
            manifest = self.build_dry_run_manifest(db_path)
            if not self._table_exists(conn, "daily_bars"):
                return {
                    "mode": "apply_market_data_snapshots",
                    "manifest_id": manifest["manifest_id"],
                    "would_delete_old_tables": False,
                    "snapshots": {"before": 0, "after": 0, "inserted": 0},
                    "markets": {},
                }

            self._import_assets(conn)
            before = int(conn.execute("SELECT COUNT(*) FROM market_data_snapshots").fetchone()[0])
            markets = self._register_daily_bar_snapshots(conn)
            after = int(conn.execute("SELECT COUNT(*) FROM market_data_snapshots").fetchone()[0])
            return {
                "mode": "apply_market_data_snapshots",
                "manifest_id": manifest["manifest_id"],
                "would_delete_old_tables": False,
                "snapshots": {"before": before, "after": after, "inserted": after - before},
                "markets": markets,
            }

    def apply_dependency_assets(self, db_path: Path | None = None) -> dict[str, Any]:
        """Rebuild model, feature, strategy, and paper dependencies in 3.0.

        This writes migration descriptors and 3.0 assets only. It does not run
        legacy model inference, legacy strategy code, or old paper trading.
        Model packages created here are non-executable placeholders when the
        legacy model file is unavailable; they exist so active paper chains can
        be audited and queued for retraining/reimplementation.
        """
        with self._connection(db_path) as conn:
            manifest = self.build_dry_run_manifest(db_path)
            self._import_assets(conn)
            self._reenter_factor_specs(conn)
            self._rebuild_universes(conn)
            label_result = self._reenter_label_specs(conn)
            pipeline_result = self._rebuild_feature_pipelines(conn)
            model_result = self._rebuild_model_assets(conn)
            strategy_result = self._rebuild_strategy_graphs(conn)
            paper_result = self._rebuild_paper_sessions(conn)
            return {
                "mode": "apply_dependency_assets",
                "manifest_id": manifest["manifest_id"],
                "would_delete_old_tables": False,
                "label_specs": label_result,
                "feature_pipelines": pipeline_result,
                "model_specs": model_result["model_specs"],
                "model_packages": model_result["model_packages"],
                "strategy_graphs": strategy_result,
                "paper_sessions": paper_result,
            }

    @staticmethod
    def render_manifest_markdown(manifest: dict[str, Any]) -> str:
        lines = [
            "# QAgent V3.2 Migration Dry-Run Manifest",
            "",
            f"- Manifest ID: `{manifest['manifest_id']}`",
            f"- Generated at: `{manifest['generated_at']}`",
            f"- Database: `{manifest['database']}`",
            f"- Would write: `{manifest['would_write']}`",
            "",
            "## Policy",
            "",
            "- Old runtime compatibility: `false`",
            "- Old tables are migration sources only, not 3.0 runtime sources.",
            "- Preserved assets must be re-entered, imported, or rebuilt in 3.0.",
            "",
            "## Source Assets",
            "",
            "| Source table | Exists | Rows | Action | Target | Content hash |",
            "| --- | --- | ---: | --- | --- | --- |",
        ]
        for table, info in manifest["assets"].items():
            lines.append(
                f"| `{table}` | `{info['exists']}` | {info['row_count']} | "
                f"`{info['action']}` | `{info['new_table']}` | `{info['content_hash']}` |"
            )

        lines.extend(["", "## Warnings", ""])
        if manifest.get("warnings"):
            lines.extend(f"- {warning}" for warning in manifest["warnings"])
        else:
            lines.append("- None.")

        lines.extend(["", "## Summary", ""])
        summary = manifest["summary"]
        lines.append(f"- Source tables checked: `{summary['source_tables_checked']}`")
        lines.append(f"- Source row count: `{summary['source_row_count']}`")
        lines.append(f"- Asset map count: `{summary['asset_map_count']}`")
        return "\n".join(lines) + "\n"

    @contextmanager
    def _connection(self, db_path: Path | None):
        if db_path is None:
            yield get_connection()
            return
        conn = duckdb.connect(str(db_path))
        try:
            yield conn
        finally:
            conn.close()

    def _table_manifest(self, conn, table: str, spec: dict[str, Any]) -> dict[str, Any]:
        exists = self._table_exists(conn, table)
        if not exists:
            return {
                "exists": False,
                "row_count": 0,
                "action": spec["action"],
                "new_table": spec["new_table"],
                "content_hash": "",
                "market_counts": {},
            }
        row_count = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        return {
            "exists": True,
            "row_count": row_count,
            "action": spec["action"],
            "new_table": spec["new_table"],
            "content_hash": self._table_hash(conn, table),
            "market_counts": self._market_counts(conn, table),
        }

    def _asset_rows(self, conn, table: str, spec: dict[str, Any]) -> dict[str, dict[str, Any]]:
        if not self._table_exists(conn, table):
            return {}
        id_columns = self._id_columns(spec)
        dependency_columns = [
            column for column in _DEPENDENCY_COLUMNS.get(table, []) if self._column_exists(conn, table, column)
        ]
        select_columns = list(dict.fromkeys(id_columns + dependency_columns))
        if not select_columns:
            return {}
        rows = conn.execute(
            f"SELECT {', '.join(select_columns)} FROM {table} ORDER BY {_ORDER_BY.get(table, ', '.join(id_columns))}"
        ).fetchall()
        result: dict[str, dict[str, Any]] = OrderedDict()
        for row in rows:
            item = dict(zip(select_columns, row, strict=False))
            source_id = self._source_id(item, id_columns)
            key = f"{table}:{source_id}"
            result[key] = {
                "source_table": table,
                "source_id": source_id,
                "action": spec["action"],
                "new_table": spec["new_table"],
                "status": "pending",
                "dependencies": {column: self._jsonable(item.get(column)) for column in dependency_columns},
            }
        return result

    def _import_assets(self, conn) -> dict[str, int]:
        if not self._table_exists(conn, "stocks"):
            return {"before": 0, "after": 0, "inserted": 0}
        before = int(conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0])
        conn.execute(
            """
            INSERT INTO assets
                (asset_id, market_profile_id, symbol, display_symbol, name,
                 exchange, sector, industry, status, metadata, created_at, updated_at)
            SELECT CASE normalize_market WHEN 'US' THEN 'US_EQ:' || ticker ELSE 'CN_A:' || ticker END,
                   CASE normalize_market WHEN 'US' THEN 'US_EQ' ELSE 'CN_A' END,
                   ticker,
                   ticker,
                   name,
                   exchange,
                   sector,
                   NULL,
                   COALESCE(status, 'active'),
                   json_object('migration', 'v3.2', 'source_table', 'stocks', 'source_market', normalize_market),
                   current_timestamp,
                   current_timestamp
              FROM (
                    SELECT CASE WHEN market = 'CN' THEN 'CN' ELSE 'US' END AS normalize_market,
                           ticker, name, exchange, sector, status
                      FROM stocks
                   ) s
             WHERE NOT EXISTS (
                    SELECT 1
                      FROM assets a
                     WHERE a.asset_id = CASE s.normalize_market
                         WHEN 'US' THEN 'US_EQ:' || s.ticker ELSE 'CN_A:' || s.ticker END
                   )
            """
        )
        conn.execute(
            """
            INSERT INTO asset_identifiers
                (asset_id, identifier_type, identifier_value, valid_from, valid_to, metadata)
            SELECT a.asset_id,
                   'ticker',
                   a.symbol,
                   DATE '1900-01-01',
                   NULL,
                   json_object('migration', 'v3.2')
              FROM assets a
             WHERE NOT EXISTS (
                    SELECT 1
                      FROM asset_identifiers i
                     WHERE i.asset_id = a.asset_id
                       AND i.identifier_type = 'ticker'
                       AND i.identifier_value = a.symbol
                   )
            """
        )
        after = int(conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0])
        return {"before": before, "after": after, "inserted": after - before}

    def _reenter_factor_specs(self, conn) -> dict[str, int]:
        if not self._table_exists(conn, "factors"):
            return {"before": 0, "after": 0, "inserted": 0}
        before = int(conn.execute("SELECT COUNT(*) FROM factor_specs").fetchone()[0])
        rows = conn.execute(
            """
            SELECT id, market, name, version, description, category, source_code,
                   params, status
              FROM factors
             ORDER BY market, name, version, id
            """
        ).fetchall()
        inserted = 0
        for row in rows:
            legacy_id, market, name, version, description, category, source_code, params, status = row
            profile_id = _profile_for_market(market)
            source_ref = {
                "migration": "v3.2",
                "source_table": "factors",
                "source_id": legacy_id,
                "source_market": market,
                "source_version": version,
            }
            exists = conn.execute(
                """
                SELECT 1
                  FROM factor_specs
                 WHERE market_profile_id = ?
                   AND source_type = 'v3_2_reentered_factor'
                   AND json_extract_string(source_ref, '$.source_id') = ?
                """,
                [profile_id, legacy_id],
            ).fetchone()
            if exists:
                continue
            factor_spec_id = f"v32_factor_{uuid.uuid5(uuid.NAMESPACE_URL, str(legacy_id)).hex[:16]}"
            project_id = self._project_id_for_profile(conn, profile_id)
            conn.execute(
                """
                INSERT INTO factor_specs
                    (id, project_id, market_profile_id, name, description, version,
                     source_type, source_ref, source_code, code_hash, params_schema,
                     default_params, required_inputs, compute_mode, expected_warmup,
                     applicable_profiles, semantic_tags, lifecycle_stage, status,
                     metadata, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 'v3_2_reentered_factor',
                        ?, ?, ?, ?, ?, ?, 'time_series', 0, ?, ?, 'experiment',
                        ?, ?, current_timestamp, current_timestamp)
                """,
                [
                    factor_spec_id,
                    project_id,
                    profile_id,
                    name,
                    description,
                    int(version or 1),
                    json.dumps(source_ref, default=str),
                    source_code,
                    hashlib.sha256(str(source_code or "").encode("utf-8")).hexdigest(),
                    json.dumps({}, default=str),
                    _json_string(params, {}),
                    json.dumps(["open", "high", "low", "close", "volume"], default=str),
                    json.dumps([profile_id], default=str),
                    json.dumps([category or "custom", "v3.2_reentered"], default=str),
                    status or "draft",
                    json.dumps({"migration": "v3.2", "source_table": "factors"}, default=str),
                ],
            )
            inserted += 1
        after = int(conn.execute("SELECT COUNT(*) FROM factor_specs").fetchone()[0])
        return {"before": before, "after": after, "inserted": inserted}

    def _rebuild_universes(self, conn) -> dict[str, int]:
        if not self._table_exists(conn, "stock_groups"):
            return {"before": 0, "after": 0, "inserted": 0}
        before = int(conn.execute("SELECT COUNT(*) FROM universes").fetchone()[0])
        groups = conn.execute(
            """
            SELECT id, market, name, description, group_type, filter_expr
              FROM stock_groups
             ORDER BY market, name, id
            """
        ).fetchall()
        inserted = 0
        for group_id, market, name, description, group_type, filter_expr in groups:
            profile_id = _profile_for_market(market)
            exists = conn.execute(
                """
                SELECT 1
                  FROM universes
                 WHERE market_profile_id = ?
                   AND json_extract_string(source_ref, '$.source_id') = ?
                   AND json_extract_string(source_ref, '$.source_table') = 'stock_groups'
                """,
                [profile_id, group_id],
            ).fetchone()
            if exists:
                continue
            tickers = [
                str(item[0])
                for item in conn.execute(
                    """
                    SELECT ticker
                      FROM stock_group_members
                     WHERE group_id = ?
                       AND market = ?
                     ORDER BY ticker
                    """,
                    [group_id, market],
                ).fetchall()
            ]
            universe_id = f"v32_universe_{uuid.uuid5(uuid.NAMESPACE_URL, str(group_id)).hex[:16]}"
            source_ref = {
                "migration": "v3.2",
                "source_table": "stock_groups",
                "source_id": group_id,
                "source_market": market,
                "tickers": tickers,
            }
            project_id = self._project_id_for_profile(conn, profile_id)
            conn.execute(
                """
                INSERT INTO universes
                    (id, project_id, market_profile_id, name, description,
                     universe_type, source_ref, filter_expr, lifecycle_stage,
                     status, metadata, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'v3_2_rebuilt_group', ?,
                        ?, 'experiment', 'draft', ?, current_timestamp,
                        current_timestamp)
                """,
                [
                    universe_id,
                    project_id,
                    profile_id,
                    name,
                    description,
                    json.dumps(source_ref, default=str),
                    filter_expr,
                    json.dumps(
                        {
                            "migration": "v3.2",
                            "source_group_type": group_type,
                            "member_count": len(tickers),
                        },
                        default=str,
                    ),
                ],
            )
            inserted += 1
        after = int(conn.execute("SELECT COUNT(*) FROM universes").fetchone()[0])
        return {"before": before, "after": after, "inserted": inserted}

    def _reenter_label_specs(self, conn) -> dict[str, int]:
        if not self._table_exists(conn, "label_definitions"):
            return {"before": 0, "after": 0, "inserted": 0}
        before = int(conn.execute("SELECT COUNT(*) FROM label_specs").fetchone()[0])
        rows = conn.execute(
            """
            SELECT id, market, name, description, target_type, horizon,
                   benchmark, config, status
              FROM label_definitions
             ORDER BY market, name, id
            """
        ).fetchall()
        inserted = 0
        for row in rows:
            legacy_id, market, name, description, target_type, horizon, benchmark, config, status = row
            profile_id = _profile_for_market(market)
            exists = conn.execute(
                """
                SELECT 1
                  FROM label_specs
                 WHERE market_profile_id = ?
                   AND source_type = 'v3_2_reentered_label'
                   AND json_extract_string(source_ref, '$.source_id') = ?
                """,
                [profile_id, legacy_id],
            ).fetchone()
            if exists:
                continue
            label_spec_id = self._stable_id("label", legacy_id)
            source_ref = {
                "migration": "v3.2",
                "source_table": "label_definitions",
                "source_id": legacy_id,
                "source_market": market,
            }
            conn.execute(
                """
                INSERT INTO label_specs
                    (id, project_id, market_profile_id, name, description,
                     target_type, horizon, benchmark, source_type, source_ref,
                     lifecycle_stage, status, metadata, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'v3_2_reentered_label', ?,
                        'experiment', ?, ?, current_timestamp, current_timestamp)
                """,
                [
                    label_spec_id,
                    self._project_id_for_profile(conn, profile_id),
                    profile_id,
                    name,
                    description,
                    target_type,
                    int(horizon or 0),
                    benchmark,
                    json.dumps(source_ref, default=str),
                    status or "draft",
                    json.dumps(
                        {
                            "migration": "v3.2",
                            "source_table": "label_definitions",
                            "legacy_config": _json_value(config, {}),
                        },
                        default=str,
                    ),
                ],
            )
            inserted += 1
        after = int(conn.execute("SELECT COUNT(*) FROM label_specs").fetchone()[0])
        return {"before": before, "after": after, "inserted": inserted}

    def _rebuild_feature_pipelines(self, conn) -> dict[str, int]:
        if not self._table_exists(conn, "feature_sets"):
            return {"before": 0, "after": 0, "inserted": 0}
        before = int(conn.execute("SELECT COUNT(*) FROM feature_pipelines").fetchone()[0])
        rows = conn.execute(
            """
            SELECT id, market, name, description, factor_refs, preprocessing, status
              FROM feature_sets
             ORDER BY market, name, id
            """
        ).fetchall()
        inserted = 0
        for row in rows:
            legacy_id, market, name, description, factor_refs, preprocessing, status = row
            profile_id = _profile_for_market(market)
            exists = conn.execute(
                """
                SELECT 1
                  FROM feature_pipelines
                 WHERE market_profile_id = ?
                   AND source_type = 'v3_2_rebuilt_feature_set'
                   AND json_extract_string(source_ref, '$.source_id') = ?
                """,
                [profile_id, legacy_id],
            ).fetchone()
            if exists:
                continue
            pipeline_id = self._stable_id("feature_pipeline", legacy_id)
            factor_refs_value = _json_value(factor_refs, [])
            preprocessing_value = _json_value(preprocessing, {})
            conn.execute(
                """
                INSERT INTO feature_pipelines
                    (id, project_id, market_profile_id, name, description,
                     source_type, source_ref, preprocessing, lifecycle_stage,
                     status, metadata, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'v3_2_rebuilt_feature_set', ?, ?,
                        'experiment', ?, ?, current_timestamp, current_timestamp)
                """,
                [
                    pipeline_id,
                    self._project_id_for_profile(conn, profile_id),
                    profile_id,
                    name,
                    description,
                    json.dumps(
                        {
                            "migration": "v3.2",
                            "source_table": "feature_sets",
                            "source_id": legacy_id,
                            "source_market": market,
                        },
                        default=str,
                    ),
                    json.dumps(preprocessing_value, default=str),
                    status or "draft",
                    json.dumps(
                        {
                            "migration": "v3.2",
                            "source_table": "feature_sets",
                            "factor_refs": factor_refs_value,
                        },
                        default=str,
                    ),
                ],
            )
            for index, ref in enumerate(factor_refs_value if isinstance(factor_refs_value, list) else []):
                if not isinstance(ref, dict):
                    ref = {"factor_ref": ref}
                factor_spec_id = self._factor_spec_id_for_ref(conn, profile_id, ref)
                input_ref = {**ref, "factor_spec_id": factor_spec_id}
                conn.execute(
                    """
                    INSERT INTO feature_pipeline_nodes
                        (id, feature_pipeline_id, node_order, node_type, name,
                         input_refs, params, created_at)
                    VALUES (?, ?, ?, 'factor_spec', ?, ?, ?, current_timestamp)
                    """,
                    [
                        self._stable_id("feature_node", f"{legacy_id}:{index}"),
                        pipeline_id,
                        index,
                        str(ref.get("factor_name") or ref.get("factor_id") or f"factor_{index}"),
                        json.dumps([input_ref], default=str),
                        json.dumps({"preprocessing": preprocessing_value}, default=str),
                    ],
                )
            inserted += 1
        after = int(conn.execute("SELECT COUNT(*) FROM feature_pipelines").fetchone()[0])
        return {"before": before, "after": after, "inserted": inserted}

    def _rebuild_model_assets(self, conn) -> dict[str, dict[str, int]]:
        if not self._table_exists(conn, "models"):
            return {
                "model_specs": {"before": 0, "after": 0, "inserted": 0},
                "model_packages": {"before": 0, "after": 0, "inserted": 0},
            }
        spec_before = int(conn.execute("SELECT COUNT(*) FROM model_specs").fetchone()[0])
        package_before = int(conn.execute("SELECT COUNT(*) FROM model_packages").fetchone()[0])
        rows = conn.execute(
            """
            SELECT id, market, name, feature_set_id, label_id, model_type,
                   model_params, train_config, eval_metrics, status
              FROM models
             ORDER BY market, name, id
            """
        ).fetchall()
        spec_inserted = 0
        package_inserted = 0
        for row in rows:
            legacy_id, market, name, feature_set_id, label_id, model_type, model_params, train_config, eval_metrics, status = row
            profile_id = _profile_for_market(market)
            project_id = self._project_id_for_profile(conn, profile_id)
            model_spec_id = self._stable_id("model_spec", legacy_id)
            eval_value = _json_value(eval_metrics, {})
            train_value = _json_value(train_config, {})
            params_value = _json_value(model_params, {})
            if not conn.execute("SELECT 1 FROM model_specs WHERE id = ?", [model_spec_id]).fetchone():
                conn.execute(
                    """
                    INSERT INTO model_specs
                        (id, project_id, market_profile_id, name, model_type,
                         objective, params_schema, default_params, lifecycle_stage,
                         status, metadata, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'experiment', ?, ?,
                            current_timestamp, current_timestamp)
                    """,
                    [
                        model_spec_id,
                        project_id,
                        profile_id,
                        name,
                        model_type or "unknown",
                        str(eval_value.get("objective_type") or eval_value.get("task_type") or "unknown"),
                        json.dumps({}, default=str),
                        json.dumps(params_value, default=str),
                        status or "draft",
                        json.dumps(
                            {
                                "migration": "v3.2",
                                "source_table": "models",
                                "source_id": legacy_id,
                                "train_config": train_value,
                            },
                            default=str,
                        ),
                    ],
                )
                spec_inserted += 1
            package_id = self._stable_id("model_package", legacy_id)
            if conn.execute("SELECT 1 FROM model_packages WHERE id = ?", [package_id]).fetchone():
                continue
            pipeline_id = self._feature_pipeline_id_for_source(conn, profile_id, feature_set_id)
            label_spec_id = self._label_spec_id_for_source(conn, profile_id, label_id)
            manifest_artifact_id = self._create_legacy_model_manifest_artifact(
                conn,
                project_id=project_id,
                profile_id=profile_id,
                legacy_id=legacy_id,
                name=name,
                payload={
                    "migration": "v3.2",
                    "source_table": "models",
                    "source_id": legacy_id,
                    "market": market,
                    "model_type": model_type,
                    "feature_set_id": feature_set_id,
                    "feature_pipeline_id": pipeline_id,
                    "label_id": label_id,
                    "label_spec_id": label_spec_id,
                    "model_params": params_value,
                    "train_config": train_value,
                    "eval_metrics": eval_value,
                    "executable": False,
                    "reason": "legacy model file must be retrained or manually reattached in 3.0",
                },
            )
            conn.execute(
                """
                INSERT INTO model_packages
                    (id, project_id, market_profile_id, name, source_experiment_id,
                     model_artifact_id, feature_schema, prediction_contract,
                     metrics, qa_summary, lifecycle_stage, status, metadata,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'candidate',
                        'requires_retrain', ?, current_timestamp, current_timestamp)
                """,
                [
                    package_id,
                    project_id,
                    profile_id,
                    name,
                    f"v32_retrain_experiment_{legacy_id}",
                    manifest_artifact_id,
                    json.dumps(
                        {
                            "feature_pipeline_id": pipeline_id,
                            "legacy_feature_set_id": feature_set_id,
                        },
                        default=str,
                    ),
                    json.dumps(
                        {
                            "input": "dataset_panel",
                            "output": "prediction_by_asset_date",
                            "executable": False,
                            "requires_retrain": True,
                        },
                        default=str,
                    ),
                    json.dumps(eval_value, default=str),
                    json.dumps({"blocking": True, "reason": "requires_retrain"}, default=str),
                    json.dumps(
                        {
                            "migration": "v3.2",
                            "source_table": "models",
                            "source_id": legacy_id,
                            "model_spec_id": model_spec_id,
                            "label_spec_id": label_spec_id,
                            "executable": False,
                            "requires_retrain": True,
                        },
                        default=str,
                    ),
                ],
            )
            package_inserted += 1
        spec_after = int(conn.execute("SELECT COUNT(*) FROM model_specs").fetchone()[0])
        package_after = int(conn.execute("SELECT COUNT(*) FROM model_packages").fetchone()[0])
        return {
            "model_specs": {"before": spec_before, "after": spec_after, "inserted": spec_inserted},
            "model_packages": {"before": package_before, "after": package_after, "inserted": package_inserted},
        }

    def _rebuild_strategy_graphs(self, conn) -> dict[str, int]:
        if not self._table_exists(conn, "strategies"):
            return {"before": 0, "after": 0, "inserted": 0}
        before = int(conn.execute("SELECT COUNT(*) FROM strategy_graphs").fetchone()[0])
        rows = conn.execute(
            """
            SELECT id, market, name, version, description, source_code,
                   required_factors, required_models, position_sizing,
                   constraint_config, status
              FROM strategies
             ORDER BY market, name, version, id
            """
        ).fetchall()
        inserted = 0
        for row in rows:
            legacy_id, market, name, version, description, source_code, required_factors, required_models, position_sizing, constraint_config, status = row
            profile_id = _profile_for_market(market)
            graph_id = self._stable_id("strategy_graph", legacy_id)
            if conn.execute("SELECT 1 FROM strategy_graphs WHERE id = ?", [graph_id]).fetchone():
                continue
            required_models_value = _json_value(required_models, [])
            model_refs = [
                {
                    "type": "model_package",
                    "id": self._model_package_id_for_source(conn, profile_id, str(model_id)),
                    "legacy_model_id": str(model_id),
                }
                for model_id in required_models_value
            ]
            required_factors_value = _json_value(required_factors, [])
            factor_refs = [
                {
                    "type": "factor_spec",
                    "id": self._factor_spec_id_for_ref(conn, profile_id, {"factor_name": factor_name}),
                    "legacy_factor_name": str(factor_name),
                }
                for factor_name in required_factors_value
            ]
            dependency_refs = [
                {"type": "legacy_strategy_source", "id": legacy_id},
                *factor_refs,
                *model_refs,
            ]
            graph_config = {
                "migration": "v3.2",
                "source_table": "strategies",
                "source_id": legacy_id,
                "version": int(version or 1),
                "position_sizing": position_sizing,
                "constraint_config": _json_value(constraint_config, {}),
                "requires_reimplementation": True,
            }
            project_id = self._project_id_for_profile(conn, profile_id)
            conn.execute(
                """
                INSERT INTO strategy_graphs
                    (id, project_id, market_profile_id, name, description,
                     graph_type, version, graph_config, dependency_refs,
                     lifecycle_stage, status, metadata, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'v3_2_reimplemented_strategy_source',
                        ?, ?, ?, 'experiment', 'requires_reimplementation', ?,
                        current_timestamp, current_timestamp)
                """,
                [
                    graph_id,
                    project_id,
                    profile_id,
                    name,
                    description,
                    int(version or 1),
                    json.dumps(graph_config, default=str),
                    json.dumps(dependency_refs, default=str),
                    json.dumps(
                        {
                            "migration": "v3.2",
                            "source_table": "strategies",
                            "source_id": legacy_id,
                            "source_code_hash": hashlib.sha256(str(source_code or "").encode("utf-8")).hexdigest(),
                        },
                        default=str,
                    ),
                ],
            )
            conn.execute(
                """
                INSERT INTO strategy_nodes
                    (id, strategy_graph_id, node_order, node_key, node_type,
                     name, input_schema, output_schema, data_requirements,
                     params, code_snapshot, explain_schema, created_at)
                VALUES (?, ?, 0, 'legacy_source_reimplementation',
                        'source_snapshot', ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                """,
                [
                    self._stable_id("strategy_node", legacy_id),
                    graph_id,
                    f"{name} source snapshot",
                    json.dumps({"legacy_strategy_id": legacy_id}, default=str),
                    json.dumps({"requires": "3.0_strategy_graph_reimplementation"}, default=str),
                    json.dumps(dependency_refs, default=str),
                    json.dumps({"required_factors": required_factors_value, "required_models": required_models_value}, default=str),
                    source_code,
                    json.dumps({"migration_status": "requires_reimplementation"}, default=str),
                ],
            )
            inserted += 1
        after = int(conn.execute("SELECT COUNT(*) FROM strategy_graphs").fetchone()[0])
        return {"before": before, "after": after, "inserted": inserted}

    def _rebuild_paper_sessions(self, conn) -> dict[str, int]:
        if not self._table_exists(conn, "paper_trading_sessions"):
            return {"before": 0, "after": 0, "inserted": 0}
        before = int(conn.execute("SELECT COUNT(*) FROM paper_sessions").fetchone()[0])
        rows = conn.execute(
            """
            SELECT id, market, name, strategy_id, universe_group_id, config,
                   status, start_date, current_date, initial_capital, current_nav
              FROM paper_trading_sessions
             WHERE status IN ('active', 'paused')
             ORDER BY market, name, id
            """
        ).fetchall()
        inserted = 0
        for row in rows:
            legacy_id, market, name, strategy_id, universe_group_id, config, status, start_date, current_date, initial_capital, current_nav = row
            profile_id = _profile_for_market(market)
            session_id = self._stable_id("paper_session", legacy_id)
            if conn.execute("SELECT 1 FROM paper_sessions WHERE id = ?", [session_id]).fetchone():
                continue
            graph_id = self._strategy_graph_id_for_source(conn, profile_id, strategy_id)
            config_value = _json_value(config, {})
            config_value.update(
                {
                    "migration": "v3.2",
                    "legacy_session_id": legacy_id,
                    "legacy_universe_group_id": universe_group_id,
                    "legacy_status": status,
                    "requires_3_0_resume_validation": True,
                }
            )
            conn.execute(
                """
                INSERT INTO paper_sessions
                    (id, project_id, market_profile_id, strategy_graph_id, name,
                     status, start_date, current_date, initial_capital,
                     current_nav, current_weights, config, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'migration_pending', ?, ?, ?, ?,
                        ?, ?, current_timestamp, current_timestamp)
                """,
                [
                    session_id,
                    self._project_id_for_profile(conn, profile_id),
                    profile_id,
                    graph_id,
                    name,
                    start_date,
                    current_date,
                    float(initial_capital or 0),
                    float(current_nav if current_nav is not None else initial_capital or 0),
                    json.dumps({}, default=str),
                    json.dumps(config_value, default=str),
                ],
            )
            inserted += 1
        after = int(conn.execute("SELECT COUNT(*) FROM paper_sessions").fetchone()[0])
        return {"before": before, "after": after, "inserted": inserted}

    def _register_daily_bar_snapshots(self, conn) -> dict[str, dict[str, Any]]:
        rows = conn.execute(
            """
            SELECT CASE WHEN market = 'CN' THEN 'CN' ELSE 'US' END AS normalize_market,
                   COUNT(*) AS row_count,
                   COUNT(DISTINCT ticker) AS ticker_count,
                   MIN(date) AS min_date,
                   MAX(date) AS max_date
              FROM daily_bars
             GROUP BY normalize_market
             ORDER BY normalize_market
            """
        ).fetchall()
        results: dict[str, dict[str, Any]] = OrderedDict()
        for market, row_count, ticker_count, min_date, max_date in rows:
            profile_id = _profile_for_market(market)
            mapped_row_count = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                      FROM daily_bars b
                      JOIN assets a
                        ON a.market_profile_id = ?
                       AND a.symbol = b.ticker
                     WHERE CASE WHEN b.market = 'CN' THEN 'CN' ELSE 'US' END = ?
                    """,
                    [profile_id, market],
                ).fetchone()[0]
            )
            mapped_ticker_count = int(
                conn.execute(
                    """
                    SELECT COUNT(DISTINCT b.ticker)
                      FROM daily_bars b
                      JOIN assets a
                        ON a.market_profile_id = ?
                       AND a.symbol = b.ticker
                     WHERE CASE WHEN b.market = 'CN' THEN 'CN' ELSE 'US' END = ?
                    """,
                    [profile_id, market],
                ).fetchone()[0]
            )
            missing_tickers = [
                str(row[0])
                for row in conn.execute(
                    """
                    SELECT DISTINCT b.ticker
                      FROM daily_bars b
                     WHERE CASE WHEN b.market = 'CN' THEN 'CN' ELSE 'US' END = ?
                       AND NOT EXISTS (
                            SELECT 1
                              FROM assets a
                             WHERE a.market_profile_id = ?
                               AND a.symbol = b.ticker
                       )
                     ORDER BY b.ticker
                     LIMIT 50
                    """,
                    [market, profile_id],
                ).fetchall()
            ]
            content_hash = self._daily_bars_market_hash(conn, market)
            snapshot_id = f"v32_daily_bars_{profile_id}_{content_hash[:16]}"
            coverage = {
                "migration": "v3.2",
                "source_table": "daily_bars",
                "market": market,
                "row_count": int(row_count or 0),
                "ticker_count": int(ticker_count or 0),
                "mapped_row_count": mapped_row_count,
                "mapped_ticker_count": mapped_ticker_count,
                "date_range": {
                    "min": str(min_date) if min_date else None,
                    "max": str(max_date) if max_date else None,
                },
                "content_hash": content_hash,
            }
            quality = {
                "status": "valid" if not missing_tickers else "needs_asset_mapping",
                "unmapped_ticker_count": len(missing_tickers),
                "missing_asset_tickers_sample": missing_tickers,
            }
            provider, data_policy_id = self._profile_policy(conn, profile_id)
            exists = conn.execute(
                "SELECT 1 FROM market_data_snapshots WHERE id = ?",
                [snapshot_id],
            ).fetchone()
            if not exists:
                conn.execute(
                    """
                    INSERT INTO market_data_snapshots
                        (id, market_profile_id, provider, data_policy_id,
                         as_of_date, coverage_summary, quality_summary, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, current_timestamp)
                    """,
                    [
                        snapshot_id,
                        profile_id,
                        provider,
                        data_policy_id,
                        max_date,
                        json.dumps(coverage, default=str),
                        json.dumps(quality, default=str),
                    ],
                )
            results[str(market)] = {
                "market_profile_id": profile_id,
                "snapshot_id": snapshot_id,
                "row_count": int(row_count or 0),
                "ticker_count": int(ticker_count or 0),
                "mapped_row_count": mapped_row_count,
                "mapped_ticker_count": mapped_ticker_count,
                "unmapped_ticker_count": len(missing_tickers),
                "missing_asset_tickers_sample": missing_tickers,
                "date_range": coverage["date_range"],
            }
        return results

    @staticmethod
    def _daily_bars_market_hash(conn, market: str) -> str:
        rows = conn.execute(
            """
            SELECT ticker, date, open, high, low, close, volume, adj_factor
              FROM daily_bars
             WHERE CASE WHEN market = 'CN' THEN 'CN' ELSE 'US' END = ?
             ORDER BY ticker, date
            """,
            [market],
        ).fetchall()
        digest = hashlib.sha256()
        for row in rows:
            digest.update(json.dumps([str(item) for item in row], sort_keys=True).encode("utf-8"))
            digest.update(b"\n")
        return digest.hexdigest()

    @staticmethod
    def _profile_policy(conn, profile_id: str) -> tuple[str, str | None]:
        row = conn.execute(
            "SELECT data_policy_id FROM market_profiles WHERE id = ?",
            [profile_id],
        ).fetchone()
        data_policy_id = str(row[0]) if row and row[0] else None
        if data_policy_id:
            policy = conn.execute(
                "SELECT provider FROM data_policies WHERE id = ?",
                [data_policy_id],
            ).fetchone()
            if policy and policy[0]:
                return str(policy[0]), data_policy_id
        return "legacy_daily_bars", data_policy_id

    def _project_id_for_profile(self, conn, profile_id: str) -> str:
        project_id = "bootstrap_cn" if profile_id == "CN_A" else "bootstrap_us"
        row = conn.execute(
            "SELECT id FROM research_projects WHERE id = ?",
            [project_id],
        ).fetchone()
        if row:
            return project_id
        profile = conn.execute(
            """
            SELECT data_policy_id, trading_rule_set_id, cost_model_id,
                   benchmark_policy_id
              FROM market_profiles
             WHERE id = ?
            """,
            [profile_id],
        ).fetchone()
        if not profile:
            raise ValueError(f"Market profile {profile_id} not found")
        conn.execute(
            """
            INSERT INTO research_projects
                (id, name, market_profile_id, data_policy_id,
                 trading_rule_set_id, cost_model_id, benchmark_policy_id,
                 metadata, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, current_timestamp)
            """,
            [
                project_id,
                "CN Research" if profile_id == "CN_A" else "US Research",
                profile_id,
                profile[0],
                profile[1],
                profile[2],
                profile[3],
                json.dumps({"bootstrap": True, "migration": "v3.2"}, default=str),
            ],
        )
        return project_id

    def _create_legacy_model_manifest_artifact(
        self,
        conn,
        *,
        project_id: str,
        profile_id: str,
        legacy_id: str,
        name: str,
        payload: dict[str, Any],
    ) -> str:
        artifact_id = self._stable_id("model_manifest_artifact", legacy_id)
        existing = conn.execute("SELECT 1 FROM artifacts WHERE id = ?", [artifact_id]).fetchone()
        if existing:
            return artifact_id
        run_id = self._ensure_migration_run(conn, project_id=project_id, profile_id=profile_id)
        artifact_dir = settings.project_root / "data" / "artifacts" / "experiments" / run_id
        artifact_dir.mkdir(parents=True, exist_ok=True)
        path = artifact_dir / f"{artifact_id}.json"
        encoded = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
        path.write_text(encoded, encoding="utf-8")
        digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        conn.execute(
            """
            INSERT INTO artifacts
                (id, run_id, project_id, artifact_type, uri, format,
                 schema_version, byte_size, content_hash, lifecycle_stage,
                 retention_class, cleanup_after, rebuildable, metadata,
                 created_at)
            VALUES (?, ?, ?, 'v3_2_legacy_model_manifest', ?, 'json', '1',
                    ?, ?, 'experiment', 'rebuildable', NULL, TRUE, ?,
                    current_timestamp)
            """,
            [
                artifact_id,
                run_id,
                project_id,
                str(path),
                len(encoded.encode("utf-8")),
                digest,
                json.dumps(
                    {
                        "migration": "v3.2",
                        "source_table": "models",
                        "source_id": legacy_id,
                        "name": name,
                    },
                    default=str,
                ),
            ],
        )
        return artifact_id

    def _ensure_migration_run(self, conn, *, project_id: str, profile_id: str) -> str:
        run_id = f"v32_dependency_migration_{profile_id}"
        if conn.execute("SELECT 1 FROM research_runs WHERE id = ?", [run_id]).fetchone():
            return run_id
        conn.execute(
            """
            INSERT INTO research_runs
                (id, project_id, market_profile_id, run_type, status,
                 lifecycle_stage, retention_class, params, input_refs,
                 output_refs, metrics_summary, qa_summary, warnings,
                 error_message, created_by, created_at, started_at,
                 completed_at, updated_at)
            VALUES (?, ?, ?, 'v3_2_dependency_asset_migration', 'completed',
                    'experiment', 'rebuildable', ?, ?, ?, ?, ?, ?, NULL,
                    'migration_3_2', current_timestamp, current_timestamp,
                    current_timestamp, current_timestamp)
            """,
            [
                run_id,
                project_id,
                profile_id,
                json.dumps({"migration": "v3.2", "phase": "dependency_assets"}, default=str),
                json.dumps([], default=str),
                json.dumps([], default=str),
                json.dumps({}, default=str),
                json.dumps({}, default=str),
                json.dumps([], default=str),
            ],
        )
        return run_id

    def _feature_pipeline_id_for_source(self, conn, profile_id: str, source_id: str | None) -> str | None:
        if not source_id:
            return None
        row = conn.execute(
            """
            SELECT id
              FROM feature_pipelines
             WHERE market_profile_id = ?
               AND source_type = 'v3_2_rebuilt_feature_set'
               AND json_extract_string(source_ref, '$.source_id') = ?
            """,
            [profile_id, source_id],
        ).fetchone()
        return str(row[0]) if row else None

    def _label_spec_id_for_source(self, conn, profile_id: str, source_id: str | None) -> str | None:
        if not source_id:
            return None
        row = conn.execute(
            """
            SELECT id
              FROM label_specs
             WHERE market_profile_id = ?
               AND source_type = 'v3_2_reentered_label'
               AND json_extract_string(source_ref, '$.source_id') = ?
            """,
            [profile_id, source_id],
        ).fetchone()
        return str(row[0]) if row else None

    def _model_package_id_for_source(self, conn, profile_id: str, source_id: str | None) -> str | None:
        if not source_id:
            return None
        row = conn.execute(
            """
            SELECT id
              FROM model_packages
             WHERE market_profile_id = ?
               AND json_extract_string(metadata, '$.source_id') = ?
            """,
            [profile_id, source_id],
        ).fetchone()
        return str(row[0]) if row else self._stable_id("model_package", source_id)

    def _strategy_graph_id_for_source(self, conn, profile_id: str, source_id: str | None) -> str | None:
        if not source_id:
            return None
        row = conn.execute(
            """
            SELECT id
              FROM strategy_graphs
             WHERE market_profile_id = ?
               AND json_extract_string(graph_config, '$.source_id') = ?
            """,
            [profile_id, source_id],
        ).fetchone()
        return str(row[0]) if row else self._stable_id("strategy_graph", source_id)

    def _factor_spec_id_for_ref(self, conn, profile_id: str, ref: dict[str, Any]) -> str | None:
        factor_id = ref.get("factor_id")
        if factor_id:
            row = conn.execute(
                """
                SELECT id
                  FROM factor_specs
                 WHERE market_profile_id = ?
                   AND json_extract_string(source_ref, '$.source_id') = ?
                """,
                [profile_id, str(factor_id)],
            ).fetchone()
            if row:
                return str(row[0])
        factor_name = ref.get("factor_name")
        if factor_name:
            row = conn.execute(
                """
                SELECT id
                  FROM factor_specs
                 WHERE market_profile_id = ?
                   AND name = ?
                 ORDER BY version DESC
                 LIMIT 1
                """,
                [profile_id, str(factor_name)],
            ).fetchone()
            if row:
                return str(row[0])
        return None

    @staticmethod
    def _stable_id(kind: str, source: Any) -> str:
        return f"v32_{kind}_{uuid.uuid5(uuid.NAMESPACE_URL, str(source)).hex[:16]}"

    @staticmethod
    def _id_columns(spec: dict[str, Any]) -> list[str]:
        if "id_columns" in spec:
            return list(spec["id_columns"])
        return [str(spec.get("id_column", "id"))]

    @staticmethod
    def _source_id(row: dict[str, Any], id_columns: list[str]) -> str:
        values = [str(row.get(column)) for column in id_columns]
        return "|".join(values)

    @staticmethod
    def _jsonable(value: Any) -> Any:
        if isinstance(value, (str, int, float, bool)) or value is None:
            try:
                return json.loads(value) if isinstance(value, str) and value[:1] in "[{" else value
            except Exception:
                return value
        return str(value)

    @staticmethod
    def _manifest_id(assets: dict[str, dict[str, Any]]) -> str:
        payload = json.dumps(
            {
                table: {
                    "row_count": info["row_count"],
                    "action": info["action"],
                    "content_hash": info["content_hash"],
                }
                for table, info in assets.items()
            },
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def _table_hash(conn, table: str) -> str:
        order_by = _ORDER_BY.get(table, "1")
        try:
            rows = conn.execute(f"SELECT * FROM {table} ORDER BY {order_by}").fetchall()
        except Exception:
            rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        digest = hashlib.sha256()
        for row in rows:
            digest.update(json.dumps([str(item) for item in row], sort_keys=True).encode("utf-8"))
            digest.update(b"\n")
        return digest.hexdigest()

    @staticmethod
    def _market_counts(conn, table: str) -> dict[str, int]:
        if not Migration32Service._column_exists(conn, table, "market"):
            return {}
        rows = conn.execute(
            f"SELECT market, COUNT(*) FROM {table} GROUP BY market ORDER BY market"
        ).fetchall()
        return {str(row[0]): int(row[1]) for row in rows}

    @staticmethod
    def _table_exists(conn, table: str) -> bool:
        return bool(
            conn.execute(
                """
                SELECT 1
                  FROM information_schema.tables
                 WHERE table_schema = 'main'
                   AND table_name = ?
                """,
                [table],
            ).fetchone()
        )

    @staticmethod
    def _column_exists(conn, table: str, column: str) -> bool:
        return bool(
            conn.execute(
                """
                SELECT 1
                  FROM information_schema.columns
                 WHERE table_schema = 'main'
                   AND table_name = ?
                   AND column_name = ?
                """,
                [table, column],
            ).fetchone()
        )


def _profile_for_market(market: str | None) -> str:
    return "CN_A" if str(market or "").upper() == "CN" else "US_EQ"


def _json_string(value: Any, default: Any) -> str:
    if value is None:
        return json.dumps(default, default=str)
    if isinstance(value, str):
        try:
            json.loads(value)
            return value
        except Exception:
            return json.dumps(default, default=str)
    return json.dumps(value, default=str)


def _json_value(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return default
    return value

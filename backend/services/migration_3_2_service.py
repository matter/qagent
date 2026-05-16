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
            conn.execute(
                """
                INSERT INTO factor_specs
                    (id, project_id, market_profile_id, name, description, version,
                     source_type, source_ref, source_code, code_hash, params_schema,
                     default_params, required_inputs, compute_mode, expected_warmup,
                     applicable_profiles, semantic_tags, lifecycle_stage, status,
                     metadata, created_at, updated_at)
                VALUES (?, 'bootstrap_us', ?, ?, ?, ?, 'v3_2_reentered_factor',
                        ?, ?, ?, ?, ?, ?, 'time_series', 0, ?, ?, 'experiment',
                        ?, ?, current_timestamp, current_timestamp)
                """,
                [
                    factor_spec_id,
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
            conn.execute(
                """
                INSERT INTO universes
                    (id, project_id, market_profile_id, name, description,
                     universe_type, source_ref, filter_expr, lifecycle_stage,
                     status, metadata, created_at, updated_at)
                VALUES (?, 'bootstrap_us', ?, ?, ?, 'v3_2_rebuilt_group', ?,
                        ?, 'experiment', 'draft', ?, current_timestamp,
                        current_timestamp)
                """,
                [
                    universe_id,
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

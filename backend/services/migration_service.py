"""Migration and legacy-adapter helpers for QAgent 3.0.

M3 keeps the 2.0 tables alive while giving 3.0 a stable migration surface:
- report legacy asset health without mutating source tables
- preview a legacy factor as a 3.0 research run
- materialize a legacy universe as a 3.0 artifact
- run a legacy strategy backtest through the existing engine

The service is intentionally conservative. It does not rewrite the legacy
tables; it creates 3.0 research facts that point at them.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from contextlib import contextmanager

import duckdb
import pandas as pd

from backend.config import settings
from backend.db import close_db, get_connection
from backend.logger import get_logger
from backend.services.backtest_service import BacktestService
from backend.services.factor_engine import FactorEngine
from backend.services.factor_service import FactorService
from backend.services.group_service import GroupService
from backend.services.market_context import normalize_market
from backend.services.market_data_foundation_service import MarketDataFoundationService
from backend.services.research_kernel_service import ResearchKernelService
from backend.services.strategy_service import StrategyService

log = get_logger(__name__)

MIGRATION_MAPPING: dict[str, list[str]] = {
    "stocks": ["assets", "asset_identifiers"],
    "daily_bars": ["daily_bars", "market_data_snapshots"],
    "index_bars": ["benchmark_policies"],
    "stock_groups": ["universes"],
    "stock_group_members": ["universe_memberships"],
    "factors": ["factor_specs"],
    "factor_values_cache": ["factor_runs", "factor_values"],
    "factor_eval_results": ["factor_runs", "qa_reports", "artifacts"],
    "feature_sets": ["feature_pipelines", "feature_pipeline_nodes"],
    "label_definitions": ["label_specs"],
    "models": ["model_experiments"],
    "strategies": ["strategy_graphs", "strategy_nodes"],
    "backtest_results": ["backtest_runs", "artifacts"],
    "signal_runs": ["production_signal_runs", "artifacts"],
    "signal_details": ["strategy_signals"],
    "paper_trading_sessions": ["paper_sessions"],
    "paper_trading_daily": ["paper_daily"],
    "paper_trading_signal_cache": ["artifacts"],
    "task_runs": ["research_runs"],
}

_CODE_TABLES = {
    "factors": ("id", "market", "name", "version", "status", "source_code"),
    "strategies": ("id", "market", "name", "version", "status", "source_code"),
    "models": (
        "id",
        "market",
        "name",
        "status",
        "model_type",
        "feature_set_id",
        "label_id",
        "model_params",
        "train_config",
    ),
    "feature_sets": ("id", "market", "name", "status", "factor_refs", "preprocessing"),
    "label_definitions": ("id", "market", "name", "status", "target_type", "horizon", "benchmark", "config"),
}

_ORDER_BY = {
    "stocks": "ticker",
    "daily_bars": "ticker, date",
    "index_bars": "symbol, date",
    "stock_groups": "id",
    "stock_group_members": "group_id, ticker",
    "factors": "name, version, id",
    "factor_values_cache": "factor_id, ticker, date",
    "factor_eval_results": "factor_id, created_at, id",
    "feature_sets": "name, id",
    "label_definitions": "name, id",
    "models": "name, id",
    "strategies": "name, version, id",
    "backtest_results": "created_at, id",
    "signal_runs": "created_at, id",
    "signal_details": "run_id, ticker",
    "paper_trading_sessions": "created_at, id",
    "paper_trading_daily": "session_id, date",
    "paper_trading_signal_cache": "session_id, signal_date",
    "task_runs": "created_at, id",
}


class MigrationService:
    """Report and legacy adapter helpers for the 2.0 -> 3.0 migration."""

    def __init__(
        self,
        *,
        kernel_service: ResearchKernelService | None = None,
        factor_engine: FactorEngine | None = None,
        factor_service: FactorService | None = None,
        group_service: GroupService | None = None,
        strategy_service: StrategyService | None = None,
        backtest_service: BacktestService | None = None,
        market_data_service: MarketDataFoundationService | None = None,
    ) -> None:
        self._kernel = kernel_service or ResearchKernelService()
        self._factor_engine = factor_engine or FactorEngine()
        self._factor_service = factor_service or FactorService()
        self._group_service = group_service or GroupService()
        self._strategy_service = strategy_service or StrategyService()
        self._backtest_service = backtest_service or BacktestService()
        self._market_data_service = market_data_service or MarketDataFoundationService()

    # ------------------------------------------------------------------
    # Reports
    # ------------------------------------------------------------------

    def build_report(self, db_path: Path | None = None) -> dict[str, Any]:
        """Build a deterministic migration report without mutating legacy tables."""
        conn, close_conn = self._open_connection(db_path)
        try:
            source_tables: dict[str, dict[str, Any]] = {}
            target_estimates: dict[str, int] = {}
            warnings: list[str] = []
            failures: list[str] = []
            archive_candidates: dict[str, dict[str, int]] = {}

            for source, targets in MIGRATION_MAPPING.items():
                table_info = self._table_report(conn, source)
                source_tables[source] = table_info
                for target in targets:
                    target_estimates[target] = target_estimates.get(target, 0) + table_info["row_count"]

                market_counts = table_info.get("market_counts", {})
                cn_count = int(market_counts.get("CN", 0))
                if cn_count:
                    warnings.append(
                        f"{source}: {cn_count} CN rows should be treated as archive/candidate, not US baseline"
                    )
                    archive_candidates[source] = {"CN": cn_count}

            legacy_signatures = {
                "factors": self._code_signatures(conn, "factors"),
                "strategies": self._code_signatures(conn, "strategies"),
                "models": self._code_signatures(conn, "models"),
                "feature_sets": self._code_signatures(conn, "feature_sets"),
                "label_definitions": self._code_signatures(conn, "label_definitions"),
            }

            for table, info in source_tables.items():
                if info["exists"] and info["row_count"] == 0 and table in {"stocks", "daily_bars", "factors", "strategies"}:
                    warnings.append(f"{table}: empty source table")

            return {
                "mode": "dry-run",
                "would_write": False,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "database": str(db_path) if db_path else str(settings.db_path),
                "source_tables": source_tables,
                "target_estimates": target_estimates,
                "legacy_signatures": legacy_signatures,
                "archive_candidates": archive_candidates,
                "warnings": warnings,
                "failures": failures,
                "summary": {
                    "legacy_tables_checked": len(MIGRATION_MAPPING),
                    "legacy_row_count": sum(item["row_count"] for item in source_tables.values()),
                    "cn_row_count": sum(info.get("market_counts", {}).get("CN", 0) for info in source_tables.values()),
                },
            }
        finally:
            if close_conn:
                conn.close()

    def write_report_files(self, report: dict[str, Any], out_dir: Path | None = None) -> tuple[Path, Path]:
        """Write JSON and Markdown copies of a migration report."""
        report_dir = out_dir or (settings.project_root / "docs" / "reports")
        report_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        mode = str(report.get("mode") or "dry-run")
        json_path = report_dir / f"3.0-migration-{mode}-{stamp}.json"
        md_path = report_dir / f"3.0-migration-{mode}-{stamp}.md"
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        md_path.write_text(self.render_report_markdown(report), encoding="utf-8")
        return json_path, md_path

    @staticmethod
    def render_report_markdown(report: dict[str, Any]) -> str:
        lines = [
            "# QAgent 3.0 Migration Report",
            "",
            f"- Generated at: `{report['generated_at']}`",
            f"- Database: `{report['database']}`",
            f"- Mode: `{report['mode']}`",
            "",
            "## Source Tables",
            "",
            "| Table | Exists | Rows | Hash | Targets |",
            "| --- | --- | ---: | --- | --- |",
        ]
        for table, info in report["source_tables"].items():
            lines.append(
                "| `{}` | {} | {} | `{}` | `{}` |".format(
                    table,
                    info["exists"],
                    info["row_count"],
                    info.get("table_hash", ""),
                    ", ".join(info.get("targets", [])),
                )
            )

        lines.extend(["", "## Legacy Code Hashes", ""])
        for table, signatures in report.get("legacy_signatures", {}).items():
            lines.append(f"### `{table}`")
            if not signatures:
                lines.append("- None.")
                lines.append("")
                continue
            for sig in signatures[:25]:
                lines.append(
                    f"- `{sig['id']}` `{sig.get('name', '')}` v{sig.get('version', '')} "
                    f"`{sig['hash']}`"
                )
            if len(signatures) > 25:
                lines.append(f"- ... {len(signatures) - 25} more")
            lines.append("")

        lines.extend(["## Archive Candidates", ""])
        if report.get("archive_candidates"):
            for table, counts in report["archive_candidates"].items():
                lines.append(f"- `{table}`: `{json.dumps(counts, ensure_ascii=False)}`")
        else:
            lines.append("- None.")

        lines.extend(["", "## Warnings", ""])
        if report.get("warnings"):
            lines.extend(f"- {warning}" for warning in report["warnings"])
        else:
            lines.append("- None.")

        lines.extend(["", "## Failures", ""])
        if report.get("failures"):
            lines.extend(f"- {failure}" for failure in report["failures"])
        else:
            lines.append("- None.")

        return "\n".join(lines) + "\n"

    def apply_migration(self, db_path: Path | None = None) -> dict[str, Any]:
        """Materialize the side-by-side migration report as a 3.0 artifact."""
        with self._db_context(db_path):
            report = self.build_report(db_path)
            report["mode"] = "apply"
            report["would_write"] = True
            project = self._kernel.get_bootstrap_project()

            us_assets = self._market_data_service.sync_assets_from_legacy_stocks("US_EQ")
            cn_assets = self._market_data_service.sync_assets_from_legacy_stocks("CN_A")

            run = self._kernel.create_run(
                run_type="migration_apply",
                project_id=project["id"],
                market_profile_id=project["market_profile_id"],
                lifecycle_stage="experiment",
                retention_class="rebuildable",
                created_by="migration",
                status="completed",
                params={
                    "mode": "apply",
                    "source_tables": sorted(MIGRATION_MAPPING.keys()),
                    "us_asset_sync": us_assets,
                    "cn_asset_sync": cn_assets,
                },
            )
            artifact = self._kernel.create_json_artifact(
                run_id=run["id"],
                artifact_type="migration_report",
                payload={
                    **report,
                    "asset_sync": {"US_EQ": us_assets, "CN_A": cn_assets},
                },
                lifecycle_stage="experiment",
                retention_class="rebuildable",
                metadata={"migration_mode": "apply"},
                rebuildable=True,
            )
            self._kernel.update_run_status(
                run["id"],
                status="completed",
                metrics_summary={
                    "artifact_id": artifact["id"],
                    "us_asset_sync": us_assets,
                    "cn_asset_sync": cn_assets,
                },
            )

            return {
                "run": run,
                "artifact": artifact,
                "report": report,
                "asset_sync": {"US_EQ": us_assets, "CN_A": cn_assets},
            }

    # ------------------------------------------------------------------
    # Legacy adapters
    # ------------------------------------------------------------------

    def preview_legacy_factor(
        self,
        *,
        factor_id: str,
        universe_group_id: str,
        start_date: str,
        end_date: str,
        market: str | None = None,
        project_id: str | None = None,
    ) -> dict[str, Any]:
        """Run a legacy factor through the 3.0 run/artifact lifecycle."""
        resolved_market = normalize_market(market)
        factor = self._factor_service.get_factor(factor_id, market=resolved_market)
        tickers = self._group_service.get_group_tickers(universe_group_id, market=resolved_market)
        if not tickers:
            raise ValueError(f"Universe group '{universe_group_id}' has no members")

        self._market_data_service.sync_assets_from_legacy_stocks(
            "US_EQ" if resolved_market == "US" else "CN_A"
        )

        project = self._kernel.get_project(project_id) if project_id else self._kernel.get_bootstrap_project()
        run = self._kernel.create_run(
            run_type="legacy_factor_preview",
            project_id=project["id"],
            market_profile_id=project["market_profile_id"],
            lifecycle_stage="scratch",
            retention_class="rebuildable",
            created_by="migration",
            params={
                "factor_id": factor_id,
                "universe_group_id": universe_group_id,
                "start_date": start_date,
                "end_date": end_date,
                "market": resolved_market,
            },
        )
        factor_df = self._factor_engine.compute_factor(
            factor_id,
            tickers,
            start_date,
            end_date,
            market=resolved_market,
        )
        if factor_df.empty:
            raise ValueError(
                f"Factor preview produced no data for factor {factor_id} on universe {universe_group_id}"
            )

        payload = {
            "factor": {
                "id": factor["id"],
                "name": factor["name"],
                "version": factor["version"],
                "market": factor["market"],
                "source_hash": self._stable_hash(factor["source_code"]),
            },
            "universe_group_id": universe_group_id,
            "market": resolved_market,
            "date_range": {"start": start_date, "end": end_date},
            "shape": {"rows": len(factor_df), "columns": len(factor_df.columns)},
            "coverage": {
                "non_null_cells": int(factor_df.notna().sum().sum()),
                "non_null_rows": int((factor_df.notna().sum(axis=1) > 0).sum()),
            },
            "summary": self._frame_summary(factor_df),
            "sample": self._frame_sample(factor_df),
        }
        artifact = self._kernel.create_json_artifact(
            run_id=run["id"],
            artifact_type="legacy_factor_preview",
            payload=payload,
            lifecycle_stage="scratch",
            retention_class="rebuildable",
            metadata={"legacy_factor_id": factor_id, "market": resolved_market},
            rebuildable=True,
        )
        self._kernel.add_lineage(
            from_type="legacy_factor",
            from_id=factor_id,
            to_type="artifact",
            to_id=artifact["id"],
            relation="previewed",
        )
        self._kernel.update_run_status(
            run["id"],
            status="completed",
            metrics_summary={"artifact_id": artifact["id"], "rows": len(factor_df)},
        )
        return {"run": run, "artifact": artifact, "preview": payload}

    def materialize_legacy_universe(
        self,
        *,
        universe_group_id: str,
        market: str | None = None,
        project_id: str | None = None,
    ) -> dict[str, Any]:
        """Materialize a legacy universe into a 3.0 artifact."""
        resolved_market = normalize_market(market)
        group = self._group_service.get_group(universe_group_id, market=resolved_market)
        profile_id = "US_EQ" if resolved_market == "US" else "CN_A"
        asset_sync = self._market_data_service.sync_assets_from_legacy_stocks(profile_id)
        tickers = group["tickers"]
        conn = get_connection()
        placeholders = ",".join("?" for _ in tickers)
        asset_rows: list[dict[str, Any]] = []
        if tickers:
            rows = conn.execute(
                f"""SELECT asset_id, symbol, display_symbol, name, exchange, sector, industry, status
                    FROM assets
                    WHERE market_profile_id = ?
                      AND symbol IN ({placeholders})
                    ORDER BY symbol""",
                [profile_id, *tickers],
            ).fetchall()
            for row in rows:
                asset_rows.append(
                    {
                        "asset_id": row[0],
                        "symbol": row[1],
                        "display_symbol": row[2],
                        "name": row[3],
                        "exchange": row[4],
                        "sector": row[5],
                        "industry": row[6],
                        "status": row[7],
                    }
                )

        project = self._kernel.get_project(project_id) if project_id else self._kernel.get_bootstrap_project()
        run = self._kernel.create_run(
            run_type="legacy_universe_materialize",
            project_id=project["id"],
            market_profile_id=project["market_profile_id"],
            lifecycle_stage="experiment",
            retention_class="rebuildable",
            created_by="migration",
            params={
                "universe_group_id": universe_group_id,
                "market": resolved_market,
                "profile_id": profile_id,
            },
        )
        payload = {
            "group": group,
            "market": resolved_market,
            "profile_id": profile_id,
            "asset_sync": asset_sync,
            "member_count": len(tickers),
            "ticker_count": len(tickers),
            "asset_count": len(asset_rows),
            "assets": asset_rows,
        }
        artifact = self._kernel.create_json_artifact(
            run_id=run["id"],
            artifact_type="legacy_universe_materialization",
            payload=payload,
            lifecycle_stage="experiment",
            retention_class="rebuildable",
            metadata={"legacy_group_id": universe_group_id, "market": resolved_market},
            rebuildable=True,
        )
        self._kernel.add_lineage(
            from_type="legacy_group",
            from_id=universe_group_id,
            to_type="artifact",
            to_id=artifact["id"],
            relation="materialized",
        )
        self._kernel.update_run_status(
            run["id"],
            status="completed",
            metrics_summary={
                "artifact_id": artifact["id"],
                "member_count": len(tickers),
                "asset_count": len(asset_rows),
            },
        )
        return {"run": run, "artifact": artifact, "materialization": payload}

    def run_legacy_strategy_backtest(
        self,
        *,
        strategy_id: str,
        universe_group_id: str,
        config: dict[str, Any],
        market: str | None = None,
        project_id: str | None = None,
    ) -> dict[str, Any]:
        """Run a legacy strategy through the existing backtest engine and save a 3.0 trace."""
        resolved_market = normalize_market(market)
        strategy = self._strategy_service.get_strategy(strategy_id, market=resolved_market)
        project = self._kernel.get_project(project_id) if project_id else self._kernel.get_bootstrap_project()
        run = self._kernel.create_run(
            run_type="legacy_strategy_backtest",
            project_id=project["id"],
            market_profile_id=project["market_profile_id"],
            lifecycle_stage="experiment",
            retention_class="rebuildable",
            created_by="migration",
            params={
                "strategy_id": strategy_id,
                "universe_group_id": universe_group_id,
                "market": resolved_market,
                "config": config,
            },
        )
        backtest = self._backtest_service.run_backtest(
            strategy_id=strategy_id,
            config_dict=config,
            universe_group_id=universe_group_id,
            market=resolved_market,
        )
        payload = {
            "strategy": {
                "id": strategy["id"],
                "name": strategy["name"],
                "version": strategy["version"],
                "market": strategy["market"],
                "source_hash": self._stable_hash(strategy["source_code"]),
            },
            "market": resolved_market,
            "universe_group_id": universe_group_id,
            "backtest": backtest,
            "config": config,
        }
        artifact = self._kernel.create_json_artifact(
            run_id=run["id"],
            artifact_type="legacy_strategy_backtest_report",
            payload=payload,
            lifecycle_stage="experiment",
            retention_class="rebuildable",
            metadata={"legacy_strategy_id": strategy_id, "market": resolved_market},
            rebuildable=True,
        )
        self._kernel.add_lineage(
            from_type="legacy_strategy",
            from_id=strategy_id,
            to_type="artifact",
            to_id=artifact["id"],
            relation="backtested",
        )
        self._kernel.update_run_status(
            run["id"],
            status="completed",
            metrics_summary={
                "artifact_id": artifact["id"],
                "backtest_id": backtest.get("backtest_id"),
                "total_return": backtest.get("total_return"),
                "sharpe_ratio": backtest.get("sharpe_ratio"),
            },
        )
        return {"run": run, "artifact": artifact, "backtest": backtest}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _open_connection(self, db_path: Path | None = None):
        if db_path is None:
            return get_connection(), False
        try:
            return duckdb.connect(str(db_path), read_only=True), True
        except duckdb.ConnectionException:
            if Path(db_path).resolve() == settings.db_path.resolve():
                return get_connection(), False
            raise

    @contextmanager
    def _db_context(self, db_path: Path | None):
        if db_path is None:
            yield
            return
        resolved = Path(db_path).resolve()
        current = settings.db_path.resolve()
        if resolved == current:
            yield
            return

        previous = settings.data.db_path
        close_db()
        settings.data.db_path = str(db_path)
        try:
            yield
        finally:
            close_db()
            settings.data.db_path = previous

    def _table_report(self, conn, table: str) -> dict[str, Any]:
        exists = self._table_exists(conn, table)
        if not exists:
            return {
                "exists": False,
                "row_count": 0,
                "table_hash": None,
                "market_counts": {},
                "targets": MIGRATION_MAPPING.get(table, []),
            }

        row_count = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        market_counts = self._market_counts(conn, table)
        table_hash = self._table_hash(conn, table)
        return {
            "exists": True,
            "row_count": row_count,
            "table_hash": table_hash,
            "market_counts": market_counts,
            "targets": MIGRATION_MAPPING.get(table, []),
        }

    def _code_signatures(self, conn, table: str) -> list[dict[str, Any]]:
        if not self._table_exists(conn, table) or table not in _CODE_TABLES:
            return []
        cols = ", ".join(_CODE_TABLES[table])
        order_by = _ORDER_BY.get(table, "id")
        rows = conn.execute(f"SELECT {cols} FROM {table} ORDER BY {order_by}").fetchall()
        signatures: list[dict[str, Any]] = []
        for row in rows:
            record = dict(zip(_CODE_TABLES[table], row, strict=True))
            source_code = record.get("source_code")
            if source_code is not None:
                record["hash"] = self._stable_hash(str(source_code))
            else:
                record["hash"] = self._stable_hash(record)
            record.pop("source_code", None)
            signatures.append(record)
        return signatures

    def _market_counts(self, conn, table: str) -> dict[str, int]:
        if not self._table_exists(conn, table):
            return {}
        try:
            rows = conn.execute(
                f"SELECT COALESCE(market, 'US') AS market, COUNT(*) FROM {table} GROUP BY 1 ORDER BY 1"
            ).fetchall()
        except Exception:
            return {}
        return {str(market): int(count) for market, count in rows}

    def _table_hash(self, conn, table: str) -> str:
        order_by = _ORDER_BY.get(table, "id")
        rows = conn.execute(f"SELECT * FROM {table} ORDER BY {order_by}").fetchall()
        return self._stable_hash(rows)

    @staticmethod
    def _stable_hash(value: Any) -> str:
        payload = json.dumps(value, ensure_ascii=False, default=str, sort_keys=True)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def _table_exists(conn, table: str) -> bool:
        row = conn.execute(
            """
            SELECT COUNT(*)
              FROM information_schema.tables
             WHERE table_schema = 'main'
               AND table_name = ?
            """,
            [table],
        ).fetchone()
        return bool(row and row[0])

    @staticmethod
    def _frame_summary(df: pd.DataFrame) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "non_null_cells": int(df.notna().sum().sum()),
            "non_null_ratio": round(float(df.notna().sum().sum() / max(1, df.size)), 6),
            "min": None,
            "max": None,
            "mean": None,
            "std": None,
        }
        numeric = pd.to_numeric(df.stack(), errors="coerce")
        numeric = numeric.dropna()
        if not numeric.empty:
            summary.update(
                {
                    "min": round(float(numeric.min()), 6),
                    "max": round(float(numeric.max()), 6),
                    "mean": round(float(numeric.mean()), 6),
                    "std": round(float(numeric.std(ddof=1)) if len(numeric) > 1 else 0.0, 6),
                }
            )
        return summary

    @staticmethod
    def _frame_sample(df: pd.DataFrame, limit: int = 5) -> list[dict[str, Any]]:
        sample = df.head(limit).copy()
        sample.index = sample.index.astype(str)
        return [
            {"date": idx, **{str(col): (None if pd.isna(val) else float(val)) for col, val in row.items()}}
            for idx, row in sample.iterrows()
        ]

"""Strategy CRUD service -- register, version, and manage strategy definitions."""

from __future__ import annotations

import json
import re
import uuid
import ast
from datetime import datetime

from backend.db import get_connection
from backend.logger import get_logger
from backend.services.market_context import normalize_market
from backend.time_utils import utc_now_naive
from backend.strategies.loader import load_strategy_from_code

log = get_logger(__name__)

SUPPORTED_POSITION_SIZING = {
    "equal_weight",
    "signal_weight",
    "max_position",
    "raw_weight",
}
STRATEGY_DEFAULT_FORBIDDEN_KEYS = {
    "market",
    "universe_group_id",
    "start_date",
    "end_date",
    "warmup_start_date",
    "evaluation_start_date",
    "benchmark",
    "initial_capital",
    "commission_rate",
    "slippage_rate",
    "debug_mode",
    "debug_level",
    "debug_tickers",
    "debug_dates",
    "result_level",
    "task_id",
}
STRATEGY_DEFAULT_ALLOWED_KEYS = {
    "position_sizing",
    "max_positions",
    "max_position_pct",
    "normalize_target_weights",
    "rebalance_freq",
    "rebalance_frequency",
    "rebalance_buffer",
    "rebalance_buffer_add",
    "rebalance_buffer_reduce",
    "rebalance_buffer_mode",
    "rebalance_buffer_reference",
    "min_holding_days",
    "reentry_cooldown_days",
    "max_holding_days",
    "execution_model",
    "planned_price_buffer_bps",
    "planned_price_fallback",
    "constraint_config",
}


class StrategyService:
    """CRUD operations for strategy definitions stored in DuckDB."""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def create_strategy(
        self,
        name: str,
        source_code: str,
        description: str | None = None,
        position_sizing: str = "equal_weight",
        constraint_config: dict | None = None,
        market: str | None = None,
    ) -> dict:
        """Create a new strategy (auto-versioned).

        If a strategy with the same *name* already exists the version is
        incremented automatically.
        """
        resolved_market = normalize_market(market)
        position_sizing = self._validate_position_sizing(position_sizing)

        # Validate source code is loadable
        try:
            instance = load_strategy_from_code(source_code)
            metadata = self.extract_strategy_metadata_from_instance(instance)
        except Exception as exc:
            raise ValueError(f"Invalid strategy source code: {exc}") from exc

        conn = get_connection()
        self._ensure_constraint_config_column(conn)

        # Auto-version: find max version for this market/name pair
        row = conn.execute(
            "SELECT MAX(version) FROM strategies WHERE market = ? AND name = ?",
            [resolved_market, name],
        ).fetchone()
        version = 1
        if row and row[0] is not None:
            version = row[0] + 1

        strategy_id = uuid.uuid4().hex[:12]
        now = utc_now_naive()

        # Extract required_factors / required_models from the instance
        required_factors = metadata["required_factors"]
        required_models = self._merge_required_models(
            metadata["required_models"],
            self._extract_model_references(source_code),
        )
        self._validate_dependencies(required_factors, required_models, resolved_market)

        # Static validation: check source code model references vs declaration
        model_warnings = self._validate_model_references(source_code, required_models)
        for w in model_warnings:
            log.warning("strategy.model_ref_mismatch", name=name, detail=w)

        # Static validation: check if custom weights are effective under position_sizing
        weight_warnings = self._validate_weight_effectiveness(source_code, position_sizing)
        for w in weight_warnings:
            log.warning("strategy.weight_ineffective", name=name, detail=w)

        conn.execute(
            """INSERT INTO strategies
               (id, market, name, version, description, source_code,
                required_factors, required_models, position_sizing,
                constraint_config, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?)""",
            [
                strategy_id,
                resolved_market,
                name,
                version,
                description or getattr(instance, "description", ""),
                source_code,
                json.dumps(required_factors),
                json.dumps(required_models),
                position_sizing,
            json.dumps(constraint_config or {}, default=str) if constraint_config else None,
                now,
                now,
            ],
        )
        log.info(
            "strategy.created",
            id=strategy_id,
            market=resolved_market,
            name=name,
            version=version,
        )
        result = self.get_strategy(strategy_id, market=resolved_market)
        result["default_backtest_config"] = metadata["default_backtest_config"]
        result["default_paper_config"] = metadata["default_paper_config"]
        if model_warnings:
            result["model_ref_warnings"] = model_warnings
        if weight_warnings:
            result["weight_warnings"] = weight_warnings
        return result

    def update_strategy(
        self,
        strategy_id: str,
        source_code: str | None = None,
        description: str | None = None,
        position_sizing: str | None = None,
        constraint_config: dict | None = None,
        status: str | None = None,
        market: str | None = None,
    ) -> dict:
        """Update a strategy -- if source_code changes, create a new version."""
        conn = get_connection()
        self._ensure_constraint_config_column(conn)
        resolved_market = normalize_market(market)
        existing = self._fetch_row(strategy_id, market=resolved_market)
        if existing is None:
            raise ValueError(f"Strategy {strategy_id} not found")
        if position_sizing is not None:
            position_sizing = self._validate_position_sizing(position_sizing)

        if source_code is not None and source_code != existing["source_code"]:
            # Validate new source code
            try:
                instance = load_strategy_from_code(source_code)
                metadata = self.extract_strategy_metadata_from_instance(instance)
            except Exception as exc:
                raise ValueError(f"Invalid strategy source code: {exc}") from exc

            # Create a new version
            max_ver = conn.execute(
                "SELECT MAX(version) FROM strategies WHERE market = ? AND name = ?",
                [existing["market"], existing["name"]],
            ).fetchone()
            new_version = (max_ver[0] or 0) + 1
            new_id = uuid.uuid4().hex[:12]
            now = utc_now_naive()

            required_factors = metadata["required_factors"]
            required_models = self._merge_required_models(
                metadata["required_models"],
                self._extract_model_references(source_code),
            )
            self._validate_dependencies(required_factors, required_models, existing["market"])

            # Static validation: check source code model references vs declaration
            model_warnings = self._validate_model_references(source_code, required_models)
            for w in model_warnings:
                log.warning("strategy.model_ref_mismatch", name=existing["name"], detail=w)

            # Static validation: check if custom weights are effective under position_sizing
            effective_sizing = position_sizing or existing["position_sizing"]
            weight_warnings = self._validate_weight_effectiveness(source_code, effective_sizing)
            for w in weight_warnings:
                log.warning("strategy.weight_ineffective", name=existing["name"], detail=w)

            conn.execute(
                """INSERT INTO strategies
                   (id, market, name, version, description, source_code,
                    required_factors, required_models, position_sizing,
                    constraint_config, status, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    new_id,
                    existing["market"],
                    existing["name"],
                    new_version,
                    description or existing["description"],
                    source_code,
                    json.dumps(required_factors),
                    json.dumps(required_models),
                    position_sizing or existing["position_sizing"],
                    json.dumps(
                        constraint_config
                        if constraint_config is not None
                        else existing.get("constraint_config") or {},
                        default=str,
                    ),
                    status or "draft",
                    now,
                    now,
                ],
            )
            log.info(
                "strategy.new_version",
                id=new_id,
                market=existing["market"],
                name=existing["name"],
                version=new_version,
            )
            result = self.get_strategy(new_id, market=existing["market"])
            result["default_backtest_config"] = metadata["default_backtest_config"]
            result["default_paper_config"] = metadata["default_paper_config"]
            if model_warnings:
                result["model_ref_warnings"] = model_warnings
            if weight_warnings:
                result["weight_warnings"] = weight_warnings
            return result

        # Simple metadata update (no version bump)
        now = utc_now_naive()
        sets: list[str] = ["updated_at = ?"]
        vals: list = [now]

        for col, val in [
            ("description", description),
            ("position_sizing", position_sizing),
            (
                "constraint_config",
                json.dumps(constraint_config, default=str)
                if constraint_config is not None
                else None,
            ),
            ("status", status),
        ]:
            if val is not None:
                sets.append(f"{col} = ?")
                vals.append(val)

        vals.append(strategy_id)
        vals.append(existing["market"])
        conn.execute(
            f"UPDATE strategies SET {', '.join(sets)} WHERE id = ? AND market = ?",
            vals,
        )
        log.info("strategy.updated", id=strategy_id, market=existing["market"])
        return self.get_strategy(strategy_id, market=existing["market"])

    def delete_strategy(self, strategy_id: str, market: str | None = None) -> None:
        """Delete a strategy definition."""
        conn = get_connection()
        self._ensure_constraint_config_column(conn)
        resolved_market = normalize_market(market)
        existing = self._fetch_row(strategy_id, market=resolved_market)
        if existing is None:
            raise ValueError(f"Strategy {strategy_id} not found")

        conn.execute(
            "DELETE FROM strategies WHERE id = ? AND market = ?",
            [strategy_id, existing["market"]],
        )
        log.info("strategy.deleted", id=strategy_id, market=existing["market"])

    def get_strategy(self, strategy_id: str, market: str | None = None) -> dict:
        """Return a single strategy definition."""
        row = self._fetch_row(strategy_id, market=market)
        if row is None:
            raise ValueError(f"Strategy {strategy_id} not found")
        return row

    def list_strategies(self, market: str | None = None) -> list[dict]:
        """List all strategies."""
        resolved_market = normalize_market(market)
        conn = get_connection()
        self._ensure_constraint_config_column(conn)
        rows = conn.execute(
            """SELECT id, market, name, version, description, source_code,
                      required_factors, required_models, position_sizing,
                      constraint_config, status, created_at, updated_at
               FROM strategies
               WHERE market = ?
               ORDER BY name, version DESC""",
            [resolved_market],
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _merge_required_models(
        declared_models: list[str] | None,
        source_models: set[str] | None,
    ) -> list[str]:
        """Return deterministic union of explicit and statically referenced models."""
        merged = set(declared_models or [])
        merged.update(source_models or set())
        return sorted(merged)

    @staticmethod
    def resolve_required_models(strategy_def: dict) -> list[str]:
        """Resolve all models a strategy may use, including source-code references."""
        return StrategyService._merge_required_models(
            strategy_def.get("required_models", []),
            StrategyService._extract_model_references(strategy_def.get("source_code", "")),
        )

    @staticmethod
    def extract_strategy_metadata_from_source(source_code: str) -> dict:
        instance = load_strategy_from_code(source_code)
        return StrategyService.extract_strategy_metadata_from_instance(instance)

    @staticmethod
    def extract_strategy_metadata_from_instance(instance) -> dict:
        default_backtest_config = StrategyService._normalize_strategy_default_config(
            getattr(instance, "default_backtest_config", {}) or {},
            kind="default_backtest_config",
        )
        default_paper_config = StrategyService._normalize_strategy_default_config(
            getattr(instance, "default_paper_config", {}) or {},
            kind="default_paper_config",
        )
        return {
            "name": getattr(instance, "name", ""),
            "description": getattr(instance, "description", ""),
            "required_factors": list(instance.required_factors()),
            "required_models": list(instance.required_models()),
            "default_backtest_config": default_backtest_config,
            "default_paper_config": default_paper_config,
        }

    @staticmethod
    def _normalize_strategy_default_config(raw: dict | None, *, kind: str) -> dict:
        if raw is None:
            return {}
        if not isinstance(raw, dict):
            raise ValueError(f"strategy default {kind} must be a dict")
        forbidden = sorted(set(raw) & STRATEGY_DEFAULT_FORBIDDEN_KEYS)
        if forbidden:
            raise ValueError(
                f"strategy default {kind} cannot set run/session-owned fields: {forbidden}"
            )
        unsupported = sorted(set(raw) - STRATEGY_DEFAULT_ALLOWED_KEYS)
        if unsupported:
            raise ValueError(
                f"strategy default {kind} has unsupported field(s): {unsupported}"
            )
        normalized = dict(raw)
        if "position_sizing" in normalized:
            normalized["position_sizing"] = StrategyService._validate_position_sizing(
                normalized.get("position_sizing")
            )
        return normalized

    @staticmethod
    def _validate_dependencies(
        required_factors: list[str] | None,
        required_models: list[str] | None,
        market: str,
    ) -> None:
        """Reject dependencies that clearly belong to another market.

        Factor dependencies are declared by name.  Missing names are allowed so
        users can register a strategy before creating every factor, but an
        existing same-name factor in another market without a matching market
        factor is rejected.  Model dependencies are concrete IDs and must exist
        in the strategy market.
        """
        conn = get_connection()
        resolved_market = normalize_market(market)

        for factor_name in required_factors or []:
            rows = conn.execute(
                "SELECT DISTINCT market FROM factors WHERE name = ?",
                [factor_name],
            ).fetchall()
            markets = sorted(str(r[0]) for r in rows if r[0])
            if markets and resolved_market not in markets:
                raise ValueError(
                    f"Strategy factor dependency '{factor_name}' is not available "
                    f"in market {resolved_market}; found markets={markets}"
                )

        for model_id in required_models or []:
            rows = conn.execute(
                "SELECT market FROM models WHERE id = ?",
                [model_id],
            ).fetchall()
            markets = sorted(str(r[0]) for r in rows if r[0])
            if not markets:
                raise ValueError(
                    f"Strategy model dependency '{model_id}' not found in market {resolved_market}"
                )
            if resolved_market not in markets:
                raise ValueError(
                    f"Strategy model dependency '{model_id}' belongs to market "
                    f"{markets[0]}, not market {resolved_market}"
                )

    @staticmethod
    def _extract_model_references(source_code: str) -> set[str]:
        """Extract model IDs used as ``model_predictions`` keys.

        Handles direct literals and simple constants such as
        ``AUX_MODEL_ID = "..."`` followed by
        ``context.model_predictions.get(AUX_MODEL_ID)`` or
        ``context.model_predictions.get(self.AUX_MODEL_ID)``.
        """
        if not source_code:
            return set()

        try:
            tree = ast.parse(source_code)
        except SyntaxError:
            return set()

        constants: dict[str, str] = {}
        for node in ast.walk(tree):
            targets: list[ast.expr] = []
            value = None
            if isinstance(node, ast.Assign):
                targets = list(node.targets)
                value = node.value
            elif isinstance(node, ast.AnnAssign):
                targets = [node.target]
                value = node.value
            if not isinstance(value, ast.Constant) or not isinstance(value.value, str):
                continue
            for target in targets:
                if isinstance(target, ast.Name):
                    constants[target.id] = value.value
                elif isinstance(target, ast.Attribute):
                    constants[target.attr] = value.value

        def is_model_predictions(expr: ast.AST) -> bool:
            if isinstance(expr, ast.Name):
                return expr.id == "model_predictions"
            if isinstance(expr, ast.Attribute):
                return expr.attr == "model_predictions"
            return False

        def resolve_key(expr: ast.AST | None) -> str | None:
            if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
                return expr.value
            if isinstance(expr, ast.Name):
                return constants.get(expr.id)
            if isinstance(expr, ast.Attribute):
                return constants.get(expr.attr)
            return None

        referenced: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Subscript) and is_model_predictions(node.value):
                key = resolve_key(node.slice)
                if key:
                    referenced.add(key)
            elif (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "get"
                and is_model_predictions(node.func.value)
                and node.args
            ):
                key = resolve_key(node.args[0])
                if key:
                    referenced.add(key)

        return referenced

    @staticmethod
    def _validate_model_references(source_code: str, declared_models: list[str]) -> list[str]:
        """Scan strategy source code for model_predictions references and warn on mismatch.

        Returns a list of warning strings.  This is intentionally best-effort:
        dynamic patterns like ``list(context.model_predictions.keys())[0]``
        won't be caught, which is fine -- the goal is to catch the most common
        mistake: hard-coded model IDs that aren't declared in required_models().
        """
        warnings: list[str] = []

        # Patterns that extract a string literal used as a model ID key:
        #   context.model_predictions["abc123"]
        #   context.model_predictions['abc123']
        #   context.model_predictions.get("abc123" ...)
        #   model_predictions["abc123"]
        #   model_predictions.get("abc123" ...)
        pattern = re.compile(
            r"""model_predictions\s*(?:\[|\.get\s*\()\s*['"]([a-zA-Z0-9_]+)['"]""",
        )
        referenced_ids = set(pattern.findall(source_code))
        referenced_ids.update(StrategyService._extract_model_references(source_code))

        declared_set = set(declared_models)

        # IDs referenced in code but missing from required_models()
        undeclared = referenced_ids - declared_set
        if undeclared:
            warnings.append(
                f"策略源码中引用了模型 {sorted(undeclared)} 但未在 required_models() 中声明。"
                f"这些模型的预测不会被加载，可能导致策略静默退化为 0 trades。"
                f"请在策略类的 required_models() 方法中添加这些模型 ID。"
            )

        # Check for generic model_predictions access without any declared models
        if not declared_models:
            # Look for any model_predictions usage (including dynamic patterns)
            has_model_access = bool(re.search(
                r"""model_predictions\s*[\[.]""", source_code
            ))
            if has_model_access:
                warnings.append(
                    f"策略源码中访问了 model_predictions 但 required_models() 返回空列表。"
                    f"运行时将不会加载任何模型预测数据。"
                )

        return warnings

    @staticmethod
    def _validate_weight_effectiveness(
        source_code: str, position_sizing: str
    ) -> list[str]:
        """Check if strategy source code customises signal weights under a sizing
        mode that would ignore them.

        Under ``equal_weight`` position sizing, the backtest engine overwrites
        every ticker's weight with ``1/n``, so custom weight logic in the
        strategy is effectively dead code.  This check looks for non-trivial
        weight manipulation patterns in source and warns when the sizing mode
        will discard those values.

        Returns a list of warning strings (empty if everything is fine).
        """
        if position_sizing != "equal_weight":
            return []

        # Patterns indicating non-trivial weight manipulation:
        #   out["weight"] = ...  (not 1/n or 1.0/n)
        #   weight_map[...] = ...
        #   row["weight"] = ...
        #   .loc[..., "weight"] = ...
        #   signals["weight"] = some_variable  (not 1/n)
        weight_assignment = re.compile(
            r"""(?:"""
            r"""\["weight"\]\s*="""  # dict/DataFrame assignment
            r"""|\.weight\s*="""     # attr assignment
            r"""|weight_map"""       # weight_map variable usage
            r""")""",
            re.VERBOSE,
        )
        matches = weight_assignment.findall(source_code)
        if not matches:
            return []

        # Heuristic: check if any weight assignment looks non-uniform
        # Simple 1/n patterns: = 1.0 / len(...), = 1 / n, = equal_w
        uniform_pattern = re.compile(
            r"""["']weight["']\]\s*=\s*(?:1(?:\.0)?\s*/\s*(?:len|n\b|num|count)|equal)""",
        )
        non_uniform_assignments = []
        for line in source_code.splitlines():
            if weight_assignment.search(line):
                if not uniform_pattern.search(line):
                    stripped = line.strip()
                    if stripped and not stripped.startswith("#"):
                        non_uniform_assignments.append(stripped)

        if not non_uniform_assignments:
            return []

        sample = non_uniform_assignments[0][:120]
        return [
            f"策略使用 position_sizing='equal_weight'，但源码中包含自定义 weight 赋值逻辑"
            f"（如 `{sample}`）。在 equal_weight 模式下，执行层会将所有持仓权重覆盖为 1/n，"
            f"策略输出的自定义权重不会生效。"
            f"如需权重生效，请将 position_sizing 改为 'signal_weight'、'max_position' 或 'raw_weight'。"
        ]

    def _fetch_row(self, strategy_id: str, market: str | None = None) -> dict | None:
        resolved_market = normalize_market(market)
        conn = get_connection()
        self._ensure_constraint_config_column(conn)
        row = conn.execute(
            """SELECT id, market, name, version, description, source_code,
                      required_factors, required_models, position_sizing,
                      constraint_config, status, created_at, updated_at
               FROM strategies WHERE id = ? AND market = ?""",
            [strategy_id, resolved_market],
        ).fetchone()
        if row is None:
            return None
        return self._row_to_dict(row)

    @staticmethod
    def _ensure_constraint_config_column(conn) -> None:
        """Ensure old local DBs/tests have the optional strategy constraints column."""
        try:
            exists = conn.execute(
                """SELECT 1 FROM information_schema.columns
                   WHERE table_schema = 'main'
                     AND table_name = 'strategies'
                     AND column_name = 'constraint_config'"""
            ).fetchone()
            if not exists:
                conn.execute("ALTER TABLE strategies ADD COLUMN constraint_config JSON")
        except Exception:
            pass

    @staticmethod
    def _validate_position_sizing(position_sizing: str | None) -> str:
        value = (position_sizing or "equal_weight").strip()
        if value not in SUPPORTED_POSITION_SIZING:
            raise ValueError(
                "Unsupported position_sizing "
                f"'{value}'. Supported values: {sorted(SUPPORTED_POSITION_SIZING)}"
            )
        return value

    @staticmethod
    def _row_to_dict(row) -> dict:
        def _parse_json(raw, default):
            if isinstance(raw, str):
                try:
                    return json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    return default
            return raw if raw else default

        defaults = {"default_backtest_config": {}, "default_paper_config": {}}
        try:
            defaults = StrategyService.extract_strategy_metadata_from_source(row[5])
        except Exception as exc:
            log.warning(
                "strategy.default_metadata_unavailable",
                strategy_id=row[0],
                error=str(exc),
            )

        return {
            "id": row[0],
            "market": row[1],
            "name": row[2],
            "version": row[3],
            "description": row[4],
            "source_code": row[5],
            "required_factors": _parse_json(row[6], []),
            "required_models": _parse_json(row[7], []),
            "position_sizing": row[8],
            "constraint_config": _parse_json(row[9], {}) if row[9] else {},
            "status": row[10],
            "created_at": str(row[11]) if row[11] else None,
            "updated_at": str(row[12]) if row[12] else None,
            "default_backtest_config": defaults.get("default_backtest_config", {}),
            "default_paper_config": defaults.get("default_paper_config", {}),
        }

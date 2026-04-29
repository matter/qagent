"""Strategy CRUD service -- register, version, and manage strategy definitions."""

from __future__ import annotations

import json
import re
import uuid
import ast
from datetime import datetime

from backend.db import get_connection
from backend.logger import get_logger
from backend.strategies.loader import load_strategy_from_code

log = get_logger(__name__)


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
    ) -> dict:
        """Create a new strategy (auto-versioned).

        If a strategy with the same *name* already exists the version is
        incremented automatically.
        """
        # Validate source code is loadable
        try:
            instance = load_strategy_from_code(source_code)
        except Exception as exc:
            raise ValueError(f"Invalid strategy source code: {exc}") from exc

        conn = get_connection()

        # Auto-version: find max version for this name
        row = conn.execute(
            "SELECT MAX(version) FROM strategies WHERE name = ?", [name]
        ).fetchone()
        version = 1
        if row and row[0] is not None:
            version = row[0] + 1

        strategy_id = uuid.uuid4().hex[:12]
        now = datetime.utcnow()

        # Extract required_factors / required_models from the instance
        required_factors = instance.required_factors()
        required_models = self._merge_required_models(
            instance.required_models(),
            self._extract_model_references(source_code),
        )

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
               (id, name, version, description, source_code,
                required_factors, required_models, position_sizing,
                status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?)""",
            [
                strategy_id,
                name,
                version,
                description or getattr(instance, "description", ""),
                source_code,
                json.dumps(required_factors),
                json.dumps(required_models),
                position_sizing,
                now,
                now,
            ],
        )
        log.info(
            "strategy.created",
            id=strategy_id,
            name=name,
            version=version,
        )
        result = self.get_strategy(strategy_id)
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
        status: str | None = None,
    ) -> dict:
        """Update a strategy -- if source_code changes, create a new version."""
        conn = get_connection()
        existing = self._fetch_row(strategy_id)
        if existing is None:
            raise ValueError(f"Strategy {strategy_id} not found")

        if source_code is not None and source_code != existing["source_code"]:
            # Validate new source code
            try:
                instance = load_strategy_from_code(source_code)
            except Exception as exc:
                raise ValueError(f"Invalid strategy source code: {exc}") from exc

            # Create a new version
            max_ver = conn.execute(
                "SELECT MAX(version) FROM strategies WHERE name = ?",
                [existing["name"]],
            ).fetchone()
            new_version = (max_ver[0] or 0) + 1
            new_id = uuid.uuid4().hex[:12]
            now = datetime.utcnow()

            required_factors = instance.required_factors()
            required_models = self._merge_required_models(
                instance.required_models(),
                self._extract_model_references(source_code),
            )

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
                   (id, name, version, description, source_code,
                    required_factors, required_models, position_sizing,
                    status, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    new_id,
                    existing["name"],
                    new_version,
                    description or existing["description"],
                    source_code,
                    json.dumps(required_factors),
                    json.dumps(required_models),
                    position_sizing or existing["position_sizing"],
                    status or "draft",
                    now,
                    now,
                ],
            )
            log.info(
                "strategy.new_version",
                id=new_id,
                name=existing["name"],
                version=new_version,
            )
            result = self.get_strategy(new_id)
            if model_warnings:
                result["model_ref_warnings"] = model_warnings
            if weight_warnings:
                result["weight_warnings"] = weight_warnings
            return result

        # Simple metadata update (no version bump)
        now = datetime.utcnow()
        sets: list[str] = ["updated_at = ?"]
        vals: list = [now]

        for col, val in [
            ("description", description),
            ("position_sizing", position_sizing),
            ("status", status),
        ]:
            if val is not None:
                sets.append(f"{col} = ?")
                vals.append(val)

        vals.append(strategy_id)
        conn.execute(
            f"UPDATE strategies SET {', '.join(sets)} WHERE id = ?", vals
        )
        log.info("strategy.updated", id=strategy_id)
        return self.get_strategy(strategy_id)

    def delete_strategy(self, strategy_id: str) -> None:
        """Delete a strategy definition."""
        conn = get_connection()
        existing = self._fetch_row(strategy_id)
        if existing is None:
            raise ValueError(f"Strategy {strategy_id} not found")

        conn.execute("DELETE FROM strategies WHERE id = ?", [strategy_id])
        log.info("strategy.deleted", id=strategy_id)

    def get_strategy(self, strategy_id: str) -> dict:
        """Return a single strategy definition."""
        row = self._fetch_row(strategy_id)
        if row is None:
            raise ValueError(f"Strategy {strategy_id} not found")
        return row

    def list_strategies(self) -> list[dict]:
        """List all strategies."""
        conn = get_connection()
        rows = conn.execute(
            """SELECT id, name, version, description, source_code,
                      required_factors, required_models, position_sizing,
                      status, created_at, updated_at
               FROM strategies
               ORDER BY name, version DESC"""
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
            f"如需权重生效，请将 position_sizing 改为 'signal_weight' 或 'max_position'。"
        ]

    def _fetch_row(self, strategy_id: str) -> dict | None:
        conn = get_connection()
        row = conn.execute(
            """SELECT id, name, version, description, source_code,
                      required_factors, required_models, position_sizing,
                      status, created_at, updated_at
               FROM strategies WHERE id = ?""",
            [strategy_id],
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
                    return []
            return raw if raw else []

        return {
            "id": row[0],
            "name": row[1],
            "version": row[2],
            "description": row[3],
            "source_code": row[4],
            "required_factors": _parse_json(row[5]),
            "required_models": _parse_json(row[6]),
            "position_sizing": row[7],
            "status": row[8],
            "created_at": str(row[9]) if row[9] else None,
            "updated_at": str(row[10]) if row[10] else None,
        }

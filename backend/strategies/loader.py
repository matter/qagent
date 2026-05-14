"""Load a StrategyBase subclass from user-supplied source code."""

from __future__ import annotations

import importlib
import hashlib
import threading
from typing import Any

from backend.strategies.base import StrategyBase, StrategyContext
from backend.logger import get_logger
from backend.services.custom_code_runner import run_user_code_isolated
from backend.services.custom_code_safety import validate_user_code_safety

log = get_logger(__name__)
_STRATEGY_METADATA_CACHE: dict[str, dict[str, Any]] = {}
_STRATEGY_METADATA_LOCKS: dict[str, threading.Lock] = {}
_STRATEGY_METADATA_LOCKS_GUARD = threading.Lock()

# Modules that strategy source code is allowed to import.
_ALLOWED_MODULES = {
    "pandas",
    "pd",
    "numpy",
    "np",
    "math",
    "backend.strategies.base",
}


def _restricted_import(name: str, globals_=None, locals_=None, fromlist=(), level=0):
    """A custom __import__ that only allows whitelisted modules."""
    if level != 0:
        raise ImportError(
            f"Relative imports are not allowed in strategy code (attempted level={level})"
        )

    if name not in _ALLOWED_MODULES:
        raise ImportError(
            f"Import of '{name}' is not allowed in strategy code. "
            f"Allowed modules: {sorted(_ALLOWED_MODULES)}"
        )
    return importlib.import_module(name)


def load_strategy_from_code(source_code: str) -> StrategyBase:
    """Execute *source_code* in a restricted namespace and return a StrategyBase instance.

    The code **must** define exactly one class that inherits from
    ``StrategyBase``.  An instance of that class is returned.

    Raises:
        ValueError: If the code does not define a valid StrategyBase subclass.
        RuntimeError: If execution of the code fails.
    """
    if not source_code or not source_code.strip():
        raise ValueError("source_code is empty")
    validate_user_code_safety(source_code, code_kind="strategy")
    cache_key = hashlib.sha256(source_code.encode("utf-8")).hexdigest()
    metadata = _STRATEGY_METADATA_CACHE.get(cache_key)
    if metadata is None:
        with _strategy_metadata_lock(cache_key):
            metadata = _STRATEGY_METADATA_CACHE.get(cache_key)
            if metadata is None:
                metadata = run_user_code_isolated(
                    source_code=source_code,
                    code_kind="strategy",
                    operation="metadata",
                    timeout_seconds=10,
                )
                _STRATEGY_METADATA_CACHE[cache_key] = dict(metadata)
    return IsolatedStrategyProxy(source_code, metadata)


def _strategy_metadata_lock(cache_key: str) -> threading.Lock:
    with _STRATEGY_METADATA_LOCKS_GUARD:
        lock = _STRATEGY_METADATA_LOCKS.get(cache_key)
        if lock is None:
            lock = threading.Lock()
            _STRATEGY_METADATA_LOCKS[cache_key] = lock
        return lock


class IsolatedStrategyProxy(StrategyBase):
    """Strategy proxy that executes user code in a child process per call."""

    def __init__(self, source_code: str, metadata: dict[str, Any]) -> None:
        self._source_code = source_code
        self._execution_timeout_seconds = 10.0
        self.name = str(metadata.get("name") or "")
        if not self.name:
            raise ValueError("Strategy class must define a non-empty 'name' attribute")
        self.description = str(metadata.get("description") or "")
        self._required_factors = list(metadata.get("required_factors") or [])
        self._required_models = list(metadata.get("required_models") or [])
        self.default_backtest_config = dict(metadata.get("default_backtest_config") or {})
        self.default_paper_config = dict(metadata.get("default_paper_config") or {})

    def generate_signals(self, context: StrategyContext):
        result = run_user_code_isolated(
            source_code=self._source_code,
            code_kind="strategy",
            operation="generate_signals",
            payload={"context": context},
            timeout_seconds=self._execution_timeout_seconds,
        )
        if isinstance(result, dict) and "signals" in result:
            diagnostics = result.get("diagnostics")
            if isinstance(diagnostics, dict):
                context.diagnostics.update(diagnostics)
            return result["signals"]
        return result

    def required_factors(self) -> list[str]:
        return self._required_factors

    def required_models(self) -> list[str]:
        return self._required_models


def _load_strategy_instance_unsafe(source_code: str) -> StrategyBase:
    """Load a strategy instance inside an already isolated worker process."""
    if not source_code or not source_code.strip():
        raise ValueError("source_code is empty")
    validate_user_code_safety(source_code, code_kind="strategy")

    import builtins as _builtins
    import math

    import numpy as np
    import pandas as pd

    safe_builtins = {
        name: getattr(_builtins, name)
        for name in (
            "__build_class__",
            "abs", "all", "any", "bool", "dict", "enumerate", "filter",
            "float", "frozenset", "getattr", "hasattr", "int", "isinstance",
            "issubclass", "len", "list", "map", "max", "min", "None",
            "print", "property", "range", "reversed", "round", "set",
            "slice", "sorted", "str", "sum", "super", "True", "False",
            "tuple", "type", "zip",
        )
        if hasattr(_builtins, name)
    }
    safe_builtins["__import__"] = _restricted_import

    namespace: dict[str, Any] = {
        "__builtins__": safe_builtins,
        "__name__": "<strategy>",
        "pd": pd,
        "np": np,
        "numpy": np,
        "pandas": pd,
        "math": math,
        "StrategyBase": StrategyBase,
        "StrategyContext": StrategyContext,
    }

    try:
        exec(compile(source_code, "<strategy>", "exec"), namespace)  # noqa: S102
    except Exception as exc:
        log.error("strategy.load.exec_failed", error=str(exc))
        raise RuntimeError(f"Failed to execute strategy source code: {exc}") from exc

    # Find the StrategyBase subclass defined in the code
    candidates: list[type] = []
    for obj in namespace.values():
        if (
            isinstance(obj, type)
            and issubclass(obj, StrategyBase)
            and obj is not StrategyBase
        ):
            candidates.append(obj)

    if not candidates:
        raise ValueError(
            "source_code must define at least one class that inherits from StrategyBase"
        )

    if len(candidates) > 1:
        log.warning(
            "strategy.load.multiple_classes",
            count=len(candidates),
            using=candidates[0].__name__,
        )

    cls = candidates[0]

    try:
        instance = cls()
    except Exception as exc:
        raise RuntimeError(
            f"Failed to instantiate {cls.__name__}: {exc}"
        ) from exc

    # Validate required attribute
    if not getattr(instance, "name", None):
        raise ValueError(
            f"Strategy class {cls.__name__} must define a non-empty 'name' attribute"
        )

    return instance

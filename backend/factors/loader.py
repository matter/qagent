"""Load a FactorBase subclass from user-supplied source code."""

from __future__ import annotations

import importlib
from typing import Any

from backend.factors.base import FactorBase
from backend.logger import get_logger
from backend.services.custom_code_safety import validate_user_code_safety

log = get_logger(__name__)

# Modules that factor source code is allowed to import.
_ALLOWED_MODULES = {
    "pandas",
    "pd",
    "numpy",
    "np",
    "math",
    "backend.factors.base",
    "backend.indicators",
    "backend.indicators.adapter",
}


def _restricted_import(name: str, globals_=None, locals_=None, fromlist=(), level=0):
    """A custom __import__ that only allows whitelisted modules."""
    if level != 0:
        # Relative imports are not supported in factor code
        raise ImportError(f"Relative imports are not allowed in factor code (attempted level={level})")

    if name not in _ALLOWED_MODULES:
        raise ImportError(
            f"Import of '{name}' is not allowed in factor code. "
            f"Allowed modules: {sorted(_ALLOWED_MODULES)}"
        )
    return importlib.import_module(name)


def load_factor_from_code(source_code: str) -> FactorBase:
    """Execute *source_code* in a restricted namespace and return a FactorBase instance.

    The code **must** define exactly one class that inherits from
    ``FactorBase``.  An instance of that class is returned.

    Raises:
        ValueError: If the code does not define a valid FactorBase subclass.
        RuntimeError: If execution of the code fails.
    """
    if not source_code or not source_code.strip():
        raise ValueError("source_code is empty")
    validate_user_code_safety(source_code, code_kind="factor")

    # Build a namespace with limited builtins plus common libraries.
    import builtins as _builtins
    import math

    import numpy as np
    import pandas as pd

    from backend.indicators import ta  # noqa: F811

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
        "__name__": "<factor>",
        "pd": pd,
        "np": np,
        "numpy": np,
        "pandas": pd,
        "math": math,
        "ta": ta,
        "FactorBase": FactorBase,
    }

    try:
        exec(compile(source_code, "<factor>", "exec"), namespace)  # noqa: S102
    except Exception as exc:
        log.error("factor.load.exec_failed", error=str(exc))
        raise RuntimeError(f"Failed to execute factor source code: {exc}") from exc

    # Find the FactorBase subclass defined in the code
    candidates: list[type] = []
    for obj in namespace.values():
        if (
            isinstance(obj, type)
            and issubclass(obj, FactorBase)
            and obj is not FactorBase
        ):
            candidates.append(obj)

    if not candidates:
        raise ValueError(
            "source_code must define at least one class that inherits from FactorBase"
        )

    if len(candidates) > 1:
        log.warning(
            "factor.load.multiple_classes",
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
            f"Factor class {cls.__name__} must define a non-empty 'name' attribute"
        )

    return instance

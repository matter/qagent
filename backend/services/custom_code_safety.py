"""Static safety checks for user-supplied factor and strategy source code."""

from __future__ import annotations

import ast
from typing import Any


_BLOCKED_CALLS = {
    "eval",
    "exec",
    "compile",
    "open",
    "input",
    "__import__",
    "breakpoint",
}

_BLOCKED_MODULES = {
    "asyncio",
    "builtins",
    "ctypes",
    "multiprocessing",
    "os",
    "pathlib",
    "pickle",
    "requests",
    "shutil",
    "socket",
    "subprocess",
    "sys",
    "threading",
    "urllib",
}


def validate_user_code_safety(source_code: str, *, code_kind: str) -> None:
    """Reject obvious unsafe constructs before executing user code.

    This is a static guardrail, not a sandbox. It catches high-risk constructs
    early while the runtime still needs separate process isolation for strong
    CPU, memory, filesystem, and network guarantees.
    """
    try:
        tree = ast.parse(source_code)
    except SyntaxError as exc:
        raise ValueError(f"{code_kind} source_code has syntax error: {exc}") from exc

    for node in ast.walk(tree):
        if isinstance(node, ast.While) and isinstance(node.test, ast.Constant) and node.test.value is True:
            raise ValueError(f"Unbounded while True is not allowed in {code_kind} code")
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            _validate_import_node(node, code_kind=code_kind)
        if isinstance(node, ast.Call):
            _validate_call_node(node, code_kind=code_kind)
        if isinstance(node, ast.Attribute) and node.attr.startswith("__"):
            raise ValueError(f"Dunder attribute access is not allowed in {code_kind} code: {node.attr}")


def _validate_import_node(node: ast.Import | ast.ImportFrom, *, code_kind: str) -> None:
    module_names: list[str] = []
    if isinstance(node, ast.Import):
        module_names = [alias.name for alias in node.names]
    elif node.module:
        module_names = [node.module]
    for module_name in module_names:
        root = module_name.split(".", 1)[0]
        if root in _BLOCKED_MODULES:
            raise ValueError(f"Import of '{module_name}' is not allowed in {code_kind} code")


def _validate_call_node(node: ast.Call, *, code_kind: str) -> None:
    call_name = _call_name(node.func)
    if call_name in _BLOCKED_CALLS:
        raise ValueError(f"Call to '{call_name}' is not allowed in {code_kind} code")
    if call_name and call_name.startswith("__"):
        raise ValueError(f"Dunder call '{call_name}' is not allowed in {code_kind} code")


def _call_name(func: Any) -> str | None:
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None

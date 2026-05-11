"""Isolated process runner for user-supplied factor and strategy code."""

from __future__ import annotations

import multiprocessing as mp
import os
import queue
import time
import traceback
from typing import Any


DEFAULT_USER_CODE_TIMEOUT_SECONDS = 10.0
DEFAULT_USER_CODE_MEMORY_BYTES = 2 * 1024 * 1024 * 1024


class UserCodeExecutionError(RuntimeError):
    """Raised when isolated user code fails or violates runtime limits."""


def run_user_code_isolated(
    *,
    source_code: str,
    code_kind: str,
    operation: str,
    payload: dict[str, Any] | None = None,
    timeout_seconds: float = DEFAULT_USER_CODE_TIMEOUT_SECONDS,
    memory_limit_bytes: int = DEFAULT_USER_CODE_MEMORY_BYTES,
) -> Any:
    """Run user code in a child process and return the operation result."""
    ctx = _multiprocessing_context()
    result_queue = ctx.Queue(maxsize=1)
    process = ctx.Process(
        target=_child_main,
        args=(
            result_queue,
            source_code,
            code_kind,
            operation,
            payload or {},
            memory_limit_bytes,
            max(1, int(timeout_seconds) + 1),
        ),
    )
    process.start()
    message = _receive_child_result(
        process=process,
        result_queue=result_queue,
        code_kind=code_kind,
        timeout_seconds=timeout_seconds,
    )

    if not message.get("ok"):
        raise UserCodeExecutionError(message.get("error") or f"{code_kind} user code failed")
    return message.get("result")


def _receive_child_result(
    *,
    process: mp.Process,
    result_queue,
    code_kind: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            process.terminate()
            process.join(timeout=1)
            raise UserCodeExecutionError(
                f"{code_kind} user code timed out after {timeout_seconds:g}s in isolated process"
            )
        try:
            message = result_queue.get(timeout=min(0.05, remaining))
            process.join(timeout=1)
            if process.is_alive():
                process.terminate()
                process.join(timeout=1)
            return message
        except queue.Empty:
            if process.is_alive():
                continue
            process.join(timeout=1)
            try:
                return result_queue.get_nowait()
            except queue.Empty:
                if process.exitcode and process.exitcode != 0:
                    raise UserCodeExecutionError(
                        f"{code_kind} user code process exited with code {process.exitcode}"
                    )
                raise UserCodeExecutionError(f"{code_kind} user code returned no result")


def _multiprocessing_context() -> mp.context.BaseContext:
    methods = mp.get_all_start_methods()
    method = "spawn" if "spawn" in methods else ("fork" if "fork" in methods and os.name != "nt" else methods[0])
    return mp.get_context(method)


def _child_main(
    result_queue,
    source_code: str,
    code_kind: str,
    operation: str,
    payload: dict[str, Any],
    memory_limit_bytes: int,
    cpu_limit_seconds: int,
) -> None:
    try:
        _apply_resource_limits(memory_limit_bytes, cpu_limit_seconds)
        result = _run_operation(source_code, code_kind, operation, payload)
        result_queue.put({"ok": True, "result": result})
    except Exception:
        result_queue.put({"ok": False, "error": traceback.format_exc()})


def _run_operation(
    source_code: str,
    code_kind: str,
    operation: str,
    payload: dict[str, Any],
) -> Any:
    if code_kind == "factor":
        from backend.factors.loader import _load_factor_instance_unsafe

        instance = _load_factor_instance_unsafe(source_code)
        if operation == "metadata":
            return {
                "name": getattr(instance, "name", ""),
                "description": getattr(instance, "description", ""),
                "params": getattr(instance, "params", {}),
                "category": getattr(instance, "category", "custom"),
            }
        if operation == "compute":
            return instance.compute(payload["data"])
    if code_kind == "strategy":
        from backend.strategies.loader import _load_strategy_instance_unsafe

        instance = _load_strategy_instance_unsafe(source_code)
        if operation == "metadata":
            return {
                "name": getattr(instance, "name", ""),
                "description": getattr(instance, "description", ""),
                "required_factors": instance.required_factors(),
                "required_models": instance.required_models(),
            }
        if operation == "generate_signals":
            context = payload["context"]
            signals = instance.generate_signals(context)
            return {
                "signals": signals,
                "diagnostics": _sanitize_diagnostics(getattr(context, "diagnostics", {}) or {}),
            }
    raise ValueError(f"Unsupported user code operation: {code_kind}.{operation}")


def _apply_resource_limits(memory_limit_bytes: int, cpu_limit_seconds: int) -> None:
    try:
        import resource

        resource.setrlimit(resource.RLIMIT_CPU, (cpu_limit_seconds, cpu_limit_seconds + 1))
        if hasattr(resource, "RLIMIT_AS"):
            resource.setrlimit(resource.RLIMIT_AS, (memory_limit_bytes, memory_limit_bytes))
    except Exception:
        return


def _sanitize_diagnostics(value: Any, *, depth: int = 0) -> Any:
    """Keep strategy diagnostics pickle/JSON friendly before crossing processes."""
    if depth > 6:
        return repr(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {
            str(key): _sanitize_diagnostics(item, depth=depth + 1)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_sanitize_diagnostics(item, depth=depth + 1) for item in value]
    if isinstance(value, set):
        items = [_sanitize_diagnostics(item, depth=depth + 1) for item in value]
        return sorted(items, key=repr)
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if value.__class__.__module__.startswith(("pandas", "numpy")):
        return repr(value)
    return repr(value)

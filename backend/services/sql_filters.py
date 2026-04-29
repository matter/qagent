"""Small DuckDB helpers for large value filters."""

from __future__ import annotations

from contextlib import contextmanager
from collections.abc import Iterable, Iterator
import re
import uuid

import pandas as pd

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@contextmanager
def registered_values_table(
    conn,
    column: str,
    values: Iterable[object],
    *,
    table_prefix: str = "_qagent_values",
) -> Iterator[str]:
    """Register values as a temporary DuckDB scan table.

    This keeps large ticker/factor filters parameterized without building very
    long ``IN (?, ?, ...)`` fragments in hot paths.
    """
    if not _IDENTIFIER_RE.match(column):
        raise ValueError(f"Unsafe SQL column identifier: {column}")
    if not _IDENTIFIER_RE.match(table_prefix):
        raise ValueError(f"Unsafe SQL table prefix: {table_prefix}")

    table_name = f"{table_prefix}_{uuid.uuid4().hex}"
    frame = pd.DataFrame({column: list(values)})
    conn.register(table_name, frame)
    try:
        yield table_name
    finally:
        try:
            conn.unregister(table_name)
        except Exception:
            pass

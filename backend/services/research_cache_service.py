"""Research cache service for daily hot paths and storage governance."""

from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from collections import OrderedDict
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from backend.config import settings
from backend.db import get_connection
from backend.logger import get_logger
from backend.services.market_context import normalize_market, normalize_ticker
from backend.tasks.store import TaskStore
from backend.time_utils import utc_now_naive

log = get_logger(__name__)

_FEATURE_MATRIX_SCHEMA_VERSION = "1"
_LABEL_VALUES_SCHEMA_VERSION = "1"
_DEFAULT_DAILY_HOT_DAYS = 2
_DEFAULT_ORPHAN_FILE_MIN_AGE_SECONDS = 3600
_CACHE_WRITE_LOCKS_GUARD = threading.Lock()
_CACHE_WRITE_LOCKS: dict[str, threading.Lock] = {}
_PROCESS_FEATURE_MATRIX_CACHE_TTL_SECONDS = 48 * 60 * 60
_PROCESS_FEATURE_MATRIX_CACHE_MAX_ENTRIES = 8
_PROCESS_FEATURE_MATRIX_CACHE_LOCK = threading.Lock()
_PROCESS_FEATURE_MATRIX_CACHE: OrderedDict[
    str,
    tuple[float, dict[str, Any], dict[str, pd.DataFrame]],
] = OrderedDict()
_CACHE_WRITER_LEASE_TTL_SECONDS = 15 * 60
_CACHE_WRITER_LEASE_WAIT_SECONDS = 60
_TRANSIENT_DUCKDB_MARKERS = (
    "TransactionContext Error",
    "Conflict on tuple deletion",
    "PRIMARY KEY or UNIQUE constraint violation",
)


class ResearchCacheService:
    """Manage reusable research caches and cache cleanup decisions.

    The first production slice focuses on preprocessed feature matrices, which
    are the most repeated daily research object after raw factor values.
    """

    def __init__(self, cache_root: Path | None = None) -> None:
        self._cache_root = cache_root or (settings.project_root / "data" / "research_cache")

    # ------------------------------------------------------------------
    # Feature matrix hot cache
    # ------------------------------------------------------------------

    def build_feature_matrix_key(
        self,
        *,
        market: str | None,
        feature_set_id: str,
        tickers: list[str],
        start_date: str,
        end_date: str,
        factor_refs: list[dict],
        preprocessing: dict | None,
        data_version: str | None = None,
    ) -> str:
        resolved_market = normalize_market(market)
        normalized_tickers = sorted({normalize_ticker(t, resolved_market) for t in tickers})
        payload = {
            "schema_version": _FEATURE_MATRIX_SCHEMA_VERSION,
            "object_type": "feature_matrix",
            "market": resolved_market,
            "feature_set_id": feature_set_id,
            "tickers": normalized_tickers,
            "start_date": str(start_date),
            "end_date": str(end_date),
            "factor_refs": _canonical_jsonable(factor_refs),
            "preprocessing": _canonical_jsonable(preprocessing or {}),
            "data_version": data_version or self.default_data_version(resolved_market, end_date),
        }
        digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
        return f"feature_matrix:{digest}"

    def build_label_values_key(
        self,
        *,
        market: str | None,
        label_id: str,
        tickers: list[str],
        start_date: str | None,
        end_date: str | None,
        label_definition: dict,
        data_version: str | None = None,
    ) -> str:
        resolved_market = normalize_market(market)
        normalized_tickers = sorted({normalize_ticker(t, resolved_market) for t in tickers})
        payload = {
            "schema_version": _LABEL_VALUES_SCHEMA_VERSION,
            "object_type": "label_values",
            "market": resolved_market,
            "label_id": label_id,
            "tickers": normalized_tickers,
            "start_date": str(start_date) if start_date else None,
            "end_date": str(end_date) if end_date else None,
            "label_definition": _label_definition_fingerprint(label_definition),
            "data_version": data_version or self.default_data_version(resolved_market, end_date),
        }
        digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
        return f"label_values:{digest}"

    def load_feature_matrix(
        self,
        *,
        market: str | None,
        feature_set_id: str,
        tickers: list[str],
        start_date: str,
        end_date: str,
        factor_refs: list[dict],
        preprocessing: dict | None,
        data_version: str | None = None,
    ) -> dict[str, Any] | None:
        cache_key = self.build_feature_matrix_key(
            market=market,
            feature_set_id=feature_set_id,
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            factor_refs=factor_refs,
            preprocessing=preprocessing,
            data_version=data_version,
        )
        cached = self._load_process_feature_matrix(cache_key)
        if cached is not None:
            return cached

        record = self.get_cache_stats(cache_key)
        if record is None or record.get("status") != "active" or not record.get("uri"):
            return None
        if self._is_expired(record):
            self._record_miss(cache_key)
            return None

        path = Path(record["uri"])
        if not path.exists():
            self._record_miss(cache_key)
            return None

        try:
            frame = pd.read_parquet(path)
        except Exception as exc:
            log.warning("research_cache.feature_matrix.load_failed", cache_key=cache_key, error=str(exc))
            self._record_miss(cache_key)
            return None
        self._record_hit(cache_key)
        payload = {
            "record": self.get_cache_stats(cache_key),
            "feature_data": self._wide_frame_to_feature_data(frame),
        }
        self._store_process_feature_matrix(
            cache_key,
            payload["record"],
            payload["feature_data"],
        )
        return payload

    def load_label_values(
        self,
        *,
        market: str | None,
        label_id: str,
        tickers: list[str],
        start_date: str | None,
        end_date: str | None,
        label_definition: dict,
        data_version: str | None = None,
    ) -> dict[str, Any] | None:
        cache_key = self.build_label_values_key(
            market=market,
            label_id=label_id,
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            label_definition=label_definition,
            data_version=data_version,
        )
        record = self.get_cache_stats(cache_key)
        if record is None or record.get("status") != "active" or not record.get("uri"):
            return None
        if self._is_expired(record):
            self._record_miss(cache_key)
            return None

        path = Path(record["uri"])
        if not path.exists():
            self._record_miss(cache_key)
            return None

        try:
            frame = pd.read_parquet(path)
        except Exception as exc:
            log.warning("research_cache.label_values.load_failed", cache_key=cache_key, error=str(exc))
            self._record_miss(cache_key)
            return None
        self._record_hit(cache_key)
        return {
            "record": self.get_cache_stats(cache_key),
            "label_values": _normalize_label_values_frame(frame),
        }

    def store_feature_matrix(
        self,
        *,
        market: str | None,
        feature_set_id: str,
        tickers: list[str],
        start_date: str,
        end_date: str,
        factor_refs: list[dict],
        preprocessing: dict | None,
        feature_data: dict[str, pd.DataFrame],
        data_version: str | None = None,
        retention_class: str = "daily_hot",
        ttl_days: int = _DEFAULT_DAILY_HOT_DAYS,
    ) -> dict[str, Any]:
        resolved_market = normalize_market(market)
        normalized_tickers = sorted({normalize_ticker(t, resolved_market) for t in tickers})
        resolved_data_version = data_version or self.default_data_version(resolved_market, end_date)
        cache_key = self.build_feature_matrix_key(
            market=resolved_market,
            feature_set_id=feature_set_id,
            tickers=normalized_tickers,
            start_date=start_date,
            end_date=end_date,
            factor_refs=factor_refs,
            preprocessing=preprocessing,
            data_version=resolved_data_version,
        )
        frame = self._feature_data_to_wide_frame(feature_data)
        if frame.empty:
            raise ValueError("feature_data must not be empty")

        path = self._feature_matrix_path(resolved_market, feature_set_id, cache_key)
        byte_size = 0
        content_hash = ""
        metadata = {
            "tickers": normalized_tickers,
            "factor_refs": _canonical_jsonable(factor_refs),
            "preprocessing": _canonical_jsonable(preprocessing or {}),
            "data_version": resolved_data_version,
            "features": sorted(feature_data.keys()),
        }
        now = utc_now_naive()
        expires_at = now + timedelta(days=ttl_days) if ttl_days > 0 else None

        insert_params = [
            cache_key,
            "feature_matrix",
            resolved_market,
            feature_set_id,
            str(path),
            _FEATURE_MATRIX_SCHEMA_VERSION,
            byte_size,
            content_hash,
            int(len(frame)),
            int(len(feature_data)),
            int(len(normalized_tickers)),
            start_date,
            end_date,
            resolved_data_version,
            retention_class,
            json.dumps(metadata, default=str),
            cache_key,
            now,
            now,
            expires_at,
            cache_key,
            cache_key,
        ]
        tmp_path = _cache_temp_path(path)
        with _cache_write_lock(cache_key), _cache_writer_lease(
            cache_key=cache_key,
            market=resolved_market,
            object_type="feature_matrix",
            object_id=feature_set_id,
            uri=path,
            tmp_uri=tmp_path,
        ):
            path = self._feature_matrix_path(resolved_market, feature_set_id, cache_key)
            path.parent.mkdir(parents=True, exist_ok=True)
            byte_size, content_hash = _write_parquet_atomically(frame, path, tmp_path, index=True)
            insert_params[4] = str(path)
            insert_params[6] = byte_size
            insert_params[7] = content_hash
            self._execute_cache_metadata_write(insert_params, cache_key)
        log.info(
            "research_cache.feature_matrix.stored",
            cache_key=cache_key,
            market=resolved_market,
            feature_set_id=feature_set_id,
            rows=len(frame),
            bytes=byte_size,
        )
        return self.get_cache_stats(cache_key)

    @classmethod
    def clear_process_feature_matrix_cache(cls) -> None:
        with _PROCESS_FEATURE_MATRIX_CACHE_LOCK:
            _PROCESS_FEATURE_MATRIX_CACHE.clear()

    @classmethod
    def process_feature_matrix_cache_stats(cls) -> dict[str, Any]:
        with _PROCESS_FEATURE_MATRIX_CACHE_LOCK:
            cls._prune_process_feature_matrix_cache_locked()
            return {
                "entries": len(_PROCESS_FEATURE_MATRIX_CACHE),
                "ttl_seconds": _PROCESS_FEATURE_MATRIX_CACHE_TTL_SECONDS,
                "max_entries": _PROCESS_FEATURE_MATRIX_CACHE_MAX_ENTRIES,
                "keys": list(_PROCESS_FEATURE_MATRIX_CACHE.keys()),
            }

    @classmethod
    def _load_process_feature_matrix(cls, cache_key: str) -> dict[str, Any] | None:
        with _PROCESS_FEATURE_MATRIX_CACHE_LOCK:
            cls._prune_process_feature_matrix_cache_locked()
            cached = _PROCESS_FEATURE_MATRIX_CACHE.get(cache_key)
            if cached is None:
                return None
            stored_at, record, feature_data = cached
            if time.time() - stored_at > _PROCESS_FEATURE_MATRIX_CACHE_TTL_SECONDS:
                _PROCESS_FEATURE_MATRIX_CACHE.pop(cache_key, None)
                return None
            _PROCESS_FEATURE_MATRIX_CACHE.move_to_end(cache_key)
            return {
                "record": dict(record or {}),
                "feature_data": cls._copy_feature_data(feature_data),
            }

    @classmethod
    def _store_process_feature_matrix(
        cls,
        cache_key: str,
        record: dict[str, Any] | None,
        feature_data: dict[str, pd.DataFrame],
    ) -> None:
        if not feature_data:
            return
        with _PROCESS_FEATURE_MATRIX_CACHE_LOCK:
            cls._prune_process_feature_matrix_cache_locked()
            _PROCESS_FEATURE_MATRIX_CACHE[cache_key] = (
                time.time(),
                dict(record or {}),
                cls._copy_feature_data(feature_data),
            )
            _PROCESS_FEATURE_MATRIX_CACHE.move_to_end(cache_key)
            while len(_PROCESS_FEATURE_MATRIX_CACHE) > _PROCESS_FEATURE_MATRIX_CACHE_MAX_ENTRIES:
                _PROCESS_FEATURE_MATRIX_CACHE.popitem(last=False)

    @classmethod
    def _prune_process_feature_matrix_cache_locked(cls) -> None:
        now = time.time()
        expired = [
            key
            for key, (stored_at, _record, _feature_data) in _PROCESS_FEATURE_MATRIX_CACHE.items()
            if now - stored_at > _PROCESS_FEATURE_MATRIX_CACHE_TTL_SECONDS
        ]
        for key in expired:
            _PROCESS_FEATURE_MATRIX_CACHE.pop(key, None)

    @staticmethod
    def _copy_feature_data(feature_data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        return {str(name): frame.copy(deep=True) for name, frame in feature_data.items()}

    def store_label_values(
        self,
        *,
        market: str | None,
        label_id: str,
        tickers: list[str],
        start_date: str | None,
        end_date: str | None,
        label_definition: dict,
        label_values: pd.DataFrame,
        data_version: str | None = None,
        retention_class: str = "daily_hot",
        ttl_days: int = _DEFAULT_DAILY_HOT_DAYS,
    ) -> dict[str, Any]:
        resolved_market = normalize_market(market)
        normalized_tickers = sorted({normalize_ticker(t, resolved_market) for t in tickers})
        resolved_data_version = data_version or self.default_data_version(resolved_market, end_date)
        cache_key = self.build_label_values_key(
            market=resolved_market,
            label_id=label_id,
            tickers=normalized_tickers,
            start_date=start_date,
            end_date=end_date,
            label_definition=label_definition,
            data_version=resolved_data_version,
        )
        frame = _normalize_label_values_frame(label_values)
        if frame.empty:
            raise ValueError("label_values must not be empty")

        path = self._label_values_path(resolved_market, label_id, cache_key)
        metadata = {
            "tickers": normalized_tickers,
            "label_definition": _label_definition_fingerprint(label_definition),
            "data_version": resolved_data_version,
        }
        now = utc_now_naive()
        expires_at = now + timedelta(days=ttl_days) if ttl_days > 0 else None
        insert_params = [
            cache_key,
            "label_values",
            resolved_market,
            label_id,
            str(path),
            _LABEL_VALUES_SCHEMA_VERSION,
            0,
            "",
            int(len(frame)),
            0,
            int(len(normalized_tickers)),
            start_date,
            end_date,
            resolved_data_version,
            retention_class,
            json.dumps(metadata, default=str),
            cache_key,
            now,
            now,
            expires_at,
            cache_key,
            cache_key,
        ]
        tmp_path = _cache_temp_path(path)
        with _cache_write_lock(cache_key), _cache_writer_lease(
            cache_key=cache_key,
            market=resolved_market,
            object_type="label_values",
            object_id=label_id,
            uri=path,
            tmp_uri=tmp_path,
        ):
            path = self._label_values_path(resolved_market, label_id, cache_key)
            path.parent.mkdir(parents=True, exist_ok=True)
            byte_size, content_hash = _write_parquet_atomically(frame, path, tmp_path, index=False)
            insert_params[4] = str(path)
            insert_params[6] = byte_size
            insert_params[7] = content_hash
            self._execute_cache_metadata_write(insert_params, cache_key)
        log.info(
            "research_cache.label_values.stored",
            cache_key=cache_key,
            market=resolved_market,
            label_id=label_id,
            rows=len(frame),
            bytes=byte_size,
        )
        return self.get_cache_stats(cache_key)

    def default_data_version(self, market: str | None, as_of_date: str | None) -> str:
        resolved_market = normalize_market(market)
        if not as_of_date:
            raise ValueError(
                "Research cache keys require a stable as_of_date or explicit data_version; "
                "unversioned latest caches are not reproducible."
            )
        return f"{resolved_market}:asof:{as_of_date}"

    # ------------------------------------------------------------------
    # Inventory
    # ------------------------------------------------------------------

    def get_cache_stats(self, cache_key: str) -> dict[str, Any] | None:
        row = get_connection().execute(
            """SELECT cache_key, object_type, market, object_id, uri, format,
                      schema_version, byte_size, content_hash, row_count,
                      feature_count, ticker_count, start_date, end_date,
                      data_version, retention_class, rebuildable, status,
                      metadata, created_at, updated_at, expires_at,
                      last_accessed_at, hit_count, miss_count
               FROM research_cache_entries
               WHERE cache_key = ?""",
            [cache_key],
        ).fetchone()
        if row is None:
            return None
        return _cache_row(row)

    def list_cache_entries(
        self,
        *,
        market: str | None = None,
        object_type: str | None = None,
        status: str = "active",
        limit: int = 100,
        include_metadata: bool = False,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if market:
            clauses.append("market = ?")
            params.append(normalize_market(market))
        if object_type:
            clauses.append("object_type = ?")
            params.append(object_type)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(int(limit))
        rows = get_connection().execute(
            f"""SELECT cache_key, object_type, market, object_id, uri, format,
                       schema_version, byte_size, content_hash, row_count,
                       feature_count, ticker_count, start_date, end_date,
                       data_version, retention_class, rebuildable, status,
                       metadata, created_at, updated_at, expires_at,
                       last_accessed_at, hit_count, miss_count
                FROM research_cache_entries
                {where}
                ORDER BY COALESCE(last_accessed_at, updated_at) DESC
                LIMIT ?""",
            params,
        ).fetchall()
        entries = [_cache_row(row) for row in rows]
        if not include_metadata:
            for entry in entries:
                entry.pop("metadata", None)
        return entries

    def inventory_summary(self, *, market: str | None = None) -> dict[str, Any]:
        resolved_market = normalize_market(market) if market else None
        params: list[Any] = []
        where = ""
        if resolved_market:
            where = "WHERE market = ?"
            params.append(resolved_market)
        rows = get_connection().execute(
            f"""SELECT object_type, retention_class, COUNT(*) AS entry_count,
                       COALESCE(SUM(byte_size), 0) AS byte_size,
                       COALESCE(SUM(row_count), 0) AS row_count
                FROM research_cache_entries
                {where}
                GROUP BY object_type, retention_class
                ORDER BY byte_size DESC""",
            params,
        ).fetchall()
        return {
            "market": resolved_market,
            "items": [
                {
                    "object_type": row[0],
                    "retention_class": row[1],
                    "entry_count": int(row[2] or 0),
                    "byte_size": int(row[3] or 0),
                    "row_count": int(row[4] or 0),
                }
                for row in rows
            ],
        }

    def apply_expired_cache_cleanup(
        self,
        *,
        market: str | None = None,
        object_type: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        clauses = ["status = 'active'", "expires_at IS NOT NULL", "expires_at <= ?"]
        params: list[Any] = [utc_now_naive()]
        resolved_market = normalize_market(market) if market else None
        if resolved_market:
            clauses.append("market = ?")
            params.append(resolved_market)
        if object_type:
            clauses.append("object_type = ?")
            params.append(object_type)
        params.append(int(limit))
        rows = get_connection().execute(
            f"""SELECT cache_key, uri, byte_size
                FROM research_cache_entries
                WHERE {' AND '.join(clauses)}
                ORDER BY expires_at
                LIMIT ?""",
            params,
        ).fetchall()
        active_writer_paths = self._active_writer_paths()
        deleted_keys: list[str] = []
        deleted_bytes = 0
        for cache_key, uri, byte_size in rows:
            path = Path(uri) if uri else None
            if path is not None and str(path) in active_writer_paths:
                continue
            if path is not None and path.exists():
                try:
                    path.unlink()
                    deleted_bytes += int(byte_size or 0)
                except OSError as exc:
                    log.warning("research_cache.expired_delete_failed", cache_key=cache_key, error=str(exc))
                    continue
            get_connection().execute(
                """UPDATE research_cache_entries
                      SET status = 'deleted',
                          uri = NULL,
                          byte_size = 0,
                          updated_at = ?
                    WHERE cache_key = ?""",
                [utc_now_naive(), cache_key],
            )
            deleted_keys.append(cache_key)
        return {
            "market": resolved_market,
            "object_type": object_type,
            "deleted_entries": len(deleted_keys),
            "deleted_bytes": deleted_bytes,
            "deleted_cache_keys": deleted_keys,
        }

    def preview_orphan_file_cleanup(
        self,
        *,
        limit: int = 100,
        min_age_seconds: int = _DEFAULT_ORPHAN_FILE_MIN_AGE_SECONDS,
    ) -> dict[str, Any]:
        min_updated_at = time.time() - max(0, int(min_age_seconds))
        tracked_uris = {
            str(row[0])
            for row in get_connection().execute(
                """SELECT uri
                   FROM research_cache_entries
                   WHERE uri IS NOT NULL
                     AND status = 'active'"""
            ).fetchall()
            if row and row[0]
        }
        active_writer_paths = self._active_writer_paths()
        candidates: list[dict[str, Any]] = []
        for path in self._iter_cache_files():
            path_str = str(path)
            if path_str in tracked_uris:
                continue
            if path_str in active_writer_paths:
                continue
            try:
                stat = path.stat()
            except OSError:
                continue
            if stat.st_mtime > min_updated_at:
                continue
            candidates.append(
                {
                    "path": path_str,
                    "byte_size": int(stat.st_size),
                    "updated_at": _timestamp_to_iso(stat.st_mtime),
                    "reason": "untracked_research_cache_file",
                }
            )
            if len(candidates) >= max(1, int(limit)):
                break
        return {
            "summary": {
                "candidate_count": len(candidates),
                "candidate_bytes": sum(item["byte_size"] for item in candidates),
                "min_age_seconds": max(0, int(min_age_seconds)),
                "policy": (
                    "delete files under data/research_cache not referenced by active "
                    "research_cache_entries and older than min_age_seconds"
                ),
            },
            "candidates": candidates,
        }

    def apply_orphan_file_cleanup(
        self,
        *,
        limit: int = 100,
        min_age_seconds: int = _DEFAULT_ORPHAN_FILE_MIN_AGE_SECONDS,
    ) -> dict[str, Any]:
        preview = self.preview_orphan_file_cleanup(limit=limit, min_age_seconds=min_age_seconds)
        deleted: list[dict[str, Any]] = []
        deleted_bytes = 0
        for item in preview["candidates"]:
            path = Path(item["path"])
            try:
                path.unlink()
            except OSError as exc:
                log.warning("research_cache.orphan_delete_failed", path=str(path), error=str(exc))
                continue
            deleted.append(item)
            deleted_bytes += int(item.get("byte_size") or 0)
        return {
            "deleted_files": len(deleted),
            "deleted_bytes": deleted_bytes,
            "deleted": deleted,
            "preview": preview,
        }

    def _iter_cache_files(self) -> Iterator[Path]:
        if not self._cache_root.exists():
            return iter(())
        return (
            path
            for path in sorted(self._cache_root.rglob("*"))
            if path.is_file()
        )

    @staticmethod
    def _active_writer_paths() -> set[str]:
        try:
            rows = get_connection().execute(
                """
                SELECT metadata
                  FROM task_resource_leases
                 WHERE status = 'active'
                   AND resource_key LIKE 'research_cache:%'
                   AND expires_at > current_timestamp
                """
            ).fetchall()
        except Exception as exc:
            log.warning("research_cache.writer_lease_paths_failed", error=str(exc))
            return set()
        paths: set[str] = set()
        for row in rows:
            raw = row[0] if row else None
            if not raw:
                continue
            try:
                metadata = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:
                continue
            if not isinstance(metadata, dict):
                continue
            for key in ("uri", "tmp_uri"):
                value = metadata.get(key)
                if value:
                    paths.add(str(value))
        return paths

    # ------------------------------------------------------------------
    # Factor cache cleanup
    # ------------------------------------------------------------------

    def preview_factor_cache_cleanup(
        self,
        *,
        market: str | None = None,
        include_recent_days: int = 0,
        limit: int = 100,
    ) -> dict[str, Any]:
        resolved_market = normalize_market(market)
        recent_filter = ""
        recent_params: list[Any] = []
        if include_recent_days > 0:
            recent_filter = "AND f.updated_at < ?"
            recent_params.append(utc_now_naive() - timedelta(days=include_recent_days))
        rows = get_connection().execute(
            f"""WITH referenced AS (
                    SELECT DISTINCT json_extract_string(ref.value, '$.factor_id') AS factor_id
                    FROM feature_sets fs, json_each(fs.factor_refs) AS ref
                    WHERE fs.market = ?
                    UNION
                    SELECT DISTINCT json_extract_string(ref.value, '$.factor_id') AS factor_id
                    FROM models m
                    JOIN feature_sets fs
                      ON fs.market = m.market AND fs.id = m.feature_set_id,
                         json_each(fs.factor_refs) AS ref
                    WHERE m.market = ?
                      AND m.status IN ('trained', 'active', 'published')
                    UNION
                    SELECT DISTINCT f.id AS factor_id
                    FROM strategies s,
                         json_each(s.required_factors) AS req
                    JOIN factors f
                      ON f.market = s.market
                     AND f.name = json_extract_string(req.value, '$')
                    WHERE s.market = ?
                      AND s.status IN ('active', 'published', 'validated', 'draft')
                ),
                candidates AS (
                    SELECT c.factor_id,
                           COUNT(*) AS row_count,
                           MIN(c.date) AS min_date,
                           MAX(c.date) AS max_date,
                           f.status AS factor_status
                    FROM factor_values_cache c
                    JOIN factors f
                      ON f.market = c.market AND f.id = c.factor_id
                    LEFT JOIN referenced r
                      ON r.factor_id = c.factor_id
                    WHERE c.market = ?
                      AND COALESCE(f.status, 'draft') = 'draft'
                      AND r.factor_id IS NULL
                      {recent_filter}
                    GROUP BY c.factor_id, f.status
                )
                SELECT factor_id, row_count, min_date, max_date, factor_status
                FROM candidates
                ORDER BY row_count DESC
                LIMIT ?""",
            [resolved_market, resolved_market, resolved_market, resolved_market, *recent_params, int(limit)],
        ).fetchall()
        candidates = [
            {
                "market": resolved_market,
                "factor_id": row[0],
                "row_count": int(row[1] or 0),
                "min_date": str(row[2]) if row[2] else None,
                "max_date": str(row[3]) if row[3] else None,
                "factor_status": row[4],
                "reason": "unreferenced_draft_factor_cache",
            }
            for row in rows
        ]
        return {
            "market": resolved_market,
            "summary": {
                "candidate_count": len(candidates),
                "candidate_rows": sum(item["row_count"] for item in candidates),
                "policy": "delete unreferenced draft factor_values_cache rows",
            },
            "candidates": candidates,
        }

    def apply_factor_cache_cleanup(
        self,
        *,
        market: str | None = None,
        include_recent_days: int = 0,
        limit: int = 100,
    ) -> dict[str, Any]:
        preview = self.preview_factor_cache_cleanup(
            market=market,
            include_recent_days=include_recent_days,
            limit=limit,
        )
        factor_ids = [item["factor_id"] for item in preview["candidates"]]
        if not factor_ids:
            return {
                "market": preview["market"],
                "deleted_rows": 0,
                "deleted_factors": [],
                "preview": preview,
            }
        conn = get_connection()
        before = conn.execute(
            "SELECT COUNT(*) FROM factor_values_cache WHERE market = ? AND factor_id = ANY(?)",
            [preview["market"], factor_ids],
        ).fetchone()[0]
        conn.execute(
            "DELETE FROM factor_values_cache WHERE market = ? AND factor_id = ANY(?)",
            [preview["market"], factor_ids],
        )
        return {
            "market": preview["market"],
            "deleted_rows": int(before or 0),
            "deleted_factors": factor_ids,
            "preview": preview,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _feature_matrix_path(self, market: str, feature_set_id: str, cache_key: str) -> Path:
        digest = cache_key.split(":", 1)[1]
        return self._cache_root / "feature_matrix" / market / feature_set_id / f"{digest}.parquet"

    def _label_values_path(self, market: str, label_id: str, cache_key: str) -> Path:
        digest = cache_key.split(":", 1)[1]
        return self._cache_root / "label_values" / market / label_id / f"{digest}.parquet"

    @staticmethod
    def _is_expired(record: dict[str, Any]) -> bool:
        raw = record.get("expires_at")
        if not raw:
            return False
        try:
            return pd.Timestamp(raw).to_pydatetime().replace(tzinfo=None) <= utc_now_naive()
        except Exception:
            return False

    @staticmethod
    def _feature_data_to_wide_frame(feature_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        pieces: list[pd.DataFrame] = []
        for feature_name in sorted(feature_data):
            df = feature_data[feature_name].copy()
            df.index = pd.to_datetime(df.index)
            df.columns = pd.MultiIndex.from_product([[feature_name], [str(c) for c in df.columns]])
            pieces.append(df)
        if not pieces:
            return pd.DataFrame()
        wide = pd.concat(pieces, axis=1).sort_index()
        wide.index.name = "date"
        return wide

    @staticmethod
    def _wide_frame_to_feature_data(frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
        if not isinstance(frame.columns, pd.MultiIndex):
            raise ValueError("feature matrix cache has invalid column index")
        result: dict[str, pd.DataFrame] = {}
        for feature_name in frame.columns.get_level_values(0).unique():
            feature_frame = frame.xs(feature_name, axis=1, level=0).copy()
            feature_frame.index = pd.to_datetime(feature_frame.index)
            feature_frame.index.name = None
            result[str(feature_name)] = feature_frame
        return result

    @staticmethod
    def _record_hit(cache_key: str) -> None:
        get_connection().execute(
            """UPDATE research_cache_entries
                  SET hit_count = hit_count + 1,
                      last_accessed_at = ?,
                      updated_at = ?
                WHERE cache_key = ?""",
            [utc_now_naive(), utc_now_naive(), cache_key],
        )

    @staticmethod
    def _record_miss(cache_key: str) -> None:
        get_connection().execute(
            """UPDATE research_cache_entries
                  SET miss_count = miss_count + 1,
                      last_accessed_at = ?,
                      updated_at = ?
                WHERE cache_key = ?""",
            [utc_now_naive(), utc_now_naive(), cache_key],
        )

    @staticmethod
    def _execute_cache_metadata_write(params: list[Any], cache_key: str) -> None:
        query = """INSERT OR REPLACE INTO research_cache_entries
           (cache_key, object_type, market, object_id, uri, format,
            schema_version, byte_size, content_hash, row_count,
            feature_count, ticker_count, start_date, end_date, data_version,
            retention_class, rebuildable, status, metadata, created_at,
            updated_at, expires_at, last_accessed_at, hit_count, miss_count)
           VALUES (?, ?, ?, ?, ?, 'parquet', ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, TRUE, 'active', ?, COALESCE(
                       (SELECT created_at FROM research_cache_entries WHERE cache_key = ?),
                       ?
                   ), ?, ?, NULL,
                   COALESCE((SELECT hit_count FROM research_cache_entries WHERE cache_key = ?), 0),
                   COALESCE((SELECT miss_count FROM research_cache_entries WHERE cache_key = ?), 0))"""
        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                get_connection().execute(query, params)
                return
            except Exception as exc:
                if not _is_transient_duckdb_conflict(exc) or attempt == 2:
                    raise
                last_exc = exc
                log.warning(
                    "research_cache.feature_matrix.metadata_write_retry",
                    cache_key=cache_key,
                    attempt=attempt + 1,
                    error=str(exc),
                )
                time.sleep(0.05 * (2 ** attempt))
        if last_exc is not None:
            raise last_exc


def _canonical_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _canonical_jsonable(value[k]) for k in sorted(value)}
    if isinstance(value, list):
        return [_canonical_jsonable(item) for item in value]
    return value


def _timestamp_to_iso(timestamp: float) -> str:
    return pd.Timestamp.fromtimestamp(timestamp).isoformat()


def _cache_temp_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")


def _write_parquet_atomically(
    frame: pd.DataFrame,
    path: Path,
    tmp_path: Path,
    *,
    index: bool,
) -> tuple[int, str]:
    try:
        frame.to_parquet(tmp_path, index=index)
        byte_size = tmp_path.stat().st_size
        content_hash = _file_hash(tmp_path)
        tmp_path.replace(path)
        return byte_size, content_hash
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError as exc:
                log.warning("research_cache.tmp_delete_failed", path=str(tmp_path), error=str(exc))


def _canonical_json(value: Any) -> str:
    return json.dumps(_canonical_jsonable(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _label_definition_fingerprint(label_definition: dict) -> dict[str, Any]:
    return _canonical_jsonable(
        {
            "id": label_definition.get("id"),
            "target_type": label_definition.get("target_type"),
            "horizon": label_definition.get("horizon"),
            "effective_horizon": label_definition.get(
                "effective_horizon",
                label_definition.get("horizon"),
            ),
            "benchmark": label_definition.get("benchmark"),
            "config": label_definition.get("config") or {},
            "updated_at": label_definition.get("updated_at"),
        }
    )


def _normalize_label_values_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["ticker", "date", "label_value"])
    result = frame[["ticker", "date", "label_value"]].copy()
    result["ticker"] = result["ticker"].astype(str)
    result["date"] = pd.to_datetime(result["date"])
    result["label_value"] = pd.to_numeric(result["label_value"], errors="coerce")
    result = result.dropna(subset=["ticker", "date", "label_value"])
    return result.sort_values(["ticker", "date"]).reset_index(drop=True)


@contextmanager
def _cache_write_lock(cache_key: str) -> Iterator[None]:
    with _CACHE_WRITE_LOCKS_GUARD:
        lock = _CACHE_WRITE_LOCKS.get(cache_key)
        if lock is None:
            lock = threading.Lock()
            _CACHE_WRITE_LOCKS[cache_key] = lock
    with lock:
        yield


@contextmanager
def _cache_writer_lease(
    *,
    cache_key: str,
    market: str,
    object_type: str,
    object_id: str,
    uri: Path,
    tmp_uri: Path,
) -> Iterator[None]:
    store = TaskStore()
    lease_task_id = f"research_cache_writer:{uuid.uuid4().hex}"
    resource_key = f"research_cache:{cache_key}"
    deadline = time.monotonic() + _CACHE_WRITER_LEASE_WAIT_SECONDS
    delay = 0.1
    acquired = False
    while True:
        result = store.acquire_resource_leases(
            task_id=lease_task_id,
            task_type="research_cache_write",
            resource_keys=[resource_key],
            market=market,
            ttl_seconds=_CACHE_WRITER_LEASE_TTL_SECONDS,
            metadata={
                "cache_key": cache_key,
                "object_type": object_type,
                "object_id": object_id,
                "uri": str(uri),
                "tmp_uri": str(tmp_uri),
            },
        )
        if result.get("acquired"):
            acquired = True
            break
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            blocked = result.get("blocked") or []
            raise TimeoutError(
                f"Timed out waiting for research cache writer lease {resource_key}; "
                f"blocked_by={blocked}"
            )
        time.sleep(min(delay, remaining))
        delay = min(2.0, delay * 1.5)
    try:
        yield
    finally:
        if acquired:
            store.release_resource_leases(
                lease_task_id,
                [resource_key],
                reason="completed",
            )


def _is_transient_duckdb_conflict(exc: Exception) -> bool:
    message = str(exc)
    return any(marker in message for marker in _TRANSIENT_DUCKDB_MARKERS)


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _cache_row(row: tuple) -> dict[str, Any]:
    metadata = row[18]
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError:
            metadata = {}
    return {
        "cache_key": row[0],
        "object_type": row[1],
        "market": row[2],
        "object_id": row[3],
        "uri": row[4],
        "format": row[5],
        "schema_version": row[6],
        "byte_size": int(row[7] or 0),
        "content_hash": row[8],
        "row_count": int(row[9] or 0),
        "feature_count": int(row[10] or 0),
        "ticker_count": int(row[11] or 0),
        "start_date": str(row[12]) if row[12] else None,
        "end_date": str(row[13]) if row[13] else None,
        "data_version": row[14],
        "retention_class": row[15],
        "rebuildable": bool(row[16]),
        "status": row[17],
        "metadata": metadata or {},
        "created_at": str(row[19]) if row[19] else None,
        "updated_at": str(row[20]) if row[20] else None,
        "expires_at": str(row[21]) if row[21] else None,
        "last_accessed_at": str(row[22]) if row[22] else None,
        "hit_count": int(row[23] or 0),
        "miss_count": int(row[24] or 0),
    }

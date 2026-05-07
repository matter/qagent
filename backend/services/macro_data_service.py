"""Macro data ingestion and query service."""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

import requests

from backend.config import settings
from backend.db import get_connection
from backend.logger import get_logger

log = get_logger(__name__)


class FredClient:
    """Small FRED API client used by the macro data service."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout_seconds: int | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self._api_key = api_key or settings.external_data.fred.api_key
        self._base_url = (base_url or settings.external_data.fred.base_url).rstrip("/")
        self._timeout_seconds = (
            timeout_seconds
            if timeout_seconds is not None
            else settings.external_data.fred.request_timeout_seconds
        )
        self._session = session or requests.Session()

    def get_series_metadata(self, series_id: str) -> dict[str, Any]:
        payload = self._get("series", {"series_id": series_id})
        series = payload.get("seriess") or []
        return dict(series[0]) if series else {"id": series_id}

    def get_series_observations(
        self,
        series_id: str,
        *,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"series_id": series_id}
        if start_date:
            params["observation_start"] = _date_str(start_date)
        if end_date:
            params["observation_end"] = _date_str(end_date)
        payload = self._get("series/observations", params)
        return [dict(row) for row in payload.get("observations", [])]

    def _get(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        if not self._api_key:
            raise ValueError("FRED API key is not configured")
        request_params = {
            **params,
            "api_key": self._api_key,
            "file_type": "json",
        }
        response = self._session.get(
            f"{self._base_url}/{endpoint.lstrip('/')}",
            params=request_params,
            timeout=self._timeout_seconds,
        )
        response.raise_for_status()
        return response.json()


class MacroDataService:
    """Persist and query point-in-time macro time series."""

    def __init__(self, client: FredClient | None = None) -> None:
        self._client = client or FredClient()

    def update_fred_series(
        self,
        *,
        series_ids: list[str],
        start_date: str | date | None = None,
        end_date: str | date | None = None,
    ) -> dict[str, Any]:
        normalized_series = _normalize_series_ids(series_ids)
        if not normalized_series:
            raise ValueError("series_ids must not be empty")

        conn = get_connection()
        total_observations = 0
        failures: list[dict[str, str]] = []
        updated_series: list[str] = []

        for series_id in normalized_series:
            try:
                metadata = self._client.get_series_metadata(series_id)
                observations = self._client.get_series_observations(
                    series_id,
                    start_date=start_date,
                    end_date=end_date,
                )
                self._upsert_series_metadata(conn, series_id, metadata)
                inserted = self._upsert_observations(conn, series_id, observations)
                total_observations += inserted
                updated_series.append(series_id)
            except Exception as exc:
                log.warning(
                    "macro_data.fred_series_failed",
                    series_id=series_id,
                    error=str(exc),
                )
                failures.append({"series_id": series_id, "error": str(exc)})

        if failures and not updated_series:
            raise RuntimeError(f"Failed to update all FRED series: {failures}")

        return {
            "provider": "fred",
            "series_ids": updated_series,
            "series_count": len(updated_series),
            "observation_count": total_observations,
            "failures": failures,
            "start_date": _date_str(start_date) if start_date else None,
            "end_date": _date_str(end_date) if end_date else None,
        }

    def query_series(
        self,
        *,
        series_ids: list[str] | None = None,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
        provider: str = "fred",
        as_of: str | date | datetime | None = None,
        limit: int = 10000,
    ) -> list[dict[str, Any]]:
        normalized_provider = provider.lower()
        normalized_series = _normalize_series_ids(series_ids or [])
        query = [
            """SELECT o.provider, o.series_id, s.title, o.date, o.realtime_start,
                      o.realtime_end, o.available_at, o.value, o.source_metadata
                 FROM macro_observations o
                 LEFT JOIN macro_series s
                   ON s.provider = o.provider AND s.series_id = o.series_id
                WHERE o.provider = ?"""
        ]
        params: list[Any] = [normalized_provider]
        if normalized_series:
            placeholders = ", ".join("?" for _ in normalized_series)
            query.append(f"AND o.series_id IN ({placeholders})")
            params.extend(normalized_series)
        if start_date:
            query.append("AND o.date >= ?")
            params.append(_date_str(start_date))
        if end_date:
            query.append("AND o.date <= ?")
            params.append(_date_str(end_date))
        if as_of:
            query.append("AND o.available_at <= ?")
            params.append(_timestamp_str(as_of))
        query.append("ORDER BY o.series_id, o.date, o.realtime_start")
        query.append("LIMIT ?")
        params.append(limit)

        rows = get_connection().execute("\n".join(query), params).fetchall()
        return [
            {
                "provider": row[0],
                "series_id": row[1],
                "title": row[2],
                "date": str(row[3]),
                "realtime_start": str(row[4]),
                "realtime_end": str(row[5]),
                "available_at": _format_datetime(row[6]),
                "value": row[7],
                "source_metadata": _parse_json(row[8]),
            }
            for row in rows
        ]

    def list_series(self, *, provider: str = "fred", limit: int = 1000) -> list[dict[str, Any]]:
        rows = get_connection().execute(
            """SELECT provider, series_id, title, frequency, units,
                      seasonal_adjustment, source, source_url, metadata,
                      created_at, updated_at
                 FROM macro_series
                WHERE provider = ?
                ORDER BY series_id
                LIMIT ?""",
            [provider.lower(), limit],
        ).fetchall()
        return [
            {
                "provider": row[0],
                "series_id": row[1],
                "title": row[2],
                "frequency": row[3],
                "units": row[4],
                "seasonal_adjustment": row[5],
                "source": row[6],
                "source_url": row[7],
                "metadata": _parse_json(row[8]),
                "created_at": _format_datetime(row[9]),
                "updated_at": _format_datetime(row[10]),
            }
            for row in rows
        ]

    def _upsert_series_metadata(
        self,
        conn,
        series_id: str,
        metadata: dict[str, Any],
    ) -> None:
        conn.execute(
            """INSERT OR REPLACE INTO macro_series
               (provider, series_id, title, frequency, units,
                seasonal_adjustment, source, source_url, metadata,
                created_at, updated_at)
               VALUES (
                   'fred', ?, ?, ?, ?, ?, ?, ?, ?,
                   COALESCE(
                       (SELECT created_at FROM macro_series WHERE provider = 'fred' AND series_id = ?),
                       current_timestamp
                   ),
                   current_timestamp
               )""",
            [
                series_id,
                metadata.get("title"),
                metadata.get("frequency_short") or metadata.get("frequency"),
                metadata.get("units_short") or metadata.get("units"),
                metadata.get("seasonal_adjustment_short")
                or metadata.get("seasonal_adjustment"),
                metadata.get("source") or "FRED",
                _fred_series_url(series_id),
                json.dumps(metadata, default=str, sort_keys=True),
                series_id,
            ],
        )

    def _upsert_observations(
        self,
        conn,
        series_id: str,
        observations: list[dict[str, Any]],
    ) -> int:
        rows = []
        for observation in observations:
            obs_date = observation.get("date")
            realtime_start = observation.get("realtime_start")
            realtime_end = observation.get("realtime_end")
            if not obs_date or not realtime_start or not realtime_end:
                continue
            rows.append(
                [
                    "fred",
                    series_id,
                    obs_date,
                    realtime_start,
                    realtime_end,
                    _available_at(realtime_start),
                    _parse_value(observation.get("value")),
                    json.dumps(observation, default=str, sort_keys=True),
                ]
            )
        if not rows:
            return 0
        conn.executemany(
            """INSERT OR REPLACE INTO macro_observations
               (provider, series_id, date, realtime_start, realtime_end,
                available_at, value, source_metadata, ingested_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)""",
            rows,
        )
        return len(rows)


def _normalize_series_ids(series_ids: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in series_ids:
        series_id = str(raw or "").strip().upper()
        if not series_id or series_id in seen:
            continue
        seen.add(series_id)
        normalized.append(series_id)
    return normalized


def _date_str(value: str | date) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _timestamp_str(value: str | date | datetime) -> str:
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _available_at(realtime_start: str) -> str:
    return f"{realtime_start} 00:00:00"


def _parse_value(value: Any) -> float | None:
    if value in (None, "", "."):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_json(raw: Any) -> Any:
    if raw is None:
        return {}
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}
    return raw


def _format_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat(sep=" ")
    return str(value)


def _fred_series_url(series_id: str) -> str:
    return f"https://fred.stlouisfed.org/series/{series_id}"

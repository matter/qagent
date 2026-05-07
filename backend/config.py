"""Application configuration loaded from config.yaml."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_CONFIG_PATH = _PROJECT_ROOT / "config.yaml"
_LOCAL_CONFIG_PATH = _PROJECT_ROOT / "config.local.yaml"


@dataclass
class DataConfig:
    provider: str = "yfinance"
    db_path: str = "./data/qagent.duckdb"
    models_dir: str = "./data/models"
    factors_dir: str = "./data/factors"
    strategies_dir: str = "./data/strategies"


@dataclass
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = 8000


@dataclass
class BacktestConfig:
    default_initial_capital: int = 1_000_000
    default_commission_rate: float = 0.001
    default_slippage_rate: float = 0.001
    default_benchmark: str = "SPY"


@dataclass
class MarketConfig:
    calendar: str = "NYSE"


@dataclass
class MarketEntryConfig:
    provider: str
    calendar: str
    benchmark: str
    default_group: str


@dataclass
class FredConfig:
    api_key: str | None = None
    base_url: str = "https://api.stlouisfed.org/fred"
    request_timeout_seconds: int = 30


@dataclass
class ExternalDataConfig:
    fred: FredConfig = field(default_factory=FredConfig)


@dataclass
class Settings:
    data: DataConfig = field(default_factory=DataConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    backtest: BacktestConfig = field(default_factory=BacktestConfig)
    market: MarketConfig = field(default_factory=MarketConfig)
    markets: dict[str, MarketEntryConfig] = field(default_factory=dict)
    external_data: ExternalDataConfig = field(default_factory=ExternalDataConfig)

    # Resolved absolute paths (set after loading)
    project_root: Path = field(default=_PROJECT_ROOT)

    def resolve_path(self, relative: str) -> Path:
        """Resolve a config-relative path to an absolute path."""
        return (self.project_root / relative).resolve()

    @property
    def db_path(self) -> Path:
        return self.resolve_path(self.data.db_path)

    @property
    def models_dir(self) -> Path:
        return self.resolve_path(self.data.models_dir)

    @property
    def factors_dir(self) -> Path:
        return self.resolve_path(self.data.factors_dir)

    @property
    def strategies_dir(self) -> Path:
        return self.resolve_path(self.data.strategies_dir)


def _build_settings(raw: dict[str, Any]) -> Settings:
    data = DataConfig(**raw.get("data", {}))
    server = ServerConfig(**raw.get("server", {}))
    backtest = BacktestConfig(**raw.get("backtest", {}))
    market = MarketConfig(**raw.get("market", {}))
    markets = _build_market_configs(raw.get("markets", {}), data, backtest, market)
    external_data = _build_external_data_config(raw.get("external_data", {}))

    return Settings(
        data=data,
        server=server,
        backtest=backtest,
        market=market,
        markets=markets,
        external_data=external_data,
    )


def _build_external_data_config(raw_external_data: dict[str, Any]) -> ExternalDataConfig:
    raw_fred = {}
    if isinstance(raw_external_data, dict):
        raw_fred = raw_external_data.get("fred", {}) or {}
    fred = FredConfig(**raw_fred)
    env_api_key = os.getenv("FRED_API_KEY")
    if env_api_key:
        fred.api_key = env_api_key
    return ExternalDataConfig(fred=fred)


def _build_market_configs(
    raw_markets: dict[str, Any],
    data: DataConfig,
    backtest: BacktestConfig,
    market: MarketConfig,
) -> dict[str, MarketEntryConfig]:
    markets = {
        "US": MarketEntryConfig(
            provider=data.provider,
            calendar=market.calendar,
            benchmark=backtest.default_benchmark,
            default_group="us_all_market",
        ),
        "CN": MarketEntryConfig(
            provider="baostock",
            calendar="XSHG",
            benchmark="sh.000300",
            default_group="cn_a_core_indices_union",
        ),
    }

    for key, value in (raw_markets or {}).items():
        if not isinstance(value, dict):
            continue
        normalized = str(key).upper()
        base = markets.get(normalized)
        if base is None:
            continue
        merged = {
            "provider": base.provider,
            "calendar": base.calendar,
            "benchmark": base.benchmark,
            "default_group": base.default_group,
            **value,
        }
        markets[normalized] = MarketEntryConfig(**merged)

    return markets


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r") as f:
        raw = yaml.safe_load(f) or {}
    return raw if isinstance(raw, dict) else {}


def load_settings(config_path: Path | None = None) -> Settings:
    """Load settings from YAML. Env vars override YAML values."""
    path = config_path or Path(os.getenv("QAGENT_CONFIG", str(_CONFIG_PATH)))
    raw = _load_yaml(path)
    local_path = os.getenv("QAGENT_LOCAL_CONFIG")
    if config_path is None:
        local_overlay_path = Path(local_path) if local_path else _LOCAL_CONFIG_PATH
        raw = _deep_merge(raw, _load_yaml(local_overlay_path))
    return _build_settings(raw)


# Module-level singleton – import this everywhere.
settings = load_settings()

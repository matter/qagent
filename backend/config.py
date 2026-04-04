"""Application configuration loaded from config.yaml."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_CONFIG_PATH = _PROJECT_ROOT / "config.yaml"


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
class Settings:
    data: DataConfig = field(default_factory=DataConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    backtest: BacktestConfig = field(default_factory=BacktestConfig)
    market: MarketConfig = field(default_factory=MarketConfig)

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
    return Settings(
        data=DataConfig(**raw.get("data", {})),
        server=ServerConfig(**raw.get("server", {})),
        backtest=BacktestConfig(**raw.get("backtest", {})),
        market=MarketConfig(**raw.get("market", {})),
    )


def load_settings(config_path: Path | None = None) -> Settings:
    """Load settings from a YAML file. Falls back to defaults if missing."""
    path = config_path or Path(os.getenv("QAGENT_CONFIG", str(_CONFIG_PATH)))
    if path.exists():
        with open(path, "r") as f:
            raw = yaml.safe_load(f) or {}
    else:
        raw = {}
    return _build_settings(raw)


# Module-level singleton – import this everywhere.
settings = load_settings()

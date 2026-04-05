"""
Atlas Terminal — centralized configuration.

All external credentials and tunables live here.  Settings are loaded from
environment variables (or a .env file if python-dotenv is installed) so the
library never hard-codes secrets.

Usage
-----
>>> from atlas_core.config import settings
>>> settings.eia_api_key
'abc123...'

API-key notes
-------------
EIA         : https://www.eia.gov/opendata/register.php  (free, instant)
NOAA CDO    : https://www.ncdc.noaa.gov/cdo-web/token    (free, email)
NASA FIRMS  : https://firms.modaps.eosdis.nasa.gov/api/  (free, MAP_KEY)
OpenSky     : anonymous OK; account gives higher rate limits
Gemini      : optional – used only for tagging/explanation layer
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ------------------------------------------------------------------ #
    # Data-source credentials                                              #
    # ------------------------------------------------------------------ #
    eia_api_key: str = Field(default="", description="EIA Open Data API key")
    noaa_cdo_token: str = Field(default="", description="NOAA CDO web-service token")
    nasa_firms_map_key: str = Field(default="DEMO_KEY", description="NASA FIRMS MAP_KEY")
    opensky_username: Optional[str] = Field(default=None, description="OpenSky username (optional)")
    opensky_password: Optional[str] = Field(default=None, description="OpenSky password (optional)")

    # Optional LLM layer
    gemini_api_key: Optional[str] = Field(default=None, description="Google Gemini API key (optional)")
    gemini_model: str = Field(default="gemini-1.5-flash", description="Gemini model name")

    # ------------------------------------------------------------------ #
    # HTTP / rate-limit settings                                           #
    # ------------------------------------------------------------------ #
    http_timeout_seconds: float = Field(default=30.0, ge=1.0)
    http_max_retries: int = Field(default=3, ge=0)
    http_retry_wait_min: float = Field(default=1.0, ge=0.0)
    http_retry_wait_max: float = Field(default=10.0, ge=1.0)

    # ------------------------------------------------------------------ #
    # Storage                                                              #
    # ------------------------------------------------------------------ #
    data_dir: Path = Field(default=Path("data"), description="Root data directory")
    cache_dir: Path = Field(default=Path("data/cache"), description="HTTP response cache")
    parquet_dir: Path = Field(default=Path("data/parquet"), description="Parquet store root")
    db_path: Path = Field(default=Path("data/atlas.duckdb"), description="DuckDB database file")

    # In-memory DuckDB (useful for tests and read-only deployments)
    db_in_memory: bool = Field(default=False)

    # ------------------------------------------------------------------ #
    # Scheduler                                                            #
    # ------------------------------------------------------------------ #
    scheduler_timezone: str = Field(default="UTC")

    # ------------------------------------------------------------------ #
    # Logging                                                              #
    # ------------------------------------------------------------------ #
    log_level: str = Field(default="INFO")
    log_json: bool = Field(default=False, description="Emit structured JSON logs")

    # ------------------------------------------------------------------ #
    # Feature flags                                                        #
    # ------------------------------------------------------------------ #
    enable_llm_layer: bool = Field(
        default=False,
        description="Enable Gemini tagging/explanation (requires gemini_api_key)",
    )
    cache_ttl_seconds: int = Field(default=3600, ge=60, description="Default HTTP cache TTL")

    # ------------------------------------------------------------------ #
    # Validators                                                           #
    # ------------------------------------------------------------------ #
    @field_validator("enable_llm_layer", mode="after")
    @classmethod
    def llm_needs_key(cls, v: bool, info: object) -> bool:  # type: ignore[override]
        # Silently disable LLM if no key provided; don't raise at import time
        return v

    def ensure_dirs(self) -> None:
        """Create data directories if they don't exist."""
        for d in (self.data_dir, self.cache_dir, self.parquet_dir):
            d.mkdir(parents=True, exist_ok=True)

    @property
    def llm_available(self) -> bool:
        return bool(self.gemini_api_key) and self.enable_llm_layer


# Singleton — import this everywhere
settings = Settings()

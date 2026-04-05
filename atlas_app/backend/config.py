"""FastAPI backend configuration."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class BackendSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8000)
    reload: bool = Field(default=False)
    cors_origins: list[str] = Field(
        default=["http://localhost:5173", "http://localhost:3000"]
    )
    api_prefix: str = Field(default="/api/v1")
    log_level: str = Field(default="info")

    @property
    def log_level_lower(self) -> str:
        return self.log_level.lower()

    # Scheduler intervals (seconds)
    energy_refresh_interval: int = Field(default=3600)       # 1h
    weather_refresh_interval: int = Field(default=900)       # 15m
    firms_refresh_interval: int = Field(default=1800)        # 30m
    ais_refresh_interval: int = Field(default=600)           # 10m
    gdelt_refresh_interval: int = Field(default=1800)        # 30m


backend_settings = BackendSettings()

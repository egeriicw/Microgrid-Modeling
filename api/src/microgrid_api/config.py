"""Settings from environment variables (prefix MICROGRID_). No secrets live in the repo."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MICROGRID_", env_file=".env", extra="ignore")

    # Local-first default is a SQLite file; Compose sets a Postgres URL (postgresql+psycopg://...).
    database_url: str = "sqlite:///./microgrid_dev.db"
    environment: str = "development"
    api_prefix: str = "/api/v1"
    cors_origins: list[str] = []

"""Settings from environment variables (prefix MICROGRID_). No secrets live in the repo.

Misconfiguration fails loudly: outside development/test the database URL and JWT secret are required, and an
unprefixed ``DATABASE_URL`` without ``MICROGRID_DATABASE_URL`` is an error, not silently ignored. Error messages name
variables only; they never include values.
"""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Only these environments may fall back to a local SQLite file and run without a JWT secret.
LOCAL_ENVIRONMENTS = frozenset({"development", "test"})
DEV_SQLITE_URL = "sqlite:///./microgrid_dev.db"


class ConfigurationError(RuntimeError):
    """Raised for missing or contradictory configuration. Messages never contain secret values."""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MICROGRID_", env_file=".env", extra="ignore")

    environment: str = "development"
    api_prefix: str = "/api/v1"
    cors_origins: list[str] = []

    # MICROGRID_DATABASE_URL (postgresql+psycopg://...). SQLite default only in development/test.
    # repr=False: connection URLs can embed passwords and must never show up when settings are printed or logged.
    database_url: str | None = Field(default=None, repr=False)
    # MICROGRID_REDIS_URL: Celery broker and the /ready Redis check. May embed a password: repr=False, never logged.
    redis_url: str | None = Field(default=None, repr=False)
    # Hard / soft per-run limits for the worker (seconds). The soft limit fires first so the run is marked failed.
    task_time_limit_s: int = Field(default=3600, gt=60)  # MICROGRID_TASK_TIME_LIMIT_S
    task_soft_time_limit_s: int = Field(default=3300, gt=0)  # MICROGRID_TASK_SOFT_TIME_LIMIT_S
    jwt_secret: SecretStr | None = None  # MICROGRID_JWT_SECRET; no default, required outside development
    data_dir: Path | None = None  # MICROGRID_DATA_DIR, read-only data root
    output_dir: Path | None = None  # MICROGRID_OUTPUT_DIR, writable outputs

    @model_validator(mode="after")
    def _check(self) -> Settings:
        local = self.environment in LOCAL_ENVIRONMENTS
        if not self.database_url:
            if os.environ.get("DATABASE_URL"):
                raise ConfigurationError(
                    "DATABASE_URL is set but MICROGRID_DATABASE_URL is not; this service only reads "
                    "MICROGRID_DATABASE_URL. Rename the variable (value not shown)."
                )
            if not local:
                raise ConfigurationError(
                    f"MICROGRID_DATABASE_URL is required when MICROGRID_ENVIRONMENT is '{self.environment}'."
                )
            self.database_url = DEV_SQLITE_URL
        if not local and self.jwt_secret is None:
            raise ConfigurationError(
                f"MICROGRID_JWT_SECRET is required when MICROGRID_ENVIRONMENT is '{self.environment}'."
            )
        if self.task_soft_time_limit_s >= self.task_time_limit_s:
            raise ConfigurationError("MICROGRID_TASK_SOFT_TIME_LIMIT_S must be smaller than MICROGRID_TASK_TIME_LIMIT_S.")
        if "*" in self.cors_origins:
            raise ConfigurationError("cors_origins (MICROGRID_CORS_ORIGINS) must not contain '*'; list origins.")
        return self

    def require_database_url(self) -> str:
        assert self.database_url  # guaranteed by validation
        return self.database_url

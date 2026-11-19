"""Settings from environment variables (prefix MICROGRID_). No secrets live in the repo.

Misconfiguration fails loudly: outside development/test the database URL and JWT secret are required, and an
unprefixed ``DATABASE_URL`` without ``MICROGRID_DATABASE_URL`` is an error, not silently ignored. Error messages name
variables only; they never include values.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, SecretStr, StrictBool, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Only these environments may fall back to a local SQLite file and run without a JWT secret.
LOCAL_ENVIRONMENTS = frozenset({"development", "test"})
DEV_SQLITE_URL = "sqlite:///./microgrid_dev.db"


class ConfigurationError(RuntimeError):
    """Raised for missing or contradictory configuration. Messages never contain secret values."""


_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")
# Temperature source -> the MICROGRID_TEMPERATURE_* fields it needs; every other one must stay unset. Mirrors the
# engine's own rule (synthetic.config), which validates the built snapshot again at run creation.
TEMPERATURE_FIELDS: dict[str, tuple[str, ...]] = {
    "none": (),
    "timeseries_column": ("temperature_column", "temperature_units"),
    "weather_file": ("temperature_weather_file", "temperature_time_column", "temperature_column", "temperature_units"),
}
TEMPERATURE_OPTIONAL_FIELDS = (
    "temperature_column",
    "temperature_units",
    "temperature_weather_file",
    "temperature_time_column",
)


def is_plain_file_name(value: str) -> bool:
    """One path component: no separators, no '.'/'..', no control characters, not empty."""
    return (
        bool(value)
        and len(value) <= 255
        and value not in (".", "..")
        and "/" not in value
        and "\\" not in value
        and not _CONTROL_CHARS.search(value)
    )


def is_relative_data_path(value: str) -> bool:
    """A forward-slash path relative to the data root whose every part is a plain file name."""
    return bool(value) and len(value) <= 1024 and all(is_plain_file_name(part) for part in value.split("/"))


class ApiKey(BaseModel):
    """One configured bearer credential: who (``subject``, stored as the run owner), what (``role``), and the secret.
    The key is a SecretStr (never shown by repr or logs)."""

    model_config = {"frozen": True}

    subject: str = Field(pattern=r"^[A-Za-z0-9_.@-]{1,64}$")
    role: Literal["admin", "client", "demo"]
    key: SecretStr

    @field_validator("key")
    @classmethod
    def _long_enough(cls, v: SecretStr) -> SecretStr:
        if len(v.get_secret_value()) < 16:
            raise ValueError("key must be at least 16 characters")
        return v


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
    # MICROGRID_API_KEYS: JSON list of {"subject","role","key"}. Required outside development/test; in development
    # with none configured every protected endpoint answers 401 (there is no built-in default credential).
    api_keys: list[ApiKey] = Field(default_factory=list, repr=False)
    # Demo role: YAML file {"compositions": {name: scenario-without-data}}; capped size; fixed stored seed.
    demo_compositions_file: Path | None = None  # MICROGRID_DEMO_COMPOSITIONS_FILE
    # Server-side size caps for EVERY role (MICROGRID_MAX_ENSEMBLE_RUNS / MICROGRID_MAX_TOTAL_BUILDINGS); the demo caps
    # below are stricter. Over-cap requests are rejected with 422 before anything is stored.
    max_ensemble_runs: int = Field(default=1000, gt=0)
    max_total_buildings: int = Field(default=5000, gt=0)
    demo_max_buildings: int = Field(default=50, gt=0)
    demo_max_runs: int = Field(default=20, gt=0)
    demo_seed: int = Field(default=20260101, ge=0)
    sse_poll_interval_s: float = Field(default=1.0, gt=0)
    # Stuck-run reaper: runs older than task_time_limit_s + reaper_margin_s are marked failed every reaper_interval_s.
    reaper_margin_s: int = Field(default=300, gt=0)
    reaper_interval_s: int = Field(default=300, gt=0)
    data_dir: Path | None = None  # MICROGRID_DATA_DIR, read-only data root
    output_dir: Path | None = None  # MICROGRID_OUTPUT_DIR, writable outputs
    # MICROGRID_PERMISSION_OVERRIDES: JSON {role: {flag: bool}} passed to microgrid.community_load.permissions.can.
    # Turning filters on for clients is {"client": {"filters.use": true}}. Unknown roles/flags, non-boolean values and
    # flags a role can never have (demo + filters.use) fail at startup.
    permission_overrides: dict[str, dict[str, StrictBool]] = Field(default_factory=dict)
    # MICROGRID_MAX_REQUEST_BYTES: request bodies larger than this are refused with 413 before they are parsed.
    max_request_bytes: int = Field(default=1_048_576, gt=0)
    # Data files the server names in every run's `data` section (FR-ENG-10, FR-DATA-8, FR-ENG-3). Always written
    # into the snapshot, so a stored run says which files it read and the engine's own defaults are never relied on.
    # MICROGRID_RESSTOCK_CHARS_FILE / MICROGRID_COMSTOCK_CHARS_FILE: plain file NAMES inside <data dir>/background/.
    # The defaults are the names the owner's local copy is believed to use [verify]; checked to exist at run creation.
    resstock_chars_file: str = "DC_upgrade0.xlsx"
    comstock_chars_file: str = "DC_upgrade0_agg.xlsx"
    # Outdoor temperature source (FR-ENG-10), declared, never guessed. Where real NREL data keeps temperature is not
    # confirmed [verify]; the default "none" makes every result say explicitly that no temperature is available.
    # MICROGRID_TEMPERATURE_SOURCE: none | timeseries_column | weather_file
    # timeseries_column needs MICROGRID_TEMPERATURE_COLUMN and MICROGRID_TEMPERATURE_UNITS (C or F);
    # weather_file needs MICROGRID_TEMPERATURE_WEATHER_FILE (a path RELATIVE to the data dir), _TIME_COLUMN, _COLUMN
    # and _UNITS. A variable the chosen source does not use is an error (it would be silently ignored).
    temperature_source: str = "none"
    temperature_column: str | None = None
    temperature_units: str | None = None
    temperature_weather_file: str | None = None
    temperature_time_column: str | None = None

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
        if not local and not self.api_keys:
            raise ConfigurationError(
                f"MICROGRID_API_KEYS is required when MICROGRID_ENVIRONMENT is '{self.environment}'."
            )
        subjects = [k.subject for k in self.api_keys]
        secrets = [k.key.get_secret_value() for k in self.api_keys]
        if len(set(subjects)) != len(subjects) or len(set(secrets)) != len(secrets):
            raise ConfigurationError("MICROGRID_API_KEYS has a duplicate subject or key (values not shown).")
        if self.task_soft_time_limit_s >= self.task_time_limit_s:
            raise ConfigurationError(
                "MICROGRID_TASK_SOFT_TIME_LIMIT_S must be smaller than MICROGRID_TASK_TIME_LIMIT_S."
            )
        self._check_permission_overrides()
        self._check_data_files()
        if "*" in self.cors_origins:
            raise ConfigurationError("cors_origins (MICROGRID_CORS_ORIGINS) must not contain '*'; list origins.")
        return self

    def _check_permission_overrides(self) -> None:
        from microgrid.community_load.permissions import FLAGS, ROLES, can

        try:
            for role in ROLES:
                for flag in FLAGS:
                    can(role, flag, self.permission_overrides)
        except ValueError as exc:
            raise ConfigurationError(f"MICROGRID_PERMISSION_OVERRIDES is invalid: {exc}") from None

    def _check_data_files(self) -> None:
        """File names and the temperature declaration. Messages name the variable, never its value."""
        for field, var in (
            ("resstock_chars_file", "MICROGRID_RESSTOCK_CHARS_FILE"),
            ("comstock_chars_file", "MICROGRID_COMSTOCK_CHARS_FILE"),
        ):
            if not is_plain_file_name(getattr(self, field)):
                raise ConfigurationError(
                    f"{var} must be a plain file name inside the data directory's background folder (no folders, "
                    "'..', backslashes or control characters; value not shown)."
                )
        source = self.temperature_source
        if source not in TEMPERATURE_FIELDS:
            raise ConfigurationError(
                f"MICROGRID_TEMPERATURE_SOURCE must be one of {', '.join(TEMPERATURE_FIELDS)} (value not shown)."
            )
        required = TEMPERATURE_FIELDS[source]
        for field in TEMPERATURE_OPTIONAL_FIELDS:
            var = f"MICROGRID_{field.upper()}"
            value = getattr(self, field)
            if field in required and not value:
                raise ConfigurationError(f"{var} is required when MICROGRID_TEMPERATURE_SOURCE is '{source}'.")
            if field not in required and value is not None:
                raise ConfigurationError(
                    f"{var} is set but MICROGRID_TEMPERATURE_SOURCE is '{source}', which does not use it (it would "
                    "be silently ignored); unset it or change the source."
                )
        if self.temperature_units is not None and self.temperature_units not in ("C", "F"):
            raise ConfigurationError("MICROGRID_TEMPERATURE_UNITS must be 'C' or 'F' (value not shown).")
        for field in ("temperature_column", "temperature_time_column"):
            value = getattr(self, field)
            if value is not None and (len(value) > 200 or _CONTROL_CHARS.search(value)):
                raise ConfigurationError(
                    f"MICROGRID_{field.upper()} must be at most 200 characters without control characters."
                )
        weather = self.temperature_weather_file
        if weather is not None and not is_relative_data_path(weather):
            raise ConfigurationError(
                "MICROGRID_TEMPERATURE_WEATHER_FILE must be a path relative to MICROGRID_DATA_DIR (no leading '/', "
                "no '..', '.' or empty parts, no backslashes or control characters; value not shown)."
            )

    def require_database_url(self) -> str:
        assert self.database_url  # guaranteed by validation
        return self.database_url

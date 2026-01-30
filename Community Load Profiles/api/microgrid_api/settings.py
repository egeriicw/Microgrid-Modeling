"""Application configuration.

Settings are read from environment variables.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings.

    Attributes:
        database_url: SQLAlchemy/psycopg connection string.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://microgrid:microgrid@localhost:5432/microgrid"


settings = Settings()

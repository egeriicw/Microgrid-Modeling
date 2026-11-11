"""Run ``alembic upgrade head`` against MICROGRID_DATABASE_URL: ``python -m microgrid_api.db.migrate``.

The migration scripts ship inside the package (``microgrid_api/migrations``), so this works from any working
directory and from a pip-installed image.
"""

from __future__ import annotations

import sys
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory

from microgrid_api.config import ConfigurationError, Settings

MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"


def alembic_config(url: str | None = None) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(MIGRATIONS_DIR))
    if url:
        # configparser treats % as interpolation; escape it.
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    return cfg


def head_revision() -> str:
    head = ScriptDirectory.from_config(alembic_config()).get_current_head()
    assert head
    return head


def upgrade_to_head(url: str | None = None) -> None:
    command.upgrade(alembic_config(url), "head")


def main() -> int:
    try:
        url = Settings().require_database_url()
    except ConfigurationError as exc:
        print(f"migrate: {exc}", file=sys.stderr)
        return 2
    upgrade_to_head(url)
    print("migrate: database is at head", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Credentials embedded in connection URLs must never appear when settings are printed or logged."""

from __future__ import annotations

import pytest

from microgrid_api.config import Settings

SECRETS = {
    "MICROGRID_DATABASE_URL": ("postgresql+psycopg://microgrid:S3CRETPW@db:5432/microgrid", "S3CRETPW"),
    "MICROGRID_REDIS_URL": ("redis://:REDISPW@redis:6379/0", "REDISPW"),
    "MICROGRID_JWT_SECRET": ("JWTSECRET123", "JWTSECRET123"),
}


@pytest.fixture
def prod_settings(monkeypatch) -> Settings:
    monkeypatch.setenv("MICROGRID_ENVIRONMENT", "production")
    for name, (value, _) in SECRETS.items():
        monkeypatch.setenv(name, value)
    return Settings()


@pytest.mark.parametrize("shown", [repr, str])
@pytest.mark.parametrize("name", sorted(SECRETS))
def test_secrets_do_not_appear_in_repr_or_str(prod_settings, shown, name):
    assert SECRETS[name][1] not in shown(prod_settings)


def test_the_values_are_still_usable_by_the_application(prod_settings):
    assert prod_settings.require_database_url() == SECRETS["MICROGRID_DATABASE_URL"][0]
    assert prod_settings.redis_url == SECRETS["MICROGRID_REDIS_URL"][0]

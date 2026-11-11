"""App factory. `uvicorn microgrid_api.main:app` or `uvicorn --factory microgrid_api.main:create_app`."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import create_engine

from microgrid_api import __version__
from microgrid_api.config import Settings
from microgrid_api.routers import v1


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    prefix = settings.api_prefix

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        app.state.engine.dispose()

    app = FastAPI(
        title="Microgrid Planner API",
        version=__version__,
        openapi_url=f"{prefix}/openapi.json",
        docs_url=f"{prefix}/docs",
        redoc_url=f"{prefix}/redoc",
        lifespan=lifespan,
    )
    app.state.settings = settings
    # Engine creation is lazy (no connection until first use), so /health works with the database down.
    app.state.engine = create_engine(settings.database_url, pool_pre_ping=True)
    app.include_router(v1.router, prefix=prefix)
    return app


def __getattr__(name: str) -> FastAPI:
    # Lazy module-level `app` so importing this module never reads settings/env unless asked.
    if name == "app":
        return create_app()
    raise AttributeError(name)

"""App factory. `uvicorn microgrid_api.main:app` or `uvicorn --factory microgrid_api.main:create_app`."""

from __future__ import annotations

import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from microgrid_api import __version__
from microgrid_api.config import Settings
from microgrid_api.db.models import Run
from microgrid_api.routers import v1
from microgrid_api.services.demo import load_demo_compositions

logger = logging.getLogger("microgrid_api")


def _enqueue(run_id: uuid.UUID) -> None:
    from microgrid_api import worker  # lazy: importing the worker builds the Celery app from the environment

    worker.enqueue_run(run_id)


def _cancel_run(session: Session, run_id: uuid.UUID) -> Run:
    from microgrid_api import worker

    return worker.cancel_run(session, run_id)


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
    url = settings.require_database_url()
    app.state.engine = create_engine(url, pool_pre_ping=True)
    # Backend name only (e.g. "postgresql"); never the host, path or credentials.
    app.state.session_factory = sessionmaker(app.state.engine, expire_on_commit=False)
    app.state.demo_compositions = load_demo_compositions(settings)  # fails loudly on a bad demo file
    app.state.enqueue = _enqueue
    app.state.cancel_run = _cancel_run
    logger.info("database backend: %s (environment=%s)", make_url(url).get_backend_name(), settings.environment)
    if settings.cors_origins:  # wildcard is rejected in Settings, so credentials are safe with explicit origins
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            allow_headers=["Authorization", "Content-Type"],
        )
    app.include_router(v1.router, prefix=prefix)
    return app


def __getattr__(name: str) -> FastAPI:
    # Lazy module-level `app` so importing this module never reads settings/env unless asked.
    if name == "app":
        return create_app()
    raise AttributeError(name)

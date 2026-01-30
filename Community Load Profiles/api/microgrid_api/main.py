"""FastAPI entrypoint."""

from __future__ import annotations

from fastapi import FastAPI

app = FastAPI(title="Microgrid API", version="0.1.0")


@app.get("/health")
def health() -> dict[str, str]:
    """Health check endpoint."""

    return {"status": "ok"}

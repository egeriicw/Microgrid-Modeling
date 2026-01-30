# Microgrid API (FastAPI)

Backend service for the Microgrid-Modeling repository.

## Quickstart (dev)

From `Community Load Profiles/api`:

```bash
uv venv
uv sync --all-extras

# Start the database (from repo root or Community Load Profiles)
cd ..
docker compose up -d

# Run migrations
cd api
uv run alembic upgrade head

# Run API
uv run uvicorn microgrid_api.main:app --reload --host 0.0.0.0 --port 8000
```

Health check:

- <http://localhost:8000/health>

## Notes

- DB connection is configured via `DATABASE_URL`.
- This is PR-1 foundation: only baseline plumbing + health endpoint.

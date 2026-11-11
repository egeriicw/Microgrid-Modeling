# One Dockerfile, two targets: `api` (uvicorn) and `worker` (Celery). Build context is the repo root
# (see .dockerignore: data/ is never sent). Slim Python, multi-arch (arm64 wheels exist for numpy/pandas/pyarrow).
FROM python:3.12-slim-bookworm AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN useradd --create-home --uid 10001 app \
 && mkdir -p /outputs /data && chown app:app /outputs
WORKDIR /app

# Layer 1: the engine package (changes least often after its dependencies).
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install .
# Layer 2: the API package (depends on the engine).
COPY api ./api
RUN pip install ./api

# No secrets here: everything arrives through environment variables at run time.
USER app
ENV DATA_DIR=/data OUTPUT_DIR=/outputs

FROM base AS api
EXPOSE 8000
# Module path is the contract with the api build (story 1.8).
CMD ["uvicorn", "microgrid_api.main:app", "--host", "0.0.0.0", "--port", "8000"]

FROM base AS worker
COPY --chown=app:app deploy/worker-entrypoint.sh /app/worker-entrypoint.sh
CMD ["sh", "/app/worker-entrypoint.sh"]

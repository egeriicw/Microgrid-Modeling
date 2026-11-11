# One Dockerfile, two targets: `api` (uvicorn) and `worker` (Celery). Build context is the repo root
# (see .dockerignore: data/ is never sent). Slim Python, multi-arch (arm64 wheels exist for numpy/pandas/pyarrow).
FROM python:3.12-slim-bookworm AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN useradd --create-home --uid 10001 app \
 && mkdir -p /outputs /data && chown app:app /outputs
WORKDIR /app

# Layer 1: the engine package (changes least often after its dependencies).
# ORDER MATTERS: the api package depends on `microgrid-planner` (this root package), which is not on any index.
# `pip install ./api` resolves it only because it is already installed here. Do not reorder the two installs.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install .
# Layer 2: the API package (depends on the engine).
COPY api ./api
RUN pip install ./api

# No secrets here: everything arrives through environment variables at run time.
USER app
ENV MICROGRID_DATA_DIR=/data MICROGRID_OUTPUT_DIR=/outputs

FROM base AS api
EXPOSE 8000
# App factory: settings are read when uvicorn calls create_app, so production refuses to start with bad settings.
CMD ["uvicorn", "--factory", "microgrid_api.main:create_app", "--host", "0.0.0.0", "--port", "8000"]

FROM base AS worker
COPY --chown=app:app deploy/worker-entrypoint.sh /app/worker-entrypoint.sh
CMD ["sh", "/app/worker-entrypoint.sh"]

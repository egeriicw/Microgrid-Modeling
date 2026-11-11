# Community Energy Profiler: dev entrypoints. Docker stack docs: docs/runbooks/local-stack.md
COMPOSE := docker compose --project-directory . --env-file deploy/.env -f deploy/docker-compose.yml
LITE := docker compose --project-directory . $(if $(wildcard deploy/.env),--env-file deploy/.env) -f deploy/docker-compose.lite.yml
s ?=

.PHONY: image-check up down logs ps config-check disk disk-plan prune-help nuke smoke test lint fmt preflight lite-up lite-down

deploy/.env:
	cp deploy/.env.example deploy/.env
	@echo "Created deploy/.env from the template (development-only defaults; git-ignored)."

# The worker is a placeholder that exits until story 1.10 (Celery). `--wait` would fail on it, so `make up`
# starts everything EXCEPT the worker. Add `worker` here when story 1.10 lands.
up: deploy/.env  ## build and start the stack (not the worker until story 1.10) and wait for health
	./deploy/preflight.sh full
	$(COMPOSE) up -d --build --wait db redis web migrate api

lite-up:         ## small-disk stack: api + migrate on SQLite, one image, no db/redis/web/worker (about 1-2 GB)
	./deploy/preflight.sh lite
	$(LITE) up -d --build --wait api

lite-down:       ## stop and remove the lite containers; the lite volumes (SQLite file, outputs) are KEPT
	$(LITE) down

preflight:       ## check free disk for the full stack (read-only; never deletes anything)
	./deploy/preflight.sh full

down:            ## stop and remove containers; named volumes (database!) are KEPT
	$(COMPOSE) down

logs:            ## tail logs, optionally one service: make logs s=api
	$(COMPOSE) logs -f $(s)

ps:              ## show service state and health
	$(COMPOSE) ps

config-check: deploy/.env  ## validate the compose file without starting anything
	$(COMPOSE) config -q && echo "compose config OK"

image-check:     ## print (never run) the commands that verify arm64 manifests and image sizes
	@echo "Run these yourself on your Mac. This target only prints; it never pulls or builds."
	@echo "The database image uses a floating tag; confirm it has an arm64 manifest before relying on it:"
	@echo "  docker buildx imagetools inspect timescale/timescaledb-ha:pg17"
	@echo "  docker buildx imagetools inspect redis:7.4-alpine"
	@echo "  docker buildx imagetools inspect nginxinc/nginx-unprivileged:1.27-alpine"
	@echo "Look for 'Platform: linux/arm64' in each. After make up, check sizes and architecture:"
	@echo "  docker image ls --format 'table {{.Repository}}:{{.Tag}}\t{{.Size}}'"
	@echo "  docker image inspect --format '{{.Architecture}}' timescale/timescaledb-ha:pg17 microgrid-api:dev"

disk:            ## show Docker disk use and what to look at
	docker system df
	@echo "Per-item detail: docker system df -v   (look for microgrid-* images and microgrid_* volumes)"
	@echo "The data/ folder is a read-only bind mount, so it is NOT counted here; check it with: du -sh data"

disk-plan:       ## print (never run) the expected disk need for the full and lite stacks
	@echo "Expected disk need (see docs/runbooks/local-stack.md, 'Small disk', for sources; images measured 2026-11-11, rest ESTIMATED):"
	@echo "  full stack (make up):      about 4 to 6 GB for images, build cache and an empty database; needs 12 GB free (preflight)."
	@echo "  lite stack (make lite-up): about 1 to 2 GB (one Python image plus build cache);          needs  5 GB free (preflight)."
	@echo "  Your data/ folder (about 11 GB) is separate: mounted read-only, never copied."
	@echo "Check free space FIRST, yourself:  df -h /System/Volumes/Data"
	@echo "A full disk can freeze macOS. Keep at least 3 GB free after Docker has grown."

prune-help:      ## print (never run) safe and unsafe reclaim commands
	@echo "SAFE (keeps volumes): docker builder prune          # build cache only"
	@echo "SAFE (keeps volumes): docker image prune             # dangling images only"
	@echo "NEVER run: docker volume prune / docker system prune --volumes / docker compose down -v"
	@echo "  Those delete named volumes, which hold the Postgres database (pgdata) and run outputs."
	@echo "To delete the stack's volumes deliberately: make nuke (asks for confirmation)."

nuke:            ## DESTRUCTIVE: remove containers AND volumes (database, outputs). Asks first.
	@echo "This permanently deletes the microgrid database and run outputs volumes."
	@read -p "Type DELETE to continue: " ans; [ "$$ans" = "DELETE" ] || { echo "Aborted; nothing deleted."; exit 1; }
	$(COMPOSE) down --volumes

smoke:           ## owner-run check: start, wait healthy, curl the API, stop (volumes kept)
	./deploy/tests/test_compose_smoke.sh

test:            ## all root tests
	uv run --extra dev pytest -q

lint:            ## lint, format check, types
	uv run --extra dev ruff check src tests
	uv run --extra dev ruff format --check src tests
	uv run --extra dev mypy

fmt:             ## auto-format
	uv run --extra dev ruff format src tests

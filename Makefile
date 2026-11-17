# Community Energy Profiler: dev entrypoints. Docker stack docs: docs/runbooks/local-stack.md
COMPOSE := docker compose --project-directory . --env-file deploy/.env -f deploy/docker-compose.yml
LITE := docker compose --project-directory . $(if $(wildcard deploy/.env),--env-file deploy/.env) -f deploy/docker-compose.lite.yml
s ?=

.PHONY: image-check up down logs ps config-check disk disk-plan prune-help nuke lite-nuke smoke test lint fmt preflight lite-up lite-down

deploy/.env:
	cp deploy/.env.example deploy/.env
	@echo "Created deploy/.env from the template (development-only defaults; git-ignored)."

# Starts all six services and waits (up to 10 minutes) until every long-running one is healthy and migrate has exited 0.
up:              ## build and start the whole stack (db, redis, migrate, api, worker, web) and wait for health
	./deploy/preflight.sh full
	@test -f deploy/.env || { cp deploy/.env.example deploy/.env && echo "Created deploy/.env from the template (development-only defaults; git-ignored)."; }
	$(COMPOSE) up -d --build --wait --wait-timeout 600 db redis migrate api worker web

lite-up:         ## small-disk stack: api + migrate on SQLite, one image, no db/redis/web/worker (about 1.5 to 3 GB)
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
	@echo "  docker buildx imagetools inspect node:22-bookworm-slim   # web build stage only"
	@echo "Look for 'Platform: linux/arm64' in each. After make up, check sizes and architecture:"
	@echo "  docker image ls --format 'table {{.Repository}}:{{.Tag}}\t{{.Size}}'"
	@echo "  docker image inspect --format '{{.Architecture}}' timescale/timescaledb-ha:pg17 microgrid-api:dev microgrid-worker:dev microgrid-web:dev"

disk:            ## show Docker disk use and what to look at
	docker system df
	@echo "Per-item detail: docker system df -v   (look for microgrid-* images, microgrid_* volumes (full stack) and microgrid-lite_* volumes (lite stack))"
	@echo "The data/ folder is a read-only bind mount, so it is NOT counted here; check it with: du -sh data"

disk-plan:       ## print (never run) the expected disk need for the full and lite stacks
	@echo "Expected disk need (see docs/runbooks/local-stack.md, 'Small disk', for sources; images measured 2026-11-11, rest ESTIMATED; the 12 and 5 GB thresholds are headroom over the estimates, not measurements):"
	@echo "  full stack (make up):      about 5.5 to 9 GB of new use (images, build cache incl. the web build, empty database); needs 12 GB free (preflight)."
	@echo "  lite stack (make lite-up): about 1.5 to 3 GB of new use (one Python image plus build cache); needs  5 GB free (preflight)."
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

lite-nuke:       ## DESTRUCTIVE: remove the LITE containers AND LITE volumes only (project microgrid-lite). Asks first.
	@echo "This permanently deletes the LITE stack's SQLite database and run outputs volumes (microgrid-lite_*). The full stack's volumes are not touched."
	@read -p "Type DELETE to continue: " ans; [ "$$ans" = "DELETE" ] || { echo "Aborted; nothing deleted."; exit 1; }
	$(LITE) down --volumes

smoke:           ## owner-run check: free-disk preflight first, then start all services, wait healthy, check API, web and worker, stop (volumes kept)
	./deploy/tests/test_compose_smoke.sh

test:            ## all root tests
	uv run --extra dev pytest -q

lint:            ## lint, format check, types
	uv run --extra dev ruff check src tests
	uv run --extra dev ruff format --check src tests
	uv run --extra dev mypy

fmt:             ## auto-format
	uv run --extra dev ruff format src tests

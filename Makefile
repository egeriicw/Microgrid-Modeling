# Community Energy Profiler: dev entrypoints. Docker stack docs: docs/runbooks/local-stack.md
COMPOSE := docker compose --project-directory . --env-file deploy/.env -f deploy/docker-compose.yml
s ?=

.PHONY: image-check up down logs ps config-check disk prune-help nuke smoke test lint fmt

deploy/.env:
	cp deploy/.env.example deploy/.env
	@echo "Created deploy/.env from the template (development-only defaults; git-ignored)."

# The worker is a placeholder that exits until story 1.10 (Celery). `--wait` would fail on it, so `make up`
# starts everything EXCEPT the worker. Add `worker` here when story 1.10 lands.
up: deploy/.env  ## build and start the stack (not the worker until story 1.10) and wait for health
	$(COMPOSE) up -d --build --wait db redis web migrate api

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

# SNLG Platform — dev entrypoints. See docs/infrastructure.md §5.
COMPOSE := docker compose -f deploy/docker-compose.yml
COMPOSE_CI := $(COMPOSE) -f deploy/docker-compose.ci.yml
s ?=

.PHONY: up down logs seed smoke \
        test test-engine test-api test-web e2e \
        db-upgrade db-revision fmt lint

up:            ## start the full stack
	$(COMPOSE) up -d

down:          ## stop and remove volumes
	$(COMPOSE) down -v

logs:          ## tail one service: make logs s=api
	$(COMPOSE) logs -f $(s)

seed:          ## template config + bucket + one smoke run
	python scripts/seed.py

smoke:         ## CI-style: bring up, wait for /ready, one mock run
	$(COMPOSE_CI) up -d
	./deploy/tests/test_compose_smoke.sh

test: test-engine test-api test-web  ## all suites

test-engine:   ## engine unit + integration on fixtures
	cd snlg && pytest -q --cov=snlg --cov-report=term-missing

test-api:      ## api unit/integration (needs a Postgres + fakeredis)
	cd api && pytest -q --cov=microgrid_api --cov-report=term-missing

test-web:      ## frontend jest suite
	npm --prefix web test -- --ci

e2e:           ## Playwright happy path against the CI compose stack
	$(COMPOSE_CI) up -d
	npm --prefix web run test:e2e

db-upgrade:    ## alembic upgrade head (inside the api container)
	$(COMPOSE) run --rm api alembic upgrade head

db-revision:   ## make db-revision m="add foo"
	$(COMPOSE) run --rm api alembic revision --autogenerate -m "$(m)"

fmt:
	ruff check --fix snlg api scripts && black snlg api scripts
	npm --prefix web run fmt

lint:
	ruff check snlg api scripts && black --check snlg api scripts
	npm --prefix web run lint

# Common developer tasks. Run `make` (or `make help`) to see them.
.DEFAULT_GOAL := help

PY_SRC := idemkit tests examples
REDIS_URL ?= redis://localhost:6379
PG_URL    ?= postgresql://postgres:test@localhost:55432/postgres

.PHONY: help install format lint typecheck test test-fast test-e2e check up down clean

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install: ## Install the package with dev + e2e extras
	pip install -e ".[dev,e2e]"

format: ## Autoformat and autofix in place (like spotlessApply)
	ruff format $(PY_SRC)
	ruff check --fix $(PY_SRC)

lint: ## Verify format + lint; fails on any violation, changes nothing (like spotlessCheck)
	ruff format --check $(PY_SRC)
	ruff check $(PY_SRC)

typecheck: ## Type-check with mypy (strict)
	mypy idemkit

test-fast: ## Run the suite on in-memory only (no Docker)
	pytest

test: ## Run the full suite on real Redis + Postgres
	IDEMKIT_TEST_REDIS_URL=$(REDIS_URL) IDEMKIT_TEST_PG_URL=$(PG_URL) pytest

test-e2e: ## Run the dockerized broker end-to-end tests
	IDEMKIT_TEST_REDIS_URL=$(REDIS_URL) IDEMKIT_TEST_PG_URL=$(PG_URL) pytest -m e2e

up: ## Start the local backends (Redis, Postgres, brokers) in Docker
	docker compose -f tests/e2e/docker-compose.yml up -d

down: ## Stop the local backends
	docker compose -f tests/e2e/docker-compose.yml down

check: lint typecheck test ## The full gate CI runs; fails on any violation, changes nothing

clean: ## Remove caches and build artifacts
	rm -rf .pytest_cache .ruff_cache .mypy_cache .hypothesis dist build *.egg-info
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

VENV ?= .venv
PYTHON ?= $(VENV)/bin/python
PIP ?= $(VENV)/bin/pip
SEED_ARGS ?=

.PHONY: help setup up down ps logs seed cdc-register cdc-status cdc-delete cdc-topics check format test clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "%-12s %s\n", $$1, $$2}'

setup: ## Create a local Python virtual environment and install dev dependencies.
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"

up: ## Start local Postgres, Kafka, Debezium Connect, and MinIO services.
	docker compose up -d

down: ## Stop local services and preserve named volumes.
	docker compose down

ps: ## Show local service status.
	docker compose ps

logs: ## Follow local service logs.
	docker compose logs -f

seed: ## Seed Postgres with synthetic retail transactions.
	PYTHONPATH=src $(PYTHON) -m demand_sense.data_generation.seed $(SEED_ARGS)

cdc-register: ## Register or update the Debezium Postgres connector.
	PYTHONPATH=src $(PYTHON) -m demand_sense.ingestion.cdc register

cdc-status: ## Show Debezium connector status.
	PYTHONPATH=src $(PYTHON) -m demand_sense.ingestion.cdc status

cdc-delete: ## Delete the Debezium connector.
	PYTHONPATH=src $(PYTHON) -m demand_sense.ingestion.cdc delete

cdc-topics: ## List Kafka topics from the local broker.
	docker compose exec -T kafka /kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list

check: ## Run formatting, linting, and tests.
	$(PYTHON) -m ruff format --check .
	$(PYTHON) -m ruff check .
	$(PYTHON) -m pytest

format: ## Format Python files.
	$(PYTHON) -m ruff format .

test: ## Run the test suite.
	$(PYTHON) -m pytest

clean: ## Remove local caches and generated test artifacts.
	rm -rf .pytest_cache .ruff_cache .mypy_cache htmlcov .coverage

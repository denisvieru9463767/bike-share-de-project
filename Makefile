# =============================================================================
# NYC Bike Share Data Engineering Pipeline - Makefile
# =============================================================================
# Usage: make <target>
# Run 'make help' to see all available commands
# =============================================================================

.PHONY: help up down restart logs shell dbt-run dbt-test dbt-docs clean clean-all init

# Default target
.DEFAULT_GOAL := help

# =============================================================================
# Help
# =============================================================================
help: ## Show this help message
	@echo "NYC Bike Share DE Pipeline - Available Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# =============================================================================
# Docker / Infrastructure
# =============================================================================
init: ## Initialize the project (first-time setup)
	@echo "Creating .env file from template..."
	@cp -n .env.example .env 2>/dev/null || echo ".env already exists"
	@echo "Creating required directories..."
	@mkdir -p logs plugins
	@echo "Setting permissions..."
	@echo "AIRFLOW_UID=$$(id -u)" >> .env
	@echo "Done! Please edit .env with your credentials."

up: ## Start all services (Airflow, Postgres, Redis)
	docker-compose up -d

down: ## Stop all services
	docker-compose down

restart: ## Restart all services
	docker-compose down && docker-compose up -d

build: ## Rebuild Docker images
	docker-compose build --no-cache

logs: ## Follow logs from all services
	docker-compose logs -f

logs-airflow: ## Follow Airflow scheduler logs
	docker-compose logs -f airflow-scheduler airflow-worker

logs-worker: ## Follow Airflow worker logs
	docker-compose logs -f airflow-worker

shell: ## Open a shell in the Airflow worker container
	docker-compose exec airflow-worker bash

ps: ## Show running containers
	docker-compose ps

# =============================================================================
# dbt Commands
# =============================================================================
dbt-run: ## Run dbt models
	cd dbt && dbt run

dbt-test: ## Run dbt tests
	cd dbt && dbt test

dbt-docs: ## Generate and serve dbt documentation
	cd dbt && dbt docs generate && dbt docs serve

dbt-debug: ## Debug dbt connection
	cd dbt && dbt debug

dbt-deps: ## Install dbt dependencies
	cd dbt && dbt deps

dbt-clean: ## Clean dbt artifacts
	rm -rf dbt/target dbt/logs dbt/dbt_packages

# =============================================================================
# Superset
# =============================================================================
superset-up: ## Start Superset
	cd superset && docker-compose up -d

superset-down: ## Stop Superset
	cd superset && docker-compose down

# =============================================================================
# Cleanup
# =============================================================================
clean: ## Clean logs and temporary files
	rm -rf logs/*
	rm -rf dbt/target
	rm -rf dbt/logs
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name ".DS_Store" -delete 2>/dev/null || true

clean-all: clean ## Clean everything including Docker volumes (DESTRUCTIVE)
	docker-compose down -v
	@echo "⚠️  All Docker volumes have been removed!"

# =============================================================================
# Development
# =============================================================================
lint: ## Run linters on Python code
	@echo "Running flake8..."
	flake8 dags/ --max-line-length=120 --ignore=E501
	@echo "Running black (check mode)..."
	black --check dags/

format: ## Format Python code with black
	black dags/

test: ## Run tests
	python -m pytest tests/ -v

# =============================================================================
# Utilities
# =============================================================================
trigger-dag: ## Trigger the bike ingestion DAG manually
	docker-compose exec airflow-worker airflow dags trigger bike_ingestion_pipeline

check-dag: ## Check DAG for errors
	docker-compose exec airflow-worker airflow dags list-import-errors


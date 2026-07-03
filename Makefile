# ── Fraud Detection — task runner ─────────────────────────────────────────────
# Stack: Cloud SQL Auth Proxy + Airflow + MLflow (see infra/docker/docker-compose.yml)
COMPOSE      := docker compose --env-file .env -f infra/docker/docker-compose.yml
AIRFLOW_DIRS := airflow/logs airflow/plugins airflow/dags airflow/etl
HOST_UID     := $(shell id -u)

.DEFAULT_GOAL := help
.PHONY: help dirs up down restart logs ps fix-perms

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

# Ensure airflow bind-mount dirs exist AS THE HOST USER before compose runs, so
# Docker never auto-creates them as root (cause of the PermissionError on logs).
dirs: ## Ensure airflow bind-mount dirs exist (prevents root-owned mounts)
	@mkdir -p $(AIRFLOW_DIRS)
	@echo "✓ ensured: $(AIRFLOW_DIRS)"

up: dirs ## Start the stack (cloud-sql-proxy + airflow + mlflow)
	$(COMPOSE) up -d cloud-sql-proxy airflow-init airflow-scheduler airflow-webserver

down: ## Stop the stack
	$(COMPOSE) down

restart: dirs ## Recreate airflow + mlflow from scratch
	$(COMPOSE) up -d --force-recreate airflow-init airflow-scheduler airflow-webserver

logs: ## Tail all service logs
	$(COMPOSE) logs -f

ps: ## Show service status
	$(COMPOSE) ps

# One-time repair for dirs already owned by root (needs sudo).
fix-perms: ## Repair root-owned bind-mount dirs (needs sudo, run once)
	sudo chown -R $(HOST_UID):0 $(AIRFLOW_DIRS)
	chmod -R g+rwX $(AIRFLOW_DIRS)
	@echo "✓ fixed ownership -> $(HOST_UID):0"

.DEFAULT_GOAL := help
.PHONY: help

app_compose_file := ./containers/app/docker-compose.yml
app_compose_env_file := ./.env
local_airflow_compose_file := containers/airflow-local/docker-compose.airflow-local.yml
local_airflow_compose_env_file := containers/airflow-local/.airflow-local.env
mkdocs_compose_file := containers/mkdocs/docker-compose.mkdocs.yml
mkdocs_compose_env_file := containers/mkdocs/.mkdocs.env

# taken from https://container-solutions.com/tagging-docker-images-the-right-way/

help: ## Print this help
	@grep -E '^[0-9a-zA-Z_\-\.]+:.*?## .*$$' Makefile | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

sh: ## run a shell in the container
	@docker compose run --rm -it --entrypoint sh app

builder.build: ## build image for development using docker build
	@docker compose -f $(app_compose_file) --env-file $(app_compose_env_file) build builder --progress=plain

builder.push: ## push app image to registry
	@docker compose -f $(app_compose_file) --env-file $(app_compose_env_file) push builder

app.build: ## build image using docker build
	@docker compose -f $(app_compose_file) --env-file $(app_compose_env_file) build app --progress=plain

app.push: ## push app image to registry
	@docker compose -f $(app_compose_file) --env-file $(app_compose_env_file) push app

app_dev.build: ## build image for development using docker build
	@docker compose -f $(app_compose_file) --env-file $(app_compose_env_file) build app-dev --progress=plain

app_dev.up: ## start development container
	@docker compose -f $(app_compose_file) --env-file $(app_compose_env_file) up -d app-dev


# ---------------------------------------------------------------------------- #
#                                mkdocs commands                               #
# ---------------------------------------------------------------------------- #

mkdocs.requirements.txt: ## update requirements.txt file from pyproject.toml
	@poetry export -f requirements.txt --only mkdocs --without-hashes > poetry-mkdocs-requirements.txt
	@echo "poetry-mkdocs-requirements.txt file updated"

mkdocs.serve: ## serve the mkdocs documentation
	@docker compose -f $(mkdocs_compose_file) --env-file $(mkdocs_compose_env_file) up

mkdocs.build-image: ## build the mkdocs image
	@docker compose -f $(mkdocs_compose_file) --env-file $(mkdocs_compose_env_file) build mkdocs

mkdocs.push-image: ## push the mkdocs image with tag: latest
	@docker compose -f $(mkdocs_compose_file) --env-file $(mkdocs_compose_env_file) push mkdocs

mkdocs.build-docs: ## build the mkdocs documentation
	@docker compose -f $(mkdocs_compose_file) --env-file $(mkdocs_compose_env_file) run --rm mkdocs build

mkdocs.logs: ## show the logs of the mkdocs container
	@docker compose -f $(mkdocs_compose_file) --env-file $(mkdocs_compose_env_file) logs -ft mkdocs

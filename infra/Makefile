COMPOSE_FILE_AIRFLOW ?= airflow/docker-compose.yaml
COMPOSE_FILE_OPEN_METADATA ?= open_metadata/custom-connector/docker/docker-compose.yml

run_airflow:
	docker compose -f $(COMPOSE_FILE_AIRFLOW) up

run_open_metadata:
	docker compose -f $(COMPOSE_FILE_OPEN_METADATA) up

stop_airflow:
	docker compose -f $(COMPOSE_FILE_AIRFLOW) down

stop_open_metadata:
	docker compose -f $(COMPOSE_FILE_OPEN_METADATA) down
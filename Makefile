IMAGE_NAME=nba_extractor_orchestrator

poetry-uninstall:
	curl -sSL https://install.python-poetry.org | python3.10 - --uninstall


poetry-install-config:
	curl -sSL https://install.python-poetry.org | python3.10 - && \
		export PATH="$$HOME/.local/bin:$$PATH"	&& \
		poetry self add keyrings.google-artifactregistry-auth

create-dev-env:
	make poetry-install-config && \
	poetry env use 3.10 && poetry install --no-root

inicialize-airflow:
	sudo docker compose build --build-arg "APPLICATION_DEFAULT_CREDENTIALS_JSON=$$(cat $$HOME/.config/gcloud/application_default_credentials.json)"
	sudo docker compose up

shutdown-airflow:
	docker compose down --rmi all -v
FROM apache/airflow:2.10.1

USER root

ENV PATH="$PATH:/root/.local/bin"

COPY pyproject.toml poetry.lock /

RUN curl -sSL https://install.python-poetry.org | python3 -
RUN poetry config virtualenvs.create true && \
    poetry self add keyrings.google-artifactregistry-auth

ARG APPLICATION_DEFAULT_CREDENTIALS_JSON
ENV GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json

RUN /bin/bash -c ' \
    mkdir -p $HOME/.config/gcloud/ && \
    echo "$APPLICATION_DEFAULT_CREDENTIALS_JSON" > $GOOGLE_APPLICATION_CREDENTIALS'

RUN poetry install --no-root --only main

USER airflow
FROM apache/airflow:2.7.3-python3.10

COPY airflow/requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt && pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}"
WORKDIR /opt/airflow

RUN mkdir -p /opt/airflow/dag_generated_configs && mkdir /opt/airflow/output

# Same as https://github.com/open-metadata/OpenMetadata/blob/b93390bb8ab94a3567a15dd85546061eff051c65/ingestion/ingestion_dependency.sh
COPY airflow/ingestion_dependency.sh ingestion_dependency.sh
COPY airflow/dags dags
COPY airflow/config config

WORKDIR /opt/airflow/ingestion
COPY airflow/data data

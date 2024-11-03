FROM apache/airflow:2.7.3-python3.10

WORKDIR /opt/airflow
COPY airflow/requirements.txt requirements.txt
COPY airflow/setup.py setup.py
COPY airflow/connector connector

RUN pip install --no-cache-dir . &&\
    pip install --no-cache-dir -r requirements.txt &&\
    pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}"

RUN mkdir -p dag_generated_configs && mkdir output

# Same as https://github.com/open-metadata/OpenMetadata/blob/b93390bb8ab94a3567a15dd85546061eff051c65/ingestion/ingestion_dependency.sh
COPY airflow/ingestion_dependency.sh ingestion_dependency.sh
COPY airflow/dags dags
COPY airflow/config config

WORKDIR /opt/airflow/ingestion
COPY airflow/data data
COPY airflow/schema_microdados_censo.csv schema_microdados_censo.csv

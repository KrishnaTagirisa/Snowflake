# Airflow image with dbt isolated in its own virtualenv (Cosmos calls it by path).
FROM apache/airflow:3.1.8-python3.12

USER root
RUN mkdir -p /usr/local/airflow && chown -R airflow: /usr/local/airflow
USER airflow

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /usr/local/airflow/dbt_venv \
 && /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt

# dbt project + pre-built manifest (Cosmos LoadMode.DBT_MANIFEST = fast DAG parsing)
COPY --chown=airflow dbt /usr/local/airflow/dbt/retail_medallion
RUN cd /usr/local/airflow/dbt/retail_medallion \
 && SNOWFLAKE_ACCOUNT=build SNOWFLAKE_USER=build SNOWFLAKE_PRIVATE_KEY_PATH=/dev/null \
    /usr/local/airflow/dbt_venv/bin/dbt deps --profiles-dir . \
 && SNOWFLAKE_ACCOUNT=build SNOWFLAKE_USER=build SNOWFLAKE_PRIVATE_KEY_PATH=/dev/null \
    /usr/local/airflow/dbt_venv/bin/dbt parse --target prod --profiles-dir .

COPY --chown=airflow airflow/dags /opt/airflow/dags

ENV DBT_PROJECT_DIR=/usr/local/airflow/dbt/retail_medallion \
    DBT_EXECUTABLE=/usr/local/airflow/dbt_venv/bin/dbt \
    DBT_TARGET=prod

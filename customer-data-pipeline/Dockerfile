# ---------------------------------------------------------------------------
# Custom Airflow image that bundles the extra Python packages our DAGs and
# dbt project need (Snowflake/AWS providers, dbt-snowflake, pandas, etc.)
# Built on top of the official Apache Airflow image for reproducibility.
# ---------------------------------------------------------------------------
FROM apache/airflow:2.9.3-python3.11

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt

# Make the dbt project available on PATH-relative location inside the image
ENV DBT_PROJECT_DIR=/opt/airflow/dbt_project
ENV DBT_PROFILES_DIR=/opt/airflow/dbt_project

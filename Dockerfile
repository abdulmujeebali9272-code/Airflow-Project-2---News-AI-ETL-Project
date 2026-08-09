FROM apache/airflow:3.1.7-python3.11

# Airflow provider packages needed by the DAG (S3Hook, SnowflakeHook)
COPY requirements-airflow.txt /requirements-airflow.txt

# The project's own dependencies (feedparser, beautifulsoup4, boto3,
# snowflake-connector-python, google-genai, etc.) so ingestion/ and
# storage/ modules import cleanly inside the Airflow containers.
COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    -r /requirements-airflow.txt \
    -r /requirements.txt
ARG AIRFLOW_VERSION=2.8.0
ARG PYTHON_VERSION=3.11

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/app:$PYTHONPATH

WORKDIR /app

COPY requirements.txt ./
COPY dags/ ./dags/

RUN pip install --no-cache-dir -r requirements.txt

# Create data directory
RUN mkdir -p /app/data
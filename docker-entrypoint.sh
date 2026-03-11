#!/bin/bash

# Wait for database to be ready
if [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ] || [ "$AIRFLOW__CORE__EXECUTOR" = "CeleryExecutor" ]; then
    echo "Waiting for PostgreSQL..."
    while ! pg_isready -h postgres -p 5432 -U airflow; do
        sleep 2
    done
    echo "PostgreSQL is ready!"
fi

# Wait for Redis if using CeleryExecutor
if [ "$AIRFLOW__CORE__EXECUTOR" = "CeleryExecutor" ]; then
    echo "Waiting for Redis..."
    while ! redis-cli -h redis ping; do
        sleep 2
    done
    echo "Redis is ready!"
fi

# Generate Fernet key if not set
if [ "$AIRFLOW__CORE__FERNET_KEY" = "your-fernet-key-here" ]; then
    echo "Generating Fernet key..."
    export AIRFLOW__CORE__FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
fi

# Generate Secret Key if not set
if [ "$AIRFLOW__WEBSERVER__SECRET_KEY" = "your-secret-key-here" ]; then
    echo "Generating Secret key..."
    export AIRFLOW__WEBSERVER__SECRET_KEY=$(python -c "import secrets; print(secrets.token_hex(16))")
fi

# Execute the command passed to the container
exec "$@"

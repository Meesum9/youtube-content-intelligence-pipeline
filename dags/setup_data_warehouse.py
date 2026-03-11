"""
Setup Data Warehouse DAG
This DAG runs once to set up the data warehouse schema and tables
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Create DAG
dag = DAG(
    'setup_data_warehouse',
    default_args=default_args,
    description='Setup YouTube Data Warehouse Schema',
    schedule_interval=None,
    catchup=False,
    tags=['setup', 'data-warehouse'],
)

def create_airflow_connection(**context):
    """
    Create PostgreSQL connection if it doesn't exist
    """
    from airflow.models.connection import Connection
    from airflow import settings
    
    session = settings.Session()
    
    # Check if connection already exists
    existing_conn = session.query(Connection).filter(Connection.conn_id == 'postgres_default').first()
    
    if existing_conn is None:
        # Create new connection
        new_conn = Connection(
            conn_id='postgres_default',
            conn_type='postgres',
            host='postgres',
            port=5432,
            login='airflow',
            password='airflow',
            schema='airflow'
        )
        session.add(new_conn)
        session.commit()
        print("Created PostgreSQL connection: postgres_default")
    else:
        print("PostgreSQL connection already exists: postgres_default")
    
    session.close()

# Define tasks
create_connection_task = PythonOperator(
    task_id='create_airflow_connection',
    python_callable=create_airflow_connection,
    dag=dag,
)

create_schemas_task = PostgresOperator(
    task_id='create_schemas',
    postgres_conn_id='postgres_default',
    sql='/sql/01_create_schemas.sql',
    dag=dag,
)

create_staging_tables_task = PostgresOperator(
    task_id='create_staging_tables',
    postgres_conn_id='postgres_default',
    sql='/sql/02_create_staging_tables.sql',
    dag=dag,
)

create_core_tables_task = PostgresOperator(
    task_id='create_core_tables',
    postgres_conn_id='postgres_default',
    sql='/sql/03_create_core_tables.sql',
    dag=dag,
)

create_analytics_tables_task = PostgresOperator(
    task_id='create_analytics_tables',
    postgres_conn_id='postgres_default',
    sql='/sql/04_create_analytics_tables.sql',
    dag=dag,
)

# Task dependencies
create_connection_task >> create_schemas_task >> create_staging_tables_task >> create_core_tables_task >> create_analytics_tables_task

"""
YouTube Data Warehouse DAG
This DAG orchestrates the ETL process for YouTube API data into the data warehouse
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import json
import os
import pandas as pd
from pathlib import Path

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'youtube_data_warehouse',
    default_args=default_args,
    description='YouTube Data Warehouse ETL Pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['youtube', 'data-warehouse', 'etl'],
)

def extract_youtube_data(**context):
    """
    Extract YouTube data from JSON files in the data folder
    """
    import logging
    
    data_path = Path('/app/data')
    json_files = list(data_path.glob('yt_date_*.json'))
    
    if not json_files:
        raise ValueError("No YouTube JSON files found in data directory")
    
    logging.info(f"Found {len(json_files)} JSON files to process")
    
    # Get the most recent file
    latest_file = max(json_files, key=os.path.getctime)
    logging.info(f"Processing file: {latest_file}")
    
    # Load JSON data
    with open(latest_file, 'r') as f:
        videos_data = json.load(f)
    
    # Store file path for next task
    context['task_instance'].xcom_push(key='source_file', value=str(latest_file))
    context['task_instance'].xcom_push(key='video_count', value=len(videos_data))
    
    return videos_data

def load_staging_raw(**context):
    """
    Load raw JSON data into staging table
    """
    videos_data = context['task_instance'].xcom_pull(task_ids='extract_youtube_data', key='return_value')
    source_file = context['task_instance'].xcom_pull(task_ids='extract_youtube_data', key='source_file')
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Insert raw data into staging table
    insert_query = """
    INSERT INTO dw_staging.youtube_videos_raw (source_file, raw_data, processed_flag)
    VALUES (%s, %s, %s)
    """
    
    for video in videos_data:
        postgres_hook.run(insert_query, parameters=(source_file, json.dumps(video), False))
    
    logging.info(f"Loaded {len(videos_data)} records into dw_staging.youtube_videos_raw")

def process_staging_data(**context):
    """
    Process raw staging data into structured staging table
    """
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Process raw data into structured format
    process_query = """
    INSERT INTO dw_staging.youtube_videos_staging 
    (video_id, title, description, published_at, view_count, like_count, duration, source_file)
    SELECT 
        (raw_data->>'video_id') as video_id,
        (raw_data->>'title') as title,
        (raw_data->>'description') as description,
        (raw_data->>'published_at')::timestamp as published_at,
        (raw_data->>'view_count')::bigint as view_count,
        (raw_data->>'like_count')::bigint as like_count,
        (raw_data->>'duration') as duration,
        source_file
    FROM dw_staging.youtube_videos_raw 
    WHERE processed_flag = FALSE
    ON CONFLICT (video_id) 
    DO UPDATE SET 
        title = EXCLUDED.title,
        description = EXCLUDED.description,
        view_count = EXCLUDED.view_count,
        like_count = EXCLUDED.like_count,
        duration = EXCLUDED.duration,
        updated_at = CURRENT_TIMESTAMP;
    
    UPDATE dw_staging.youtube_videos_raw 
    SET processed_flag = TRUE 
    WHERE processed_flag = FALSE;
    """
    
    postgres_hook.run(process_query)
    
    # Get count of processed records
    count_query = "SELECT COUNT(*) FROM dw_staging.youtube_videos_staging WHERE updated_at = CURRENT_DATE"
    result = postgres_hook.get_first(count_query)
    logging.info(f"Processed {result[0]} records into staging table")

def load_core_data(**context):
    """
    Load processed data into core data warehouse tables with SCD Type 2
    """
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Load into core videos table with SCD Type 2 logic
    scd_query = """
    WITH new_data AS (
        SELECT 
            video_id,
            title,
            description,
            published_at,
            view_count,
            like_count,
            -- Convert duration PT30M4S to seconds
            CASE 
                WHEN duration ~ '^PT[0-9]+M[0-9]+S$' THEN 
                    (regexp_replace(duration, '^PT([0-9]+)M([0-9]+)S$', '\1')::int * 60) + 
                    regexp_replace(duration, '^PT[0-9]+M([0-9]+)S$', '\1')::int
                WHEN duration ~ '^PT[0-9]+S$' THEN 
                    regexp_replace(duration, '^PT([0-9]+)S$', '\1')::int
                WHEN duration ~ '^PT[0-9]+M$' THEN 
                    regexp_replace(duration, '^PT([0-9]+)M$', '\1')::int * 60
                ELSE 0
            END as duration_seconds,
            duration as duration_formatted,
            'DEFAULT_CHANNEL' as channel_id
        FROM dw_staging.youtube_videos_staging
        WHERE updated_at = CURRENT_DATE
    ),
    changes_detected AS (
        SELECT nd.video_id
        FROM new_data nd
        LEFT JOIN dw_core.youtube_videos cv ON nd.video_id = cv.video_id AND cv.is_current = TRUE
        WHERE cv.video_id IS NULL 
           OR nd.title != cv.title 
           OR nd.description != cv.description 
           OR nd.view_count != cv.view_count 
           OR nd.like_count != cv.like_count
    )
    UPDATE dw_core.youtube_videos 
    SET effective_to = CURRENT_TIMESTAMP, is_current = FALSE
    WHERE video_id IN (SELECT video_id FROM changes_detected) AND is_current = TRUE;
    
    INSERT INTO dw_core.youtube_videos 
    (video_id, title, description, published_at, view_count, like_count, 
     duration_seconds, duration_formatted, channel_id)
    SELECT * FROM new_data
    WHERE video_id IN (SELECT video_id FROM changes_detected)
    ON CONFLICT DO NOTHING;
    """
    
    postgres_hook.run(scd_query)
    
    # Load daily metrics snapshot
    metrics_query = """
    INSERT INTO dw_core.youtube_video_metrics_daily 
    (video_id, snapshot_date, view_count, like_count, engagement_rate)
    SELECT 
        video_id,
        CURRENT_DATE as snapshot_date,
        view_count,
        like_count,
        CASE 
            WHEN view_count > 0 THEN ROUND((like_count::decimal / view_count::decimal) * 100, 4)
            ELSE 0
        END as engagement_rate
    FROM dw_staging.youtube_videos_staging
    WHERE updated_at = CURRENT_DATE
    ON CONFLICT (video_id, snapshot_date) 
    DO UPDATE SET 
        view_count = EXCLUDED.view_count,
        like_count = EXCLUDED.like_count,
        engagement_rate = EXCLUDED.engagement_rate;
    """
    
    postgres_hook.run(metrics_query)
    logging.info("Loaded data into core tables")

def update_analytics_tables(**context):
    """
    Update analytics tables with aggregated metrics
    """
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Update daily channel performance
    daily_analytics_query = """
    INSERT INTO dw_analytics.channel_performance_daily 
    (date_key, total_videos, total_views, total_likes, new_videos_published,
     avg_views_per_video, avg_likes_per_video, total_engagement_rate,
     top_performing_video_id, top_performing_video_title, top_performing_video_views)
    SELECT 
        CURRENT_DATE as date_key,
        COUNT(*) as total_videos,
        COALESCE(SUM(view_count), 0) as total_views,
        COALESCE(SUM(like_count), 0) as total_likes,
        COUNT(CASE WHEN published_at::date = CURRENT_DATE THEN 1 END) as new_videos_published,
        COALESCE(AVG(view_count), 0) as avg_views_per_video,
        COALESCE(AVG(like_count), 0) as avg_likes_per_video,
        CASE 
            WHEN SUM(view_count) > 0 THEN ROUND((SUM(like_count)::decimal / SUM(view_count)::decimal) * 100, 4)
            ELSE 0
        END as total_engagement_rate,
        (SELECT video_id FROM dw_core.youtube_videos WHERE is_current = TRUE ORDER BY view_count DESC LIMIT 1) as top_performing_video_id,
        (SELECT title FROM dw_core.youtube_videos WHERE is_current = TRUE ORDER BY view_count DESC LIMIT 1) as top_performing_video_title,
        (SELECT view_count FROM dw_core.youtube_videos WHERE is_current = TRUE ORDER BY view_count DESC LIMIT 1) as top_performing_video_views
    FROM dw_core.youtube_videos 
    WHERE is_current = TRUE
    ON CONFLICT (date_key) 
    DO UPDATE SET 
        total_videos = EXCLUDED.total_videos,
        total_views = EXCLUDED.total_views,
        total_likes = EXCLUDED.total_likes,
        new_videos_published = EXCLUDED.new_videos_published,
        avg_views_per_video = EXCLUDED.avg_views_per_video,
        avg_likes_per_video = EXCLUDED.avg_likes_per_video,
        total_engagement_rate = EXCLUDED.total_engagement_rate,
        top_performing_video_id = EXCLUDED.top_performing_video_id,
        top_performing_video_title = EXCLUDED.top_performing_video_title,
        top_performing_video_views = EXCLUDED.top_performing_video_views;
    """
    
    postgres_hook.run(daily_analytics_query)
    logging.info("Updated analytics tables")

# Define tasks
extract_task = PythonOperator(
    task_id='extract_youtube_data',
    python_callable=extract_youtube_data,
    dag=dag,
)

load_staging_raw_task = PythonOperator(
    task_id='load_staging_raw',
    python_callable=load_staging_raw,
    dag=dag,
)

process_staging_task = PythonOperator(
    task_id='process_staging_data',
    python_callable=process_staging_data,
    dag=dag,
)

load_core_task = PythonOperator(
    task_id='load_core_data',
    python_callable=load_core_data,
    dag=dag,
)

update_analytics_task = PythonOperator(
    task_id='update_analytics_tables',
    python_callable=update_analytics_tables,
    dag=dag,
)

# Task dependencies
extract_task >> load_staging_raw_task >> process_staging_task >> load_core_task >> update_analytics_task

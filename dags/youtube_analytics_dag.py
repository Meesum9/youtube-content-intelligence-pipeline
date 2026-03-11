from datetime import datetime, timedelta
import json
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dotenv import load_dotenv

load_dotenv('/app/.env')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'youtube_analytics_pipeline',
    default_args=default_args,
    description='YouTube analytics and data processing pipeline',
    schedule_interval=timedelta(hours=6),
    catchup=False,
)

def load_data_to_postgres(**context):
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Find the latest data file
        data_dir = '/app/data'
        files = [f for f in os.listdir(data_dir) if f.startswith('yt_data_') and f.endswith('.json')]
        
        if not files:
            raise ValueError("No YouTube data files found")
            
        latest_file = sorted(files)[-1]
        file_path = os.path.join(data_dir, latest_file)
        
        with open(file_path, 'r', encoding='utf-8') as f:
            video_data = json.load(f)
        
        # Convert to DataFrame
        df = pd.DataFrame(video_data)
        df['extracted_at'] = datetime.now()
        df['published_at'] = pd.to_datetime(df['published_at'])
        
        # Insert into database
        for _, row in df.iterrows():
            postgres_hook.run("""
                INSERT INTO youtube_videos 
                (video_id, title, description, published_at, view_count, like_count, duration, extracted_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (video_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    view_count = EXCLUDED.view_count,
                    like_count = EXCLUDED.like_count,
                    extracted_at = EXCLUDED.extracted_at
            """, parameters=(
                row['video_id'], row['title'], row['description'], 
                row['published_at'], row['view_count'], row['like_count'], 
                row['duration'], row['extracted_at']
            ))
        
        print(f"Loaded {len(video_data)} videos to PostgreSQL")
        return len(video_data)
        
    except Exception as e:
        raise Exception(f"Failed to load data to PostgreSQL: {e}")

def generate_analytics(**context):
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Generate analytics queries
        analytics_queries = {
            'top_videos_by_views': """
                SELECT video_id, title, view_count, like_count, 
                       view_count::float / NULLIF(like_count, 0) as views_per_like
                FROM youtube_videos 
                WHERE published_at >= NOW() - INTERVAL '30 days'
                ORDER BY view_count DESC 
                LIMIT 10
            """,
            'daily_upload_stats': """
                SELECT DATE(published_at) as upload_date, 
                       COUNT(*) as video_count,
                       AVG(view_count) as avg_views,
                       AVG(like_count) as avg_likes
                FROM youtube_videos 
                WHERE published_at >= NOW() - INTERVAL '30 days'
                GROUP BY DATE(published_at)
                ORDER BY upload_date DESC
            """,
            'engagement_metrics': """
                SELECT 
                    AVG(view_count::float / NULLIF(like_count, 0)) as avg_views_per_like,
                    SUM(view_count) as total_views,
                    SUM(like_count) as total_likes,
                    COUNT(*) as total_videos
                FROM youtube_videos 
                WHERE published_at >= NOW() - INTERVAL '7 days'
            """
        }
        
        results = {}
        for name, query in analytics_queries.items():
            results[name] = postgres_hook.get_records(query)
        
        # Save analytics results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        analytics_file = f"/app/data/analytics_{timestamp}.json"
        
        with open(analytics_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=4, default=str)
        
        print(f"Analytics saved to {analytics_file}")
        return analytics_file
        
    except Exception as e:
        raise Exception(f"Failed to generate analytics: {e}")

# Create table task
create_table = PostgresOperator(
    task_id='create_youtube_table',
    postgres_conn_id='postgres_default',
    sql="""
        CREATE TABLE IF NOT EXISTS youtube_videos (
            video_id VARCHAR(50) PRIMARY KEY,
            title TEXT,
            description TEXT,
            published_at TIMESTAMP,
            view_count BIGINT,
            like_count BIGINT,
            duration VARCHAR(20),
            extracted_at TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_published_at ON youtube_videos(published_at);
        CREATE INDEX IF NOT EXISTS idx_view_count ON youtube_videos(view_count);
    """,
    dag=dag,
)

# Define tasks
load_data_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    dag=dag,
)

generate_analytics_task = PythonOperator(
    task_id='generate_analytics',
    python_callable=generate_analytics,
    dag=dag,
)

# Set task dependencies
create_table >> load_data_task >> generate_analytics_task

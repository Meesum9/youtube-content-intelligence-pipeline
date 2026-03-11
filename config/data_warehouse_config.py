"""
Data Warehouse Configuration
"""

# Database configuration
DATABASE_CONFIG = {
    'conn_id': 'postgres_default',
    'schema_staging': 'dw_staging',
    'schema_core': 'dw_core',
    'schema_analytics': 'dw_analytics'
}

# Data retention policies (in days)
DATA_RETENTION = {
    'staging_raw_data': 30,      # Keep raw staging data for 30 days
    'staging_processed': 90,     # Keep processed staging data for 90 days
    'core_metrics_daily': 365,   # Keep daily metrics for 1 year
    'core_performance': 1095,     # Keep performance data for 3 years
    'analytics_daily': 730,       # Keep daily analytics for 2 years
    'analytics_weekly': 1825     # Keep weekly analytics for 5 years
}

# Data quality thresholds
DATA_QUALITY_THRESHOLDS = {
    'min_views_per_video': 100,      # Minimum expected views
    'max_views_per_video': 10_000_000_000,  # Maximum reasonable views
    'min_engagement_rate': 0.0,      # Minimum engagement rate
    'max_engagement_rate': 50.0,     # Maximum reasonable engagement rate
    'min_title_length': 1,           # Minimum title length
    'max_title_length': 200,          # Maximum title length
}

# Performance metrics
PERFORMANCE_CONFIG = {
    'top_videos_count': 10,           # Number of top videos to track
    'ranking_refresh_interval': 'daily',  # How often to update rankings
    'aggregation_periods': ['daily', 'weekly', 'monthly'],
    'trend_analysis_days': 30,        # Days to consider for trend analysis
}

# Alerting configuration
ALERTING_CONFIG = {
    'enable_alerts': True,
    'alert_channels': ['email'],  # Can add 'slack' later
    'alert_thresholds': {
        'data_load_failure_rate': 5,      # Percentage
        'missing_data_threshold': 10,      # Percentage
        'performance_drop_threshold': 20,  # Percentage
    }
}

# YouTube API configuration
YOUTUBE_CONFIG = {
    'api_key': '{{ var.value.youtube_api_key }}',
    'channel_handle': '{{ var.value.channel_handle }}',
    'max_results_per_request': 50,
    'rate_limit_per_minute': 100,
}

# File paths
FILE_PATHS = {
    'data_dir': '/app/data',
    'sql_dir': '/sql',
    'dags_dir': '/opt/airflow/dags',
    'logs_dir': '/opt/airflow/logs',
}

# DAG configuration
DAG_CONFIG = {
    'default_args': {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': '2026-03-11',
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': 300,  # 5 minutes
    },
    'schedule_interval': '@daily',
    'catchup': False,
    'max_active_runs': 1,
}

# Table schemas reference
TABLE_SCHEMAS = {
    'staging': {
        'youtube_videos_raw': [
            'id', 'load_timestamp', 'source_file', 'raw_data', 
            'processed_flag', 'created_at'
        ],
        'youtube_videos_staging': [
            'video_id', 'title', 'description', 'published_at',
            'view_count', 'like_count', 'duration', 'load_timestamp',
            'source_file', 'created_at', 'updated_at'
        ]
    },
    'core': {
        'youtube_videos': [
            'surrogate_key', 'video_id', 'title', 'description',
            'published_at', 'view_count', 'like_count', 'duration_seconds',
            'duration_formatted', 'channel_id', 'effective_from',
            'effective_to', 'is_current', 'load_timestamp', 'updated_at'
        ],
        'youtube_video_metrics_daily': [
            'id', 'video_id', 'snapshot_date', 'view_count',
            'like_count', 'view_count_change', 'like_count_change',
            'engagement_rate', 'load_timestamp', 'created_at'
        ]
    },
    'analytics': {
        'channel_performance_daily': [
            'date_key', 'total_videos', 'total_views', 'total_likes',
            'new_videos_published', 'avg_views_per_video', 'avg_likes_per_video',
            'total_engagement_rate', 'top_performing_video_id',
            'top_performing_video_title', 'top_performing_video_views',
            'load_timestamp'
        ]
    }
}

# SQL queries for common operations
SQL_QUERIES = {
    'get_latest_video_data': """
        SELECT * FROM dw_core.youtube_videos 
        WHERE is_current = TRUE 
        ORDER BY published_at DESC
    """,
    
    'get_daily_metrics': """
        SELECT * FROM dw_core.youtube_video_metrics_daily 
        WHERE snapshot_date >= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY snapshot_date DESC, video_id
    """,
    
    'get_channel_performance': """
        SELECT * FROM dw_analytics.channel_performance_daily 
        WHERE date_key >= CURRENT_DATE - INTERVAL '90 days'
        ORDER BY date_key DESC
    """,
    
    'cleanup_old_staging_data': f"""
        DELETE FROM dw_staging.youtube_videos_raw 
        WHERE load_timestamp < CURRENT_DATE - INTERVAL '{DATA_RETENTION['staging_raw_data']} days';
        
        DELETE FROM dw_staging.youtube_videos_staging 
        WHERE updated_at < CURRENT_DATE - INTERVAL '{DATA_RETENTION['staging_processed']} days';
    """,
}

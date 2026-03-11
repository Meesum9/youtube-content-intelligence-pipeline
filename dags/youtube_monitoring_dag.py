from datetime import datetime, timedelta
import json
import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from dotenv import load_dotenv

load_dotenv('/app/.env')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'youtube_monitoring_alerts',
    default_args=default_args,
    description='YouTube API monitoring and alerting pipeline',
    schedule_interval=timedelta(minutes=30),
    catchup=False,
)

API_KEY = os.getenv("API_KEY")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "admin@example.com")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

def check_api_health(**context):
    try:
        # Test API connectivity
        url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet&id=dQw4w9WgXcQ&key={API_KEY}"
        response = requests.get(url, timeout=10)
        
        health_status = {
            'timestamp': datetime.now().isoformat(),
            'api_status': 'healthy' if response.status_code == 200 else 'unhealthy',
            'response_time_ms': response.elapsed.total_seconds() * 1000,
            'status_code': response.status_code
        }
        
        if response.status_code != 200:
            health_status['error'] = response.text
            raise Exception(f"API returned status code: {response.status_code}")
        
        # Save health status
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        health_file = f"/app/data/api_health_{timestamp}.json"
        
        os.makedirs('/app/data', exist_ok=True)
        with open(health_file, 'w', encoding='utf-8') as f:
            json.dump(health_status, f, indent=4)
        
        print(f"API Health Check: {health_status['api_status']}")
        print(f"Response Time: {health_status['response_time_ms']:.2f}ms")
        
        return health_status
        
    except Exception as e:
        health_status = {
            'timestamp': datetime.now().isoformat(),
            'api_status': 'unhealthy',
            'error': str(e)
        }
        raise Exception(f"API Health Check Failed: {e}")

def check_data_quality(**context):
    try:
        # Check latest data files for quality issues
        data_dir = '/app/data'
        files = [f for f in os.listdir(data_dir) if f.startswith('yt_data_') and f.endswith('.json')]
        
        if not files:
            raise ValueError("No YouTube data files found")
        
        latest_file = sorted(files)[-1]
        file_path = os.path.join(data_dir, latest_file)
        
        with open(file_path, 'r', encoding='utf-8') as f:
            video_data = json.load(f)
        
        quality_report = {
            'timestamp': datetime.now().isoformat(),
            'file_name': latest_file,
            'total_videos': len(video_data),
            'missing_titles': 0,
            'missing_descriptions': 0,
            'zero_views': 0,
            'invalid_dates': 0,
            'quality_score': 0
        }
        
        for video in video_data:
            if not video.get('title') or video['title'].strip() == '':
                quality_report['missing_titles'] += 1
            
            if not video.get('description') or video['description'].strip() == '':
                quality_report['missing_descriptions'] += 1
            
            if video.get('view_count', 0) == 0:
                quality_report['zero_views'] += 1
            
            # Check if published_at is a valid date
            try:
                if video.get('published_at'):
                    datetime.fromisoformat(video['published_at'].replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                quality_report['invalid_dates'] += 1
        
        # Calculate quality score (0-100)
        total_checks = len(video_data) * 4  # 4 checks per video
        failed_checks = (quality_report['missing_titles'] + 
                        quality_report['missing_descriptions'] + 
                        quality_report['zero_views'] + 
                        quality_report['invalid_dates'])
        
        quality_report['quality_score'] = max(0, 100 - (failed_checks / total_checks * 100))
        
        # Save quality report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        quality_file = f"/app/data/quality_report_{timestamp}.json"
        
        with open(quality_file, 'w', encoding='utf-8') as f:
            json.dump(quality_report, f, indent=4)
        
        print(f"Data Quality Score: {quality_report['quality_score']:.2f}%")
        
        if quality_report['quality_score'] < 80:
            raise Exception(f"Data quality below threshold: {quality_report['quality_score']:.2f}%")
        
        return quality_report
        
    except Exception as e:
        raise Exception(f"Data Quality Check Failed: {e}")

def check_pipeline_performance(**context):
    try:
        # Monitor DAG execution times and success rates
        performance_report = {
            'timestamp': datetime.now().isoformat(),
            'dag_performance': {},
            'alerts': []
        }
        
        # Check if DAGs are running within acceptable timeframes
        # This would typically query Airflow's metadata database
        # For now, we'll simulate performance checks
        
        performance_report['dag_performance'] = {
            'youtube_data_pipeline': {
                'avg_runtime_minutes': 15,
                'success_rate_24h': 100,
                'last_run_status': 'success'
            },
            'youtube_analytics_pipeline': {
                'avg_runtime_minutes': 8,
                'success_rate_24h': 100,
                'last_run_status': 'success'
            },
            'youtube_trending_tracker': {
                'avg_runtime_minutes': 5,
                'success_rate_24h': 95,
                'last_run_status': 'success'
            }
        }
        
        # Generate alerts for performance issues
        for dag_name, metrics in performance_report['dag_performance'].items():
            if metrics['avg_runtime_minutes'] > 30:
                performance_report['alerts'].append({
                    'dag': dag_name,
                    'type': 'performance',
                    'message': f"DAG {dag_name} average runtime exceeds 30 minutes"
                })
            
            if metrics['success_rate_24h'] < 90:
                performance_report['alerts'].append({
                    'dag': dag_name,
                    'type': 'reliability',
                    'message': f"DAG {dag_name} success rate below 90% in last 24 hours"
                })
        
        # Save performance report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        perf_file = f"/app/data/performance_report_{timestamp}.json"
        
        with open(perf_file, 'w', encoding='utf-8') as f:
            json.dump(performance_report, f, indent=4)
        
        print(f"Performance report generated with {len(performance_report['alerts'])} alerts")
        
        return performance_report
        
    except Exception as e:
        raise Exception(f"Performance Check Failed: {e}")

# Define tasks
api_health_task = PythonOperator(
    task_id='check_api_health',
    python_callable=check_api_health,
    dag=dag,
)

data_quality_task = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag,
)

performance_task = PythonOperator(
    task_id='check_pipeline_performance',
    python_callable=check_pipeline_performance,
    dag=dag,
)

# Email alert task (conditional)
send_alert_email = EmailOperator(
    task_id='send_alert_email',
    to=ALERT_EMAIL,
    subject='YouTube Pipeline Alert - {{ ds }}',
    html_content="""
    <h3>YouTube Pipeline Alert</h3>
    <p>One or more checks have failed. Please review the latest reports in the data directory.</p>
    <p>Timestamp: {{ ds }}</p>
    """,
    dag=dag,
    trigger_rule='one_failed',
)

# Slack alert task (if webhook URL is provided)
if SLACK_WEBHOOK_URL:
    send_slack_alert = SlackAPIPostOperator(
        task_id='send_slack_alert',
        slack_webhook_conn_id='slack_webhook_default',
        text="🚨 YouTube Pipeline Alert: One or more monitoring checks have failed",
        dag=dag,
        trigger_rule='one_failed',
    )

# Set task dependencies
api_health_task >> data_quality_task >> performance_task

# Add alert tasks on failure
if SLACK_WEBHOOK_URL:
    [api_health_task, data_quality_task, performance_task] >> send_alert_email >> send_slack_alert
else:
    [api_health_task, data_quality_task, performance_task] >> send_alert_email

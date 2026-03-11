from datetime import datetime, timedelta
import json
import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
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
    'youtube_trending_tracker',
    default_args=default_args,
    description='YouTube trending videos tracking pipeline',
    schedule_interval=timedelta(hours=2),
    catchup=False,
)

API_KEY = os.getenv("API_KEY")
REGION_CODE = os.getenv("REGION_CODE", "US")

def get_trending_videos(**context):
    try:
        url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics,contentDetails&chart=mostPopular&regionCode={REGION_CODE}&maxResults=50&key={API_KEY}"
        
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        trending_videos = []
        for item in data['items']:
            video_info = {
                'video_id': item['id'],
                'title': item['snippet']['title'],
                'channel_title': item['snippet']['channelTitle'],
                'published_at': item['snippet']['publishedAt'],
                'view_count': int(item['statistics'].get('viewCount', 0)),
                'like_count': int(item['statistics'].get('likeCount', 0)),
                'comment_count': int(item['statistics'].get('commentCount', 0)),
                'duration': item['contentDetails']['duration'],
                'category_id': item['snippet']['categoryId'],
                'tags': item['snippet'].get('tags', []),
                'trending_at': datetime.now().isoformat()
            }
            trending_videos.append(video_info)
        
        # Save trending data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"/app/data/trending_{timestamp}.json"
        
        os.makedirs('/app/data', exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(trending_videos, f, indent=4, ensure_ascii=False)
        
        print(f"Trending data saved to {filename}")
        print(f"Retrieved {len(trending_videos)} trending videos")
        
        return filename
        
    except requests.exceptions.RequestException as e:
        raise Exception(f"API request failed: {e}")
    except KeyError as e:
        raise Exception(f"Missing expected data in API response: {e}")

def analyze_trending_patterns(**context):
    try:
        task_instance = context['task_instance']
        trending_file = task_instance.xcom_pull(task_ids='get_trending_videos')
        
        if not trending_file:
            raise ValueError("No trending file found from previous task")
        
        with open(trending_file, 'r', encoding='utf-8') as f:
            trending_data = json.load(f)
        
        # Analyze patterns
        analysis = {
            'total_videos': len(trending_data),
            'avg_views': sum(video['view_count'] for video in trending_data) // len(trending_data),
            'avg_likes': sum(video['like_count'] for video in trending_data) // len(trending_data),
            'avg_comments': sum(video['comment_count'] for video in trending_data) // len(trending_data),
            'top_channels': {},
            'categories': {},
            'duration_stats': {}
        }
        
        # Channel analysis
        for video in trending_data:
            channel = video['channel_title']
            if channel not in analysis['top_channels']:
                analysis['top_channels'][channel] = 0
            analysis['top_channels'][channel] += 1
        
        # Category analysis
        for video in trending_data:
            category = video['category_id']
            if category not in analysis['categories']:
                analysis['categories'][category] = 0
            analysis['categories'][category] += 1
        
        # Sort and limit results
        analysis['top_channels'] = dict(sorted(analysis['top_channels'].items(), 
                                             key=lambda x: x[1], reverse=True)[:10])
        analysis['categories'] = dict(sorted(analysis['categories'].items(), 
                                           key=lambda x: x[1], reverse=True))
        
        # Save analysis
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        analysis_file = f"/app/data/trending_analysis_{timestamp}.json"
        
        with open(analysis_file, 'w', encoding='utf-8') as f:
            json.dump(analysis, f, indent=4, ensure_ascii=False)
        
        print(f"Trending analysis saved to {analysis_file}")
        print(f"Top channels: {list(analysis['top_channels'].keys())[:3]}")
        
        return analysis_file
        
    except Exception as e:
        raise Exception(f"Failed to analyze trending patterns: {e}")

def cleanup_old_files(**context):
    try:
        data_dir = '/app/data'
        current_time = datetime.now()
        
        # Remove files older than 7 days
        for filename in os.listdir(data_dir):
            if filename.startswith(('trending_', 'trending_analysis_')):
                file_path = os.path.join(data_dir, filename)
                file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                
                if (current_time - file_time).days > 7:
                    os.remove(file_path)
                    print(f"Removed old file: {filename}")
        
        return "Cleanup completed"
        
    except Exception as e:
        raise Exception(f"Failed to cleanup old files: {e}")

# Define tasks
get_trending_task = PythonOperator(
    task_id='get_trending_videos',
    python_callable=get_trending_videos,
    dag=dag,
)

analyze_trending_task = PythonOperator(
    task_id='analyze_trending_patterns',
    python_callable=analyze_trending_patterns,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_old_files',
    python_callable=cleanup_old_files,
    dag=dag,
)

# Set task dependencies
get_trending_task >> analyze_trending_task >> cleanup_task

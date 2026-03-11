from datetime import datetime, timedelta
import json
import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

# Load environment variables
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
    'youtube_data_pipeline',
    default_args=default_args,
    description='YouTube channel data extraction pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = os.getenv("CHANNEL_HANDLE", "MrBeast")

def get_playlist_id(**context):
    try:
        url = f"https://www.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if 'items' not in data or not data['items']:
            raise ValueError(f"No channel found for handle: {CHANNEL_HANDLE}")
            
        channel_items = data['items'][0]
        channel_playlist_id = channel_items['contentDetails']['relatedPlaylists']['uploads']
        
        # Push playlist_id to XCom for next task
        task_instance = context['task_instance']
        task_instance.xcom_push(key='playlist_id', value=channel_playlist_id)
        
        print(f"Playlist ID: {channel_playlist_id}")
        return channel_playlist_id

    except requests.exceptions.RequestException as e:
        raise Exception(f"API request failed: {e}")
    except KeyError as e:
        raise Exception(f"Missing expected data in API response: {e}")

def get_video_ids(**context):
    try:
        task_instance = context['task_instance']
        playlist_id = task_instance.xcom_pull(task_ids='get_playlist_id', key='playlist_id')
        
        if not playlist_id:
            raise ValueError("No playlist_id found from previous task")
            
        video_ids = []
        next_page_token = None
        
        while True:
            url = f"https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId={playlist_id}&maxResults=50&key={API_KEY}"
            if next_page_token:
                url += f"&pageToken={next_page_token}"
            
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            for item in data['items']:
                video_ids.append(item['contentDetails']['videoId'])
            
            if 'nextPageToken' not in data:
                break
            next_page_token = data['nextPageToken']
        
        # Push video_ids to XCom for next task
        task_instance.xcom_push(key='video_ids', value=video_ids)
        print(f"Found {len(video_ids)} videos")
        return video_ids
        
    except requests.exceptions.RequestException as e:
        raise Exception(f"API request failed: {e}")
    except KeyError as e:
        raise Exception(f"Missing expected data in API response: {e}")

def get_video_data(**context):
    try:
        task_instance = context['task_instance']
        video_ids = task_instance.xcom_pull(task_ids='get_video_ids', key='video_ids')
        
        if not video_ids:
            raise ValueError("No video_ids found from previous task")
            
        video_data = []
        
        # Process in batches of 50 (YouTube API limit)
        for i in range(0, len(video_ids), 50):
            batch_ids = video_ids[i:i+50]
            ids_string = ','.join(batch_ids)
            
            url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics,contentDetails&id={ids_string}&key={API_KEY}"
            
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            for item in data['items']:
                video_info = {
                    'video_id': item['id'],
                    'title': item['snippet']['title'],
                    'description': item['snippet']['description'],
                    'published_at': item['snippet']['publishedAt'],
                    'view_count': int(item['statistics'].get('viewCount', 0)),
                    'like_count': int(item['statistics'].get('likeCount', 0)),
                    'duration': item['contentDetails']['duration']
                }
                video_data.append(video_info)
        
        # Save video data to JSON file with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"/app/data/yt_data_{timestamp}.json"
        
        os.makedirs('/app/data', exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(video_data, f, indent=4, ensure_ascii=False)
        
        print(f"Video data saved to {filename}")
        print(f"Retrieved data for {len(video_data)} videos")
        
        # Print first 5 videos as example
        print("\nFirst 5 videos:")
        for i, video in enumerate(video_data[:5]):
            print(f"{i+1}. {video['title']} - {video['view_count']} views")
        
        return filename
        
    except requests.exceptions.RequestException as e:
        raise Exception(f"API request failed: {e}")
    except KeyError as e:
        raise Exception(f"Missing expected data in API response: {e}")

# Define tasks
get_playlist_id_task = PythonOperator(
    task_id='get_playlist_id',
    python_callable=get_playlist_id,
    dag=dag,
)

get_video_ids_task = PythonOperator(
    task_id='get_video_ids',
    python_callable=get_video_ids,
    dag=dag,
)

get_video_data_task = PythonOperator(
    task_id='get_video_data',
    python_callable=get_video_data,
    dag=dag,
)

# Set task dependencies
get_playlist_id_task >> get_video_ids_task >> get_video_data_task

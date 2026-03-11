import requests
import json
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = "MrBeast"

def get_playlistId():
    try:
        url = f"https://www.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        print(json.dumps(data, indent=4))
        channel_items = data['items'][0]
        channel_playlistId = channel_items['contentDetails']['relatedPlaylists']['uploads']
        return channel_playlistId

    except requests.exceptions.RequestException as e:
        raise e


def get_video_ids(playlist_id):
    try:
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
        
        return video_ids
        
    except requests.exceptions.RequestException as e:
        raise e


def get_video_data(video_ids):
    try:
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
                    'view_count': item['statistics'].get('viewCount', 0),
                    'like_count': item['statistics'].get('likeCount', 0),
                    'duration': item['contentDetails']['duration']
                }
                video_data.append(video_info)
        
        return video_data
        
    except requests.exceptions.RequestException as e:
        raise e

if __name__ == "__main__":
    try:
        # Get the channel's upload playlist ID
        playlist_id = get_playlistId()
        print(f"Playlist ID: {playlist_id}")
        
        # Get all video IDs from the playlist
        video_ids = get_video_ids(playlist_id)
        print(f"Found {len(video_ids)} videos")
        
        # Get detailed video data
        video_data = get_video_data(video_ids)
        print(f"Retrieved data for {len(video_data)} videos")
        
        # Save video data to JSON file with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"yt_date_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(video_data, f, indent=4, ensure_ascii=False)
        
        print(f"Video data saved to {filename}")
        
        # Print first 5 videos as example
        print("\nFirst 5 videos:")
        for i, video in enumerate(video_data[:5]):
            print(f"{i+1}. {video['title']} - {video['view_count']} views")
            
    except Exception as e:
        print(f"Error: {e}")
        
        
# Set task dependencies
extract_playlist_task >> extract_video_ids_task >> extract_video_data_task
 
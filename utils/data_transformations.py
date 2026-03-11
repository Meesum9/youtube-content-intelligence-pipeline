"""
Data transformation utilities for YouTube data warehouse
"""

import re
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

def parse_duration(duration_str: str) -> int:
    """
    Convert ISO 8601 duration format (PT30M4S) to seconds
    """
    if not duration_str or not duration_str.startswith('PT'):
        return 0
    
    # Remove PT prefix
    duration = duration_str[2:]
    
    hours = 0
    minutes = 0
    seconds = 0
    
    # Extract hours
    hour_match = re.search(r'(\d+)H', duration)
    if hour_match:
        hours = int(hour_match.group(1))
    
    # Extract minutes
    minute_match = re.search(r'(\d+)M', duration)
    if minute_match:
        minutes = int(minute_match.group(1))
    
    # Extract seconds
    second_match = re.search(r'(\d+)S', duration)
    if second_match:
        seconds = int(second_match.group(1))
    
    return hours * 3600 + minutes * 60 + seconds

def calculate_engagement_rate(likes: int, views: int) -> float:
    """
    Calculate engagement rate as percentage
    """
    if views == 0:
        return 0.0
    return round((likes / views) * 100, 4)

def calculate_days_since_published(published_at: datetime) -> int:
    """
    Calculate days since video was published
    """
    return (datetime.now() - published_at).days

def format_number(num: int) -> str:
    """
    Format large numbers for display (K, M, B)
    """
    if num >= 1_000_000_000:
        return f"{num/1_000_000_000:.1f}B"
    elif num >= 1_000_000:
        return f"{num/1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num/1_000:.1f}K"
    else:
        return str(num)

def validate_video_data(video_data: Dict[str, Any]) -> bool:
    """
    Validate video data structure
    """
    required_fields = ['video_id', 'title', 'published_at', 'view_count', 'like_count']
    
    for field in required_fields:
        if field not in video_data or video_data[field] is None:
            return False
    
    # Validate data types
    try:
        int(video_data['view_count'])
        int(video_data['like_count'])
        datetime.fromisoformat(video_data['published_at'].replace('Z', '+00:00'))
        return True
    except (ValueError, TypeError):
        return False

def calculate_growth_rate(current: int, previous: int) -> float:
    """
    Calculate growth rate percentage
    """
    if previous == 0:
        return 0.0
    return round(((current - previous) / previous) * 100, 4)

def calculate_percentile_rank(values: list, value: int) -> float:
    """
    Calculate percentile rank of a value in a list
    """
    if not values:
        return 0.0
    
    sorted_values = sorted(values)
    rank = sum(1 for v in sorted_values if v <= value)
    return round((rank / len(sorted_values)) * 100, 2)

class YouTubeDataProcessor:
    """
    Class for processing YouTube data
    """
    
    def __init__(self):
        self.processed_videos = []
        self.errors = []
    
    def process_video(self, video_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a single video record
        """
        try:
            if not validate_video_data(video_data):
                self.errors.append(f"Invalid data for video {video_data.get('video_id', 'unknown')}")
                return None
            
            processed = {
                'video_id': video_data['video_id'],
                'title': video_data['title'],
                'description': video_data.get('description', ''),
                'published_at': video_data['published_at'],
                'view_count': int(video_data['view_count']),
                'like_count': int(video_data['like_count']),
                'duration_seconds': parse_duration(video_data.get('duration', '')),
                'duration_formatted': video_data.get('duration', ''),
                'engagement_rate': calculate_engagement_rate(
                    int(video_data['like_count']), 
                    int(video_data['view_count'])
                ),
                'days_since_published': calculate_days_since_published(
                    datetime.fromisoformat(video_data['published_at'].replace('Z', '+00:00'))
                )
            }
            
            self.processed_videos.append(processed)
            return processed
            
        except Exception as e:
            self.errors.append(f"Error processing video {video_data.get('video_id', 'unknown')}: {str(e)}")
            return None
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """
        Get summary of processing results
        """
        return {
            'total_processed': len(self.processed_videos),
            'total_errors': len(self.errors),
            'errors': self.errors,
            'avg_views': sum(v['view_count'] for v in self.processed_videos) // len(self.processed_videos) if self.processed_videos else 0,
            'avg_likes': sum(v['like_count'] for v in self.processed_videos) // len(self.processed_videos) if self.processed_videos else 0,
            'avg_engagement': sum(v['engagement_rate'] for v in self.processed_videos) / len(self.processed_videos) if self.processed_videos else 0
        }

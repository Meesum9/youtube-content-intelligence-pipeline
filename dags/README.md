# YouTube Airflow DAGs

This directory contains Apache Airflow DAGs for YouTube data extraction, processing, and monitoring.

## DAGs Overview

### 1. `youtube_pipeline_dag.py`
**Purpose**: Extract video data from a specific YouTube channel
**Schedule**: Daily (every 24 hours)
**Tasks**:
- Get channel playlist ID
- Extract all video IDs from playlist
- Fetch detailed video data and save to JSON

### 2. `youtube_analytics_dag.py`
**Purpose**: Process YouTube data and generate analytics
**Schedule**: Every 6 hours
**Tasks**:
- Load JSON data to PostgreSQL
- Generate analytics reports (top videos, daily stats, engagement metrics)
- Create database tables and indexes

### 3. `youtube_trending_dag.py`
**Purpose**: Track trending videos in specific regions
**Schedule**: Every 2 hours
**Tasks**:
- Fetch trending videos from YouTube API
- Analyze trending patterns (channels, categories, engagement)
- Clean up old trending data files

### 4. `youtube_monitoring_dag.py`
**Purpose**: Monitor pipeline health and send alerts
**Schedule**: Every 30 minutes
**Tasks**:
- Check YouTube API health and response times
- Validate data quality in latest files
- Monitor DAG performance metrics
- Send email/Slack alerts on failures

## Configuration

### Environment Variables
Create a `.env` file with the following variables:
```
API_KEY=your_youtube_api_key
CHANNEL_HANDLE=MrBeast
REGION_CODE=US
ALERT_EMAIL=admin@example.com
SLACK_WEBHOOK_URL=your_slack_webhook_url
```

### Database Setup
The analytics DAG requires PostgreSQL. Set up the connection in Airflow:
- Connection ID: `postgres_default`
- Configure host, port, database, username, password

## Dependencies

Install required packages:
```bash
pip install -r requirements.txt
```

## File Structure

```
dags/
├── youtube_pipeline_dag.py      # Main data extraction
├── youtube_analytics_dag.py     # Data processing & analytics
├── youtube_trending_dag.py      # Trending videos tracker
├── youtube_monitoring_dag.py    # Health monitoring & alerts
└── README.md                    # This documentation

data/                            # Generated data files
├── yt_data_*.json              # Channel video data
├── analytics_*.json            # Analytics reports
├── trending_*.json             # Trending videos data
├── trending_analysis_*.json    # Trending analysis
├── api_health_*.json           # API health reports
├── quality_report_*.json       # Data quality reports
└── performance_report_*.json   # Performance metrics
```

## DAG Dependencies

```
youtube_pipeline_dag.py (Daily)
    ↓
youtube_analytics_dag.py (Every 6 hours)

youtube_trending_dag.py (Every 2 hours) - Independent

youtube_monitoring_dag.py (Every 30 minutes) - Independent
```

## Monitoring & Alerts

The monitoring DAG provides:
- **API Health**: Response time monitoring and connectivity checks
- **Data Quality**: Validation of extracted data completeness
- **Performance**: DAG execution time and success rate tracking
- **Alerts**: Email and Slack notifications for failures

## Usage

1. **Setup**: Configure environment variables and database connections
2. **Enable DAGs**: Turn on the required DAGs in Airflow UI
3. **Monitor**: Check the monitoring DAG for health status
4. **Scale**: Adjust schedules based on your API quota and needs

## Troubleshooting

### Common Issues
- **API Quota Exceeded**: Reduce DAG frequency or request batch sizes
- **Database Connection**: Verify PostgreSQL connection settings
- **Missing Data Files**: Check pipeline DAG logs for extraction errors
- **Alerts Not Working**: Verify email/Slack configuration

### Logs & Monitoring
- Check Airflow UI for DAG execution logs
- Monitor data quality reports in `/app/data/`
- Review API health reports for connectivity issues
- Use performance reports to identify bottlenecks

## Customization

### Adding New Channels
Modify `CHANNEL_HANDLE` in environment variables or create multiple DAG instances.

### Custom Analytics
Add new analytics queries in `youtube_analytics_dag.py` under the `generate_analytics` function.

### Additional Monitoring
Extend `youtube_monitoring_dag.py` to include custom health checks and alerting logic.

# YouTube Content Intelligence Pipeline

A comprehensive data pipeline and analytics solution for YouTube content analysis using Apache Airflow, PostgreSQL, and Python.

## 🎯 Project Overview

This project automates the collection, processing, and analysis of YouTube channel data to provide actionable insights for content creators and marketers. It includes a complete data warehouse infrastructure with ETL pipelines, analytics dashboards, and automated reporting.

## 🏗️ Architecture

### System Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   YouTube API   │───▶│   Airflow DAGs  │───▶│  PostgreSQL DW  │
│                 │    │                 │    │                 │
│ • Video Data    │    │ • ETL Pipeline  │    │ • Staging Layer │
│ • Channel Info  │    │ • Scheduling    │    │ • Core Layer    │
│ • Analytics     │    │ • Monitoring    │    │ • Analytics     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Data Warehouse Layers

1. **Staging Layer (`dw_staging`)**
   - `youtube_videos_raw`: Raw JSON data from YouTube API
   - `youtube_videos_staging`: Processed structured data

2. **Core Layer (`dw_core`)**
   - `youtube_videos`: SCD Type 2 dimension table
   - `youtube_video_metrics_daily`: Daily metric snapshots
   - `youtube_video_performance`: Aggregated performance metrics

3. **Analytics Layer (`dw_analytics`)**
   - `channel_performance_daily`: Daily channel summaries
   - `channel_performance_weekly`: Weekly trends and growth
   - `video_rankings`: Performance rankings
   - `content_performance_by_period`: Time-based aggregations

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- YouTube API Key
- At least 4GB RAM available

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd youtube-content-intelligence-pipeline
   ```

2. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your YouTube API key and other settings
   ```

3. **Start the infrastructure**
   ```bash
   docker-compose up -d
   ```

4. **Access Airflow**
   - URL: http://localhost:8081
   - Username: `admin`
   - Password: `admin`

5. **Initialize the data warehouse**
   - In Airflow UI, trigger the `setup_data_warehouse` DAG
   - Enable the `youtube_data_warehouse` DAG

## 📁 Project Structure

```
youtube-content-intelligence-pipeline/
├── dags/                          # Airflow DAGs
│   ├── setup_data_warehouse.py    # Database setup
│   ├── youtube_data_warehouse.py  # Main ETL pipeline
│   └── youtube_api_etl.py        # API data collection
├── sql/                          # Database schemas
│   ├── 01_create_schemas.sql
│   ├── 02_create_staging_tables.sql
│   ├── 03_create_core_tables.sql
│   └── 04_create_analytics_tables.sql
├── utils/                        # Utility functions
│   ├── data_transformations.py
│   ├── youtube_api_client.py
│   └── data_quality.py
├── config/                       # Configuration files
│   └── data_warehouse_config.py
├── data/                         # Raw data storage
│   └── yt_date_*.json
├── logs/                         # Airflow logs
├── docker-compose.yml           # Infrastructure setup
├── Dockerfile                    # Container definition
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

## 🔄 Data Pipeline Process

### Daily ETL Workflow

1. **Data Extraction**
   - Fetch video data from YouTube API
   - Download channel analytics
   - Store raw JSON in data directory

2. **Data Loading**
   - Load raw data into staging tables
   - Validate data quality and structure
   - Handle duplicates and errors

3. **Data Transformation**
   - Apply business logic and calculations
   - Convert duration formats to seconds
   - Calculate engagement rates and growth metrics

4. **Data Warehousing**
   - Implement SCD Type 2 for historical tracking
   - Create daily snapshots for trend analysis
   - Populate analytics tables

5. **Analytics & Reporting**
   - Generate performance summaries
   - Calculate rankings and percentiles
   - Update dashboard metrics

## 📊 Key Features

### Data Collection
- **Automated API Calls**: Scheduled YouTube API data fetching
- **Error Handling**: Robust error recovery and retry logic
- **Rate Limiting**: Respect API quotas and limits
- **Data Validation**: Ensure data quality and integrity

### Data Processing
- **SCD Type 2**: Track historical changes to video metadata
- **Incremental Loads**: Only process new or changed data
- **Data Quality**: Built-in validation and cleansing
- **Performance Metrics**: Calculate engagement, growth rates

### Analytics & Insights
- **Daily Reports**: Automated daily performance summaries
- **Trend Analysis**: Track performance over time
- **Video Rankings**: Identify top-performing content
- **Channel Health**: Monitor overall channel metrics

### Monitoring & Alerting
- **Pipeline Health**: Monitor ETL pipeline status
- **Data Quality Alerts**: Notify on data issues
- **Performance Monitoring**: Track system performance
- **Error Notifications**: Alert on failures

## 🎛️ Configuration

### Environment Variables

```bash
# YouTube API Configuration
API_KEY=your_youtube_api_key
CHANNEL_HANDLE=your_channel_name
REGION_CODE=US

# Database Configuration
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Alerting
ALERT_EMAIL=admin@example.com
SLACK_WEBHOOK_URL=your_slack_webhook
```

### Data Warehouse Settings

Configuration is centralized in `config/data_warehouse_config.py`:

- **Data Retention**: Configure how long to keep different data types
- **Quality Thresholds**: Set data quality validation rules
- **Performance Metrics**: Configure analytics calculations
- **Alerting Rules**: Set up monitoring and notifications

## 📈 Analytics Available

### Channel Performance
- Total views, likes, and engagement metrics
- Daily/weekly/monthly growth rates
- Content publishing patterns
- Audience engagement trends

### Video Analytics
- Individual video performance tracking
- View count evolution over time
- Engagement rate analysis
- Peak performance identification

### Comparative Analysis
- Video-to-video performance comparisons
- Time period comparisons
- Content type performance
- Ranking and percentile analysis

## 🔧 Maintenance

### Daily Operations (Automated)
- ETL pipeline execution
- Data quality checks
- Performance metric calculations
- System health monitoring

### Weekly Tasks (Manual)
- Review pipeline performance
- Check data quality metrics
- Monitor storage usage
- Review alert configurations

### Monthly Tasks (Manual)
- Update API configurations
- Review data retention policies
- Optimize database performance
- Update business logic

## 🐛 Troubleshooting

### Common Issues

1. **DAG Failures**
   - Check Airflow logs for error details
   - Verify API credentials and quotas
   - Ensure database connectivity

2. **Data Quality Issues**
   - Review data validation logs
   - Check API response formats
   - Verify transformation logic

3. **Performance Issues**
   - Monitor database query performance
   - Check container resource usage
   - Review DAG execution times

### Logs and Monitoring

- **Airflow Logs**: `/opt/airflow/logs/`
- **Database Logs**: PostgreSQL container logs
- **System Metrics**: Container resource usage
- **API Logs**: YouTube API request/response logs

## 🔐 Security

### Data Protection
- API keys stored in environment variables
- Database connections encrypted
- Access control through Airflow roles
- Audit logging for data access

### Best Practices
- Regular credential rotation
- Network security through Docker
- Minimal privilege principle
- Regular security updates

## 🚀 Scaling

### Horizontal Scaling
- Add more Airflow workers
- Database read replicas
- Distributed processing
- Load balancing

### Vertical Scaling
- Increase container resources
- Database performance tuning
- Storage optimization
- Memory management

## 📚 API Reference

### YouTube Data Endpoints
- Video details and statistics
- Channel analytics
- Playlist information
- Comment data (optional)

### Database Schema
- Complete table definitions
- Relationship diagrams
- Index specifications
- Query examples

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 📞 Support

For support and questions:
- Create an issue in the repository
- Check the troubleshooting guide
- Review the documentation
- Contact the maintainers

---

**Built with ❤️ using Apache Airflow, PostgreSQL, and Python**

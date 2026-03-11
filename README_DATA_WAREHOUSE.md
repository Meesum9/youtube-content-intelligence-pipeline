# YouTube Data Warehouse

A comprehensive data warehouse solution for YouTube analytics data built with Apache Airflow and PostgreSQL.

## 🏗️ Architecture

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

## 📁 Project Structure

```
├── sql/                          # SQL schema files
│   ├── 01_create_schemas.sql
│   ├── 02_create_staging_tables.sql
│   ├── 03_create_core_tables.sql
│   └── 04_create_analytics_tables.sql
├── dags/                         # Airflow DAGs
│   ├── setup_data_warehouse.py    # One-time setup
│   └── youtube_data_warehouse.py # Daily ETL pipeline
├── utils/                        # Utility functions
│   └── data_transformations.py
├── config/                       # Configuration files
│   └── data_warehouse_config.py
├── data/                         # Raw data files
└── logs/                         # Airflow logs
```

## 🚀 Setup Instructions

### 1. Prerequisites
- Airflow running (already set up)
- PostgreSQL running (already set up)
- YouTube API data in JSON format

### 2. Run Setup DAG
1. Go to Airflow UI: http://localhost:8081
2. Enable the `setup_data_warehouse` DAG
3. Trigger it manually to create schemas and tables

### 3. Configure Data Pipeline
1. Enable the `youtube_data_warehouse` DAG
2. It will run daily to process new YouTube data
3. Place new JSON files in the `/app/data` directory

## 📊 Data Flow

```
YouTube API → JSON Files → Staging Raw → Staging Processed → Core Tables → Analytics Tables
```

### Daily ETL Process

1. **Extract**: Read JSON files from data directory
2. **Load Raw**: Store raw JSON in staging
3. **Process**: Transform to structured format
4. **Core Load**: Apply SCD Type 2 logic
5. **Analytics**: Update aggregated metrics

## 🔧 Key Features

### Slowly Changing Dimensions (SCD Type 2)
- Track historical changes to video metadata
- Maintain full audit trail of data changes
- `effective_from` and `effective_to` timestamps

### Data Quality
- Validation of video data structure
- Engagement rate calculations
- Growth rate computations
- Data retention policies

### Performance Optimizations
- Indexes on frequently queried columns
- Partitioned tables for large datasets
- Efficient SQL queries with CTEs

## 📈 Analytics Available

### Channel Performance
- Daily/weekly/monthly summaries
- Growth rates and trends
- Top performing videos

### Video Analytics
- View count evolution
- Engagement metrics
- Performance rankings

### Time-based Analysis
- Daily snapshots for trend analysis
- Weekly aggregations for reporting
- Monthly summaries for strategic insights

## 🗄️ Schema Details

### Core Tables

#### `dw_core.youtube_videos`
- SCD Type 2 dimension table
- Tracks video metadata changes over time
- Current records identified by `is_current = TRUE`

#### `dw_core.youtube_video_metrics_daily`
- Daily snapshot of video metrics
- Enables trend analysis
- Calculates day-over-day changes

#### `dw_analytics.channel_performance_daily`
- Pre-aggregated daily metrics
- Optimized for dashboard queries
- Includes top performing video info

## 🔍 Sample Queries

### Get Top 10 Videos by Views
```sql
SELECT video_id, title, view_count, like_count
FROM dw_core.youtube_videos 
WHERE is_current = TRUE 
ORDER BY view_count DESC 
LIMIT 10;
```

### Daily Channel Performance
```sql
SELECT date_key, total_views, total_likes, new_videos_published
FROM dw_analytics.channel_performance_daily
ORDER BY date_key DESC;
```

### Video Growth Analysis
```sql
SELECT video_id, 
       snapshot_date,
       view_count,
       LAG(view_count) OVER (PARTITION BY video_id ORDER BY snapshot_date) as prev_views,
       view_count - LAG(view_count) OVER (PARTITION BY video_id ORDER BY snapshot_date) as daily_growth
FROM dw_core.youtube_video_metrics_daily
WHERE snapshot_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY video_id, snapshot_date;
```

## 📅 Data Retention

- **Staging Raw Data**: 30 days
- **Staging Processed**: 90 days
- **Core Daily Metrics**: 1 year
- **Core Performance**: 3 years
- **Analytics Daily**: 2 years
- **Analytics Weekly**: 5 years

## 🚨 Monitoring & Alerts

The data warehouse includes:
- Data quality validation
- Load failure detection
- Performance monitoring
- Automated cleanup processes

## 🔧 Configuration

All configuration is centralized in `config/data_warehouse_config.py`:
- Database connections
- Data retention policies
- Quality thresholds
- Alerting settings

## 📋 Maintenance Tasks

### Daily (Automated)
- ETL pipeline execution
- Data quality checks
- Analytics table updates

### Weekly (Manual)
- Review data quality metrics
- Monitor storage usage
- Check performance trends

### Monthly (Manual)
- Review data retention policies
- Optimize slow queries
- Update configuration as needed

## 🐛 Troubleshooting

### Common Issues

1. **DAG not running**: Check if setup DAG completed successfully
2. **Missing data**: Verify JSON files are in the correct directory
3. **Connection errors**: Ensure PostgreSQL connection is configured
4. **Slow performance**: Check if indexes exist and statistics are up to date

### Logs Location
- Airflow logs: `/opt/airflow/logs/`
- Database logs: PostgreSQL container logs

## 📞 Support

For issues with the data warehouse:
1. Check Airflow UI for DAG failures
2. Review PostgreSQL container logs
3. Verify data file formats
4. Check configuration settings

---

**Note**: This data warehouse is designed to be scalable and can handle large volumes of YouTube data efficiently.

-- Analytics tables for business reporting
-- These tables contain pre-calculated metrics for dashboards and reporting

-- Daily channel performance summary
CREATE TABLE IF NOT EXISTS dw_analytics.channel_performance_daily (
    date_key DATE PRIMARY KEY,
    total_videos INTEGER,
    total_views BIGINT,
    total_likes BIGINT,
    new_videos_published INTEGER,
    avg_views_per_video DECIMAL(15,2),
    avg_likes_per_video DECIMAL(15,2),
    total_engagement_rate DECIMAL(10,4),
    top_performing_video_id VARCHAR(50),
    top_performing_video_title TEXT,
    top_performing_video_views BIGINT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Weekly performance trends
CREATE TABLE IF NOT EXISTS dw_analytics.channel_performance_weekly (
    week_start_date DATE PRIMARY KEY,
    week_end_date DATE,
    total_videos INTEGER,
    total_views BIGINT,
    total_likes BIGINT,
    new_videos_published INTEGER,
    avg_daily_views DECIMAL(15,2),
    avg_daily_likes DECIMAL(15,2),
    views_growth_rate DECIMAL(10,4),
    likes_growth_rate DECIMAL(10,4),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Video performance rankings
CREATE TABLE IF NOT EXISTS dw_analytics.video_rankings (
    ranking_date DATE,
    video_id VARCHAR(50),
    title TEXT,
    published_at TIMESTAMP,
    views_rank INTEGER,
    likes_rank INTEGER,
    engagement_rank INTEGER,
    total_views BIGINT,
    total_likes BIGINT,
    engagement_rate DECIMAL(10,4),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ranking_date, video_id)
);

-- Content performance by time periods
CREATE TABLE IF NOT EXISTS dw_analytics.content_performance_by_period (
    period_type VARCHAR(20), -- 'daily', 'weekly', 'monthly'
    period_key DATE,
    total_videos INTEGER,
    total_views BIGINT,
    avg_views_per_video DECIMAL(15,2),
    median_views_per_video DECIMAL(15,2),
    top_quartile_views BIGINT,
    bottom_quartile_views BIGINT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (period_type, period_key)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_analytics_daily_date ON dw_analytics.channel_performance_daily(date_key);
CREATE INDEX IF NOT EXISTS idx_analytics_weekly_week ON dw_analytics.channel_performance_weekly(week_start_date);
CREATE INDEX IF NOT EXISTS idx_analytics_rankings_date ON dw_analytics.video_rankings(ranking_date);
CREATE INDEX IF NOT EXISTS idx_analytics_rankings_views_rank ON dw_analytics.video_rankings(views_rank);
CREATE INDEX IF NOT EXISTS idx_analytics_period_type_date ON dw_analytics.content_performance_by_period(period_type, period_key);

-- Add comments
COMMENT ON TABLE dw_analytics.channel_performance_daily IS 'Daily aggregated channel performance metrics';
COMMENT ON TABLE dw_analytics.channel_performance_weekly IS 'Weekly aggregated channel performance with growth rates';
COMMENT ON TABLE dw_analytics.video_rankings IS 'Daily video performance rankings by different metrics';
COMMENT ON TABLE dw_analytics.content_performance_by_period IS 'Content performance aggregated by different time periods';
COMMENT ON COLUMN dw_analytics.content_performance_by_period.period_type IS 'Type of period: daily, weekly, or monthly';

-- Core data warehouse tables
-- These tables contain cleaned, validated, and transformed data

-- Core videos table with slowly changing dimension (SCD Type 2)
CREATE TABLE IF NOT EXISTS dw_core.youtube_videos (
    surrogate_key SERIAL PRIMARY KEY,
    video_id VARCHAR(50) NOT NULL,
    title TEXT,
    description TEXT,
    published_at TIMESTAMP,
    view_count BIGINT,
    like_count BIGINT,
    duration_seconds INTEGER,
    duration_formatted VARCHAR(20),
    channel_id VARCHAR(50),
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN DEFAULT TRUE,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily video metrics snapshot table
CREATE TABLE IF NOT EXISTS dw_core.youtube_video_metrics_daily (
    id SERIAL PRIMARY KEY,
    video_id VARCHAR(50) NOT NULL,
    snapshot_date DATE NOT NULL,
    view_count BIGINT,
    like_count BIGINT,
    view_count_change BIGINT,
    like_count_change BIGINT,
    engagement_rate DECIMAL(10,4),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Video performance summary table
CREATE TABLE IF NOT EXISTS dw_core.youtube_video_performance (
    video_id VARCHAR(50) PRIMARY KEY,
    title TEXT,
    published_at TIMESTAMP,
    days_since_published INTEGER,
    total_views BIGINT,
    total_likes BIGINT,
    avg_daily_views DECIMAL(15,2),
    avg_daily_likes DECIMAL(15,2),
    peak_views_date DATE,
    peak_views_count BIGINT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_core_videos_video_id ON dw_core.youtube_videos(video_id);
CREATE INDEX IF NOT EXISTS idx_core_videos_effective_dates ON dw_core.youtube_videos(effective_from, effective_to);
CREATE INDEX IF NOT EXISTS idx_core_videos_current ON dw_core.youtube_videos(is_current);
CREATE INDEX IF NOT EXISTS idx_core_videos_published_at ON dw_core.youtube_videos(published_at);

CREATE INDEX IF NOT EXISTS idx_metrics_daily_video_date ON dw_core.youtube_video_metrics_daily(video_id, snapshot_date);
CREATE INDEX IF NOT EXISTS idx_metrics_daily_date ON dw_core.youtube_video_metrics_daily(snapshot_date);

CREATE INDEX IF NOT EXISTS idx_performance_video_id ON dw_core.youtube_video_performance(video_id);
CREATE INDEX IF NOT EXISTS idx_performance_published_at ON dw_core.youtube_video_performance(published_at);

-- Add unique constraints
ALTER TABLE dw_core.youtube_video_metrics_daily 
ADD CONSTRAINT uk_metrics_video_date UNIQUE (video_id, snapshot_date);

-- Add comments
COMMENT ON TABLE dw_core.youtube_videos IS 'Core video dimension table with SCD Type 2';
COMMENT ON TABLE dw_core.youtube_video_metrics_daily IS 'Daily snapshot of video metrics';
COMMENT ON TABLE dw_core.youtube_video_performance IS 'Aggregated video performance metrics';
COMMENT ON COLUMN dw_core.youtube_videos.effective_from IS 'Start date for this version of the record';
COMMENT ON COLUMN dw_core.youtube_videos.effective_to IS 'End date for this version of the record';
COMMENT ON COLUMN dw_core.youtube_videos.is_current IS 'Flag to identify current record';

-- Staging tables for YouTube API data
-- These tables store raw data from API responses

-- Raw video data staging table
CREATE TABLE IF NOT EXISTS dw_staging.youtube_videos_raw (
    id SERIAL PRIMARY KEY,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255),
    raw_data JSONB NOT NULL,
    processed_flag BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Staging table for processed video data
CREATE TABLE IF NOT EXISTS dw_staging.youtube_videos_staging (
    video_id VARCHAR(50) PRIMARY KEY,
    title TEXT,
    description TEXT,
    published_at TIMESTAMP,
    view_count BIGINT,
    like_count BIGINT,
    duration VARCHAR(20),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_youtube_videos_raw_processed ON dw_staging.youtube_videos_raw(processed_flag);
CREATE INDEX IF NOT EXISTS idx_youtube_videos_raw_load_timestamp ON dw_staging.youtube_videos_raw(load_timestamp);
CREATE INDEX IF NOT EXISTS idx_youtube_videos_staging_video_id ON dw_staging.youtube_videos_staging(video_id);
CREATE INDEX IF NOT EXISTS idx_youtube_videos_staging_published_at ON dw_staging.youtube_videos_staging(published_at);

-- Add comments
COMMENT ON TABLE dw_staging.youtube_videos_raw IS 'Raw JSON data from YouTube API';
COMMENT ON TABLE dw_staging.youtube_videos_staging IS 'Processed video data in staging layer';
COMMENT ON COLUMN dw_staging.youtube_videos_raw.raw_data IS 'Original JSON response from YouTube API';
COMMENT ON COLUMN dw_staging.youtube_videos_raw.processed_flag IS 'Flag to indicate if raw data has been processed';

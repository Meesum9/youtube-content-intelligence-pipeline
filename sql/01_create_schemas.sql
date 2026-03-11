-- YouTube Data Warehouse Schema Setup
-- Create schemas for different layers of the data warehouse

CREATE SCHEMA IF NOT EXISTS dw_staging;
CREATE SCHEMA IF NOT EXISTS dw_core;
CREATE SCHEMA IF NOT EXISTS dw_analytics;

-- Grant permissions to airflow user
GRANT USAGE ON SCHEMA dw_staging TO airflow;
GRANT USAGE ON SCHEMA dw_core TO airflow;
GRANT USAGE ON SCHEMA dw_analytics TO airflow;

GRANT CREATE ON SCHEMA dw_staging TO airflow;
GRANT CREATE ON SCHEMA dw_core TO airflow;
GRANT CREATE ON SCHEMA dw_analytics TO airflow;

-- Create comments for documentation
COMMENT ON SCHEMA dw_staging IS 'Staging area for raw data from YouTube API';
COMMENT ON SCHEMA dw_core IS 'Core data warehouse with cleaned and transformed data';
COMMENT ON SCHEMA dw_analytics IS 'Analytics-ready tables for business reporting';

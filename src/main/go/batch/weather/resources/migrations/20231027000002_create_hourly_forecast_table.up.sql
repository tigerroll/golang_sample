-- 20231027000002_create_hourly_forecast_table.up.sql
CREATE TABLE IF NOT EXISTS hourly_forecast (
    id SERIAL PRIMARY KEY, -- Auto-incrementing primary key
    time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    weather_code INTEGER NOT NULL,
    temperature_2m NUMERIC(5, 2) NOT NULL, -- Assuming temperature can have 2 decimal places
    latitude NUMERIC(9, 6) NOT NULL,  -- Latitude with 6 decimal places
    longitude NUMERIC(9, 6) NOT NULL, -- Longitude with 6 decimal places
    collected_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

-- Add indexes for hourly_forecast table
CREATE INDEX IF NOT EXISTS idx_hourly_forecast_time ON hourly_forecast (time);
CREATE INDEX IF NOT EXISTS idx_hourly_forecast_location ON hourly_forecast (latitude, longitude);

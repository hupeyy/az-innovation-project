-- =============================================================
-- API Data Pipeline Schema
-- =============================================================


-- -------------------------------------------------------------
-- API Sources
-- Tracks each external API being ingested
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS api_sources (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,         -- e.g. 'openweather', 'newsapi'
    base_url VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL      -- e.g. 'weather', 'news'
);


-- -------------------------------------------------------------
-- API Requests
-- One row per API call made, regardless of source
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS api_requests (
    id SERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES api_sources(id),
    endpoint VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    http_status INTEGER,
    response_time_ms INTEGER
);


-- -------------------------------------------------------------
-- Weather Data (Processed)
-- Extracted fields from OpenWeather API responses
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    request_id INTEGER NOT NULL REFERENCES api_requests(id),
    city_name VARCHAR(255) NOT NULL,
    country VARCHAR(10) NOT NULL,
    units VARCHAR(20) NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    temp_min FLOAT NOT NULL,
    temp_max FLOAT NOT NULL,
    humidity INTEGER NOT NULL,
    wind_speed FLOAT NOT NULL,
    sunrise TIMESTAMP NOT NULL,
    sunset TIMESTAMP NOT NULL
);

-- -------------------------------------------------------------
-- News Articles (Processed)
-- Extracted fields from NewsAPI responses
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS news_data (
    id SERIAL PRIMARY KEY,
    request_id INTEGER NOT NULL REFERENCES api_requests(id),
    source_name VARCHAR(255) NOT NULL,      -- e.g. 'BBC News', 'CNN'
    author VARCHAR(255),                     -- nullable, not always provided
    title TEXT NOT NULL,
    description TEXT,
    url TEXT NOT NULL,
    image_url TEXT,
    published_at TIMESTAMP NOT NULL,
    content TEXT
);

-- -------------------------------------------------------------
-- Extracted Entities
-- Generic table for parsed entities from any API source
-- e.g. company names, categories, dates
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS extracted_entities (
    id SERIAL PRIMARY KEY,
    request_id INTEGER NOT NULL REFERENCES api_requests(id),
    entity_type VARCHAR(100) NOT NULL,  -- e.g. 'company', 'category', 'date'
    entity_value TEXT NOT NULL
);


-- -------------------------------------------------------------
-- Raw Data
-- Stores the full JSON response for every request (for debugging)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_data (
    id SERIAL PRIMARY KEY,
    request_id INTEGER NOT NULL REFERENCES api_requests(id),
    raw_data JSONB NOT NULL
);


-- -------------------------------------------------------------
-- API Errors
-- Logs failed API calls and parsing/validation errors
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS api_errors (
    id SERIAL PRIMARY KEY,
    request_id INTEGER NOT NULL REFERENCES api_requests(id),
    error_message TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL
);
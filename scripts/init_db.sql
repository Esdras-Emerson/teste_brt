CREATE TABLE IF NOT EXISTS raw_data (
    id SERIAL PRIMARY KEY,
    bus_id VARCHAR(50),
    latitude FLOAT,
    longitude FLOAT,
    speed FLOAT,
    captured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Création du schéma RAW
CREATE SCHEMA IF NOT EXISTS raw;

-- Table RAW météo horaire
CREATE TABLE IF NOT EXISTS raw.weather_hourly (
    city TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    ts_utc TIMESTAMPTZ NOT NULL,
    temperature_c DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    wind_speed_kmh DOUBLE PRECISION,
    ingested_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (city, ts_utc)
);

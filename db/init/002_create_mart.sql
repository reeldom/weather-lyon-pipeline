-- Création du schéma MART
CREATE SCHEMA IF NOT EXISTS mart;

-- Table mart météo journalière agrégée
CREATE TABLE IF NOT EXISTS mart.weather_daily (
    city TEXT NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    date_local DATE NOT NULL,
    avg_temperature_c DOUBLE PRECISION,
    min_temperature_c DOUBLE PRECISION,
    max_temperature_c DOUBLE PRECISION,
    total_precipitation_mm DOUBLE PRECISION,
    avg_wind_speed_kmh DOUBLE PRECISION,
    hours_observed INTEGER NOT NULL,
    hours_expected INTEGER NOT NULL,
    completeness_rate DOUBLE PRECISION NOT NULL,
    is_complete BOOLEAN NOT NULL,
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (city, date_local)
);
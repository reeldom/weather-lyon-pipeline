# Weather Data Pipeline

This project demonstrates the implementation of an end-to-end data pipeline,
from API ingestion to data visualization, using weather data for the city of Lyon, France.

# Project Overview
The goal of this project is to build a reliable and reproducible data pipeline that collects weather data, processes it into daily KPIs, and exposes it to a BI tool for analysis.

The pipeline focuses on:
- temperature
- wind speed
- precipitation
- data completeness and quality

# Architecture
The pipeline follows a classic layered architecture:

Open-Meteo API
→ Python ingestion
→ PostgreSQL (RAW layer)
→ PostgreSQL (MART layer)
→ Power BI

# Data Model
RAW layer:
- weather_hourly: hourly weather observations (1 row = 1 hour)

MART layer:
- weather_daily: daily aggregated KPIs (1 row = 1 day)

# Pipeline Execution
The pipeline is executed in two main steps:

1. load_raw.py
   - fetches hourly data from the Open-Meteo API
   - performs idempotent upserts into the RAW table

2. load_mart.py
   - aggregates hourly data into daily KPIs
   - computes data completeness metrics
   - upserts results into the MART table

# Power BI Dashboard
The MART table is connected to Power BI Desktop to visualize:
- daily average temperature & wind trends
- data completeness indicators
- comparison of observed vs expected hours

The dashboard is refreshed manually after pipeline execution.

# Data quality & assumptions
To ensure data reliability:
- each day is expected to contain 24 hourly records
- completeness_rate = hours_observed / hours_expected
- is_complete flags whether the day is fully observed

All KPIs are computed only on the available data.


## Tech Stack
- Python (pandas, requests, psycopg2)
- PostgreSQL
- Docker & Docker Compose
- Power BI Desktop
- Open-Meteo API

# How to run 
1. Start PostgreSQL:
   docker compose up -d

2. Load RAW data:
   python src/load_raw.py

3. Build MART table:
   python src/load_mart.py

4. Open Power BI and refresh the dataset

## What i learned
This project helped me practice:
- building idempotent data pipelines
- designing RAW vs MART data layers
- handling time zones and daily aggregations
- integrating data quality metrics
- connecting a database to a BI tool

## Possible improvements
- automate pipeline execution with a scheduler
- extend the pipeline to multiple cities
- add historical backfill
- publish the dashboard to Power BI Service
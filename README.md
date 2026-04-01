# Lufthansa Data Pipeline

## Overview
Automated pipeline that ingests Lufthansa flight and airport data,
transforms it through the medallion architecture, and serves it
via a Databricks dashboard.

## Pipeline Architecture
Bronze → Silver → Gold → Dashboard

- **Bronze**: Raw JSON from Lufthansa API stored as Delta tables with metadata
- **Silver**: Cleaned, typed, deduplicated tables per entity
- **Gold**: Aggregated flight statistics per airport per hour

## Data Sources
### Lufthansa API:
- airports
- cities
- countries
- airlines
- aircrafts
- flights

## Schedule
- Reference data (airports, cities, countries, airlines, aircraft): monthly
- Flight status: every 4 hours
- Gold table rebuild: immediately after each silver flights run

## Tables
1. bronze.airports -> silver.airports
2. bronze.flights -> silver.flights
3. silver.airports + silver.flights -> gold.airports_daily_stats

## Data Quality
- Null checks on primary keys
- Deduplication based on api's double entries
- Delay calculated only where both scheduled and actual times are present

## Setup
1. Add your Lufthansa API tokens to Databricks secrets
2. Run reference data job manually once to populate bronze and silver
3. Enable scheduled jobs in Databricks Jobs UI
4. Open the dashboard in Databricks SQL

## Jobs
- `reference_data_pipeline`: monthly, ingests and silverizes all reference data
- `flight_pipeline`: every 4 hours, ingests and silverizes flight status
- `gold_rebuild`: triggered after flight_pipeline completes

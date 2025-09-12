#!/bin/bash

# Script to run dbt transformations with Docker
set -e

echo "Building Docker image..."
docker-compose build

echo "Running dbt model: stg_sleep..."
docker-compose --profile dbt run dbt-run

echo "Running dbt tests for stg_sleep..."
docker-compose --profile dbt run dbt-test

echo "dbt pipeline completed successfully!"
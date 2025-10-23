#!/bin/bash
# Helper script to run parquet reader in Docker

DATE_ARG="${1:-}"

if [ -z "$DATE_ARG" ]; then
    echo "Reading all parquet files..."
    docker-compose run --rm parquet-reader
else
    echo "Reading parquet files for: $DATE_ARG"
    docker-compose run --rm parquet-reader python parquet_to_polars.py "$DATE_ARG"
fi

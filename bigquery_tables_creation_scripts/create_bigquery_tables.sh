#!/bin/bash

# Set dataset name
PROJECT_ID="nyc-taxi-batch-dataflow"
DATASET_NAME="trip_data"

# Create BigQuery Dataset
echo "Creating BigQuery dataset: $PROJECT_ID:$DATASET_NAME..."
bq mk --location=US $PROJECT_ID:$DATASET_NAME

# Create Tables
echo "Creating tables..."
bq mk --table $PROJECT_ID:$DATASET_NAME.vendor_dim ./table_schemas/vendor_dim.json
bq mk --table $PROJECT_ID:$DATASET_NAME.time_dim ./table_schemas/time_dim.json
bq mk --table $PROJECT_ID:$DATASET_NAME.calendar_dim ./table_schemas/calendar_dim.json
bq mk --table $PROJECT_ID:$DATASET_NAME.fare_rate_dim ./table_schemas/fare_rate_dim.json
bq mk --table $PROJECT_ID:$DATASET_NAME.taxi_zone_dim ./table_schemas/taxi_zone_dim.json
bq mk --table $PROJECT_ID:$DATASET_NAME.payment_method_dim ./table_schemas/payment_method_dim.json
bq mk --table $PROJECT_ID:$DATASET_NAME.trip_records ./table_schemas/trip_records.json

echo "All tables created successfully!"

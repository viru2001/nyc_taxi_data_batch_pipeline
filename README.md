# NYC Taxi Data Batch Pipeline

> A batch data pipeline built on Google Cloud Platform to ingest, transform, and analyze NYC taxi trip data. The project leverages GCS, Dataflow, BigQuery, Cloud Functions, and Airflow Composer to automate ETL workflows and power a Looker Studio dashboard.


---

## 🏗️ Architecture Overview

![Pipeline Architecture](https://github.com/user-attachments/assets/d9ca94a5-825c-4615-8ed9-60643e57cfd0)

1. **Trip Data Ingestion**: CSV files are uploaded to a GCS bucket, triggering a Dataflow (Apache Beam) job.
2. **Data Transformation**: The Dataflow job parses, enriches, and writes records into the fact table in BigQuery.
3. **Dimension Tables Update**: Dimension CSVs (vendor, zone, payment method, fare rate) uploaded to GCS bucket trigger a Cloud Function, which loads the corresponding dimension tables data into bigquery tables.
4. **Orchestration**: Airflow Composer schedules and manages the daily ETL workflows.
5. **Visualization**: BigQuery serves as the central data warehouse, and Looker Studio dashboards visualize aggregated metrics and geospatial insights.

---

## 🧱 Data Model

![Star Schema Data Model](https://github.com/user-attachments/assets/e6fdb004-d1f3-4666-80fc-441a5fe37fcd)

The data is structured following a star schema, optimized for analytical queries.

- **Fact Table** :  `trip_records` — stores individual trip details with foreign-key references to all dimensions.
- **Dimension Tables**:
  - `vendor_dim` : stores details about the taxi vendor
  - `calendar_dim` : stores temporal information related to the trip date (date_id, day, month, year).
  - `time_dim` : stores temporal information related to the trip time (time_id, hour, minute, second).
  - `taxi_zone_dim` : stores details about the pickup and dropoff locations (borough, zone, service zone).
  - `fare_rate_dim` : stores information about the fare type.
  - `payment_method_dim`: stores information about the payment type used.

---

## 🔧 Code Highlights

- **Airflow DAG** (`airflow_dags/taxi_pipeline_dag.py`)  
  - Schedules a daily workflow to pick up new CSV files from GCS, kick off Dataflow jobs, and bigquery jobs to load data into tables.

- **Dataflow Pipeline** (`dataflow_pipeline/`)  
  - `main.py`  
    Entrypoint for launching on DataflowRunner.  
  - `pipeline.py`  
    Beam transforms: parsing, filtering, date/time key enrichment, and loading transformed data into BigQuery.  

- **BigQuery DDL Scripts** (`bigquery_tables_creation_scripts/`)  
  - Defines schemas for:  
    - `trip_records` (fact table)  
    - `vendor_dim`, `fare_rate_dim`, `time_dim`, `calendar_dim`, `taxi_zone_dim`, `payment_method_dim`  

- **Dimension Data** (`dimension_tables_data/`)  
  - Static CSV files used by the Cloud Function to populate and update each dimension table.

- **Cloud Function** (`update_dim_tables_cloud_function/`)  
  - Fetches CSVs from GCS, transforms them into BigQuery load jobs, and load data into dimension tables.

- **Analytics SQL** (`data_analysis_queries/`)  
  - Pre-written queries for getting insights from data like —trip counts over time, fare distributions, zone-based analyses, etc.

---

> _All code is implemented in Python 3.x and leverages Google Cloud SDK, Apache Beam SDK, and the BigQuery client library._


---

## 📊 Looker Studio Dashboard

An interactive dashboard built in Looker Studio to explore and analyze NYC taxi trip data powered by BigQuery as the central data store.

The dashboard includes visualizations of key metrics and stat cards along with dynamic filters mentioned below :
 
- Fare Amount Distribution  
- Monthly Taxi Trip Volume  
- Weekday Trips by Day vs. Night  
- Trips by Passenger Count  
- Hourly Trip Volume  
- Pickup Density Heatmap  
- Top 10 Pickup and Dropoff Locations  

**Filters:** Date range | Pickup Service Zone | Dropoff Service Zone | Pickup Borough | Dropoff Borough  
**Stat cards:** Total Trips | Total Fare Amt. | Total Passengers | Avg Trip Duration | Avg Fare Amt. | Total Tip Amt.

![Dashboard Screenshot](https://github.com/user-attachments/assets/a300cf9e-b232-43c4-9969-182c70949f7d)

import functions_framework
import logging
import sys
from google.cloud import bigquery
from google.cloud import storage
import io
import csv

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def update_dimension_tables(cloud_event):
    # set logging level and format
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(levelname)s: %(message)s')
    data = cloud_event.data

    # event_id = cloud_event["id"]
    # event_type = cloud_event["type"]

    file_name = data["name"]
    updated_time = data["updated"]
    bucket_name = data['bucket']

    logging.info(f"File Name: {file_name}")
    logging.info(f"Updated Time: {updated_time}")
    logging.info(f"Bucket Name: {bucket_name}")


    # Process only CSV files.
    if not file_name.lower().endswith(".csv"):
        logging.info("File is not a CSV. Exiting.")
        return


    # BigQuery configuration
    project_id = "nyc-taxi-batch-dataflow" 
    dataset_id = "trip_data"  
    table_id = file_name.split(".")[0] 

    try:
        # Download the CSV file from Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        csv_content = blob.download_as_text()

        # Load CSV data into BigQuery
        client = bigquery.Client(project=project_id)
        table_ref = client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header row if present
            autodetect=True,  # Automatically detect schema
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE # overwrite data in bigquery table
        )

        load_job = client.load_table_from_file(
            io.StringIO(csv_content), table_ref, job_config=job_config
        )
        load_job.result()  # Wait for the job to complete

        logging.info(f"Loaded {load_job.output_rows} rows into {project_id}.{dataset_id}.{table_id}")

    except Exception as e:
        logging.error(f"Error processing file: {e}")

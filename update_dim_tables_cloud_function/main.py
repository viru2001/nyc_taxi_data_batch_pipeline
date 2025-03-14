import functions_framework
import logging
import sys
from google.cloud import bigquery
from google.cloud import storage
import json

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

    file_name_without_extension = file_name.split(".")[0] 

    # BigQuery configuration
    project_id = "nyc-taxi-batch-dataflow" 
    dataset_id = "trip_data"  
    table_id = file_name_without_extension
    schema_blob_path = f"schemas/{file_name_without_extension}.json"

    try:
        # Initialize BigQuery and Storage clients
        bq_client = bigquery.Client(project=project_id)
        storage_client = storage.Client(project=project_id)

         # Get the bucket and blob for the schema file
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(schema_blob_path)

        # Download and parse the schema JSON
        schema_json = blob.download_as_text()
        schema_fields = json.loads(schema_json)
        # Convert 'type' to 'field_type' for each field dictionary
        schema = []
        for field in schema_fields:
            # Rename key if it exists
            if 'type' in field:
                field['field_type'] = field.pop('type')
            schema.append(bigquery.SchemaField(**field))


        # get bigquery table ref
        table_ref = bq_client.dataset(dataset_id).table(table_id)

        # Construct the source URI
        source_uri = f"gs://{bucket_name}/{file_name}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header row if present
            schema=schema, #use defined schema.
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE # overwrite data in bigquery table
        )

        # Load CSV data into BigQuery
        load_job = bq_client.load_table_from_uri(source_uri, table_ref, job_config=job_config)
        load_job.result()  # Wait for the job to complete

        logging.info(f"Loaded {load_job.output_rows} rows into {project_id}.{dataset_id}.{table_id}")

    except Exception as e:
        logging.error(f"Error processing file: {e}")

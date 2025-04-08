from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

def download_and_execute_sql(bucket_name, object_name, **context):
    """Downloads SQL from GCS and executes it in BigQuery."""
    hook = GCSHook()
    try:
        sql_bytes = hook.download(bucket_name=bucket_name, object_name=object_name)
        sql_content = sql_bytes.decode('utf-8') if isinstance(sql_bytes, bytes) else sql_bytes

        bq_op = BigQueryInsertJobOperator(
            task_id='execute_bq_query',
            configuration={
                "query": {
                    "query": sql_content,
                    "useLegacySql": False,
                }
            },
            # use_legacy_sql=False,
        )
        bq_op.execute(context)

    except Exception as e:
        logging.error(f"Error: {e}")
        raise


# Set a start date in the past to allow backfilling if needed.
default_args = {
    "owner": "DE-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay ": timedelta(minutes=1)
}

with DAG(
    'taxi_data_etl_daily',
    default_args=default_args,
    # schedule_interval='0 */2 * * *',  # For testing: runs every 10 minutes.
    schedule_interval='@daily',       # For production: run once daily.
    catchup=False,
    description="DAG to  taxi data Dataflow pipeline with dynamic bus_date",
    params={
        # You can provide a default bus_date if desired; if not, it will fall back to the execution date.
        "bus_date": "2024-01-04",
        "zone":"us-central1-b"
    },
    start_date= datetime(2024, 1, 4),
) as dag:

    # Sensor: Wait for the CSV file matching the execution date (e.g., tripdata_2024-01-01.csv)
    raw_data_file_trigger = GCSObjectExistenceSensor(
        task_id='raw_data_file_trigger',
        bucket='nyc_taxi_raw_data',  # Replace with your bucket name.
        object='tripdata_{{ params.bus_date or ds }}.csv',
        timeout=60 * 60,   # Timeout after 1 hour.
        poke_interval=60,  # Check every 60 seconds.
    )

    # Dataflow Task: Run the Dataflow pipeline (pipeline.py) with the parameters.
    run_dataflow_pipeline = BeamRunPythonPipelineOperator(
        task_id='run_dataflow_pipeline_job',
        runner='DataflowRunner',
        py_file='gs://dataflow_pipeline_nyc_taxi/pipeline.py',  # Path to your pipeline.py file.
        pipeline_options={
            'project': 'nyc-taxi-batch-dataflow',
            'region': 'us-central1',
            # 'zone' : '{{ params.zone }}',
            'temp_location': 'gs://dataflow_pipeline_nyc_taxi/temp',
            'staging_location': 'gs://dataflow_pipeline_nyc_taxi/staging',
            # The job name is built using the execution date without dashes.
            'job_name': 'trip-records-data-load-{{ (params.bus_date or ds)|replace("-", "") }}',
            # Service account to run the job.
            'service_account_email': 'taxi-batch-data-dataflowrunner@nyc-taxi-batch-dataflow.iam.gserviceaccount.com',
            # The bus_date parameter is dynamically populated from the run date.
            'bus_date': '{{ params.bus_date or ds }}',
            # 'machine_type': 'n1-standard-4'
        },
        dataflow_config={
            'location': 'us-central1',
            'job_name': 'trip-records-data-load-{{ (params.bus_date or ds)|replace("-", "") }}'
        }
    )

    run_query_on_bigquery_table = PythonOperator(
        task_id='download_and_execute_bigquery_sql',
        python_callable=download_and_execute_sql,
        op_kwargs={
            'bucket_name': 'taxi_data_batch_pipeline_airflow', 
            'object_name': 'data/merge_time_and_calender_dim.sql',
        },
        provide_context=True,
    )


    raw_data_file_trigger >> run_dataflow_pipeline >> run_query_on_bigquery_table

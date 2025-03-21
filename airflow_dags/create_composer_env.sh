gcloud composer environments create taxi-data-pipeline-composer-env \
    --location=us-central1 \
    --service-account=taxi-data-airflow-runner@nyc-taxi-batch-dataflow.iam.gserviceaccount.com \
    --image-version=composer-3-airflow-2.10.2-build.10 \
    --storage-bucket=taxi_data_batch_pipeline_airflow
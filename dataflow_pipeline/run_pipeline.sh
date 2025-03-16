python pipeline.py \
  --runner=DataflowRunner \
  --project=nyc-taxi-batch-dataflow \
  --region=us-central1 \
  --temp_location=gs://dataflow_pipeline_nyc_taxi/temp \
  --staging_location=gs://dataflow_pipeline_nyc_taxi/staging \
  --job_name=trip-records-data-load \
  --service_account_email=taxi-batch-data-dataflowrunner@nyc-taxi-batch-dataflow.iam.gserviceaccount.com \
  --bus_date 2024-01-01
  # --zone=us-central1-a \

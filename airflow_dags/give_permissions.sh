SERVICE_ACCOUNT_EMAIL="taxi-data-airflow-runner@nyc-taxi-batch-dataflow.iam.gserviceaccount.com"
PROJECT_ID="nyc-taxi-batch-dataflow"

# Grant Composer worker role.
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
  --role="roles/composer.worker"

# Grant Cloud SQL Client role (for accessing the Composer metadata database).
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
  --role="roles/cloudsql.client"

# Grant Logging role (to write logs).
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
  --role="roles/logging.logWriter"

# Grant Monitoring role (to write metrics).
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
  --role="roles/monitoring.metricWriter"

# Grant Dataflow Admin role (to create/manage Dataflow jobs).
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
  --role="roles/dataflow.admin"

# Grant BigQuery Data Editor role
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
  --role="roles/bigquery.dataEditor"

# Grant BigQuery Job User role
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
  --role="roles/bigquery.jobUser"


# Grant Service Account User role (if impersonation of other service accounts is needed).
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
  --role="roles/iam.serviceAccountUser"

# Grant Storage Object Viewer role on the specific bucket.
gsutil iam ch serviceAccount:$SERVICE_ACCOUNT_EMAIL:roles/storage.objectViewer gs://taxi_data_batch_pipeline_airflow

# Grant Storage Object Creator role on the specific bucket.
gsutil iam ch serviceAccount:$SERVICE_ACCOUNT_EMAIL:roles/storage.objectCreator gs://taxi_data_batch_pipeline_airflow

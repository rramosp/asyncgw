# Terraform Outputs for Async Gateway

output "gateway_url" {
  description = "Public URL of the Cloud Run API Gateway and UI service"
  value       = google_cloud_run_v2_service.gateway.uri
}

output "pubsub_requests_topic" {
  description = "Pub/Sub Topic ID for incoming inference requests"
  value       = google_pubsub_topic.requests.id
}

output "pubsub_batch_items_topic" {
  description = "Pub/Sub Topic ID for decomposed batch items"
  value       = google_pubsub_topic.batch_items.id
}

output "pubsub_dlq_topic" {
  description = "Pub/Sub Dead Letter Queue Topic ID"
  value       = google_pubsub_topic.dlq.id
}

output "bigquery_dataset_id" {
  description = "BigQuery dataset ID for request tracking"
  value       = google_bigquery_dataset.metrics.dataset_id
}

output "bigquery_table_id" {
  description = "BigQuery partitioned table ID"
  value       = google_bigquery_table.request_tracker.table_id
}

output "gcs_bucket_name" {
  description = "Google Cloud Storage bucket name for responses"
  value       = google_storage_bucket.responses.name
}

output "gateway_service_account" {
  description = "Email of the Gateway service account"
  value       = google_service_account.gateway_sa.email
}

output "worker_service_account" {
  description = "Email of the Worker fleet service account"
  value       = google_service_account.worker_sa.email
}

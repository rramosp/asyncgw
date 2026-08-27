variable "project_id" {
  type        = string
  description = "Google Cloud Project ID"
  default     = "asyncgw-demo-project"
}

variable "region" {
  type        = string
  description = "Google Cloud Region for resources"
  default     = "us-central1"
}

variable "zone" {
  type        = string
  description = "Google Cloud Zone for resources"
  default     = "us-central1-a"
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev, staging, prod)"
  default     = "prod"
}

variable "pubsub_requests_topic_name" {
  type        = string
  description = "Primary Pub/Sub topic name for incoming inference requests"
  default     = "asyncgw-requests-topic"
}

variable "pubsub_batch_items_topic_name" {
  type        = string
  description = "Secondary Pub/Sub topic name for decomposed batch items"
  default     = "asyncgw-batch-items-topic"
}

variable "pubsub_dlq_topic_name" {
  type        = string
  description = "Pub/Sub Dead Letter Queue topic name"
  default     = "asyncgw-dlq-topic"
}

variable "bq_dataset_id" {
  type        = string
  description = "BigQuery dataset ID for request tracking metrics"
  default     = "asyncgw_metrics"
}

variable "bq_table_id" {
  type        = string
  description = "BigQuery partitioned table ID for request state"
  default     = "request_tracker"
}

variable "gcs_bucket_name" {
  type        = string
  description = "GCS bucket name for storing LLM responses and batch files"
  default     = "asyncgw-responses-storage"
}

variable "gcs_retention_days" {
  type        = number
  description = "Number of days before GCS response blobs are automatically purged"
  default     = 7
}

variable "container_image_gateway" {
  type        = string
  description = "Container image URI for the Gateway & UI service"
  default     = "us-central1-docker.pkg.dev/asyncgw-demo-project/asyncgw-docker/asyncgw-gateway:latest"
}

variable "container_image_worker" {
  type        = string
  description = "Container image URI for Cloud Run workers"
  default     = "us-central1-docker.pkg.dev/asyncgw-demo-project/asyncgw-docker/asyncgw-worker:latest"
}

variable "worker_min_instances" {
  type        = number
  description = "Minimum worker instances"
  default     = 1
}

variable "worker_max_instances" {
  type        = number
  description = "Maximum worker instances for scaling under high queue depth"
  default     = 50
}

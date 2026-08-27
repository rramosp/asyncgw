# BigQuery Dataset and Date-Partitioned Table for Async Gateway

resource "google_bigquery_dataset" "metrics" {
  delete_contents_on_destroy = true
  dataset_id                  = var.bq_dataset_id
  friendly_name               = "Async Gateway Metrics and Tracking"
  description                 = "Stores asynchronous LLM inference request states, sequence numbers, latencies, and metadata"
  location                    = var.region
  default_table_expiration_ms = var.gcs_retention_days * 86400 * 1000

  labels = {
    app = "asyncgw"
    env = var.environment
  }
}

resource "google_bigquery_table" "request_tracker" {
  deletion_protection = false
  dataset_id = google_bigquery_dataset.metrics.dataset_id
  table_id   = var.bq_table_id

  description = "Partitioned request tracking table for LLM inference requests and decomposed batch queries"

  time_partitioning {
    type          = "DAY"
    field         = "created_at"
    expiration_ms = var.gcs_retention_days * 86400 * 1000
  }

  clustering = ["status", "request_id", "parent_request_id"]

  schema = jsonencode([
    {
      name        = "request_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Unique ID for the request or batch item"
    },
    {
      name        = "parent_request_id"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Parent request ID if broken down from a batch"
    },
    {
      name        = "sequence_number"
      type        = "INT64"
      mode        = "NULLABLE"
      description = "Sequence order index within a decomposed batch (0..N-1)"
    },
    {
      name        = "total_items"
      type        = "INT64"
      mode        = "NULLABLE"
      description = "Total number of items in parent batch"
    },
    {
      name        = "custom_id"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Client provided custom_id per batch item"
    },
    {
      name        = "request_type"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "chat.completion, text.completion, embeddings, batch, batch.sub_request"
    },
    {
      name        = "status"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "PENDING, PROCESSING, COMPLETED, FAILED, TIMED_OUT, CANCELLED"
    },
    {
      name        = "model"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "LLM model requested (e.g. gemini-2.0-flash, gpt-4o)"
    },
    {
      name        = "max_wait_seconds"
      type        = "INT64"
      mode        = "NULLABLE"
      description = "User-specified maximum wait time before timeout"
    },
    {
      name        = "created_at"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Submission timestamp"
    },
    {
      name        = "expires_at"
      type        = "TIMESTAMP"
      mode        = "NULLABLE"
      description = "Deadline timestamp calculated from max_wait_seconds"
    },
    {
      name        = "started_at"
      type        = "TIMESTAMP"
      mode        = "NULLABLE"
      description = "Timestamp when worker began processing"
    },
    {
      name        = "completed_at"
      type        = "TIMESTAMP"
      mode        = "NULLABLE"
      description = "Timestamp when processing reached terminal state"
    },
    {
      name        = "elapsed_seconds"
      type        = "FLOAT64"
      mode        = "NULLABLE"
      description = "Total execution latency in seconds"
    },
    {
      name        = "backend_service_id"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "ID of backend service that executed request"
    },
    {
      name        = "backend_batch_service_mode"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "native, decomposed, or null"
    },
    {
      name        = "backend_endpoint"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Target API URL of backend"
    },
    {
      name        = "response_status_code"
      type        = "INT64"
      mode        = "NULLABLE"
      description = "HTTP status code returned from backend or gateway"
    },
    {
      name        = "response_content_length"
      type        = "INT64"
      mode        = "NULLABLE"
      description = "Payload byte size of response"
    },
    {
      name        = "response_gcs_uri"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "gs:// URI pointing to JSON response stored in GCS"
    },
    {
      name        = "error_message"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Detailed error message on failure or timeout"
    },
    {
      name        = "retry_count"
      type        = "INT64"
      mode        = "NULLABLE"
      description = "Number of retry / failover attempts"
    },
    {
      name        = "content_tokens"
      type        = "INT64"
      mode        = "NULLABLE"
      description = "Total tokens consumed by LLM completion"
    },
    {
      name        = "metadata_json"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "JSON serialized client metadata and tags"
    }
  ])

  labels = {
    app = "asyncgw"
    env = var.environment
  }
}

# Google Cloud Storage for LLM Response and Batch Payload Persistence

resource "google_storage_bucket" "responses" {
  name          = "${var.gcs_bucket_name}-${var.project_id}"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = var.gcs_retention_days # 7 days default
    }
    action {
      type = "Delete"
    }
  }

  cors {
    origin          = ["*"]
    method          = ["GET", "HEAD", "PUT", "POST"]
    response_header = ["*"]
    max_age_seconds = 3600
  }

  labels = {
    app = "asyncgw"
    env = var.environment
  }
}

# Cloud Run Services and Jobs for Async Gateway & Workers

# 1. API Gateway & UI Web Service
resource "google_cloud_run_v2_service" "gateway" {
  deletion_protection = false
  name     = "asyncgw-gateway"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = google_service_account.gateway_sa.email

    scaling {
      min_instance_count = 1
      max_instance_count = 20
    }

    containers {
      image = var.container_image_gateway
      args  = ["gateway"]

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "2"
          memory = "2Gi"
        }
      }

      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "GCP_LOCATION"
        value = var.region
      }
      env {
        name  = "PUBSUB_TOPIC_REQUESTS"
        value = google_pubsub_topic.requests.name
      }
      env {
        name  = "PUBSUB_TOPIC_BATCH_ITEMS"
        value = google_pubsub_topic.batch_items.name
      }
      env {
        name  = "BQ_DATASET"
        value = google_bigquery_dataset.metrics.dataset_id
      }
      env {
        name  = "BQ_TABLE"
        value = google_bigquery_table.request_tracker.table_id
      }
      env {
        name  = "GCS_BUCKET_NAME"
        value = google_storage_bucket.responses.name
      }
      env {
        name  = "ASYNCGW_ENV_MODE"
        value = "gcp"
      }
    }
  }

  labels = {
    app = "asyncgw"
    env = var.environment
  }
}

# 2. Continuous Worker Service for Auto-Scaling Primary and Batch Processing
resource "google_cloud_run_v2_service" "worker_service" {
  deletion_protection = false
  name     = "asyncgw-worker-fleet"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.worker_sa.email

    scaling {
      min_instance_count = var.worker_min_instances
      max_instance_count = var.worker_max_instances
    }

    containers {
      image = var.container_image_worker
      args  = ["worker-all"]

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "4"
          memory = "4Gi"
        }
        cpu_idle = false
      }

      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "GCP_LOCATION"
        value = var.region
      }
      env {
        name  = "PUBSUB_TOPIC_REQUESTS"
        value = google_pubsub_topic.requests.name
      }
      env {
        name  = "PUBSUB_SUB_REQUESTS"
        value = google_pubsub_subscription.requests_sub.name
      }
      env {
        name  = "PUBSUB_TOPIC_BATCH_ITEMS"
        value = google_pubsub_topic.batch_items.name
      }
      env {
        name  = "PUBSUB_SUB_BATCH_ITEMS"
        value = google_pubsub_subscription.batch_items_sub.name
      }
      env {
        name  = "BQ_DATASET"
        value = google_bigquery_dataset.metrics.dataset_id
      }
      env {
        name  = "BQ_TABLE"
        value = google_bigquery_table.request_tracker.table_id
      }
      env {
        name  = "GCS_BUCKET_NAME"
        value = google_storage_bucket.responses.name
      }
      env {
        name  = "ASYNCGW_ENV_MODE"
        value = "gcp"
      }
    }
  }

  labels = {
    app = "asyncgw"
    env = var.environment
  }
}

# 3. Cloud Run Jobs for Scheduled/Triggered Batch Execution
resource "google_cloud_run_v2_job" "primary_worker_job" {
  deletion_protection = false
  name     = "asyncgw-job-primary"
  location = var.region

  template {
    task_count = 5
    template {
      service_account = google_service_account.worker_sa.email

      containers {
        image = var.container_image_worker
        args  = ["worker-primary"]

        resources {
          limits = {
            cpu    = "2"
            memory = "2Gi"
          }
        }

        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "ASYNCGW_ENV_MODE"
          value = "gcp"
        }
      }
    }
  }
}

resource "google_cloud_run_v2_job" "batch_worker_job" {
  deletion_protection = false
  name     = "asyncgw-job-batch"
  location = var.region

  template {
    task_count = 10
    template {
      service_account = google_service_account.worker_sa.email

      containers {
        image = var.container_image_worker
        args  = ["worker-batch"]

        resources {
          limits = {
            cpu    = "2"
            memory = "2Gi"
          }
        }

        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "ASYNCGW_ENV_MODE"
          value = "gcp"
        }
      }
    }
  }
}

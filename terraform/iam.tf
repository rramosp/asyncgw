# IAM Service Accounts and Permissions for Async Gateway & Workers

# 1. Gateway Service Account
resource "google_service_account" "gateway_sa" {
  account_id   = "asyncgw-gateway-sa"
  display_name = "Async Gateway Service Account"
  description  = "Service account for the entrypoint Gateway API service"
}

# 2. Worker Fleet Service Account
resource "google_service_account" "worker_sa" {
  account_id   = "asyncgw-worker-sa"
  display_name = "Async Gateway Worker Service Account"
  description  = "Service account for Cloud Run jobs and worker instances"
}

# --- Gateway SA Permissions ---
resource "google_project_iam_member" "gateway_pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.gateway_sa.email}"
}

resource "google_project_iam_member" "gateway_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.gateway_sa.email}"
}

resource "google_project_iam_member" "gateway_bq_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.gateway_sa.email}"
}

resource "google_project_iam_member" "gateway_storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.gateway_sa.email}"
}

# --- Worker SA Permissions ---
resource "google_project_iam_member" "worker_pubsub_subscriber" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.worker_sa.email}"
}

resource "google_project_iam_member" "worker_pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.worker_sa.email}"
}

resource "google_project_iam_member" "worker_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.worker_sa.email}"
}

resource "google_project_iam_member" "worker_bq_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.worker_sa.email}"
}

resource "google_project_iam_member" "worker_storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.worker_sa.email}"
}

resource "google_project_iam_member" "worker_vertex_user" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.worker_sa.email}"
}

resource "google_project_iam_member" "gateway_ar_reader" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.gateway_sa.email}"
}

resource "google_project_iam_member" "worker_ar_reader" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.worker_sa.email}"
}

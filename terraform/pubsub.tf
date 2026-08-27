# Pub/Sub Topics & Subscriptions for Async Gateway

# Dead Letter Queue Topic
resource "google_pubsub_topic" "dlq" {
  name = var.pubsub_dlq_topic_name
  labels = {
    app = "asyncgw"
    env = var.environment
  }
}

resource "google_pubsub_subscription" "dlq_sub" {
  name  = "${var.pubsub_dlq_topic_name}-sub"
  topic = google_pubsub_topic.dlq.name

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s" # 7 days
}

# 1. Primary Requests Topic (Online requests & full batch envelopes)
resource "google_pubsub_topic" "requests" {
  name = var.pubsub_requests_topic_name
  labels = {
    app = "asyncgw"
    env = var.environment
  }
}

resource "google_pubsub_subscription" "requests_sub" {
  name  = "${var.pubsub_requests_topic_name}-sub"
  topic = google_pubsub_topic.requests.name

  ack_deadline_seconds       = 60
  message_retention_duration = "86400s" # 24 hours

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dlq.id
    max_delivery_attempts = 5
  }

  retry_policy {
    minimum_backoff = "1s"
    maximum_backoff = "60s"
  }
}

# 2. Secondary Topic for Decomposed Batch Sub-Requests
resource "google_pubsub_topic" "batch_items" {
  name = var.pubsub_batch_items_topic_name
  labels = {
    app = "asyncgw"
    env = var.environment
  }
}

resource "google_pubsub_subscription" "batch_items_sub" {
  name  = "${var.pubsub_batch_items_topic_name}-sub"
  topic = google_pubsub_topic.batch_items.name

  ack_deadline_seconds       = 60
  message_retention_duration = "86400s" # 24 hours

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dlq.id
    max_delivery_attempts = 5
  }

  retry_policy {
    minimum_backoff = "1s"
    maximum_backoff = "60s"
  }
}

# Operational Runbooks - GCP Asynchronous LLM Gateway

This document outlines standard operating procedures for operating, scaling, monitoring, and recovering the Asynchronous LLM Gateway in production.

---

## Runbook 1: High Queue Backlog & Worker Fleet Scaling

### Symptoms:
- `asyncgw-requests-sub` or `asyncgw-batch-items-sub` unacknowledged message count grows rapidly.
- P95 latency from submission to completion exceeds expected SLA.

### Triage:
1. Inspect Pub/Sub subscription metrics in Cloud Monitoring:
   ```bash
   gcloud monitoring dashboards list --filter="displayName:AsyncGW"
   ```
2. Check Cloud Run worker fleet CPU & concurrency:
   ```bash
   gcloud run services describe asyncgw-worker-fleet --region=us-central1 --format="value(status.conditions)"
   ```

### Remediation:
1. Increase maximum instance scaling limit on the Cloud Run Worker Fleet:
   ```bash
   gcloud run services update asyncgw-worker-fleet \
     --region=us-central1 \
     --max-instances=100 \
     --min-instances=10
   ```
2. Trigger auxiliary Cloud Run Jobs to parallelize batch processing:
   ```bash
   gcloud run jobs execute asyncgw-job-batch --region=us-central1 --tasks=25
   ```

---

## Runbook 2: Backend Provider Outage & Failover Management

### Symptoms:
- Backend endpoint (e.g. `gcp-provisioned-gemini`) returns persistent 500/503 errors or 429 Rate Limits.
- Health monitor circuit breaker trips, marking backend as UNHEALTHY.

### Automated Behavior:
- The `RoutingEngine` automatically routes new requests and retries existing requests on the next candidate in the preference order (e.g. `gemini-flex` or `openai-direct`).

### Manual Intervention (if required):
1. Test backend health using the admin API probe endpoint:
   ```bash
   curl -X POST https://api.asyncgw.example.com/v1/admin/backends/gcp-provisioned-gemini/probe \
     -H "X-API-Key: ${ADMIN_API_KEY}"
   ```
2. Update active policy to temporarily switch default strategy to `gemini-flex`:
   ```bash
   curl -X PUT https://api.asyncgw.example.com/v1/admin/policies \
     -H "Content-Type: application/json" \
     -H "X-API-Key: ${ADMIN_API_KEY}" \
     -d '{
       "default_policy": "latency_sensitive",
       "routing_strategies": [...],
       "content_rules": [...]
     }'
   ```

---

## Runbook 3: Dead Letter Queue (DLQ) Inspection & Reprocessing

### Symptoms:
- Messages present in `asyncgw-dlq-topic-sub`.

### Investigation:
1. Pull messages from the DLQ subscription without acknowledging:
   ```bash
   gcloud pubsub subscriptions pull asyncgw-dlq-topic-sub \
     --limit=5 \
     --auto-ack=false \
     --format=json
   ```
2. Inspect `dlq_reason` attribute and payload `request_id`.
3. Search BigQuery for error logs:
   ```sql
   SELECT request_id, status, error_message, created_at, retry_count
   FROM `asyncgw_metrics.request_tracker`
   WHERE status = 'FAILED'
   ORDER BY created_at DESC
   LIMIT 10;
   ```

### Replay:
After resolving underlying downstream outage, republish messages from DLQ back to `asyncgw-requests-topic`:
```bash
python -m asyncgw.tools.dlq_replayer --source-sub=asyncgw-dlq-topic-sub --target-topic=asyncgw-requests-topic
```

---

## Runbook 4: BigQuery Partitioning & Storage Maintenance

### Verification:
Ensure partition expiration and table clustering are functioning properly:
```bash
bq show --schema --format=prettyjson asyncgw_metrics.request_tracker
```

### Partition Query Best Practices:
Always include `DATE(created_at)` filter to prune partition scans and minimize query costs:
```sql
SELECT
  DATE(created_at) as req_date,
  backend_service_id,
  COUNT(1) as total_queries,
  AVG(elapsed_seconds) as avg_latency_sec,
  SUM(content_tokens) as total_tokens
FROM `asyncgw_metrics.request_tracker`
WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY 1, 2
ORDER BY 1 DESC;
```

---

## Runbook 5: Configuration Drift Detection and Sync

Run the drift check script:
```bash
./check_infra.sh
```
If drift is detected:
```bash
cd terraform
terraform plan -out=tfplan
terraform apply tfplan
```

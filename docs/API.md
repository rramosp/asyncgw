# Asynchronous LLM Gateway - API Reference

The Gateway exposes an OpenAI-compatible REST API. All inference requests are enqueued and processed asynchronously. Responses are persisted in Google Cloud Storage for 7 days.

---

## 1. Inference Endpoints

### 1.1. Submit Chat Completion
`POST /v1/chat/completions`

Accepts standard OpenAI chat completion payloads. Immediately returns `202 Accepted` with a `request_id` and URLs for polling status and fetching results.

#### Headers:
- `X-API-Key` (string, optional/required by Apigee): Client authentication key.
- `X-Max-Wait-Seconds` (integer, optional): Maximum time in seconds before expiring the request.
- `X-Routing-Override` (string, optional): Specific backend ID override (e.g. `gcp-provisioned-gemini`).

#### Request Body Example:
```json
{
  "model": "gemini-2.0-flash",
  "messages": [
    {
      "role": "system",
      "content": "You are a helpful GCP cloud architect."
    },
    {
      "role": "user",
      "content": "Explain how BigQuery date partitioning optimizes query billing."
    }
  ],
  "temperature": 0.7,
  "max_tokens": 1024,
  "max_wait_seconds": 120,
  "priority": "normal",
  "tags": {
    "app_id": "analytics-service",
    "cost_center": "engineering"
  }
}
```

#### Response (202 Accepted):
```json
{
  "request_id": "req_63f92b7c4d814234ba44a613d7890a12",
  "status": "PENDING",
  "created_at": "2026-08-26T10:15:00Z",
  "status_url": "/v1/requests/req_63f92b7c4d814234ba44a613d7890a12",
  "response_url": "/v1/requests/req_63f92b7c4d814234ba44a613d7890a12/response",
  "max_wait_seconds": 120,
  "model": "gemini-2.0-flash",
  "message": "Chat completion request enqueued for asynchronous processing"
}
```

---

### 1.2. Submit Batch Request
`POST /v1/batches`

Accepts bulk inference requests. If the chosen backend does not support bulk batching (e.g. Gemini Flex), the Gateway automatically splits the batch into individual sub-requests and reassembles them in sequence order.

#### Request Body Example:
```json
{
  "endpoint": "/v1/chat/completions",
  "completion_window": "24h",
  "max_wait_seconds": 600,
  "requests": [
    {
      "custom_id": "doc-chunk-01",
      "method": "POST",
      "url": "/v1/chat/completions",
      "body": {
        "model": "gemini-2.0-flash",
        "messages": [{"role": "user", "content": "Summarize paragraph 1."}]
      }
    },
    {
      "custom_id": "doc-chunk-02",
      "method": "POST",
      "url": "/v1/chat/completions",
      "body": {
        "model": "gemini-2.0-flash",
        "messages": [{"role": "user", "content": "Summarize paragraph 2."}]
      }
    }
  ]
}
```

#### Response (202 Accepted):
```json
{
  "request_id": "batch_a98f12c44e994112",
  "batch_id": "batch_a98f12c44e994112",
  "status": "PENDING",
  "created_at": "2026-08-26T10:15:00Z",
  "status_url": "/v1/batches/batch_a98f12c44e994112",
  "response_url": "/v1/batches/batch_a98f12c44e994112/output",
  "total_items": 2,
  "message": "Batch request accepted and enqueued for asynchronous processing"
}
```

---

## 2. Polling & Result Retrieval Endpoints

### 2.1. Get Request Status
`GET /v1/requests/{request_id}`

#### Response Example (200 OK):
```json
{
  "request_id": "req_63f92b7c4d814234ba44a613d7890a12",
  "status": "COMPLETED",
  "model": "gemini-2.0-flash",
  "request_type": "chat.completion",
  "created_at": "2026-08-26T10:15:00Z",
  "started_at": "2026-08-26T10:15:01Z",
  "completed_at": "2026-08-26T10:15:03Z",
  "elapsed_seconds": 2.14,
  "backend_service_id": "gcp-provisioned-gemini",
  "response_status_code": 200,
  "response_gcs_uri": "gs://asyncgw-responses-storage/responses/req_63f92b7c4d814234ba44a613d7890a12.json",
  "content_tokens": 348,
  "retry_count": 0
}
```

---

### 2.2. Retrieve Response Payload
`GET /v1/requests/{request_id}/response`

#### Status Returns:
- `200 OK`: Response is ready. Returns full OpenAI completion payload.
- `202 Accepted`: Request is still pending or processing in the queue.
- `408 Request Timeout`: Request exceeded `max_wait_seconds` deadline.
- `500 Internal Server Error`: Backend execution failed.

#### Successful Payload (200 OK):
```json
{
  "id": "chatcmpl-gemini-1724667302",
  "object": "chat.completion",
  "created": 1724667302,
  "model": "gemini-2.0-flash",
  "choices": [
    {
      "index": 0,
      "message": {
        "role": "assistant",
        "content": "BigQuery partition pruning significantly reduces byte scan volumes..."
      },
      "finish_reason": "stop"
    }
  ],
  "usage": {
    "prompt_tokens": 42,
    "completion_tokens": 306,
    "total_tokens": 348
  }
}
```

---

### 2.3. Retrieve Batch Output
`GET /v1/batches/{batch_id}/output`

Returns aggregated batch output with pagination control via `max_batch_items_in_api` (configured in `config/asyncgw.yaml`).

#### Response Example (200 OK):
```json
{
  "id": "batch_a98f12c44e994112",
  "object": "batch",
  "endpoint": "/v1/chat/completions",
  "status": "COMPLETED",
  "backend_service_id": "gcp-provisioned-gemini",
  "backend_batch_service_mode": "native",
  "total_items": 250,
  "returned_items": 100,
  "results_uri": "gs://asyncgw-responses-storage/responses/batch_a98f12c44e994112.json",
  "results": [
    {
      "id": "batch_req_0",
      "custom_id": "doc-chunk-01",
      "response": {
        "status_code": 200,
        "body": { ... }
      },
      "error": null
    }
  ]
}
```

*Note:* If running locally, `results_uri` will point to the local download URL `/v1/batches/{batch_id}/download`.

---

### 2.4. Download Complete Batch Output
`GET /v1/batches/{batch_id}/download`

Downloads the complete untruncated batch output JSON file as an attachment (`Content-Disposition: attachment; filename="{batch_id}_results.json"`).

---

## 3. Admin & Configuration Endpoints

- `GET /v1/admin/backends`: List all configured backend services and their live health status.
- `POST /v1/admin/backends/{backend_id}/probe`: Trigger an immediate live health probe on an endpoint.
- `GET /v1/admin/policies`: View active routing policies and content rules.
- `PUT /v1/admin/policies`: Update routing policies dynamically.
- `GET /v1/admin/requests`: List recently tracked requests with optional `?status=` and `?limit=` filters.
- `GET /v1/admin/stats`: Get system summary metrics and status breakdowns.

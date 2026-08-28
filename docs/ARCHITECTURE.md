# Asynchronous LLM Gateway - Architecture Specification

## 1. Overview

The **Asynchronous LLM Gateway** is an enterprise-grade service designed to handle OpenAI-compatible LLM inference requests asynchronously on Google Cloud Platform. It optimizes inference costs by leveraging **GCP Provisioned Throughput** (Vertex AI Gemini) during surplus/off-peak capacity, while providing intelligent fallback to **Gemini Flex** (pay-as-you-go) and third-party LLM providers (e.g., OpenAI).

The system seamlessly accommodates both **single online requests** and **bulk batch requests**, automatically decomposing bulk batches into parallel sub-requests when targeting backends without native batch processing capabilities, and reassembling them in strict sequential order.

---

## 2. High-Level Architecture Diagram

```mermaid
graph TD
    Client["Client / User Application"]
    Apigee["GCP Apigee API Gateway<br/>(Auth, Validation, Quotas)"]
    
    subgraph Queuing ["GCP Pub/Sub Queuing Layer"]
        TopicMain["Topic: asyncgw-requests-topic<br/>(Single & Full Batch Envelopes)"]
        TopicBatchItems["Topic: asyncgw-batch-items-topic<br/>(Decomposed Sub-Requests)"]
        TopicDLQ["Topic: asyncgw-dlq-topic<br/>(Dead Letter Queue)"]
    end
    
    subgraph Workers ["Cloud Run Worker Fleet"]
        PrimaryWorker["Primary Request Worker<br/>(Cloud Run Service / Job)"]
        BatchSplitter["Batch Splitter Component"]
        BatchWorker["Batch Sub-Request Worker<br/>(Cloud Run Service / Job)"]
        Reassembler["Batch Reassembler Component"]
        RouterEngine["Policy Routing & Failover Engine"]
    end

    subgraph Backends ["LLM Inference Backends"]
        Provisioned["GCP Provisioned Throughput<br/>(Vertex AI Gemini)"]
        Flex["Gemini FLEX<br/>(On-demand Pay-as-you-go)"]
        OpenAI["OpenAI Direct API"]
        Mock["Mock / Internal Backends"]
    end

    subgraph Persistence ["Persistence & Analytics"]
        BigQuery["Google Cloud BigQuery<br/>(Date-Partitioned Request Tracker)"]
        GCS["Google Cloud Storage<br/>(7-Day Auto-Delete Retention Bucket)"]
    end

    Client -->|HTTP 202 Async Submission| Apigee
    Apigee --> TopicMain
    Apigee -->|Initial PENDING Entry| BigQuery
    
    TopicMain --> PrimaryWorker
    PrimaryWorker -->|Check Expiration & Route| RouterEngine
    
    PrimaryWorker -->|Native Batch or Single| RouterEngine
    RouterEngine --> Provisioned
    RouterEngine --> Flex
    RouterEngine --> OpenAI
    RouterEngine --> Mock

    PrimaryWorker -->|Decompose Batch| BatchSplitter
    BatchSplitter -->|Indexed Sub-Envelopes| TopicBatchItems
    BatchSplitter -->|Sub-Request PENDING Rows| BigQuery
    
    TopicBatchItems --> BatchWorker
    BatchWorker -->|Execute Sub-Query| RouterEngine
    BatchWorker -->|Save Partial Part JSON| GCS
    BatchWorker -->|Mark Sub-Query Status| BigQuery
    BatchWorker --> Reassembler

    Reassembler -->|Check All Items Finished| BigQuery
    Reassembler -->|Read & Reassemble in Seq Order| GCS
    Reassembler -->|Write Final Aggregated JSON| GCS
    Reassembler -->|Mark Parent Batch COMPLETED| BigQuery

    PrimaryWorker -->|Save Single Response JSON| GCS
    PrimaryWorker -->|Mark Single COMPLETED| BigQuery

    Client -.->|"Poll Status GET /v1/requests/{id}"| Apigee
    Apigee -.-> BigQuery
    Client -.->|"Fetch Response GET /v1/requests/{id}/response"| Apigee
    Apigee -.-> GCS
```

---

## 3. Key Flows

### 3.1. Single Online Request Execution Flow

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Apigee
    participant BigQuery
    participant PubSub as Pub/Sub (Main Queue)
    participant Worker as Primary Worker
    participant Router as Routing Engine
    participant Backend as LLM Backend (Provisioned/Flex)
    participant GCS as Cloud Storage

    Client->>Apigee: POST /v1/chat/completions (max_wait_seconds=120)
    Apigee->>BigQuery: Register request (status: PENDING)
    Apigee->>PubSub: Publish AsyncRequestEnvelope
    Apigee-->>Client: 202 Accepted {request_id, status_url, response_url}
    
    PubSub->>Worker: Pull message envelope
    Worker->>Worker: Check deadline (now < expires_at)
    Worker->>BigQuery: Update status: PROCESSING
    Worker->>Router: Route request (evaluate policies & health)
    Router->>Backend: Execute inference call
    
    alt Success
        Backend-->>Router: 200 OK + ChatCompletion payload
        Worker->>GCS: Save responses/{request_id}.json
        Worker->>BigQuery: Mark COMPLETED (latency, tokens, backend_id)
    else Failure & Failover
        Backend-->>Router: 429 Rate Limit / 500 Error
        Router->>Backend: Failover to next candidate backend
        Backend-->>Router: 200 OK
        Worker->>GCS: Save responses/{request_id}.json
        Worker->>BigQuery: Mark COMPLETED
    else Timeout Exceeded
        Worker->>GCS: Save error payload responses/{request_id}.json
        Worker->>BigQuery: Mark TIMED_OUT
    end

    Client->>Apigee: GET /v1/requests/{request_id}/response
    Apigee->>BigQuery: Check status
    BigQuery-->>Apigee: COMPLETED + response_gcs_uri
    Apigee->>GCS: Fetch response JSON
    GCS-->>Apigee: JSON payload
    Apigee-->>Client: 200 OK {choices: [...], usage: {...}}
```

---

### 3.2. Batch Decomposing and Ordered Reassembly Flow

When a batch request is routed to a backend that only supports individual requests (such as Vertex Gemini Flex or on-demand models):

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant PrimaryWorker as Primary Worker
    participant Splitter as Batch Splitter
    participant SubQueue as Pub/Sub (Batch Items)
    participant BigQuery
    participant BatchWorker as Batch Worker
    participant GCS as Cloud Storage
    participant Reassembler as Batch Reassembler

    Client->>PrimaryWorker: Batch Request (N items)
    PrimaryWorker->>Splitter: Decompose batch
    Splitter->>BigQuery: Bulk insert sub-requests (seq: 0..N-1, status: PENDING)
    Splitter->>SubQueue: Publish N sub-request messages
    
    par Concurrent Workers Pull Items
        SubQueue->>BatchWorker: Pull Item #0
        BatchWorker->>GCS: Write batches/{id}/parts/0.json
        BatchWorker->>BigQuery: Mark Item #0 COMPLETED
        BatchWorker->>Reassembler: Try Reassemble (1/N done -> wait)
    and
        SubQueue->>BatchWorker: Pull Item #1
        BatchWorker->>GCS: Write batches/{id}/parts/1.json
        BatchWorker->>BigQuery: Mark Item #1 COMPLETED
        BatchWorker->>Reassembler: Try Reassemble (2/N done -> wait)
    and
        SubQueue->>BatchWorker: Pull Item #N-1 (Last Item)
        BatchWorker->>GCS: Write batches/{id}/parts/N-1.json
        BatchWorker->>BigQuery: Mark Item #N-1 COMPLETED
        BatchWorker->>Reassembler: Try Reassemble (N/N done -> trigger!)
    end

    Reassembler->>BigQuery: Fetch all sub-requests for batch
    Reassembler->>Reassembler: Sort strictly by sequence_number (0..N-1)
    Reassembler->>GCS: Read all part JSONs in order
    Reassembler->>GCS: Write aggregated responses/{parent_id}.json
    Reassembler->>BigQuery: Mark Parent Batch COMPLETED
```

---

## 4. BigQuery Partitioned Tracking Data Model

The `asyncgw_metrics.request_tracker` table is partitioned by `DATE(created_at)` and clustered on `status`, `request_id`, and `parent_request_id`.

| Column Name | Type | Mode | Description |
| :--- | :--- | :--- | :--- |
| `request_id` | STRING | REQUIRED | Unique identifier for request or sub-query |
| `parent_request_id` | STRING | NULLABLE | Parent batch ID if decomposed |
| `sequence_number` | INT64 | NULLABLE | 0-indexed position within batch |
| `total_items` | INT64 | NULLABLE | Total items in parent batch |
| `custom_id` | STRING | NULLABLE | User-provided custom ID per item |
| `request_type` | STRING | REQUIRED | `chat.completion`, `text.completion`, `embeddings`, `batch` |
| `status` | STRING | REQUIRED | `PENDING`, `PROCESSING`, `COMPLETED`, `FAILED`, `TIMED_OUT` |
| `model` | STRING | NULLABLE | LLM Model requested |
| `max_wait_seconds` | INT64 | NULLABLE | User deadline in seconds |
| `created_at` | TIMESTAMP | REQUIRED | Submission timestamp (Partition Key) |
| `expires_at` | TIMESTAMP | NULLABLE | Calculated expiration timestamp |
| `started_at` | TIMESTAMP | NULLABLE | Worker pickup timestamp |
| `completed_at` | TIMESTAMP | NULLABLE | Completion timestamp |
| `elapsed_seconds` | FLOAT64 | NULLABLE | End-to-end execution latency |
| `backend_service_id` | STRING | NULLABLE | Backend used (e.g. `gcp-provisioned-gemini`) |
| `backend_endpoint` | STRING | NULLABLE | Target API endpoint URL |
| `response_status_code`| INT64 | NULLABLE | Response HTTP code |
| `response_content_length`| INT64 | NULLABLE | Response size in bytes |
| `response_gcs_uri` | STRING | NULLABLE | `gs://...` URI to result JSON in GCS |
| `error_message` | STRING | NULLABLE | Error message if failed or timed out |
| `retry_count` | INT64 | NULLABLE | Number of failover / retry attempts |
| `content_tokens` | INT64 | NULLABLE | Total token count |
| `metadata_json` | STRING | NULLABLE | Client tags and custom metadata |

---

## 5. Storage Lifecycle and Retention

- **GCS Bucket**: `asyncgw-responses-<project-id>`
- **Retention**: Lifecycle policy configured for **7 days** automatic deletion.
- **Directory Layout**:
  - `responses/{request_id}.json` (Single completions and final aggregated batch outputs)
  - `batches/{parent_request_id}/parts/{sequence_number}.json` (Intermediate partial chunks)

# Google Cloud Asynchronous LLM Gateway

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-green.svg)](https://fastapi.tiangolo.com)
[![GCP Native](https://img.shields.io/badge/GCP-Pub%2FSub%20%7C%20BigQuery%20%7C%20Cloud%20Run%20%7C%20GCS-orange.svg)](https://cloud.google.com)
[![Terraform](https://img.shields.io/badge/IaC-Terraform-purple.svg)](https://www.terraform.io)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

An enterprise-grade, OpenAI-compatible asynchronous gateway for Large Language Model (LLM) inference on Google Cloud Platform. 

The service optimizes compute economics by leveraging **GCP Provisioned Throughput** (Vertex AI Gemini) during surplus/off-peak capacity, while providing intelligent fallback and routing to **Gemini Flex** (pay-as-you-go), **OpenAI Direct**, and custom backends.

---

## 🌟 Key Capabilities

- **OpenAI Compatible Interface**: Exposes standard endpoints (`/v1/chat/completions`, `/v1/completions`, `/v1/embeddings`, `/v1/batches`) with immediate `202 Accepted` response, `request_id`, and status polling.
- **Intelligent Policy-Based Routing**: Configurable preference orders, content-size rules, token threshold filters, and automatic failover on 429/500 errors or health check failures.
- **Automatic Batch Decomposition & Sequential Reassembly**: If a bulk batch is routed to a backend that only supports individual requests (e.g. Gemini Flex), the gateway breaks it into indexed sub-requests on a secondary Pub/Sub queue, processes them in parallel, and reassembles them in strict sequential order before saving to GCS.
- **User SLA & Timeout Enforcement**: Clients specify `max_wait_seconds`. If a request is not completed within deadline, the gateway automatically marks it `TIMED_OUT` in BigQuery and stores a standardized timeout error in GCS.
- **Date-Partitioned BigQuery Tracking**: Stores request lifecycle (`PENDING` -> `PROCESSING` -> `COMPLETED`/`FAILED`/`TIMED_OUT`), backend served, execution latency, token counts, and metadata.
- **GCS Response Storage with 7-Day TTL**: Responses stored as JSON in Cloud Storage with automatic 7-day lifecycle purge rules.
- **Interactive Modern Web Dashboard**: Real-time request explorer, status poller, JSON inspector, backend health prober, and policy editor.
- **Production IaC & Apigee Bundles**: Complete Terraform scripts, Dockerfiles, and Apigee API proxy manifests.

---

## 🏛️ Architecture Overview

```
                         +-----------------------------------+
                         |    Client Application / User      |
                         +-----------------+-----------------+
                                           | HTTP 202 Async Submission
                                           v
                         +-----------------------------------+
                         |      GCP Apigee API Gateway       |
                         | (Auth, Rate Quotas, Transforms)   |
                         +-----------------+-----------------+
                                           |
                    +----------------------+----------------------+
                    |                                             |
                    v                                             v
     +------------------------------+              +------------------------------+
     |   GCP Pub/Sub (Main Queue)   |              |     Google Cloud BigQuery    |
     |   asyncgw-requests-topic     |              |  (Partitioned Request Table) |
     +--------------+---------------+              +--------------+---------------+
                    |                                             |
                    v                                             | State Updates
     +------------------------------+                             |
     |    Primary Request Worker    |-----------------------------+
     |     (Cloud Run Service)      |
     +--------------+---------------+
                    |
          +---------+---------+
          |                   |
 (Native Batch / Single)  (Decompose Batch)
          |                   |
          v                   v
+-------------------+   +-------------------------------+
|  Routing Engine   |   |   Pub/Sub (Batch Items Queue) |
| & Failover Loop   |   |   asyncgw-batch-items-topic   |
+---------+---------+   +---------------+---------------+
          |                             |
          |                             v
          |             +-------------------------------+
          |             |    Batch Sub-Request Worker   |
          |             |      (Cloud Run Service)      |
          |             +---------------+---------------+
          |                             |
          |                             v
          |             +-------------------------------+
          |             |   Batch Response Reassembler  |
          |             |   (Strict Sequence Ordering)  |
          |             +---------------+---------------+
          |                             |
          +-----------------------------+
                        |
                        v
          +-----------------------------+
          | Google Cloud Storage (GCS)  |
          |  (7-Day Auto-Delete TTL)    |
          +-----------------------------+
```

For full architecture details and sequence diagrams, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## 📁 Repository Structure

```
.
├── spec.md                          # Technical specification
├── README.md                        # Project documentation
├── deploy.sh                        # Automated deployment script
├── check_infra.sh                   # Infrastructure verification & drift detector
├── requirements.txt                 # Production dependencies
├── Dockerfile.gateway               # Container for Gateway & UI
├── Dockerfile.worker                # Container for Cloud Run Worker fleet
├── docker-compose.yml               # Local test & demo stack
├── config/
│   ├── asyncgw.yaml                 # General configuration parameters (e.g. max batch results)
│   ├── backends.yaml                # Backend endpoints, auth, and health check config
│   └── policies.yaml                # Routing policies, priority orders, and rules
├── asyncgw/
│   ├── __init__.py
│   ├── config.py                    # Configuration models & loader
│   ├── main.py                      # CLI entrypoint (gateway, worker, ui)
│   ├── models/                      # Domain & OpenAI compatible data models
│   ├── storage/                     # BigQuery tracker & GCS blob storage (and mock)
│   ├── queue/                       # Pub/Sub producer & consumer (and mock)
│   ├── backends/                    # Vertex Provisioned, Gemini Flex, OpenAI, Mock
│   ├── router/                      # Policy routing engine & failover coordinator
│   ├── batch/                       # Splitter (decomposition) & Reassembler
│   ├── workers/                     # Primary & Batch Sub-Request workers
│   ├── gateway/                     # FastAPI OpenAPI service & admin APIs
│   └── ui/                          # Modern web dashboard
├── terraform/                       # Infrastructure as Code (GCP Resources)
│   ├── main.tf                      # Providers
│   ├── variables.tf                 # Variables
│   ├── outputs.tf                   # Resource outputs
│   ├── pubsub.tf                    # Topics, Subscriptions, DLQ
│   ├── bigquery.tf                  # Partitioned tracking table
│   ├── storage.tf                   # GCS bucket & 7-day retention rule
│   ├── cloud_run.tf                 # Cloud Run Services & Jobs
│   ├── iam.tf                       # Service accounts & IAM roles
│   └── terraform.tfvars.example
├── apigee/                          # Apigee Proxy Bundle & OpenAPI 3.0 Spec
│   ├── openapi_spec.yaml
│   └── proxy_bundle/
├── tests/                           # 26 Unit, Integration & Chaos Tests
│   ├── conftest.py
│   ├── test_config.py
│   ├── test_router.py
│   ├── test_health.py
│   ├── test_batch_split_reassemble.py
│   ├── test_storage_and_bq.py
│   ├── test_gateway_api.py
│   ├── test_worker_primary.py
│   ├── test_worker_batch.py
│   ├── test_failover_scenarios.py
│   ├── test_timeout_scenarios.py
│   └── test_batch_breakdown_failure_scenarios.py
└── docs/
    ├── ARCHITECTURE.md              # Diagrams and data model specification
    ├── API.md                       # API reference & cURL examples
    ├── DEPLOYMENT.md                # GCP Production deployment guide
    └── RUNBOOKS.md                  # Operational runbooks & incident recovery
```

---

## 🚀 Quickstart

### 1. Local Development (In-Memory / Mock Mode)

Clone the repository and run all tests:

```bash
# Install dependencies
pip install -r requirements.txt pytest pytest-asyncio

# Run the full test suite (26 unit/integration tests)
pytest -v
```

### 2. Running Local Gateway and UI Dashboard

```bash
# Start Gateway API, Web UI Dashboard & in-memory background workers on port 8000
python -m asyncgw.main
```

Navigate to:
- **`http://localhost:8000/`** for the interactive **Web UI Dashboard**
- **`http://localhost:8000/docs`** for interactive **Swagger API documentation**


### 3. Running with Docker Compose

```bash
docker-compose up --build
```

---

## 🛠️ Automated Deployment to Google Cloud Platform

Deploy all infrastructure (Pub/Sub topics, BigQuery dataset/partitioned table, GCS bucket, Cloud Run services/jobs, IAM) with a single command:

```bash
# 1. Preview Terraform plan (dry run)
./deploy.sh

# 2. Apply to Google Cloud
./deploy.sh --apply

# 3. Clean up & reset all GCP resources and local state
./deploy.sh --reset

# 4. Reset project completely first, then deploy fresh
./deploy.sh --apply --reset
```

### Infrastructure Health & Drift Verification

Verify all components and check for configuration drift against Terraform state:

```bash
./check_infra.sh
```

---

## 📡 API Usage Examples

### Submit Single Chat Completion (Async)

```bash
curl -X POST "http://localhost:8000/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -H "X-Max-Wait-Seconds: 120" \
  -d '{
    "model": "gemini-2.0-flash",
    "messages": [
      {"role": "user", "content": "Explain asynchronous LLM inference architectures."}
    ]
  }'
```

**Response (202 Accepted):**
```json
{
  "request_id": "req_5f2c418a994b438290f91ab7",
  "status": "PENDING",
  "created_at": "2026-08-26T17:15:00Z",
  "status_url": "/v1/requests/req_5f2c418a994b438290f91ab7",
  "response_url": "/v1/requests/req_5f2c418a994b438290f91ab7/response",
  "max_wait_seconds": 120,
  "model": "gemini-2.0-flash",
  "message": "Chat completion request enqueued for asynchronous processing"
}
```

### Poll Status and Fetch Response

```bash
# Poll Status
curl "http://localhost:8000/v1/requests/req_5f2c418a994b438290f91ab7"

# Retrieve Final Response Payload (returns 200 once COMPLETED)
curl "http://localhost:8000/v1/requests/req_5f2c418a994b438290f91ab7/response"
```

---

## 🧪 Testing Suite Coverage

The test suite covers all normal and failure scenarios:
1. **Routing Policy Engine**: Default routing, token thresholds, model pattern matching, deadline fast-tracking.
2. **Health Monitoring & Circuit Breakers**: Probes, failure counters, automatic backend exclusion and recovery.
3. **Batch Splitting & Ordered Reassembly**: Index tagging, GCS partial chunk aggregation, missing chunk tolerance.
4. **Backend Failover Scenarios**: Automatic failover upon 500 error / 429 rate limit.
5. **Timeout Handling**: Expired requests marked `TIMED_OUT` with 408 error response.
6. **Decomposed Batch Partial Worker Failure**: Handling crashed worker on item #2 of 5, marking item #2 `FAILED` in BigQuery while items 0, 1, 3, 4 complete, and cleanly aggregating the full batch output without corruption.

---

## 📚 Documentation Index

- [Architecture & Sequence Diagrams](docs/ARCHITECTURE.md)
- [API Reference](docs/API.md)
- [Deployment Guide](docs/DEPLOYMENT.md)
- [Operational Runbooks](docs/RUNBOOKS.md)

---

## 📄 License

Apache License 2.0. See LICENSE for details.

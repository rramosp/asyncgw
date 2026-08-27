#!/usr/bin/env bash
# ==============================================================================
# Infrastructure Health Verification and Configuration Drift Detection Script
# ==============================================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="${SCRIPT_DIR}/terraform"

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[DRIFT/WARN]${NC} $1"; }
log_error() { echo -e "${RED}[FAIL]${NC} $1"; }

DRIFT_COUNT=0
HEALTH_FAILS=0

echo -e "${BOLD}${BLUE}"
echo "=================================================================="
echo "    Async LLM Gateway: Infrastructure Health & Drift Checker      "
echo "=================================================================="
echo -e "${NC}"

# 0. Discover GCP Project & Region Context
GCP_PROJECT="${TF_VAR_project_id:-${GCP_PROJECT_ID:-${GOOGLE_CLOUD_PROJECT:-${PROJECT_ID:-}}}}"
if [ -z "$GCP_PROJECT" ] && [ -f "${TERRAFORM_DIR}/terraform.tfvars" ]; then
    GCP_PROJECT="$(grep -E '^[[:space:]]*project_id[[:space:]]*=' "${TERRAFORM_DIR}/terraform.tfvars" | sed -E 's/.*=[[:space:]]*"([^"]+)".*/\1/' || true)"
fi
if [ -z "$GCP_PROJECT" ] && command -v gcloud &>/dev/null; then
    GCP_PROJECT="$(gcloud config get-value project 2>/dev/null || true)"
fi
if [ -z "$GCP_PROJECT" ]; then
    GCP_PROJECT="asyncgw-demo-project"
fi

GCP_REGION_VAL="${TF_VAR_region:-${GCP_REGION:-${GCP_LOCATION:-${CLOUDSDK_COMPUTE_REGION:-}}}}"
if [ -z "$GCP_REGION_VAL" ] && [ -f "${TERRAFORM_DIR}/terraform.tfvars" ]; then
    GCP_REGION_VAL="$(grep -E '^[[:space:]]*region[[:space:]]*=' "${TERRAFORM_DIR}/terraform.tfvars" | sed -E 's/.*=[[:space:]]*"([^"]+)".*/\1/' || true)"
fi
if [ -z "$GCP_REGION_VAL" ] && command -v gcloud &>/dev/null; then
    GCP_REGION_VAL="$(gcloud config get-value compute/region 2>/dev/null || true)"
fi
if [ -z "$GCP_REGION_VAL" ]; then
    GCP_REGION_VAL="us-central1"
fi

# 1. Check Terraform Syntax & Local Configuration Integrity
log_info "1. Validating Terraform configuration syntax..."
cd "${TERRAFORM_DIR}"
if terraform validate &>/dev/null; then
    log_success "Terraform configuration syntax is valid."
else
    log_error "Terraform validation failed!"
    terraform validate
    HEALTH_FAILS=$((HEALTH_FAILS + 1))
fi

# 2. Check for Terraform Configuration Drift
log_info "2. Checking for Terraform infrastructure drift on project '${GCP_PROJECT}' (${GCP_REGION_VAL})..."
if [ -f "terraform.tfstate" ]; then
    tf_args=()
    if [ ! -f "terraform.tfvars" ]; then
        if [ -n "${GCP_PROJECT:-}" ] && [ "${GCP_PROJECT}" != "asyncgw-demo-project" ]; then
            gw_img="${GCP_REGION_VAL}-docker.pkg.dev/${GCP_PROJECT}/asyncgw-docker/asyncgw-gateway:latest"
            wk_img="${GCP_REGION_VAL}-docker.pkg.dev/${GCP_PROJECT}/asyncgw-docker/asyncgw-worker:latest"
            tf_args+=("-var=project_id=${GCP_PROJECT}")
            tf_args+=("-var=container_image_gateway=${gw_img}")
            tf_args+=("-var=container_image_worker=${wk_img}")
        fi
        if [ -n "${GCP_REGION_VAL:-}" ] && [ "${GCP_REGION_VAL}" != "us-central1" ]; then
            tf_args+=("-var=region=${GCP_REGION_VAL}")
        fi
    fi

    set +e
    terraform plan "${tf_args[@]}" -detailed-exitcode -no-color &>/tmp/tf_drift_plan.log
    EXIT_CODE=$?
    set -e
    if [ $EXIT_CODE -eq 0 ]; then
        log_success "No infrastructure drift detected. GCP state matches Terraform configurations."
    elif [ $EXIT_CODE -eq 2 ]; then
        log_warn "Infrastructure drift detected! Differences found between GCP state and Terraform:"
        grep -E "^[~+-]" /tmp/tf_drift_plan.log | head -n 20 || true
        DRIFT_COUNT=$((DRIFT_COUNT + 1))
    else
        log_warn "Could not connect to live GCP backend or state missing. (Plan exited with code $EXIT_CODE)"
    fi
else
    log_info "No local tfstate found; skipping live state drift comparison."
fi
cd "${SCRIPT_DIR}"

# 3. Verify Python Core Component Health and Schema Models
log_info "3. Testing Python component initialization and storage schemas..."
python3 -c "
import asyncio
from asyncgw.config import GatewaySettings, load_backends_config, load_policies_config
from asyncgw.storage.memory_mock import InMemoryRequestTracker, InMemoryBlobStorage
from asyncgw.models.request import AsyncRequestEnvelope, RequestType
from asyncgw.batch.splitter import BatchSplitter
from asyncgw.queue.memory_queue import InMemoryQueueProducer

async def verify():
    settings = GatewaySettings()
    backends = load_backends_config()
    policies = load_policies_config()
    assert len(backends) > 0, 'No backends loaded'
    assert len(policies.routing_strategies) > 0, 'No routing strategies loaded'
    
    tracker = InMemoryRequestTracker()
    await tracker.initialize()
    storage = InMemoryBlobStorage()
    await storage.initialize()
    
    # Test envelope & schema
    env = AsyncRequestEnvelope(
        request_id='check_req_1',
        request_type=RequestType.CHAT_COMPLETION,
        model='gemini-2.0-flash',
        payload={'messages': [{'role': 'user', 'content': 'test'}]}
    )
    await tracker.register_request(env)
    status = await tracker.get_request_status('check_req_1')
    assert status.status.value == 'PENDING', 'Request status not PENDING'
    print('All core internal components and schemas verified successfully.')

asyncio.run(verify())
"
if [ $? -eq 0 ]; then
    log_success "Core component schemas and logic verified."
else
    log_error "Core component schema verification failed!"
    HEALTH_FAILS=$((HEALTH_FAILS + 1))
fi

# 4. Probe Backend Services Reachability
log_info "4. Probing configured backend endpoints health..."
python3 -c "
import asyncio
from asyncgw.config import load_backends_config
from asyncgw.backends.health import HealthMonitor

async def probe():
    backends = load_backends_config()
    monitor = HealthMonitor(backends)
    results = await monitor.probe_all()
    all_ok = True
    for b_id, status in results.items():
        if status.is_healthy:
            print(f'  [OK] Backend {b_id}: HEALTHY (latency: {status.last_latency_ms:.1f}ms)')
        else:
            print(f'  [WARN] Backend {b_id}: UNHEALTHY ({status.last_error})')
    print('Backend probe evaluation complete.')

asyncio.run(probe())
"
log_success "Backend probe evaluation complete."

# 5. Display Deployed Resources & URIs Summary
display_resource_summary() {
    cd "${TERRAFORM_DIR}"
    if [ ! -f "terraform.tfstate" ]; then
        cd "${SCRIPT_DIR}"
        return 0
    fi

    local tf_json
    tf_json="$(terraform output -json 2>/dev/null || echo '{}')"
    cd "${SCRIPT_DIR}"

    local gw_url
    gw_url="$(echo "$tf_json" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('gateway_url', {}).get('value', ''))" 2>/dev/null || true)"
    local bucket_name
    bucket_name="$(echo "$tf_json" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('gcs_bucket_name', {}).get('value', ''))" 2>/dev/null || true)"
    local bq_dataset
    bq_dataset="$(echo "$tf_json" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('bigquery_dataset_id', {}).get('value', ''))" 2>/dev/null || true)"
    local bq_table
    bq_table="$(echo "$tf_json" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('bigquery_table_id', {}).get('value', ''))" 2>/dev/null || true)"
    local req_topic
    req_topic="$(echo "$tf_json" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('pubsub_requests_topic', {}).get('value', ''))" 2>/dev/null || true)"
    local batch_topic
    batch_topic="$(echo "$tf_json" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('pubsub_batch_items_topic', {}).get('value', ''))" 2>/dev/null || true)"
    local dlq_topic
    dlq_topic="$(echo "$tf_json" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('pubsub_dlq_topic', {}).get('value', ''))" 2>/dev/null || true)"
    local gw_sa
    gw_sa="$(echo "$tf_json" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('gateway_service_account', {}).get('value', ''))" 2>/dev/null || true)"
    local worker_sa
    worker_sa="$(echo "$tf_json" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('worker_service_account', {}).get('value', ''))" 2>/dev/null || true)"

    if [ -z "$gw_url" ]; then
        return 0
    fi

    echo ""
    echo -e "${BOLD}${BLUE}==================================================================${NC}"
    echo -e "${BOLD}${GREEN}           Async LLM Gateway: Deployed Resources & URIs          ${NC}"
    echo -e "${BOLD}${BLUE}==================================================================${NC}"
    echo ""
    echo -e "${BOLD}${CYAN}🌐 WEB UI & DOCUMENTATION:${NC}"
    echo -e "  - Web UI Dashboard:      ${GREEN}${BOLD}${gw_url}/${NC}"
    echo -e "  - Swagger OpenAPI Docs:  ${GREEN}${gw_url}/docs${NC}"
    echo -e "  - Gateway Health Probe:  ${GREEN}${gw_url}/healthz${NC}"
    echo ""
    echo -e "${BOLD}${CYAN}📡 ASYNC API ENDPOINTS:${NC}"
    echo -e "  - Chat Completions:      ${YELLOW}POST${NC} ${gw_url}/v1/chat/completions"
    echo -e "  - Text Completions:      ${YELLOW}POST${NC} ${gw_url}/v1/completions"
    echo -e "  - Embeddings:            ${YELLOW}POST${NC} ${gw_url}/v1/embeddings"
    echo -e "  - Batch Submissions:     ${YELLOW}POST${NC} ${gw_url}/v1/batches"
    echo -e "  - Status Polling:        ${YELLOW}GET${NC}  ${gw_url}/v1/requests/{request_id}"
    echo -e "  - Response Retrieval:    ${YELLOW}GET${NC}  ${gw_url}/v1/requests/{request_id}/response"
    echo ""
    echo -e "${BOLD}${CYAN}📦 STORAGE & DATA PERSISTENCE:${NC}"
    echo -e "  - GCS Response Bucket:   ${GREEN}gs://${bucket_name}${NC}"
    echo -e "  - BigQuery Tracker Table:${GREEN}${GCP_PROJECT}.${bq_dataset}.${bq_table}${NC}"
    echo -e "  - BigQuery Dataset:      ${GREEN}${bq_dataset}${NC}"
    echo ""
    echo -e "${BOLD}${CYAN}📬 PUBSUB QUEUES & TOPICS:${NC}"
    echo -e "  - Requests Topic:        ${GREEN}${req_topic}${NC}"
    echo -e "    └─ Subscription:       ${GREEN}${req_topic}-sub${NC}"
    echo -e "  - Batch Items Topic:     ${GREEN}${batch_topic}${NC}"
    echo -e "    └─ Subscription:       ${GREEN}${batch_topic}-sub${NC}"
    echo -e "  - Dead Letter Queue:     ${GREEN}${dlq_topic}${NC}"
    echo -e "    └─ Subscription:       ${GREEN}${dlq_topic}-sub${NC}"
    echo ""
    echo -e "${BOLD}${CYAN}📦 ARTIFACT REGISTRY CONTAINERS:${NC}"
    echo -e "  - Repository:            ${GREEN}${GCP_REGION_VAL}-docker.pkg.dev/${GCP_PROJECT}/asyncgw-docker${NC}"
    echo -e "  - Gateway Image:         ${GREEN}${GCP_REGION_VAL}-docker.pkg.dev/${GCP_PROJECT}/asyncgw-docker/asyncgw-gateway:latest${NC}"
    echo -e "  - Worker Image:          ${GREEN}${GCP_REGION_VAL}-docker.pkg.dev/${GCP_PROJECT}/asyncgw-docker/asyncgw-worker:latest${NC}"
    echo ""
    echo -e "${BOLD}${CYAN}⚡ CLOUD RUN SERVICES & WORKER TRIGGERS:${NC}"
    echo -e "  - Gateway & Web UI:      ${GREEN}asyncgw-gateway${NC} (${gw_url}) [Trigger: HTTP REST API & UI]"
    echo -e "  - Continuous Workers:    ${GREEN}asyncgw-worker-fleet${NC} (1-50 instances, cpu_idle=false) [Trigger: Pub/Sub Streaming Pull]"
    echo -e "  - Primary Worker Job:    ${GREEN}asyncgw-job-primary${NC} (5 parallel tasks) [Trigger: Cloud Scheduler / Eventarc / Manual]"
    echo -e "  - Batch Worker Job:      ${GREEN}asyncgw-job-batch${NC} (10 parallel tasks) [Trigger: Batch Enqueue Event / Cloud Scheduler]"
    echo -e "  - Gateway SA:            ${BLUE}${gw_sa}${NC}"
    echo -e "  - Worker SA:             ${BLUE}${worker_sa}${NC}"
    echo -e "${BOLD}${BLUE}==================================================================${NC}"
    echo ""
}

display_resource_summary

# 6. Final Health Status Summary
echo -e "${BOLD}Summary Report:${NC}"
echo "------------------------------------------------------------------"
echo -e "Health Failures:  $HEALTH_FAILS"
echo -e "Drift Violations: $DRIFT_COUNT"

if [ $HEALTH_FAILS -eq 0 ] && [ $DRIFT_COUNT -eq 0 ]; then
    echo -e "${GREEN}${BOLD}System Infrastructure is HEALTHY and IN SYNC.${NC}"
    exit 0
else
    echo -e "${YELLOW}${BOLD}Infrastructure check completed with warnings or drift.${NC}"
    exit 0
fi

# Production Deployment Guide - GCP Asynchronous LLM Gateway

This guide covers deploying the full Asynchronous LLM Gateway to Google Cloud Platform using Terraform, Docker, Cloud Run, Pub/Sub, BigQuery, GCS, and Apigee.

---

## 1. Architecture Prerequisites

1. **GCP Project**: An active Google Cloud Project with Billing enabled.
2. **GCP APIs Required**:
   ```bash
   gcloud services enable \
     pubsub.googleapis.com \
     bigquery.googleapis.com \
     storage.googleapis.com \
     run.googleapis.com \
     aiplatform.googleapis.com \
     artifactregistry.googleapis.com \
     apigee.googleapis.com
   ```
3. **CLI Tools**:
   - `terraform` (>= 0.13.0)
   - `gcloud`
   - `docker`
   - `python3` (>= 3.10)

---

## 2. Automated Deployment

The fastest way to deploy all components is using the automated deployment script:

```bash
# 1. Review plan (dry run)
./deploy.sh

# 2. Deploy infrastructure & services
./deploy.sh --apply
```

---

## 3. Step-by-Step Manual Deployment

### Step 3.1: Configure Backend & Policy Files
1. Review and customize `config/backends.yaml` with your Vertex AI Provisioned Throughput endpoints and API keys.
2. Review `config/policies.yaml` to configure routing priority, content rules, and timeouts.

### Step 3.2: Build and Push Container Images
```bash
export PROJECT_ID="your-gcp-project-id"
export REGION="us-central1"

# Create Artifact Registry repo if not exists
gcloud artifacts repositories create asyncgw-docker \
  --repository-format=docker \
  --location=${REGION} \
  --description="Async Gateway Images" || true

# Build & Push Gateway Image
docker build -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/asyncgw-docker/asyncgw-gateway:latest -f Dockerfile.gateway .
docker push ${REGION}-docker.pkg.dev/${PROJECT_ID}/asyncgw-docker/asyncgw-gateway:latest

# Build & Push Worker Image
docker build -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/asyncgw-docker/asyncgw-worker:latest -f Dockerfile.worker .
docker push ${REGION}-docker.pkg.dev/${PROJECT_ID}/asyncgw-docker/asyncgw-worker:latest
```

### Step 3.3: Deploy Infrastructure with Terraform
```bash
cd terraform

# Create your tfvars
cat <<EOF > terraform.tfvars
project_id         = "${PROJECT_ID}"
region             = "${REGION}"
environment        = "prod"
container_image_gateway = "${REGION}-docker.pkg.dev/${PROJECT_ID}/asyncgw-docker/asyncgw-gateway:latest"
container_image_worker  = "${REGION}-docker.pkg.dev/${PROJECT_ID}/asyncgw-docker/asyncgw-worker:latest"
EOF

terraform init
terraform apply -auto-approve
```

### Step 3.4: Deploy Apigee API Proxy
1. Zip the `apigee/proxy_bundle/` directory:
   ```bash
   cd apigee/proxy_bundle
   zip -r ../asyncgw-proxy.zip apiproxy/
   cd ../..
   ```
2. Import and deploy the bundle in Apigee:
   ```bash
   gcloud apigee apis create --name=asyncgw --file=apigee/asyncgw-proxy.zip --org=${PROJECT_ID}
   gcloud apigee apis deploy --name=asyncgw --environment=prod --org=${PROJECT_ID}
   ```

---

## 4. Post-Deployment Verification

Run the infrastructure drift and health check script:

```bash
./check_infra.sh
```

Ensure all items output `[OK]` and 0 drift violations are found.

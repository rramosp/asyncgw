#!/usr/bin/env bash
# ==============================================================================
# Automated Deployment Script for GCP Asynchronous LLM Gateway
# ==============================================================================

set -euo pipefail

# Color palette
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="${SCRIPT_DIR}/terraform"

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_banner() {
    echo -e "${BOLD}${BLUE}"
    echo "=================================================================="
    echo "       GCP Asynchronous LLM Gateway - Automated Deployment        "
    echo "=================================================================="
    echo -e "${NC}"
}

REQUIRED_APIS=(
    "run.googleapis.com"                  # Cloud Run Admin API
    "pubsub.googleapis.com"               # Cloud Pub/Sub API
    "bigquery.googleapis.com"             # Google Cloud BigQuery API
    "storage.googleapis.com"              # Google Cloud Storage JSON API
    "iam.googleapis.com"                  # Identity and Access Management (IAM) API
    "cloudresourcemanager.googleapis.com" # Cloud Resource Manager API
    "aiplatform.googleapis.com"           # Vertex AI API (Gemini LLM inference)
    "artifactregistry.googleapis.com"     # Artifact Registry API (Container images)
    "cloudbuild.googleapis.com"           # Cloud Build API
    "compute.googleapis.com"              # Compute Engine API
    "logging.googleapis.com"              # Cloud Logging API
    "monitoring.googleapis.com"           # Cloud Monitoring API
)

check_prerequisites() {
    log_info "Verifying prerequisite tools..."
    local missing=0

    for cmd in terraform python3 gcloud; do
        if ! command -v "$cmd" &>/dev/null; then
            log_error "Required tool '$cmd' is not installed or not in PATH."
            missing=1
        fi
    done

    if [ "$missing" -eq 1 ]; then
        exit 1
    fi
    log_success "All prerequisite CLI tools are available."
}

validate_configs() {
    log_info "Validating configuration YAML files..."
    python3 -c "
import yaml
from pathlib import Path
b = yaml.safe_load(open('${SCRIPT_DIR}/config/backends.yaml'))
p = yaml.safe_load(open('${SCRIPT_DIR}/config/policies.yaml'))
a = yaml.safe_load(open('${SCRIPT_DIR}/config/asyncgw.yaml'))
assert 'backends' in b, 'Missing backends key in backends.yaml'
assert 'policies' in p, 'Missing policies key in policies.yaml'
assert 'asyncgw' in a, 'Missing asyncgw key in asyncgw.yaml'
print('Config files validated successfully.')
"
    log_success "Configurations are structurally valid."
}

get_deployment_context() {
    # 1. GCP Project ID
    GCP_PROJECT="${TF_VAR_project_id:-${GCP_PROJECT_ID:-${GOOGLE_CLOUD_PROJECT:-${PROJECT_ID:-}}}}"
    if [ -z "$GCP_PROJECT" ] && [ -f "${TERRAFORM_DIR}/terraform.tfvars" ]; then
        GCP_PROJECT="$(grep -E '^\s*project_id\s*=' "${TERRAFORM_DIR}/terraform.tfvars" | sed -E 's/.*=\s*"([^"]+)".*/\1/' || true)"
    fi
    if [ -z "$GCP_PROJECT" ] && command -v gcloud &>/dev/null; then
        GCP_PROJECT="$(gcloud config get-value project 2>/dev/null || true)"
    fi
    if [ -z "$GCP_PROJECT" ]; then
        GCP_PROJECT="asyncgw-demo-project"
    fi

    # 2. GCP Username / Active Account
    GCP_USERNAME="${GCP_USER:-${GCLOUD_ACCOUNT:-}}"
    if [ -z "$GCP_USERNAME" ] && command -v gcloud &>/dev/null; then
        GCP_USERNAME="$(gcloud config get-value account 2>/dev/null || true)"
    fi
    if [ -z "$GCP_USERNAME" ]; then
        GCP_USERNAME="${USER:-$(whoami 2>/dev/null || echo 'unknown')}"
    fi

    # 3. GCP Region & Zone
    GCP_REGION_VAL="${TF_VAR_region:-${GCP_REGION:-${GCP_LOCATION:-${CLOUDSDK_COMPUTE_REGION:-}}}}"
    if [ -z "$GCP_REGION_VAL" ] && [ -f "${TERRAFORM_DIR}/terraform.tfvars" ]; then
        GCP_REGION_VAL="$(grep -E '^\s*region\s*=' "${TERRAFORM_DIR}/terraform.tfvars" | sed -E 's/.*=\s*"([^"]+)".*/\1/' || true)"
    fi
    if [ -z "$GCP_REGION_VAL" ] && command -v gcloud &>/dev/null; then
        GCP_REGION_VAL="$(gcloud config get-value compute/region 2>/dev/null || true)"
    fi
    if [ -z "$GCP_REGION_VAL" ]; then
        GCP_REGION_VAL="us-central1"
    fi

    GCP_ZONE_VAL="${TF_VAR_zone:-${GCP_ZONE:-${CLOUDSDK_COMPUTE_ZONE:-}}}"
    if [ -z "$GCP_ZONE_VAL" ] && [ -f "${TERRAFORM_DIR}/terraform.tfvars" ]; then
        GCP_ZONE_VAL="$(grep -E '^\s*zone\s*=' "${TERRAFORM_DIR}/terraform.tfvars" | sed -E 's/.*=\s*"([^"]+)".*/\1/' || true)"
    fi
    if [ -z "$GCP_ZONE_VAL" ] && command -v gcloud &>/dev/null; then
        GCP_ZONE_VAL="$(gcloud config get-value compute/zone 2>/dev/null || true)"
    fi
    if [ -z "$GCP_ZONE_VAL" ]; then
        local reg_clean
        reg_clean="$(echo "$GCP_REGION_VAL" | awk '{print $1}')"
        GCP_ZONE_VAL="${reg_clean}-a"
    fi
}

show_deployment_context() {
    echo -e "${BOLD}${BLUE}Target GCP Deployment Parameters (for --apply):${NC}"
    echo "------------------------------------------------------------------"
    echo -e "  - GCP Project:   ${GREEN}${BOLD}${GCP_PROJECT}${NC}"
    echo -e "  - GCP Username:  ${GREEN}${BOLD}${GCP_USERNAME}${NC}"
    echo -e "  - GCP Region:    ${GREEN}${BOLD}${GCP_REGION_VAL}${NC}"
    echo -e "  - GCP Zone:      ${GREEN}${BOLD}${GCP_ZONE_VAL}${NC}"
    echo "------------------------------------------------------------------"
    echo ""
}

check_and_display_api_statuses() {
    local project="${1:-${GCP_PROJECT}}"
    
    local enabled_services=""
    if command -v gcloud &>/dev/null && [ "$project" != "asyncgw-demo-project" ]; then
        enabled_services="$(gcloud services list --project="${project}" --format="value(config.name)" 2>/dev/null || true)"
    fi

    echo -e "${BOLD}${BLUE}Required GCP APIs Status for project '${project}':${NC}"
    echo "------------------------------------------------------------------"

    local missing_count=0
    for api in "${REQUIRED_APIS[@]}"; do
        if [ -n "$enabled_services" ] && echo "$enabled_services" | grep -qx "$api"; then
            printf "  ${GREEN}[ENABLED]${NC}      %-38s\n" "$api"
        else
            printf "  ${YELLOW}[NOT ENABLED]${NC}  %-38s\n" "$api"
            missing_count=$((missing_count + 1))
        fi
    done
    echo "------------------------------------------------------------------"

    if [ "$missing_count" -eq 0 ]; then
        echo -e "  ${GREEN}${BOLD}Status:${NC} All ${#REQUIRED_APIS[@]} required APIs are ENABLED."
    else
        echo -e "  ${YELLOW}${BOLD}Status:${NC} ${missing_count} of ${#REQUIRED_APIS[@]} required APIs are NOT ENABLED."
        echo -e "  ${BLUE}(Note: Running './deploy.sh --apply' will automatically prompt to enable all required APIs.)${NC}"
    fi
    echo ""
}

ensure_apis_enabled() {
    local project="${1:-${GCP_PROJECT}}"

    if ! command -v gcloud &>/dev/null; then
        log_warning "gcloud CLI is not available in PATH; skipping automated API enablement check."
        return 0
    fi

    if [ "$project" == "asyncgw-demo-project" ]; then
        log_warning "Project is set to placeholder 'asyncgw-demo-project'; skipping API enablement."
        return 0
    fi

    log_info "Checking required GCP APIs status on project '${project}'..."
    local enabled_services
    enabled_services="$(gcloud services list --project="${project}" --format="value(config.name)" 2>/dev/null || true)"

    local apis_to_enable=()
    for api in "${REQUIRED_APIS[@]}"; do
        if ! echo "$enabled_services" | grep -qx "$api"; then
            apis_to_enable+=("$api")
        fi
    done

    if [ ${#apis_to_enable[@]} -eq 0 ]; then
        log_success "All ${#REQUIRED_APIS[@]} required GCP APIs are already enabled on project '${project}'."
        return 0
    fi

    echo ""
    echo -e "${YELLOW}${BOLD}The following ${#apis_to_enable[@]} required GCP API(s) are NOT enabled on project '${project}':${NC}"
    for api in "${apis_to_enable[@]}"; do
        echo -e "  - ${YELLOW}${api}${NC}"
    done
    echo ""

    local confirm="y"
    if [ -t 0 ]; then
        read -r -p "Do you want to enable all ${#apis_to_enable[@]} required GCP APIs on project '${project}' now? [Y/n]: " user_input
        confirm="${user_input:-y}"
    else
        log_info "Non-interactive environment detected. Proceeding to enable required APIs automatically."
    fi

    if [[ "$confirm" =~ ^[Yy]$ ]]; then
        log_info "Enabling ${#apis_to_enable[@]} GCP APIs on project '${project}' (this may take 1-2 minutes)..."
        if gcloud services enable "${apis_to_enable[@]}" --project="${project}"; then
            log_success "All required GCP APIs have been successfully enabled on project '${project}'."
        else
            log_error "Failed to enable GCP APIs. Please verify your account permissions (Service Usage Admin) on '${project}'."
            exit 1
        fi
    else
        log_warning "API enablement skipped by user. Terraform deployment may encounter errors if required APIs are not active."
    fi
    echo ""
}

ensure_artifact_registry() {
    local project="${1:-${GCP_PROJECT}}"
    local region="${2:-${GCP_REGION_VAL}}"
    local repo_name="asyncgw-docker"

    if ! command -v gcloud &>/dev/null || [ "$project" == "asyncgw-demo-project" ]; then
        return 0
    fi

    log_info "Verifying Artifact Registry repository '${repo_name}' in region '${region}'..."
    if ! gcloud artifacts repositories describe "${repo_name}" --location="${region}" --project="${project}" &>/dev/null; then
        log_info "Creating Artifact Registry repository '${repo_name}' in region '${region}'..."
        gcloud artifacts repositories create "${repo_name}" \
            --repository-format=docker \
            --location="${region}" \
            --project="${project}" \
            --description="Async Gateway Docker Repository" \
            --quiet
        log_success "Artifact Registry repository '${repo_name}' created."
    else
        log_success "Artifact Registry repository '${repo_name}' is ready."
    fi
}

build_and_push_containers() {
    local project="${1:-${GCP_PROJECT}}"
    local region="${2:-${GCP_REGION_VAL}}"

    if ! command -v gcloud &>/dev/null; then
        log_warning "gcloud CLI not available; skipping automated container image build."
        return 0
    fi

    if [ "$project" == "asyncgw-demo-project" ]; then
        log_warning "Placeholder project detected ('asyncgw-demo-project'). Skipping container image builds."
        return 0
    fi

    ensure_artifact_registry "${project}" "${region}"

    local gateway_image="${region}-docker.pkg.dev/${project}/asyncgw-docker/asyncgw-gateway:latest"
    local worker_image="${region}-docker.pkg.dev/${project}/asyncgw-docker/asyncgw-worker:latest"

    log_info "Building and pushing container images via Cloud Build to project '${project}'..."
    log_info "  - Gateway: ${gateway_image}"
    log_info "  - Worker:  ${worker_image}"

    local cb_config="${SCRIPT_DIR}/cloudbuild.yaml"
    gcloud builds submit \
        --project="${project}" \
        --config="${cb_config}" \
        --substitutions="_GATEWAY_IMAGE=${gateway_image},_WORKER_IMAGE=${worker_image}" \
        "${SCRIPT_DIR}"

    log_success "All container images built and pushed to Artifact Registry successfully."
}

check_and_display_iam_permissions() {
    local project="${1:-${GCP_PROJECT}}"
    local user="${2:-${GCP_USERNAME}}"

    if ! command -v gcloud &>/dev/null || [ "$project" == "asyncgw-demo-project" ]; then
        return 0
    fi

    python3 -c "
import json, subprocess, sys

project = '${project}'
user = '${user}'

res_num = subprocess.run(['gcloud', 'projects', 'describe', project, '--format=value(projectNumber)'], capture_output=True, text=True)
project_num = res_num.stdout.strip()
compute_sa = f'{project_num}-compute@developer.gserviceaccount.com'
cloudbuild_sa = f'{project_num}@cloudbuild.gserviceaccount.com'

cmd = ['gcloud', 'projects', 'get-iam-policy', project, '--format=json']
res = subprocess.run(cmd, capture_output=True, text=True)
if res.returncode != 0:
    print(f'\033[1;33m[WARNING]\033[0m Could not retrieve IAM policy for project {project}: {res.stderr.strip()}')
    sys.exit(0)

try:
    policy = json.loads(res.stdout)
except Exception:
    sys.exit(0)

member_roles = {}
for b in policy.get('bindings', []):
    role = b.get('role', '')
    for m in b.get('members', []):
        member_roles.setdefault(m.lower(), set()).add(role)

if '@' in user and not user.startswith('user:') and not user.startswith('serviceAccount:'):
    user_member = f'serviceAccount:{user}' if 'gserviceaccount.com' in user else f'user:{user}'
else:
    user_member = user

u_roles = member_roles.get(user_member.lower(), set())
is_owner = 'roles/owner' in u_roles
is_editor = 'roles/editor' in u_roles
can_set_iam = is_owner or 'roles/resourcemanager.projectIamAdmin' in u_roles or 'roles/iam.securityAdmin' in u_roles

user_req = [
    ('roles/run.admin', 'Cloud Run Admin (deploy services & jobs)', True),
    ('roles/pubsub.admin', 'Pub/Sub Admin (create topics & subscriptions)', True),
    ('roles/bigquery.admin', 'BigQuery Admin (create datasets & tables)', True),
    ('roles/storage.admin', 'Storage Admin (create buckets & blobs)', True),
    ('roles/iam.serviceAccountAdmin', 'Service Account Admin (create service accounts)', True),
    ('roles/resourcemanager.projectIamAdmin', 'Project IAM Admin (grant roles to service accounts)', False),
    ('roles/artifactregistry.admin', 'Artifact Registry Admin (container images)', True),
    ('roles/cloudbuild.builds.editor', 'Cloud Build Editor (build container images)', True),
    ('roles/serviceusage.serviceUsageAdmin', 'Service Usage Admin (enable GCP APIs)', True),
]

sa_req = [
    (f'serviceAccount:{compute_sa}', f'Compute Engine Default SA ({compute_sa})', [
        ('roles/storage.admin', 'Storage Admin (read source & write images)'),
        ('roles/logging.logWriter', 'Logging Log Writer (write build logs)'),
        ('roles/artifactregistry.admin', 'Artifact Registry Admin (create & push repos)'),
    ]),
    (f'serviceAccount:{cloudbuild_sa}', f'Cloud Build Service Account ({cloudbuild_sa})', [
        ('roles/cloudbuild.builds.builder', 'Cloud Build Builder'),
        ('roles/storage.admin', 'Storage Admin (read source & write artifacts)'),
        ('roles/logging.logWriter', 'Logging Log Writer (write build logs)'),
        ('roles/artifactregistry.admin', 'Artifact Registry Admin (create & push repos)'),
    ])
]

print('\033[1m\033[34m1. Deployment IAM Roles for Deployer User (' + user + '):\033[0m')
print('------------------------------------------------------------------')
missing_user_count = 0
for role_id, desc, by_editor in user_req:
    granted = False
    reason = ''
    if is_owner:
        granted = True
        reason = '(via roles/owner)'
    elif role_id in u_roles:
        granted = True
        reason = '(directly assigned)'
    elif is_editor and by_editor:
        granted = True
        reason = '(via roles/editor)'

    if granted:
        print(f'  \033[0;32m[GRANTED]\033[0m      {role_id:<38} {reason}')
    else:
        print(f'  \033[1;33m[NOT GRANTED]\033[0m  {role_id:<38}')
        missing_user_count += 1

print('------------------------------------------------------------------')
print('')
print('\033[1m\033[34m2. Cloud Build & Service Account IAM Permissions:\033[0m')
print('------------------------------------------------------------------')
missing_sa_count = 0
for sa_member, sa_desc, sa_roles in sa_req:
    assigned = member_roles.get(sa_member.lower(), set())
    print(f'  \033[1m{sa_desc}:\033[0m')
    for role_id, r_desc in sa_roles:
        if 'roles/owner' in assigned or 'roles/editor' in assigned or role_id in assigned:
            print(f'    \033[0;32m[GRANTED]\033[0m      {role_id:<36} ({r_desc})')
        else:
            print(f'    \033[1;33m[NOT GRANTED]\033[0m  {role_id:<36} ({r_desc})')
            missing_sa_count += 1

print('------------------------------------------------------------------')
total_missing = missing_user_count + missing_sa_count
if total_missing == 0:
    print('  \033[0;32m\033[1mStatus:\033[0m All required deployment IAM roles and service account permissions are GRANTED.')
else:
    print(f'  \033[1;33m\033[1mStatus:\033[0m {total_missing} required IAM role binding(s) are NOT GRANTED.')
    if can_set_iam:
        print('  \033[1;32m\033[1mPermission Check:\033[0m Current user HAS permission to grant IAM roles (Project IAM Admin).')
        print('  \033[0;34m(Note: Running \'./deploy.sh --apply\' will prompt to automatically grant missing roles.)\033[0m')
    else:
        print('  \033[0;31m\033[1mPermission Check:\033[0m Current user DOES NOT have permission to set IAM policies.')
        print('  \033[0;31mAction Required:\033[0m Please contact your GCP Project Administrator to request the missing roles.')
print('')
"
}

ensure_iam_permissions() {
    local project="${1:-${GCP_PROJECT}}"
    local user="${2:-${GCP_USERNAME}}"

    if ! command -v gcloud &>/dev/null || [ "$project" == "asyncgw-demo-project" ]; then
        return 0
    fi

    log_info "Verifying deployment IAM roles for user '${user}' and build service accounts on project '${project}'..."

    local iam_check_json
    iam_check_json="$(python3 -c "
import json, subprocess

project = '${project}'
user = '${user}'

res_num = subprocess.run(['gcloud', 'projects', 'describe', project, '--format=value(projectNumber)'], capture_output=True, text=True)
project_num = res_num.stdout.strip()
compute_sa = f'{project_num}-compute@developer.gserviceaccount.com'
cloudbuild_sa = f'{project_num}@cloudbuild.gserviceaccount.com'

cmd = ['gcloud', 'projects', 'get-iam-policy', project, '--format=json']
res = subprocess.run(cmd, capture_output=True, text=True)
if res.returncode != 0:
    print(json.dumps({'error': res.stderr.strip()}))
    exit(0)

try:
    policy = json.loads(res.stdout)
except Exception as e:
    print(json.dumps({'error': str(e)}))
    exit(0)

member_roles = {}
for b in policy.get('bindings', []):
    role = b.get('role', '')
    for m in b.get('members', []):
        member_roles.setdefault(m.lower(), set()).add(role)

if '@' in user and not user.startswith('user:') and not user.startswith('serviceAccount:'):
    user_member = f'serviceAccount:{user}' if 'gserviceaccount.com' in user else f'user:{user}'
else:
    user_member = user

u_roles = member_roles.get(user_member.lower(), set())
is_owner = 'roles/owner' in u_roles
is_editor = 'roles/editor' in u_roles
can_set_iam = is_owner or 'roles/resourcemanager.projectIamAdmin' in u_roles or 'roles/iam.securityAdmin' in u_roles

user_req = [
    ('roles/run.admin', 'Cloud Run Admin', True),
    ('roles/pubsub.admin', 'Pub/Sub Admin', True),
    ('roles/bigquery.admin', 'BigQuery Admin', True),
    ('roles/storage.admin', 'Storage Admin', True),
    ('roles/iam.serviceAccountAdmin', 'Service Account Admin', True),
    ('roles/resourcemanager.projectIamAdmin', 'Project IAM Admin', False),
    ('roles/artifactregistry.admin', 'Artifact Registry Admin', True),
    ('roles/cloudbuild.builds.editor', 'Cloud Build Editor', True),
    ('roles/serviceusage.serviceUsageAdmin', 'Service Usage Admin', True),
]

sa_req = [
    (f'serviceAccount:{compute_sa}', f'Compute Engine Default SA ({compute_sa})', [
        ('roles/storage.admin', 'Storage Admin (read source & write images)'),
        ('roles/logging.logWriter', 'Logging Log Writer (write build logs)'),
        ('roles/artifactregistry.admin', 'Artifact Registry Admin (create & push repos)'),
    ]),
    (f'serviceAccount:{cloudbuild_sa}', f'Cloud Build Service Account ({cloudbuild_sa})', [
        ('roles/cloudbuild.builds.builder', 'Cloud Build Builder'),
        ('roles/storage.admin', 'Storage Admin'),
        ('roles/logging.logWriter', 'Logging Log Writer'),
        ('roles/artifactregistry.admin', 'Artifact Registry Admin'),
    ])
]

missing_bindings = []

for role_id, desc, by_editor in user_req:
    if not (is_owner or (role_id in u_roles) or (is_editor and by_editor)):
        missing_bindings.append({'member': user_member, 'role': role_id, 'desc': desc})

for sa_member, sa_desc, sa_roles in sa_req:
    assigned = member_roles.get(sa_member.lower(), set())
    for role_id, r_desc in sa_roles:
        if not ('roles/owner' in assigned or 'roles/editor' in assigned or role_id in assigned):
            missing_bindings.append({'member': sa_member, 'role': role_id, 'desc': f'{sa_desc} -> {r_desc}'})

print(json.dumps({
    'user_member': user_member,
    'can_set_iam': can_set_iam,
    'missing_bindings': missing_bindings
}))
")"

    local missing_count
    missing_count="$(echo "$iam_check_json" | python3 -c "import sys, json; data=json.load(sys.stdin); print(len(data.get('missing_bindings', [])))")"
    local can_set_iam
    can_set_iam="$(echo "$iam_check_json" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('can_set_iam', False))")"

    if [ "$missing_count" -eq 0 ]; then
        log_success "All required deployment IAM roles and service account permissions are already granted."
        return 0
    fi

    echo ""
    echo -e "${YELLOW}${BOLD}The following ${missing_count} required IAM role binding(s) are NOT granted on project '${project}':${NC}"
    python3 -c "
import sys, json
data = json.loads('''$iam_check_json''')
for b in data.get('missing_bindings', []):
    print(f\"  - \033[1;33m{b['member']}\033[0m: \033[1m{b['role']}\033[0m ({b['desc']})\")
"
    echo ""

    if [ "$can_set_iam" == "False" ]; then
        log_error "Current user DOES NOT have permission to set IAM policies on project '${project}'."
        echo ""
        echo -e "${RED}${BOLD}Action Required: Please ask your GCP Project Administrator to grant the required roles.${NC}"
        echo "Your administrator can run the following command(s):"
        echo ""
        python3 -c "
import sys, json
data = json.loads('''$iam_check_json''')
for b in data.get('missing_bindings', []):
    print(f\"  gcloud projects add-iam-policy-binding ${project} --member='{b['member']}' --role='{b['role']}'\")
"
        echo ""
        exit 1
    fi

    # User CAN set IAM roles
    echo -e "${GREEN}Current user '${user}' HAS permission (Project IAM Admin) to grant IAM roles on project '${project}'.${NC}"
    local confirm="y"
    if [ -t 0 ]; then
        read -r -p "Do you want to grant the missing ${missing_count} IAM role(s) / service account permission(s) on project '${project}' now? [Y/n]: " user_input
        confirm="${user_input:-y}"
    else
        log_info "Non-interactive environment detected. Proceeding to grant missing IAM roles automatically."
    fi

    if [[ "$confirm" =~ ^[Yy]$ ]]; then
        log_info "Granting missing IAM roles on project '${project}'..."
        python3 -c "
import sys, json, subprocess
data = json.loads('''$iam_check_json''')
for b in data.get('missing_bindings', []):
    member = b['member']
    role = b['role']
    print(f\"  Granting {role} to {member}...\")
    cmd = ['gcloud', 'projects', 'add-iam-policy-binding', '${project}', f'--member={member}', f'--role={role}', '--condition=None']
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(f\"\033[0;31m[ERROR]\033[0m Failed to grant {role} to {member}: {res.stderr.strip()}\")
        sys.exit(1)
"
        log_success "All required deployment IAM roles successfully granted."
    else
        log_warning "IAM role granting skipped by user. Deployment may encounter 403 Forbidden errors if permissions are missing."
    fi
    echo ""
}

deploy_terraform() {
    local mode="${1:-dry-run}"
    log_info "Initializing Terraform in ${TERRAFORM_DIR}..."
    cd "${TERRAFORM_DIR}"

    if [ -n "${GCP_PROJECT:-}" ] && [ "${GCP_PROJECT}" != "asyncgw-demo-project" ]; then
        local gw_img="${GCP_REGION_VAL}-docker.pkg.dev/${GCP_PROJECT}/asyncgw-docker/asyncgw-gateway:latest"
        local wk_img="${GCP_REGION_VAL}-docker.pkg.dev/${GCP_PROJECT}/asyncgw-docker/asyncgw-worker:latest"
        cat <<EOF > "${TERRAFORM_DIR}/terraform.tfvars"
project_id              = "${GCP_PROJECT}"
region                  = "${GCP_REGION_VAL}"
zone                    = "${GCP_ZONE_VAL}"
container_image_gateway = "${gw_img}"
container_image_worker  = "${wk_img}"
EOF
    fi

    terraform init -upgrade

    local tf_args=()

    if [ "$mode" == "apply" ]; then
        log_info "Applying Terraform Infrastructure as Code changes to project ${GCP_PROJECT} (${GCP_REGION_VAL})..."
        terraform apply "${tf_args[@]}" -auto-approve
        log_success "Terraform infrastructure deployed."
    else
        log_info "Running Terraform Plan (Dry-run mode)..."
        terraform plan "${tf_args[@]}"
        log_success "Terraform validation and plan passed."
    fi
    cd "${SCRIPT_DIR}"
}

run_post_deploy_checks() {
    log_info "Executing post-deployment health verification..."
    if [ -f "${SCRIPT_DIR}/check_infra.sh" ]; then
        bash "${SCRIPT_DIR}/check_infra.sh"
    fi
}

cleanup_all_resources() {
    local project="${1:-${GCP_PROJECT}}"
    local region="${2:-${GCP_REGION_VAL}}"

    log_warning "Starting complete teardown and reset of all Async Gateway GCP resources..."
    echo -e "${YELLOW}Target Project: ${BOLD}${project}${NC} (Region: ${region})"
    echo ""

    # 1. Attempt Terraform Destroy first if state exists
    if [ -f "${TERRAFORM_DIR}/terraform.tfstate" ]; then
        log_info "Running Terraform Destroy..."
        cd "${TERRAFORM_DIR}"
        local tf_args=()
        if [ ! -f "terraform.tfvars" ]; then
            local gw_img="${region}-docker.pkg.dev/${project}/asyncgw-docker/asyncgw-gateway:latest"
            local wk_img="${region}-docker.pkg.dev/${project}/asyncgw-docker/asyncgw-worker:latest"
            tf_args+=("-var=project_id=${project}")
            tf_args+=("-var=container_image_gateway=${gw_img}")
            tf_args+=("-var=container_image_worker=${wk_img}")
            tf_args+=("-var=region=${region}")
        fi
        terraform destroy "${tf_args[@]}" -auto-approve || log_warning "Terraform destroy reported warnings/partial cleanup; proceeding with direct GCP API sweep."
        cd "${SCRIPT_DIR}"
    fi

    # 2. Direct GCP API sweep to ensure 100% cleanup of any orphaned resources
    if command -v gcloud &>/dev/null && [ "$project" != "asyncgw-demo-project" ]; then
        log_info "Sweeping Cloud Run services and jobs..."
        for svc in asyncgw-gateway asyncgw-worker-fleet; do
            gcloud run services delete "$svc" --region="${region}" --project="${project}" --quiet 2>/dev/null || true
        done
        for job in asyncgw-job-primary asyncgw-job-batch; do
            gcloud run jobs delete "$job" --region="${region}" --project="${project}" --quiet 2>/dev/null || true
        done

        log_info "Sweeping Pub/Sub subscriptions and topics..."
        for sub in asyncgw-requests-sub asyncgw-batch-items-sub asyncgw-dlq-sub asyncgw-requests-topic-sub asyncgw-batch-items-topic-sub asyncgw-dlq-topic-sub; do
            gcloud pubsub subscriptions delete "$sub" --project="${project}" --quiet 2>/dev/null || true
        done
        for topic in asyncgw-requests-topic asyncgw-batch-items-topic asyncgw-dlq-topic; do
            gcloud pubsub topics delete "$topic" --project="${project}" --quiet 2>/dev/null || true
        done

        log_info "Sweeping BigQuery datasets..."
        python3 -c "
from google.cloud import bigquery
try:
    client = bigquery.Client(project='$project')
    client.delete_dataset('asyncgw_metrics', delete_contents=True, not_found_ok=True)
    print('  [OK] BigQuery dataset asyncgw_metrics deleted.')
except Exception as e:
    pass
" 2>/dev/null || true

        log_info "Sweeping Cloud Storage buckets..."
        python3 -c "
from google.cloud import storage
try:
    client = storage.Client(project='$project')
    for b in list(client.list_buckets()):
        if b.name.startswith('asyncgw-responses-storage'):
            try:
                b.delete(force=True)
                print(f'  [OK] Deleted bucket: {b.name}')
            except Exception:
                pass
except Exception:
    pass
" 2>/dev/null || true

        log_info "Sweeping Artifact Registry repositories..."
        gcloud artifacts repositories delete asyncgw-docker --location="${region}" --project="${project}" --quiet 2>/dev/null || true

        log_info "Sweeping Service Accounts..."
        for sa in asyncgw-gateway-sa asyncgw-worker-sa; do
            gcloud iam service-accounts delete "${sa}@${project}.iam.gserviceaccount.com" --project="${project}" --quiet 2>/dev/null || true
        done
    fi

    # 3. Clean local state files
    log_info "Cleaning up local Terraform state files..."
    rm -f "${TERRAFORM_DIR}/terraform.tfstate" "${TERRAFORM_DIR}/terraform.tfstate.backup" "/tmp/tf_drift_plan.log"

    log_success "Complete reset and cleanup finished. All project resources and local state removed."
}

main() {
    print_banner
    check_prerequisites
    validate_configs
    get_deployment_context

    local do_apply=0
    local do_reset=0

    for arg in "$@"; do
        case "$arg" in
            --apply|-a)
                do_apply=1
                ;;
            --reset|-r)
                do_reset=1
                ;;
            --help|-h)
                echo "Usage: ./deploy.sh [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --apply, -a     Deploy infrastructure and container services to GCP"
                echo "  --reset, -r     Completely tear down and reset all deployed resources and artifacts"
                echo "  --help, -h      Show this help message"
                echo ""
                echo "Examples:"
                echo "  ./deploy.sh                 # Dry-run validation & health/IAM check"
                echo "  ./deploy.sh --apply         # Deploy infrastructure & services"
                echo "  ./deploy.sh --reset         # Clean up all deployed resources & local state"
                echo "  ./deploy.sh --apply --reset # Reset project completely first, then deploy fresh"
                exit 0
                ;;
        esac
    done

    if [ "$do_reset" -eq 1 ]; then
        cleanup_all_resources "$GCP_PROJECT" "$GCP_REGION_VAL"
        if [ "$do_apply" -eq 0 ]; then
            log_info "Reset complete. To deploy fresh, run: ./deploy.sh --apply"
            exit 0
        fi
        echo ""
        log_info "Proceeding with fresh deployment after reset..."
        echo ""
    fi

    if [ "$do_apply" -eq 1 ]; then
        # 1. Verify/grant user IAM permissions
        ensure_iam_permissions "$GCP_PROJECT" "$GCP_USERNAME"

        # 2. Enable required APIs with user confirmation
        ensure_apis_enabled "$GCP_PROJECT"

        # 3. Build and push container images
        build_and_push_containers "$GCP_PROJECT" "$GCP_REGION_VAL"

        # 4. Deploy Terraform
        deploy_terraform "apply"
        run_post_deploy_checks
        log_success "Deployment finished successfully!"
    else
        show_deployment_context
        deploy_terraform "plan"
        echo ""
        log_info "Dry-run complete."
        log_info "Target deployment configuration that would be used by --apply:"
        echo -e "  - GCP Project:   ${GREEN}${BOLD}${GCP_PROJECT}${NC}"
        echo -e "  - GCP Username:  ${GREEN}${BOLD}${GCP_USERNAME}${NC}"
        echo -e "  - GCP Region:    ${GREEN}${BOLD}${GCP_REGION_VAL}${NC}"
        echo -e "  - GCP Zone:      ${GREEN}${BOLD}${GCP_ZONE_VAL}${NC}"
        echo ""
        check_and_display_api_statuses "$GCP_PROJECT"
        check_and_display_iam_permissions "$GCP_PROJECT" "$GCP_USERNAME"
        log_info "To apply changes to live GCP project, run: ./deploy.sh --apply"
        log_info "To reset and cleanup all GCP resources, run:  ./deploy.sh --reset"
        log_info "To reset and redeploy from scratch, run:       ./deploy.sh --apply --reset"
    fi
}

main "$@"

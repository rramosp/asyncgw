"""Configuration loader and management for the Asynchronous Gateway."""

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
import yaml
from pydantic import BaseModel, Field


def _substitute_env_vars(data: Any) -> Any:
    """Recursively substitute environment variables in string values (e.g., ${VAR_NAME})."""
    if isinstance(data, dict):
        return {k: _substitute_env_vars(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [_substitute_env_vars(item) for item in data]
    elif isinstance(data, str):
        pattern = re.compile(r"\$\{([^}]+)\}")
        matches = pattern.findall(data)
        result = data
        for var in matches:
            default_val = ""
            var_name = var
            if ":-" in var:
                var_name, default_val = var.split(":-", 1)
            env_val = os.getenv(var_name)
            if not env_val:
                if var_name in ("PROJECT_ID", "GCP_PROJECT", "GCP_PROJECT_ID", "GOOGLE_CLOUD_PROJECT"):
                    env_val = (
                        os.getenv("GCP_PROJECT_ID")
                        or os.getenv("GCP_PROJECT")
                        or os.getenv("GOOGLE_CLOUD_PROJECT")
                        or os.getenv("PROJECT_ID")
                        or default_val
                    )
                elif var_name in ("REGION", "LOCATION", "GCP_REGION", "GCP_LOCATION"):
                    env_val = (
                        os.getenv("GCP_LOCATION")
                        or os.getenv("GCP_REGION")
                        or os.getenv("LOCATION")
                        or os.getenv("REGION")
                        or default_val
                    )
                else:
                    env_val = default_val
            result = result.replace(f"${{{var}}}", env_val or "")
        return result
    return data


class AuthConfig(BaseModel):
    type: str = "none"  # "google_adc", "api_key", "bearer_token", "none"
    secret_env: Optional[str] = None
    audience: Optional[str] = None
    header_name: Optional[str] = "Authorization"
    header_prefix: Optional[str] = "Bearer "
    api_key_value: Optional[str] = None


class CapabilitiesConfig(BaseModel):
    supports_online: bool = True
    supports_batch: bool = False
    max_batch_size: int = 1000
    concurrency_limit: int = 50


class HealthCheckConfig(BaseModel):
    endpoint_url: str
    method: str = "GET"
    interval_seconds: int = 30
    timeout_seconds: int = 5
    expected_status: int = 200
    max_consecutive_failures: int = 3


class BackendConfig(BaseModel):
    id: str
    name: str
    description: str = ""
    endpoint_url: str
    auth: AuthConfig = Field(default_factory=AuthConfig)
    capabilities: CapabilitiesConfig = Field(default_factory=CapabilitiesConfig)
    health_check: Optional[HealthCheckConfig] = None
    supported_models: List[str] = Field(default_factory=list)
    cost_tier: str = "medium"  # "low", "medium", "high"
    priority_weight: int = 50
    is_active: bool = True


class FailoverConfig(BaseModel):
    enabled: bool = True
    max_retries_per_backend: int = 2
    retry_delay_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    failover_on_statuses: List[Any] = Field(
        default_factory=lambda: [429, 500, 502, 503, 504, "TIMEOUT", "HEALTH_CHECK_FAILED"]
    )


class RoutingStrategy(BaseModel):
    id: str
    name: str
    description: str = ""
    preference_order: List[str] = Field(default_factory=list)
    failover: FailoverConfig = Field(default_factory=FailoverConfig)


class ContentRuleCondition(BaseModel):
    min_estimated_tokens: Optional[int] = None
    max_wait_seconds_under: Optional[int] = None
    has_multimodal_content: Optional[bool] = None


class ContentRule(BaseModel):
    name: str
    condition: Optional[ContentRuleCondition] = None
    target_backend: Optional[str] = None
    target_policy: Optional[str] = None
    fallback_policy: Optional[str] = None
    model_mappings: Optional[Dict[str, str]] = None


class GlobalTimeouts(BaseModel):
    default_max_wait_seconds: int = 3600
    absolute_max_wait_seconds: int = 86400
    min_wait_seconds: int = 5


class PoliciesConfig(BaseModel):
    default_policy: str = "cost_optimized_with_failover"
    routing_strategies: List[RoutingStrategy] = Field(default_factory=list)
    content_rules: List[ContentRule] = Field(default_factory=list)
    global_timeouts: GlobalTimeouts = Field(default_factory=GlobalTimeouts)


class AsyncGWConfig(BaseModel):
    """General configuration parameters for the Asynchronous Gateway."""
    max_batch_items_in_api: int = 100



class GatewaySettings(BaseModel):
    # GCP Project & Region
    project_id: str = Field(default_factory=lambda: os.getenv("GCP_PROJECT_ID", "asyncgw-demo-project"))
    location: str = Field(default_factory=lambda: os.getenv("GCP_LOCATION", "us-central1"))

    # PubSub Queues
    pubsub_topic_requests: str = Field(
        default_factory=lambda: os.getenv("PUBSUB_TOPIC_REQUESTS", "asyncgw-requests-topic")
    )
    pubsub_subscription_requests: str = Field(
        default_factory=lambda: os.getenv("PUBSUB_SUB_REQUESTS", "asyncgw-requests-sub")
    )
    pubsub_topic_batch_items: str = Field(
        default_factory=lambda: os.getenv("PUBSUB_TOPIC_BATCH_ITEMS", "asyncgw-batch-items-topic")
    )
    pubsub_subscription_batch_items: str = Field(
        default_factory=lambda: os.getenv("PUBSUB_SUB_BATCH_ITEMS", "asyncgw-batch-items-sub")
    )
    pubsub_dlq_topic: str = Field(
        default_factory=lambda: os.getenv("PUBSUB_DLQ_TOPIC", "asyncgw-dlq-topic")
    )

    # BigQuery Tracking
    bq_dataset: str = Field(default_factory=lambda: os.getenv("BQ_DATASET", "asyncgw_metrics"))
    bq_table: str = Field(default_factory=lambda: os.getenv("BQ_TABLE", "request_tracker"))

    # Cloud Storage
    gcs_bucket_name: str = Field(
        default_factory=lambda: os.getenv("GCS_BUCKET_NAME", "asyncgw-responses-storage")
    )
    gcs_retention_days: int = Field(default_factory=lambda: int(os.getenv("GCS_RETENTION_DAYS", "7")))

    # Gateway API & Worker
    api_host: str = Field(default_factory=lambda: os.getenv("API_HOST", "0.0.0.0"))
    api_port: int = Field(default_factory=lambda: int(os.getenv("PORT", os.getenv("API_PORT", "8080"))))
    ui_port: int = Field(default_factory=lambda: int(os.getenv("UI_PORT", "8080")))
    api_key_header: str = "X-API-Key"
    admin_api_key: str = Field(default_factory=lambda: os.getenv("ADMIN_API_KEY", "asyncgw-admin-secret-key"))

    # Mode: "gcp" or "mock" (for offline/in-memory local testing)
    environment_mode: str = Field(default_factory=lambda: os.getenv("ASYNCGW_ENV_MODE", "mock"))

    # File paths
    backends_config_path: str = Field(
        default_factory=lambda: os.getenv("BACKENDS_CONFIG_PATH", "config/backends.yaml")
    )
    policies_config_path: str = Field(
        default_factory=lambda: os.getenv("POLICIES_CONFIG_PATH", "config/policies.yaml")
    )
    asyncgw_config_path: str = Field(
        default_factory=lambda: os.getenv("ASYNCGW_CONFIG_PATH", "config/asyncgw.yaml")
    )


def load_backends_config(file_path: Optional[str] = None) -> List[BackendConfig]:
    """Load and parse backends.yaml."""
    path = Path(file_path or os.getenv("BACKENDS_CONFIG_PATH", "config/backends.yaml"))
    if not path.is_absolute():
        base_dir = Path(__file__).resolve().parent.parent
        path = base_dir / path

    if not path.exists():
        return []

    with open(path, "r", encoding="utf-8") as f:
        raw_data = yaml.safe_load(f)

    substituted = _substitute_env_vars(raw_data)
    backend_list = substituted.get("backends", [])
    return [BackendConfig(**b) for b in backend_list]


def load_policies_config(file_path: Optional[str] = None) -> PoliciesConfig:
    """Load and parse policies.yaml."""
    path = Path(file_path or os.getenv("POLICIES_CONFIG_PATH", "config/policies.yaml"))
    if not path.is_absolute():
        base_dir = Path(__file__).resolve().parent.parent
        path = base_dir / path

    if not path.exists():
        return PoliciesConfig()

    with open(path, "r", encoding="utf-8") as f:
        raw_data = yaml.safe_load(f)

    substituted = _substitute_env_vars(raw_data)
    policies_dict = substituted.get("policies", {})
    return PoliciesConfig(**policies_dict)


def load_asyncgw_config(file_path: Optional[str] = None) -> AsyncGWConfig:
    """Load and parse asyncgw.yaml."""
    path = Path(file_path or os.getenv("ASYNCGW_CONFIG_PATH", "config/asyncgw.yaml"))
    if not path.is_absolute():
        base_dir = Path(__file__).resolve().parent.parent
        path = base_dir / path

    if not path.exists():
        return AsyncGWConfig()

    with open(path, "r", encoding="utf-8") as f:
        raw_data = yaml.safe_load(f)

    substituted = _substitute_env_vars(raw_data) if raw_data else {}
    if not isinstance(substituted, dict):
        return AsyncGWConfig()

    if "asyncgw" in substituted and isinstance(substituted["asyncgw"], dict):
        config_dict = substituted["asyncgw"]
    else:
        config_dict = substituted

    return AsyncGWConfig(**config_dict)


"""Unit tests for configuration loaders and validation."""

import os
from asyncgw.config import (
    AsyncGWConfig,
    BackendConfig,
    GatewaySettings,
    PoliciesConfig,
    _substitute_env_vars,
    load_asyncgw_config,
    load_backends_config,
    load_policies_config,
)


def test_env_var_substitution(monkeypatch):
    monkeypatch.setenv("TEST_PROJECT", "my-gcp-project")
    data = {
        "endpoint": "https://aiplatform.googleapis.com/v1/projects/${TEST_PROJECT}/locations/us-central1",
        "fallback": "${NON_EXISTENT_VAR:-default_val}",
        "nested": ["item-${TEST_PROJECT}"],
    }
    result = _substitute_env_vars(data)
    assert result["endpoint"] == "https://aiplatform.googleapis.com/v1/projects/my-gcp-project/locations/us-central1"
    assert result["fallback"] == "default_val"
    assert result["nested"] == ["item-my-gcp-project"]


def test_load_backends_config():
    backends = load_backends_config()
    assert len(backends) >= 3
    backend_ids = [b.id for b in backends]
    assert "gcp-provisioned-gemini" in backend_ids
    assert "gemini-flex" in backend_ids
    assert "openai-direct" in backend_ids

    provisioned = next(b for b in backends if b.id == "gcp-provisioned-gemini")
    assert provisioned.capabilities.supports_batch is True
    assert provisioned.cost_tier == "low"

    flex = next(b for b in backends if b.id == "gemini-flex")
    assert flex.capabilities.supports_batch is False


def test_load_policies_config():
    policies = load_policies_config()
    assert policies.default_policy == "cost_optimized_with_failover"
    assert len(policies.routing_strategies) >= 2
    strat_ids = [s.id for s in policies.routing_strategies]
    assert "cost_optimized_with_failover" in strat_ids

    default_strat = next(s for s in policies.routing_strategies if s.id == "cost_optimized_with_failover")
    assert "gcp-provisioned-gemini" in default_strat.preference_order
    assert default_strat.failover.enabled is True


def test_load_asyncgw_config():
    # Load default asyncgw.yaml
    cfg = load_asyncgw_config()
    assert isinstance(cfg, AsyncGWConfig)
    assert cfg.max_batch_items_in_api == 100

    # Non-existent file returns default
    cfg_nonexistent = load_asyncgw_config("config/non_existent.yaml")
    assert cfg_nonexistent.max_batch_items_in_api == 100


def test_gateway_settings_port_override(monkeypatch):
    monkeypatch.setenv("PORT", "8080")
    settings = GatewaySettings()
    assert settings.api_port == 8080

    monkeypatch.delenv("PORT", raising=False)
    monkeypatch.setenv("API_PORT", "9000")
    settings2 = GatewaySettings()
    assert settings2.api_port == 9000

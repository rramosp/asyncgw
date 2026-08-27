"""Unit tests for the Health Monitor and Circuit Breaker."""

import pytest
from asyncgw.backends.health import HealthMonitor
from asyncgw.models.request import AsyncRequestEnvelope, RequestType
from asyncgw.router.engine import RoutingEngine


@pytest.mark.asyncio
async def test_health_probe_and_failure_counter(mock_backends):
    monitor = HealthMonitor(mock_backends)
    assert monitor.is_backend_available("gcp-provisioned-gemini") is True

    # Record consecutive failures
    monitor.record_execution_outcome("gcp-provisioned-gemini", success=False, status_code=500, error="Server error")
    assert monitor.is_backend_available("gcp-provisioned-gemini") is True # 1 failure < 2 max

    monitor.record_execution_outcome("gcp-provisioned-gemini", success=False, status_code=500, error="Server error")
    # 2 consecutive failures reached threshold
    assert monitor.is_backend_available("gcp-provisioned-gemini") is False

    # Successful call restores health
    monitor.record_execution_outcome("gcp-provisioned-gemini", success=True, status_code=200)
    assert monitor.is_backend_available("gcp-provisioned-gemini") is True


def test_unhealthy_backend_routing_failover(mock_backends, mock_policies, backend_clients):
    monitor = HealthMonitor(mock_backends)
    engine = RoutingEngine(mock_backends, mock_policies, monitor, backend_clients)

    # Initial route: provisioned is primary
    env = AsyncRequestEnvelope(
        request_id="req_h1",
        request_type=RequestType.CHAT_COMPLETION,
        model="gemini-2.0-flash",
    )
    decision = engine.route_request(env)
    assert decision.primary_backend.id == "gcp-provisioned-gemini"

    # Mark provisioned unhealthy
    monitor.record_execution_outcome("gcp-provisioned-gemini", success=False, status_code=503)
    monitor.record_execution_outcome("gcp-provisioned-gemini", success=False, status_code=503)
    assert monitor.is_backend_available("gcp-provisioned-gemini") is False

    # Route now promotes gemini-flex as primary and pushes provisioned to backup
    decision2 = engine.route_request(env)
    assert decision2.primary_backend.id == "gemini-flex"
    assert "gcp-provisioned-gemini" in [b.id for b in decision2.backup_backends]

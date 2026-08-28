"""Pytest fixtures for Async LLM Gateway test suite."""

import asyncio
from typing import Dict, List
import pytest
from httpx import ASGITransport, AsyncClient

from asyncgw.backends.base import BaseLLMBackend
from asyncgw.backends.health import HealthMonitor
from asyncgw.backends.mock_backend import MockBackend
from asyncgw.batch.reassembler import BatchReassembler
from asyncgw.batch.splitter import BatchSplitter
from asyncgw.config import (
    AuthConfig,
    BackendConfig,
    CapabilitiesConfig,
    FailoverConfig,
    GatewaySettings,
    GlobalTimeouts,
    HealthCheckConfig,
    PoliciesConfig,
    RoutingStrategy,
    ContentRule,
    ContentRuleCondition,
)
from asyncgw.gateway.api import create_app
from asyncgw.queue.memory_queue import InMemoryQueueConsumer, InMemoryQueueProducer
from asyncgw.router.engine import RoutingEngine
from asyncgw.storage.memory_mock import InMemoryBlobStorage, InMemoryRequestTracker


@pytest.fixture
def test_settings(tmp_path) -> GatewaySettings:
    return GatewaySettings(
        project_id="test-project",
        location="us-central1",
        bq_dataset="test_metrics",
        bq_table="request_tracker",
        gcs_bucket_name="test-responses-bucket",
        environment_mode="mock",
        backends_config_path=str(tmp_path / "test_backends.yaml"),
        policies_config_path=str(tmp_path / "test_policies.yaml"),
    )


@pytest.fixture
def mock_backends() -> List[BackendConfig]:
    return [
        BackendConfig(
            id="gcp-provisioned-gemini",
            name="GCP Provisioned Throughput",
            endpoint_url="mock://vertex/provisioned",
            capabilities=CapabilitiesConfig(supports_online=True, supports_batch=True, max_batch_size=5000),
            health_check=HealthCheckConfig(endpoint_url="mock://vertex/health", max_consecutive_failures=2),
            supported_models=["gemini-2.0-flash", "gemini-1.5-pro"],
            cost_tier="low",
            priority_weight=100,
        ),
        BackendConfig(
            id="gemini-flex",
            name="Vertex AI Gemini FLEX",
            endpoint_url="mock://vertex/flex",
            capabilities=CapabilitiesConfig(supports_online=True, supports_batch=False, max_batch_size=1),
            health_check=HealthCheckConfig(endpoint_url="mock://vertex/flex/health", max_consecutive_failures=2),
            supported_models=["gemini-2.0-flash", "gemini-1.5-pro"],
            cost_tier="medium",
            priority_weight=70,
        ),
        BackendConfig(
            id="openai-direct",
            name="OpenAI Direct API",
            endpoint_url="mock://openai/v1",
            capabilities=CapabilitiesConfig(supports_online=True, supports_batch=True, max_batch_size=10000),
            health_check=HealthCheckConfig(endpoint_url="mock://openai/health", max_consecutive_failures=2),
            supported_models=["gpt-4o", "gpt-4o-mini"],
            cost_tier="high",
            priority_weight=40,
        ),
    ]


@pytest.fixture
def mock_policies() -> PoliciesConfig:
    return PoliciesConfig(
        default_policy="cost_optimized_with_failover",
        routing_strategies=[
            RoutingStrategy(
                id="cost_optimized_with_failover",
                name="Cost Optimized with Failover",
                preference_order=["gcp-provisioned-gemini", "gemini-flex", "openai-direct"],
                failover=FailoverConfig(
                    enabled=True,
                    max_retries_per_backend=2,
                    retry_delay_seconds=0.01,
                    backoff_multiplier=1.5,
                ),
            ),
            RoutingStrategy(
                id="latency_sensitive",
                name="Low Latency",
                preference_order=["gemini-flex", "gcp-provisioned-gemini"],
                failover=FailoverConfig(enabled=True, max_retries_per_backend=1),
            ),
        ],
        content_rules=[
            ContentRule(
                name="large_payload_provisioned",
                condition=ContentRuleCondition(min_estimated_tokens=5000),
                target_backend="gcp-provisioned-gemini",
            ),
            ContentRule(
                name="urgent_deadline",
                condition=ContentRuleCondition(max_wait_seconds_under=10),
                target_policy="latency_sensitive",
            ),
            ContentRule(
                name="model_gpt",
                model_mappings={"gpt-4.*": "openai-direct"},
            ),
        ],
        global_timeouts=GlobalTimeouts(
            default_max_wait_seconds=60,
            absolute_max_wait_seconds=3600,
            min_wait_seconds=1,
        ),
    )


@pytest.fixture
def mock_storage():
    return InMemoryRequestTracker(), InMemoryBlobStorage()


@pytest.fixture
def mock_queues():
    q1 = asyncio.Queue()
    q2 = asyncio.Queue()
    q3 = asyncio.Queue()
    producer = InMemoryQueueProducer(q1, q2, q3)
    consumer = InMemoryQueueConsumer(q1, q2, q3)
    return producer, consumer, q1, q2, q3


@pytest.fixture
def backend_clients(mock_backends) -> Dict[str, MockBackend]:
    clients = {}
    for b in mock_backends:
        client = MockBackend(b)
        client.simulated_latency_seconds = 0.001
        clients[b.id] = client
    return clients


@pytest.fixture
def routing_engine(mock_backends, mock_policies, backend_clients):
    health = HealthMonitor(mock_backends)
    return RoutingEngine(mock_backends, mock_policies, health, backend_clients)


@pytest.fixture
async def app_and_client(test_settings, mock_backends, mock_policies, backend_clients, mock_storage, mock_queues):
    tracker, storage = mock_storage
    producer, consumer, q1, q2, q3 = mock_queues

    app = create_app(
        settings=test_settings,
        backends=mock_backends,
        policies=mock_policies,
        request_tracker=tracker,
        blob_storage=storage,
        queue_producer=producer,
        queue_consumer=consumer,
    )
    # Inject backend clients
    app.state.backend_clients = backend_clients
    app.state.routing_engine = RoutingEngine(
        mock_backends, mock_policies, app.state.health_monitor, backend_clients
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield app, client, tracker, storage, producer, consumer, q1, q2, q3

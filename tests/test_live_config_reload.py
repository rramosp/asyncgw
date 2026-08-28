"""Tests for automated live configuration reloading across Gateway and Workers."""

import asyncio
import os
import time
from datetime import datetime, timezone
import pytest
from httpx import AsyncClient, ASGITransport

from asyncgw.backends.mock_backend import MockBackend
from asyncgw.config import (
    AuthConfig,
    BackendConfig,
    CapabilitiesConfig,
    GatewaySettings,
    PoliciesConfig,
    RoutingStrategy,
    save_backends_config,
    save_policies_config,
)
from asyncgw.gateway.api import create_app
from asyncgw.main import WorkerFleetManager
from asyncgw.models.request import AsyncRequestEnvelope, RequestType
from asyncgw.queue.memory_queue import InMemoryQueueConsumer, InMemoryQueueProducer
from asyncgw.storage.memory_mock import InMemoryBlobStorage, InMemoryRequestTracker


@pytest.mark.asyncio
async def test_worker_fleet_live_reloads_file_modification(tmp_path):
    backends_file = str(tmp_path / "backends.yaml")
    policies_file = str(tmp_path / "policies.yaml")

    initial_backends = [
        BackendConfig(
            id="backend-v1",
            name="Backend Initial",
            endpoint_url="mock://initial/v1",
            supported_models=["mock-model"],
            is_active=True,
        )
    ]
    initial_policies = PoliciesConfig(
        default_policy="initial_strat",
        routing_strategies=[
            RoutingStrategy(
                id="initial_strat",
                name="Initial Strategy",
                preference_order=["backend-v1"],
            )
        ],
    )
    save_backends_config(initial_backends, backends_file)
    save_policies_config(initial_policies, policies_file)

    settings = GatewaySettings(
        environment_mode="mock",
        backends_config_path=backends_file,
        policies_config_path=policies_file,
    )
    fleet = WorkerFleetManager(settings, mode="worker-primary")
    await fleet.initialize()

    assert "backend-v1" in fleet.routing_engine.backends_map
    assert fleet.routing_engine.policies.default_policy == "initial_strat"

    # Now update backends file on disk (simulate UI or user edit)
    time.sleep(0.05)  # Ensure distinct mtime
    updated_backends = [
        BackendConfig(
            id="backend-v1",
            name="Backend Updated",
            endpoint_url="https://aiplatform.googleapis.com/v1/projects/test/locations/global/publishers/google/models",
            supported_models=["gemini-2.0-flash"],
            is_active=True,
        ),
        BackendConfig(
            id="backend-v2-new",
            name="New Second Backend",
            endpoint_url="mock://new-backend/v1",
            supported_models=["custom-model"],
            is_active=True,
        ),
    ]
    save_backends_config(updated_backends, backends_file)

    # Trigger reload check
    reloaded = await fleet.check_and_reload_config()
    assert reloaded is True
    assert "backend-v2-new" in fleet.routing_engine.backends_map
    assert (
        fleet.routing_engine.backends_map["backend-v1"].endpoint_url
        == "https://aiplatform.googleapis.com/v1/projects/test/locations/global/publishers/google/models"
    )
    assert "backend-v2-new" in fleet.backend_clients


@pytest.mark.asyncio
async def test_worker_fleet_live_reloads_from_blob_storage(tmp_path):
    backends_file = str(tmp_path / "backends.yaml")
    policies_file = str(tmp_path / "policies.yaml")

    save_backends_config([], backends_file)
    save_policies_config(PoliciesConfig(), policies_file)

    settings = GatewaySettings(
        environment_mode="mock",
        backends_config_path=backends_file,
        policies_config_path=policies_file,
    )
    fleet = WorkerFleetManager(settings, mode="worker-primary")
    await fleet.initialize()

    # Simulate Gateway writing new config to blob storage (GCS)
    now_iso = datetime.now(timezone.utc).isoformat()
    await fleet.blob_storage.save_json(
        "system/config/backends.json",
        {
            "backends": [
                {
                    "id": "remote-gcs-backend",
                    "name": "Remote GCS Backend",
                    "endpoint_url": "mock://gcs-synced/v1",
                    "supported_models": ["gcs-model"],
                    "is_active": True,
                }
            ],
            "updated_at": now_iso,
        },
    )
    await fleet.blob_storage.save_json(
        "system/config/version.json",
        {"version": time.time() + 100, "updated_at": now_iso},
    )

    reloaded = await fleet.check_and_reload_config()
    assert reloaded is True
    assert "remote-gcs-backend" in fleet.routing_engine.backends_map
    assert "remote-gcs-backend" in fleet.backend_clients


@pytest.mark.asyncio
async def test_admin_api_persists_and_updates_worker_routing(tmp_path):
    backends_file = str(tmp_path / "backends.yaml")
    policies_file = str(tmp_path / "policies.yaml")

    initial_backends = [
        BackendConfig(
            id="gemini-flex",
            name="Gemini Flex Initial",
            endpoint_url="https://us-central1-aiplatform.googleapis.com/v1/projects/demo/locations/us-central1/publishers/google/models",
            supported_models=["gemini-2.0-flash"],
            is_active=True,
        )
    ]
    save_backends_config(initial_backends, backends_file)
    save_policies_config(PoliciesConfig(), policies_file)

    settings = GatewaySettings(
        environment_mode="mock",
        backends_config_path=backends_file,
        policies_config_path=policies_file,
    )
    app = create_app(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        # Edit backend via Admin API (simulating UI update)
        update_payload = {
            "id": "gemini-flex",
            "name": "Gemini Flex Global",
            "endpoint_url": "https://aiplatform.googleapis.com/v1/projects/asyncgw/locations/global/publishers/google/models",
            "is_active": True,
            "supported_models": ["gemini-2.0-flash", "gemini-1.5-flash"],
            "auth": {"type": "google_adc"},
            "capabilities": {"supports_online": True, "supports_batch": False},
            "cost_tier": "medium",
            "priority_weight": 70,
        }

        res = await client.put("/v1/admin/backends/gemini-flex", json=update_payload)
        assert res.status_code == 200
        data = res.json()
        assert (
            data["backend"]["endpoint_url"]
            == "https://aiplatform.googleapis.com/v1/projects/asyncgw/locations/global/publishers/google/models"
        )

        # Verify persisted on disk
        from asyncgw.config import load_backends_config
        persisted = load_backends_config(backends_file)
        assert len(persisted) == 1
        assert (
            persisted[0].endpoint_url
            == "https://aiplatform.googleapis.com/v1/projects/asyncgw/locations/global/publishers/google/models"
        )

        # Verify policies update persists
        policy_payload = {
            "default_policy": "latency_sensitive",
            "routing_strategies": [
                {
                    "id": "latency_sensitive",
                    "name": "Low Latency Strategy",
                    "preference_order": ["gemini-flex"],
                    "failover": {"enabled": True},
                }
            ],
            "content_rules": [],
            "global_timeouts": {"default_max_wait_seconds": 60},
        }
        res_pol = await client.put("/v1/admin/policies", json=policy_payload)
        assert res_pol.status_code == 200
        
        from asyncgw.config import load_policies_config
        persisted_pol = load_policies_config(policies_file)
        assert persisted_pol.default_policy == "latency_sensitive"


@pytest.mark.asyncio
async def test_end_to_end_worker_processes_with_reloaded_backend_config(tmp_path):
    backends_file = str(tmp_path / "backends.yaml")
    policies_file = str(tmp_path / "policies.yaml")

    initial_backends = [
        BackendConfig(
            id="gemini-flex",
            name="Gemini Flex Initial",
            endpoint_url="mock://initial-url/v1",
            supported_models=["gemini-2.0-flash"],
            is_active=True,
        )
    ]
    initial_policies = PoliciesConfig(
        default_policy="test_policy",
        routing_strategies=[
            RoutingStrategy(
                id="test_policy",
                name="Test Policy",
                preference_order=["gemini-flex"],
            )
        ],
    )
    save_backends_config(initial_backends, backends_file)
    save_policies_config(initial_policies, policies_file)

    settings = GatewaySettings(
        environment_mode="mock",
        backends_config_path=backends_file,
        policies_config_path=policies_file,
    )
    app = create_app(settings)

    fleet = WorkerFleetManager(settings, mode="worker-all")
    fleet.request_tracker = app.state.request_tracker
    fleet.blob_storage = app.state.blob_storage
    fleet.queue_producer = app.state.queue_producer
    fleet.queue_consumer = app.state.queue_consumer
    fleet.routing_engine = app.state.routing_engine
    fleet.batch_splitter = app.state.batch_splitter
    fleet.batch_reassembler = app.state.batch_reassembler

    from asyncgw.workers.primary_worker import PrimaryRequestWorker
    fleet.primary_worker = PrimaryRequestWorker(
        request_tracker=app.state.request_tracker,
        blob_storage=app.state.blob_storage,
        routing_engine=app.state.routing_engine,
        batch_splitter=app.state.batch_splitter,
        config_reloader=fleet.check_and_reload_config,
    )
    app.state.local_fleet = fleet

    # Start consumer
    await fleet.queue_consumer.consume_requests(fleet.primary_worker.process_envelope)

    # 1. Update backend via Admin API to new endpoint URL
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        update_payload = {
            "id": "gemini-flex",
            "name": "Gemini Flex Live Updated",
            "endpoint_url": "mock://global-url/v1",
            "is_active": True,
            "supported_models": ["gemini-2.0-flash"],
            "cost_tier": "low",
            "priority_weight": 100,
        }
        res = await client.put("/v1/admin/backends/gemini-flex", json=update_payload)
        assert res.status_code == 200

        # Submit chat completion
        chat_req = {
            "model": "gemini-2.0-flash",
            "messages": [{"role": "user", "content": "Test prompt"}],
            "routing_override": "gemini-flex",
        }
        submit_res = await client.post("/v1/chat/completions", json=chat_req)
        assert submit_res.status_code == 202
        req_id = submit_res.json()["request_id"]

        # Give worker a moment to process from queue
        await asyncio.sleep(0.1)

        # Check status
        status_res = await client.get(f"/v1/requests/{req_id}")
        assert status_res.status_code == 200
        st = status_res.json()
        assert st["status"] == "COMPLETED"
        assert st["backend_service_id"] == "gemini-flex"
        resp_res = await client.get(f"/v1/requests/{req_id}/response")
        assert resp_res.status_code == 200
        assert resp_res.json()["backend_service_id"] == "gemini-flex"


@pytest.mark.asyncio
async def test_end_to_end_worker_processes_with_live_default_policy_switch(tmp_path):
    backends_file = str(tmp_path / "backends.yaml")
    policies_file = str(tmp_path / "policies.yaml")

    initial_backends = [
        BackendConfig(
            id="backend-a",
            name="Backend Alpha",
            endpoint_url="mock://backend-a/v1",
            supported_models=["custom-model"],
            is_active=True,
        ),
        BackendConfig(
            id="backend-b",
            name="Backend Beta",
            endpoint_url="mock://backend-b/v1",
            supported_models=["custom-model"],
            is_active=True,
        )
    ]
    initial_policies = PoliciesConfig(
        default_policy="prefer_a",
        routing_strategies=[
            RoutingStrategy(
                id="prefer_a",
                name="Prefer Alpha Strategy",
                preference_order=["backend-a", "backend-b"],
                failover={"enabled": True},
            ),
            RoutingStrategy(
                id="prefer_b",
                name="Prefer Beta Strategy",
                preference_order=["backend-b", "backend-a"],
                failover={"enabled": True},
            )
        ],
        content_rules=[],
    )
    save_backends_config(initial_backends, backends_file)
    save_policies_config(initial_policies, policies_file)

    settings = GatewaySettings(
        environment_mode="mock",
        backends_config_path=backends_file,
        policies_config_path=policies_file,
    )
    app = create_app(settings)

    fleet = WorkerFleetManager(settings, mode="worker-all")
    fleet.request_tracker = app.state.request_tracker
    fleet.blob_storage = app.state.blob_storage
    fleet.queue_producer = app.state.queue_producer
    fleet.queue_consumer = app.state.queue_consumer
    fleet.routing_engine = app.state.routing_engine
    fleet.batch_splitter = app.state.batch_splitter
    fleet.batch_reassembler = app.state.batch_reassembler

    from asyncgw.workers.primary_worker import PrimaryRequestWorker
    fleet.primary_worker = PrimaryRequestWorker(
        request_tracker=app.state.request_tracker,
        blob_storage=app.state.blob_storage,
        routing_engine=app.state.routing_engine,
        batch_splitter=app.state.batch_splitter,
        config_reloader=fleet.check_and_reload_config,
    )
    app.state.local_fleet = fleet
    await fleet.queue_consumer.consume_requests(fleet.primary_worker.process_envelope)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        # 1. Request with default policy 'prefer_a' -> routes to backend-a
        req1 = {
            "model": "custom-model",
            "messages": [{"role": "user", "content": "Request 1"}],
        }
        res1 = await client.post("/v1/chat/completions", json=req1)
        assert res1.status_code == 202
        req_id1 = res1.json()["request_id"]
        await asyncio.sleep(0.1)
        st1 = (await client.get(f"/v1/requests/{req_id1}")).json()
        assert st1["status"] == "COMPLETED"
        assert st1["backend_service_id"] == "backend-a"

        # 2. Switch default policy to 'prefer_b' via PUT /v1/admin/policies/default
        switch_res = await client.put(
            "/v1/admin/policies/default",
            json={"default_policy": "prefer_b"}
        )
        assert switch_res.status_code == 200
        assert switch_res.json()["default_policy"] == "prefer_b"

        # 3. Next request with no override now immediately routes to backend-b
        req2 = {
            "model": "custom-model",
            "messages": [{"role": "user", "content": "Request 2"}],
        }
        res2 = await client.post("/v1/chat/completions", json=req2)
        assert res2.status_code == 202
        req_id2 = res2.json()["request_id"]
        await asyncio.sleep(0.1)
        st2 = (await client.get(f"/v1/requests/{req_id2}")).json()
        assert st2["status"] == "COMPLETED"
        assert st2["backend_service_id"] == "backend-b"

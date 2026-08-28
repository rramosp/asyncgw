"""Integration tests for FastAPI Gateway HTTP endpoints."""

import pytest
from asyncgw.models.response import RequestStatusEnum


@pytest.mark.asyncio
async def test_submit_chat_completion_endpoint(app_and_client):
    app, client, tracker, storage, producer, consumer, q1, q2, q3 = app_and_client

    payload = {
        "model": "gemini-2.0-flash",
        "messages": [{"role": "user", "content": "What is GCP Pub/Sub?"}],
        "max_wait_seconds": 120,
    }
    res = await client.post("/v1/chat/completions", json=payload)
    assert res.status_code == 202
    data = res.json()

    assert "request_id" in data
    assert data["status"] == "PENDING"
    assert data["max_wait_seconds"] == 120
    assert q1.qsize() == 1  # Envelope enqueued

    # Check status polling endpoint
    req_id = data["request_id"]
    status_res = await client.get(f"/v1/requests/{req_id}")
    assert status_res.status_code == 200
    assert status_res.json()["status"] == "PENDING"


@pytest.mark.asyncio
async def test_submit_batch_endpoint(app_and_client):
    app, client, tracker, storage, producer, consumer, q1, q2, q3 = app_and_client

    payload = {
        "endpoint": "/v1/chat/completions",
        "max_wait_seconds": 300,
        "requests": [
            {"custom_id": "b-1", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "gemini-2.0-flash", "messages": [{"role": "user", "content": "1"}]}},
            {"custom_id": "b-2", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "gemini-2.0-flash", "messages": [{"role": "user", "content": "2"}]}},
        ],
    }
    res = await client.post("/v1/batches", json=payload)
    assert res.status_code == 202
    data = res.json()

    assert "batch_id" in data
    assert data["total_items"] == 2
    assert q1.qsize() == 1


@pytest.mark.asyncio
async def test_response_retrieval_states(app_and_client):
    app, client, tracker, storage, producer, consumer, q1, q2, q3 = app_and_client

    # 1. Non-existent request -> 404
    r_notfound = await client.get("/v1/requests/non_existent_123/response")
    assert r_notfound.status_code == 404

    # 2. Pending request -> 202 (not ready)
    payload = {"model": "gemini-2.0-flash", "messages": [{"role": "user", "content": "Hi"}]}
    res = await client.post("/v1/chat/completions", json=payload)
    req_id = res.json()["request_id"]

    r_pending = await client.get(f"/v1/requests/{req_id}/response")
    assert r_pending.status_code == 202
    assert r_pending.json()["status"] == "PENDING"

    # 3. Complete the request manually in storage
    gcs_uri = await storage.save_json(
        f"responses/{req_id}.json",
        {"id": "cmpl-1", "choices": [{"message": {"role": "assistant", "content": "Answer from LLM"}}]},
    )
    await tracker.mark_completed(
        request_id=req_id,
        response_gcs_uri=gcs_uri,
        response_status_code=200,
        response_content_length=100,
        elapsed_seconds=0.2,
        backend_service_id="gcp-provisioned-gemini",
    )

    r_completed = await client.get(f"/v1/requests/{req_id}/response")
    assert r_completed.status_code == 200
    data_completed = r_completed.json()
    assert data_completed["choices"][0]["message"]["content"] == "Answer from LLM"

    # Verify single request status has backend_service_id and NO backend_batch_service_mode
    status_check = await client.get(f"/v1/requests/{req_id}")
    assert status_check.status_code == 200
    status_json = status_check.json()
    assert status_json["backend_service_id"] == "gcp-provisioned-gemini"
    assert status_json.get("backend_batch_service_mode") is None


@pytest.mark.asyncio
async def test_batch_response_backend_tags(app_and_client):
    app, client, tracker, storage, producer, consumer, q1, q2, q3 = app_and_client
    from asyncgw.models.request import AsyncRequestEnvelope, RequestType
    from datetime import datetime, timezone

    # 1. Native batch completion
    batch_id = "batch_test_native_1"
    env = AsyncRequestEnvelope(
        request_id=batch_id,
        request_type=RequestType.BATCH,
        model="gemini-2.0-flash",
        target_backend="gcp-provisioned-gemini",
        created_at=datetime.now(timezone.utc),
    )
    await tracker.register_request(env)

    gcs_uri = await storage.save_json(
        f"responses/{batch_id}.json",
        {
            "id": batch_id,
            "object": "batch",
            "status": "COMPLETED",
            "backend_service_id": "gcp-provisioned-gemini",
            "backend_batch_service_mode": "native",
            "results": [
                {"id": "req-1", "custom_id": "c1", "response": {"status_code": 200, "body": {"model": "gemini-2.0-flash"}}},
            ],
        },
    )
    await tracker.mark_completed(
        request_id=batch_id,
        response_gcs_uri=gcs_uri,
        response_status_code=200,
        response_content_length=200,
        elapsed_seconds=0.5,
        backend_service_id="gcp-provisioned-gemini",
        backend_batch_service_mode="native",
    )

    b_res = await client.get(f"/v1/batches/{batch_id}")
    assert b_res.status_code == 200
    b_data = b_res.json()
    assert b_data["backend_service_id"] == "gcp-provisioned-gemini"
    assert b_data["backend_batch_service_mode"] == "native"

    out_res = await client.get(f"/v1/batches/{batch_id}/output")
    assert out_res.status_code == 200
    out_data = out_res.json()
    assert out_data["backend_service_id"] == "gcp-provisioned-gemini"
    assert out_data["backend_batch_service_mode"] == "native"


@pytest.mark.asyncio
async def test_admin_backends_and_stats(app_and_client):
    app, client, tracker, storage, producer, consumer, q1, q2, q3 = app_and_client

    # List backends
    b_res = await client.get("/v1/admin/backends")
    assert b_res.status_code == 200
    backends = b_res.json()["backends"]
    assert len(backends) >= 3

    # Probe backend
    p_res = await client.post("/v1/admin/backends/gcp-provisioned-gemini/probe")
    assert p_res.status_code == 200
    assert p_res.json()["status"]["is_healthy"] is True

    # Stats
    stats_res = await client.get("/v1/admin/stats")
    assert stats_res.status_code == 200
    assert "status_breakdown" in stats_res.json()


@pytest.mark.asyncio
async def test_batch_results_api_pagination_and_fields(app_and_client):
    app, client, tracker, storage, producer, consumer, q1, q2, q3 = app_and_client
    from asyncgw.models.request import AsyncRequestEnvelope, RequestType
    from datetime import datetime, timezone

    batch_id = "batch_large_pagination_test"
    total_items_count = 150

    # 1. Create a large batch with 150 items
    env = AsyncRequestEnvelope(
        request_id=batch_id,
        request_type=RequestType.BATCH,
        model="gemini-2.0-flash",
        total_items=total_items_count,
        created_at=datetime.now(timezone.utc),
    )
    await tracker.register_request(env)

    # 2. Store 150 results in storage
    results_payload = [
        {
            "id": f"batch_req_{i}",
            "custom_id": f"custom_{i}",
            "response": {"status_code": 200, "body": {"item": i}},
            "error": None,
        }
        for i in range(total_items_count)
    ]
    gcs_uri = await storage.save_json(
        f"responses/{batch_id}.json",
        {
            "id": batch_id,
            "object": "batch",
            "status": "COMPLETED",
            "backend_service_id": "gcp-provisioned-gemini",
            "backend_batch_service_mode": "native",
            "results": results_payload,
        },
    )
    await tracker.mark_completed(
        request_id=batch_id,
        response_gcs_uri=gcs_uri,
        response_status_code=200,
        response_content_length=len(str(results_payload)),
        elapsed_seconds=1.2,
        backend_service_id="gcp-provisioned-gemini",
        backend_batch_service_mode="native",
    )

    # 3. Query batch output endpoint (default limit is 100)
    res = await client.get(f"/v1/batches/{batch_id}/output")
    assert res.status_code == 200
    data = res.json()

    assert data["total_items"] == 150
    assert data["returned_items"] == 100
    assert len(data["results"]) == 100
    assert data["results"][0]["custom_id"] == "custom_0"
    assert data["results"][99]["custom_id"] == "custom_99"
    # Local environment_mode -> results_uri is download URL
    assert data["results_uri"] == f"/v1/batches/{batch_id}/download"

    # Also check /v1/requests/{batch_id}/response
    res_req = await client.get(f"/v1/requests/{batch_id}/response")
    assert res_req.status_code == 200
    data_req = res_req.json()
    assert data_req["total_items"] == 150
    assert data_req["returned_items"] == 100
    assert len(data_req["results"]) == 100
    assert data_req["results_uri"] == f"/v1/batches/{batch_id}/download"

    # 4. Test download endpoint retrieves full 150 items
    dl_res = await client.get(f"/v1/batches/{batch_id}/download")
    assert dl_res.status_code == 200
    assert "attachment" in dl_res.headers.get("content-disposition", "")
    dl_data = dl_res.json()
    assert len(dl_data["results"]) == 150


@pytest.mark.asyncio
async def test_batch_results_api_custom_max_limit(app_and_client):
    app, client, tracker, storage, producer, consumer, q1, q2, q3 = app_and_client
    from asyncgw.models.request import AsyncRequestEnvelope, RequestType
    from datetime import datetime, timezone

    # Set custom limit of 3
    app.state.asyncgw_config.max_batch_items_in_api = 3

    batch_id = "batch_custom_limit_test"
    total_items_count = 10

    env = AsyncRequestEnvelope(
        request_id=batch_id,
        request_type=RequestType.BATCH,
        total_items=total_items_count,
        created_at=datetime.now(timezone.utc),
    )
    await tracker.register_request(env)

    results_payload = [
        {
            "id": f"batch_req_{i}",
            "custom_id": f"custom_{i}",
            "response": {"status_code": 200, "body": {"item": i}},
            "error": None,
        }
        for i in range(total_items_count)
    ]
    gcs_uri = await storage.save_json(
        f"responses/{batch_id}.json",
        {
            "id": batch_id,
            "object": "batch",
            "status": "COMPLETED",
            "results": results_payload,
        },
    )
    await tracker.mark_completed(
        request_id=batch_id,
        response_gcs_uri=gcs_uri,
        response_status_code=200,
        response_content_length=len(str(results_payload)),
        elapsed_seconds=0.5,
        backend_service_id="gemini-flex",
        backend_batch_service_mode="decomposed",
    )

    res = await client.get(f"/v1/batches/{batch_id}/output")
    assert res.status_code == 200
    data = res.json()

    assert data["total_items"] == 10
    assert data["returned_items"] == 3
    assert len(data["results"]) == 3
    assert data["results_uri"] == f"/v1/batches/{batch_id}/download"


@pytest.mark.asyncio
async def test_batch_results_api_gcp_mode(app_and_client):
    app, client, tracker, storage, producer, consumer, q1, q2, q3 = app_and_client
    from asyncgw.models.request import AsyncRequestEnvelope, RequestType
    from datetime import datetime, timezone

    # Switch to GCP mode
    app.state.settings.environment_mode = "gcp"

    batch_id = "batch_gcp_mode_test"
    env = AsyncRequestEnvelope(
        request_id=batch_id,
        request_type=RequestType.BATCH,
        total_items=2,
        created_at=datetime.now(timezone.utc),
    )
    await tracker.register_request(env)

    gcs_uri = "gs://asyncgw-responses-storage/responses/batch_gcp_mode_test.json"
    # Save into storage using normalized path
    await storage.save_json(
        "responses/batch_gcp_mode_test.json",
        {
            "id": batch_id,
            "object": "batch",
            "status": "COMPLETED",
            "results": [{"id": "1", "custom_id": "c1", "response": {"status_code": 200}}],
        },
    )
    await tracker.mark_completed(
        request_id=batch_id,
        response_gcs_uri=gcs_uri,
        response_status_code=200,
        response_content_length=100,
        elapsed_seconds=0.3,
        backend_service_id="gcp-provisioned-gemini",
        backend_batch_service_mode="native",
    )

    res = await client.get(f"/v1/batches/{batch_id}/output")
    assert res.status_code == 200
    data = res.json()

    assert data["total_items"] == 1
    assert data["returned_items"] == 1
    assert data["results_uri"] == "gs://asyncgw-responses-storage/responses/batch_gcp_mode_test.json"



@pytest.mark.asyncio
async def test_admin_infra_metadata_and_containers(app_and_client):
    app, client, tracker, storage, producer, consumer, q1, q2, q3 = app_and_client

    # 1. Test /v1/admin/info
    res_info = await client.get("/v1/admin/info")
    assert res_info.status_code == 200
    info_data = res_info.json()

    assert "project_id" in info_data
    assert "artifact_registry" in info_data
    ar_info = info_data["artifact_registry"]
    assert ar_info["repository"] == "asyncgw-docker"
    assert len(ar_info["images"]) >= 2
    
    img_names = [img["name"] for img in ar_info["images"]]
    assert "asyncgw-gateway" in img_names
    assert "asyncgw-worker" in img_names

    # Check Cloud Run metadata
    assert "cloud_run" in info_data
    cr_info = info_data["cloud_run"]
    assert len(cr_info["services"]) >= 2
    assert len(cr_info["jobs"]) >= 2

    svc_names = [s["name"] for s in cr_info["services"]]
    assert "asyncgw-worker-fleet" in svc_names
    assert "asyncgw-gateway" in svc_names

    job_names = [j["name"] for j in cr_info["jobs"]]
    assert "asyncgw-job-primary" in job_names
    assert "asyncgw-job-batch" in job_names

    # Check trigger details
    fleet_svc = next(s for s in cr_info["services"] if s["name"] == "asyncgw-worker-fleet")
    assert "Pub/Sub Streaming Pull" in fleet_svc["trigger_type"]
    assert "asyncgw-requests-sub" in fleet_svc["trigger_details"]

    primary_job = next(j for j in cr_info["jobs"] if j["name"] == "asyncgw-job-primary")
    assert "Cloud Scheduler" in primary_job["trigger_type"]

    batch_job = next(j for j in cr_info["jobs"] if j["name"] == "asyncgw-job-batch")
    assert "Batch Event" in batch_job["trigger_type"] or "Cloud Scheduler" in batch_job["trigger_type"]

    # 2. Test /v1/admin/infra alias endpoint
    res_infra = await client.get("/v1/admin/infra")
    assert res_infra.status_code == 200
    assert res_infra.json() == info_data


@pytest.mark.asyncio
async def test_ui_dashboard_rendered_content(app_and_client):
    from asyncgw.ui.app import create_ui_app
    app, client, tracker, storage, producer, consumer, q1, q2, q3 = app_and_client
    
    ui_app = create_ui_app(app)
    from httpx import AsyncClient, ASGITransport
    async with AsyncClient(transport=ASGITransport(app=ui_app), base_url="http://test") as ui_client:
        res = await ui_client.get("/")
        assert res.status_code == 200
        html = res.text

        # Verify GCP Infrastructure sections are present
        assert "GCP Infrastructure" in html
        assert "Artifact Registry Container Images" in html
        assert "asyncgw-docker" in html
        assert "asyncgw-gateway" in html
        assert "asyncgw-worker" in html
        assert "Dockerfile.gateway" in html
        assert "Dockerfile.worker" in html

        # Verify Cloud Run workers and triggers are present
        assert "Cloud Run Workers & Trigger Architecture" in html
        assert "asyncgw-worker-fleet" in html
        assert "Trigger: Continuous Pub/Sub Streaming Pull" in html
        assert "asyncgw-job-primary" in html
        assert "Trigger: Cloud Scheduler / Eventarc / Manual" in html
        assert "asyncgw-job-batch" in html
        assert "Trigger: Batch Enqueue Event / Cloud Scheduler / Manual" in html

        # Verify GCP Console management links on each card
        assert "infra-ar-console-link" in html
        assert "infra-link-gw-image" in html
        assert "infra-link-wk-image" in html
        assert "infra-link-worker-fleet" in html
        assert "infra-link-job-primary" in html
        assert "infra-link-job-batch" in html
        assert "infra-link-gateway-service" in html
        assert "infra-link-pubsub" in html
        assert "infra-link-bigquery" in html
        assert "infra-link-storage" in html
        assert "infra-link-gw-sa" in html
        assert "infra-link-wk-sa" in html


@pytest.mark.asyncio
async def test_get_request_status_metadata_policy_and_backends_tried(app_and_client):
    from asyncgw.batch.splitter import BatchSplitter
    from asyncgw.workers.primary_worker import PrimaryRequestWorker
    from asyncgw.models.request import AsyncRequestEnvelope, RequestType

    app, client, tracker, storage, producer, consumer, q1, q2, q3 = app_and_client
    backend_clients = app.state.backend_clients
    routing_engine = app.state.routing_engine

    # Configure primary backend to fail with 500
    backend_clients["gcp-provisioned-gemini"].configure_failure(status_code=500, message="Out of capacity", count=5)

    # 1. Submit request via API
    payload = {
        "model": "gemini-2.0-flash",
        "messages": [{"role": "user", "content": "Explain metadata"}],
        "tags": {"user_id": "alice_123"},
    }
    res = await client.post("/v1/chat/completions", json=payload)
    assert res.status_code == 202
    req_id = res.json()["request_id"]

    # 2. Check pending status has initial policy info
    status_pending = await client.get(f"/v1/requests/{req_id}")
    assert status_pending.status_code == 200
    pending_data = status_pending.json()
    assert pending_data["status"] == "PENDING"
    assert "routing_policy" in pending_data["metadata"]
    assert pending_data["metadata"]["user_id"] == "alice_123"

    # 3. Simulate worker processing with failover
    splitter = BatchSplitter(tracker, storage, producer)
    worker = PrimaryRequestWorker(tracker, storage, routing_engine, splitter)

    # Dequeue envelope from q1
    envelope = await q1.get()
    await worker.process_envelope(envelope)

    # 4. Check completed status via API endpoint
    status_res = await client.get(f"/v1/requests/{req_id}")
    assert status_res.status_code == 200
    data = status_res.json()

    assert data["status"] == "COMPLETED"
    assert data["backend_service_id"] == "gemini-flex"

    # Verify policy & strategy info in metadata
    meta = data["metadata"]
    assert "routing_policy" in meta
    assert meta["routing_policy"]["strategy_id"] == "cost_optimized_with_failover"
    assert "gcp-provisioned-gemini" in meta["routing_policy"]["preference_order"]
    assert "gemini-flex" in meta["routing_policy"]["preference_order"]

    # Verify backends tried and failover reasons
    assert "backends_tried" in meta
    attempts = meta["backends_tried"]
    assert len(attempts) >= 2

    first_attempt = attempts[0]
    assert first_attempt["backend_service_id"] == "gcp-provisioned-gemini"
    assert first_attempt["status_code"] == 500
    assert first_attempt["success"] is False
    assert "Out of capacity" in first_attempt["error"]
    assert "cost_optimized_with_failover" in first_attempt["reason"] or "policy" in first_attempt["reason"]

    last_attempt = attempts[-1]
    assert last_attempt["backend_service_id"] == "gemini-flex"
    assert last_attempt["status_code"] == 200
    assert last_attempt["success"] is True

    # User tags preserved
    assert meta["user_id"] == "alice_123"

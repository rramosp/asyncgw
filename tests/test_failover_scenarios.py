"""Tests simulating backend failure and automatic failover scenarios."""

import pytest
from asyncgw.backends.mock_backend import MockBackend
from asyncgw.batch.splitter import BatchSplitter
from asyncgw.models.request import AsyncRequestEnvelope, RequestType
from asyncgw.models.response import RequestStatusEnum
from asyncgw.workers.primary_worker import PrimaryRequestWorker


@pytest.mark.asyncio
async def test_automatic_failover_on_500_server_error(mock_storage, mock_queues, backend_clients, routing_engine):
    tracker, storage = mock_storage
    producer, consumer, q1, q2, q3 = mock_queues

    splitter = BatchSplitter(tracker, storage, producer)
    worker = PrimaryRequestWorker(tracker, storage, routing_engine, splitter)

    # Configure primary backend (gcp-provisioned-gemini) to fail with 500
    backend_clients["gcp-provisioned-gemini"].configure_failure(status_code=500, message="Capacity saturated", count=5)
    # Secondary backend (gemini-flex) is healthy

    env = AsyncRequestEnvelope(
        request_id="req_failover_500",
        request_type=RequestType.CHAT_COMPLETION,
        model="gemini-2.0-flash",
        payload={"messages": [{"role": "user", "content": "Hello"}]},
    )
    await tracker.register_request(env)

    # Process envelope
    await worker.process_envelope(env)

    # Request should succeed via failover to gemini-flex
    status = await tracker.get_request_status("req_failover_500")
    assert status.status == RequestStatusEnum.COMPLETED
    assert status.backend_service_id == "gemini-flex"
    assert status.response_gcs_uri is not None


@pytest.mark.asyncio
async def test_automatic_failover_on_429_rate_limit(mock_storage, mock_queues, backend_clients, routing_engine):
    tracker, storage = mock_storage
    producer, consumer, q1, q2, q3 = mock_queues

    splitter = BatchSplitter(tracker, storage, producer)
    worker = PrimaryRequestWorker(tracker, storage, routing_engine, splitter)

    # Configure provisioned to return 429
    backend_clients["gcp-provisioned-gemini"].configure_failure(status_code=429, message="Rate limit exceeded", count=5)

    env = AsyncRequestEnvelope(
        request_id="req_failover_429",
        request_type=RequestType.CHAT_COMPLETION,
        model="gemini-2.0-flash",
        payload={"messages": [{"role": "user", "content": "Test"}]},
    )
    await tracker.register_request(env)

    await worker.process_envelope(env)

    status = await tracker.get_request_status("req_failover_429")
    assert status.status == RequestStatusEnum.COMPLETED
    assert status.backend_service_id == "gemini-flex"


@pytest.mark.asyncio
async def test_all_backends_fail_scenario(mock_storage, mock_queues, backend_clients, routing_engine):
    tracker, storage = mock_storage
    producer, consumer, q1, q2, q3 = mock_queues

    splitter = BatchSplitter(tracker, storage, producer)
    worker = PrimaryRequestWorker(tracker, storage, routing_engine, splitter)

    # All backends fail
    for client in backend_clients.values():
        client.configure_failure(status_code=503, message="Global outage", count=10)

    env = AsyncRequestEnvelope(
        request_id="req_all_fail",
        request_type=RequestType.CHAT_COMPLETION,
        model="gemini-2.0-flash",
        payload={"messages": [{"role": "user", "content": "Test"}]},
    )
    await tracker.register_request(env)

    await worker.process_envelope(env)

    # Should be marked FAILED in BigQuery
    status = await tracker.get_request_status("req_all_fail")
    assert status.status == RequestStatusEnum.FAILED
    assert "Global outage" in status.error_message


@pytest.mark.asyncio
async def test_failover_metadata_and_policy_trace(mock_storage, mock_queues, backend_clients, routing_engine):
    tracker, storage = mock_storage
    producer, consumer, q1, q2, q3 = mock_queues

    splitter = BatchSplitter(tracker, storage, producer)
    worker = PrimaryRequestWorker(tracker, storage, routing_engine, splitter)

    # Primary (gcp-provisioned-gemini) fails with 500
    backend_clients["gcp-provisioned-gemini"].configure_failure(status_code=500, message="Service saturated", count=5)

    env = AsyncRequestEnvelope(
        request_id="req_trace_test",
        request_type=RequestType.CHAT_COMPLETION,
        model="gemini-2.0-flash",
        payload={"messages": [{"role": "user", "content": "Hello failover"}]},
        tags={"client_tag": "test_client"},
    )
    await tracker.register_request(env)

    # Process envelope
    await worker.process_envelope(env)

    status = await tracker.get_request_status("req_trace_test")
    assert status.status == RequestStatusEnum.COMPLETED
    assert status.backend_service_id == "gemini-flex"

    # Verify metadata fields
    assert status.metadata is not None
    assert "routing_policy" in status.metadata
    policy = status.metadata["routing_policy"]
    assert policy["strategy_id"] == "cost_optimized_with_failover"
    assert "gcp-provisioned-gemini" in policy["preference_order"]
    assert "gemini-flex" in policy["preference_order"]

    assert "backends_tried" in status.metadata
    attempts = status.metadata["backends_tried"]
    assert len(attempts) >= 2

    # First attempt: failed on gcp-provisioned-gemini with 500
    first_failed = attempts[0]
    assert first_failed["backend_service_id"] == "gcp-provisioned-gemini"
    assert first_failed["status_code"] == 500
    assert first_failed["success"] is False
    assert "Service saturated" in first_failed["error"]
    assert "Retrying" in first_failed["reason"] or "Failing over" in first_failed["reason"]

    # Final attempt: succeeded on gemini-flex
    successful_attempt = attempts[-1]
    assert successful_attempt["backend_service_id"] == "gemini-flex"
    assert successful_attempt["status_code"] == 200
    assert successful_attempt["success"] is True
    assert "success" in successful_attempt["reason"].lower()

    # User tag preserved
    assert status.metadata["client_tag"] == "test_client"

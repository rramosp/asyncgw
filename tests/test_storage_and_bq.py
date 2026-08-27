"""Unit tests for storage and BigQuery request tracker operations."""

import pytest
from asyncgw.models.request import AsyncRequestEnvelope, RequestType
from asyncgw.models.response import RequestStatusEnum


@pytest.mark.asyncio
async def test_request_tracker_lifecycle_transitions(mock_storage):
    tracker, storage = mock_storage

    env = AsyncRequestEnvelope(
        request_id="req_tracker_1",
        request_type=RequestType.CHAT_COMPLETION,
        model="gemini-2.0-flash",
        max_wait_seconds=60,
        tags={"client": "test_app"},
    )

    # 1. Register PENDING
    await tracker.register_request(env)
    status = await tracker.get_request_status("req_tracker_1")
    assert status.status == RequestStatusEnum.PENDING
    assert status.model == "gemini-2.0-flash"
    assert status.metadata == {"client": "test_app"}

    # 2. Mark PROCESSING
    await tracker.mark_processing(
        request_id="req_tracker_1",
        backend_service_id="gcp-provisioned-gemini",
        backend_endpoint="https://aiplatform.googleapis.com",
    )
    status2 = await tracker.get_request_status("req_tracker_1")
    assert status2.status == RequestStatusEnum.PROCESSING
    assert status2.started_at is not None
    assert status2.backend_service_id == "gcp-provisioned-gemini"

    # 3. Mark COMPLETED
    gcs_uri = await storage.save_json("responses/req_tracker_1.json", {"choices": [{"message": {"content": "Hello"}}]})
    await tracker.mark_completed(
        request_id="req_tracker_1",
        response_gcs_uri=gcs_uri,
        response_status_code=200,
        response_content_length=42,
        elapsed_seconds=0.45,
        backend_service_id="gcp-provisioned-gemini",
        content_tokens=15,
    )
    status3 = await tracker.get_request_status("req_tracker_1")
    assert status3.status == RequestStatusEnum.COMPLETED
    assert status3.response_gcs_uri == gcs_uri
    assert status3.elapsed_seconds == 0.45
    assert status3.content_tokens == 15

    # Verify recent list
    recent = await tracker.list_recent_requests(limit=10)
    assert len(recent) == 1
    assert recent[0].request_id == "req_tracker_1"


@pytest.mark.asyncio
async def test_request_tracker_failed_and_timed_out(mock_storage):
    tracker, storage = mock_storage

    # Test FAILED
    env_failed = AsyncRequestEnvelope(
        request_id="req_failed_1",
        request_type=RequestType.CHAT_COMPLETION,
    )
    await tracker.register_request(env_failed)
    await tracker.mark_failed(
        request_id="req_failed_1",
        error_message="Backend connection refused",
        response_status_code=502,
        backend_service_id="openai-direct",
    )
    status_f = await tracker.get_request_status("req_failed_1")
    assert status_f.status == RequestStatusEnum.FAILED
    assert "refused" in status_f.error_message

    # Test TIMED_OUT
    env_to = AsyncRequestEnvelope(
        request_id="req_to_1",
        request_type=RequestType.CHAT_COMPLETION,
    )
    await tracker.register_request(env_to)
    await tracker.mark_timed_out(
        request_id="req_to_1",
        error_message="User deadline expired",
    )
    status_to = await tracker.get_request_status("req_to_1")
    assert status_to.status == RequestStatusEnum.TIMED_OUT

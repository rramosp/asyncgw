"""Tests simulating deadline expiration and user-specified max wait time timeout scenarios."""

from datetime import datetime, timedelta, timezone
import pytest
from asyncgw.batch.splitter import BatchSplitter
from asyncgw.models.request import AsyncRequestEnvelope, RequestType
from asyncgw.models.response import RequestStatusEnum
from asyncgw.workers.primary_worker import PrimaryRequestWorker


@pytest.mark.asyncio
async def test_user_max_wait_time_expiration(mock_storage, mock_queues, routing_engine):
    tracker, storage = mock_storage
    producer, consumer, q1, q2, q3 = mock_queues

    splitter = BatchSplitter(tracker, storage, producer)
    worker = PrimaryRequestWorker(tracker, storage, routing_engine, splitter)

    # Request that expired in the past
    past_time = datetime.now(timezone.utc) - timedelta(seconds=10)
    env = AsyncRequestEnvelope(
        request_id="req_expired_user",
        request_type=RequestType.CHAT_COMPLETION,
        model="gemini-2.0-flash",
        max_wait_seconds=5,
        created_at=past_time,
        expires_at=past_time + timedelta(seconds=5), # expired 5s ago
        payload={"messages": [{"role": "user", "content": "Too late"}]},
    )
    await tracker.register_request(env)

    # Process through worker
    await worker.process_envelope(env)

    # Status must be TIMED_OUT in BigQuery
    status = await tracker.get_request_status("req_expired_user")
    assert status.status == RequestStatusEnum.TIMED_OUT
    assert "exceeded maximum wait" in status.error_message

    # GCS response payload must contain timeout error
    err_data = await storage.get_json(f"responses/{env.request_id}.json")
    assert err_data["error"]["code"] == 408
    assert err_data["error"]["type"] == "timeout_error"

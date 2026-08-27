"""Unit and integration tests for Primary Request Worker."""

import pytest
from asyncgw.batch.splitter import BatchSplitter
from asyncgw.models.request import AsyncRequestEnvelope, RequestType
from asyncgw.models.response import RequestStatusEnum
from asyncgw.workers.primary_worker import PrimaryRequestWorker


@pytest.mark.asyncio
async def test_primary_worker_processes_online_request(mock_storage, mock_queues, routing_engine):
    tracker, storage = mock_storage
    producer, consumer, q1, q2, q3 = mock_queues

    splitter = BatchSplitter(tracker, storage, producer)
    worker = PrimaryRequestWorker(tracker, storage, routing_engine, splitter)

    env = AsyncRequestEnvelope(
        request_id="test_worker_req_1",
        request_type=RequestType.CHAT_COMPLETION,
        model="gemini-2.0-flash",
        payload={"messages": [{"role": "user", "content": "Explain async gateways"}]},
    )
    await tracker.register_request(env)

    # Process envelope through worker
    await worker.process_envelope(env)

    # Check status
    status = await tracker.get_request_status("test_worker_req_1")
    assert status.status == RequestStatusEnum.COMPLETED
    assert status.backend_service_id == "gcp-provisioned-gemini"
    assert status.response_gcs_uri is not None
    assert status.elapsed_seconds is not None

    # Check response payload stored in GCS
    data = await storage.get_json(status.response_gcs_uri)
    assert "choices" in data
    assert len(data["choices"]) > 0


@pytest.mark.asyncio
async def test_primary_worker_handles_batch_decomposing(mock_storage, mock_queues, routing_engine):
    tracker, storage = mock_storage
    producer, consumer, q1, q2, q3 = mock_queues

    splitter = BatchSplitter(tracker, storage, producer)
    worker = PrimaryRequestWorker(tracker, storage, routing_engine, splitter)

    # Batch targeting gemini-flex (which does NOT support batch, requiring decomposing)
    items = [
        {"custom_id": "c1", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "gemini-2.0-flash", "messages": [{"role": "user", "content": "1"}]}},
        {"custom_id": "c2", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "gemini-2.0-flash", "messages": [{"role": "user", "content": "2"}]}},
    ]
    env = AsyncRequestEnvelope(
        request_id="batch_decomp_1",
        request_type=RequestType.BATCH,
        target_backend="gemini-flex",
        model="gemini-2.0-flash",
        payload={"requests": items},
    )
    await tracker.register_request(env)

    await worker.process_envelope(env)

    # Parent batch should be in PROCESSING (sub-requests enqueued)
    parent_status = await tracker.get_request_status("batch_decomp_1")
    assert parent_status.status == RequestStatusEnum.PROCESSING

    # Check secondary queue has 2 items
    assert q2.qsize() == 2
    sub_records = await tracker.get_batch_sub_requests("batch_decomp_1")
    assert len(sub_records) == 2

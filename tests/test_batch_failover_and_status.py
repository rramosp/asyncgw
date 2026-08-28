"""Tests verifying batch request fallback strategies, failure status behavior, and per-item metadata."""

import pytest
from asyncgw.backends.mock_backend import MockBackend
from asyncgw.batch.reassembler import BatchReassembler
from asyncgw.batch.splitter import BatchSplitter
from asyncgw.models.request import AsyncRequestEnvelope, RequestType
from asyncgw.models.response import RequestStatusEnum
from asyncgw.workers.batch_worker import BatchSubRequestWorker
from asyncgw.workers.primary_worker import PrimaryRequestWorker


@pytest.mark.asyncio
async def test_native_batch_individual_request_failover_success(mock_storage, mock_queues, backend_clients, routing_engine):
    """Test that individual items within a native batch go through fallback strategy when primary backend fails."""
    tracker, storage = mock_storage
    producer, consumer, q1, q2, q3 = mock_queues

    splitter = BatchSplitter(tracker, storage, producer)
    worker = PrimaryRequestWorker(tracker, storage, routing_engine, splitter)

    # Configure primary backend (gcp-provisioned-gemini) to fail with 500
    # Secondary backend (gemini-flex) is healthy
    backend_clients["gcp-provisioned-gemini"].configure_failure(status_code=500, message="Provisioned capacity unavailable", count=10)

    items = [
        {"custom_id": "item_0", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "gemini-2.0-flash", "messages": [{"role": "user", "content": "Question 0"}]}},
        {"custom_id": "item_1", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "gemini-2.0-flash", "messages": [{"role": "user", "content": "Question 1"}]}},
    ]
    parent_env = AsyncRequestEnvelope(
        request_id="batch_native_failover_success",
        request_type=RequestType.BATCH,
        model="gemini-2.0-flash",
        payload={"requests": items},
    )
    await tracker.register_request(parent_env)

    # Process batch envelope
    await worker.process_envelope(parent_env)

    # The batch should complete successfully because individual requests fell back to gemini-flex
    parent_record = await tracker.get_request_status("batch_native_failover_success")
    assert parent_record.status == RequestStatusEnum.COMPLETED
    assert parent_record.backend_service_id == "gemini-flex"
    assert parent_record.response_gcs_uri is not None

    # Parent metadata should only have request counts and not collated backends_tried
    assert parent_record.metadata is not None
    assert "request_counts" in parent_record.metadata
    assert parent_record.metadata["request_counts"]["total"] == 2
    assert parent_record.metadata["request_counts"]["completed"] == 2
    assert parent_record.metadata["request_counts"]["failed"] == 0
    assert "backends_tried" not in parent_record.metadata
    assert "failover_trace" not in parent_record.metadata

    final_output = await storage.get_json(parent_record.response_gcs_uri)
    assert final_output["status"] == "COMPLETED"
    assert final_output["request_counts"]["total"] == 2
    assert final_output["request_counts"]["completed"] == 2
    assert final_output["request_counts"]["failed"] == 0
    assert len(final_output["results"]) == 2

    # Item 0 experienced in-flight failover from provisioned to flex
    item_0 = final_output["results"][0]
    assert item_0["response"] is not None
    assert "metadata" in item_0
    assert item_0["metadata"] is not None
    assert "backends_tried" in item_0["metadata"]
    assert len(item_0["metadata"]["backends_tried"]) >= 2
    assert item_0["metadata"]["backends_tried"][0]["backend_service_id"] == "gcp-provisioned-gemini"
    assert item_0["metadata"]["backends_tried"][-1]["backend_service_id"] == "gemini-flex"
    assert item_0["metadata"]["backends_tried"][-1]["success"] is True

    # Item 1 was served by healthy gemini-flex
    item_1 = final_output["results"][1]
    assert item_1["response"] is not None
    assert "metadata" in item_1
    assert item_1["metadata"] is not None
    assert item_1["metadata"]["backends_tried"][-1]["backend_service_id"] == "gemini-flex"
    assert item_1["metadata"]["backends_tried"][-1]["success"] is True


@pytest.mark.asyncio
async def test_native_batch_marks_failed_when_individual_request_fails(mock_storage, mock_queues, backend_clients, routing_engine):
    """Test that a native batch is marked as FAILED if at least one individual request fails."""
    tracker, storage = mock_storage
    producer, consumer, q1, q2, q3 = mock_queues

    splitter = BatchSplitter(tracker, storage, producer)
    worker = PrimaryRequestWorker(tracker, storage, routing_engine, splitter)

    # Configure ALL backends to fail for 1 call (simulating one request failing completely across all backends)
    for client in backend_clients.values():
        client.configure_failure(status_code=500, message="Total outage on item 1", count=10)

    items = [
        {"custom_id": "item_0", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "gemini-2.0-flash", "messages": [{"role": "user", "content": "Question 0"}]}},
        {"custom_id": "item_1", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "gemini-2.0-flash", "messages": [{"role": "user", "content": "Question 1"}]}},
    ]
    parent_env = AsyncRequestEnvelope(
        request_id="batch_native_failed_status",
        request_type=RequestType.BATCH,
        model="gemini-2.0-flash",
        payload={"requests": items},
    )
    await tracker.register_request(parent_env)

    await worker.process_envelope(parent_env)

    # The batch should appear as FAILED because at least one individual request failed
    parent_record = await tracker.get_request_status("batch_native_failed_status")
    assert parent_record.status == RequestStatusEnum.FAILED
    assert "Batch failed" in parent_record.error_message
    assert parent_record.response_gcs_uri is not None

    # Parent metadata contains counts only
    assert parent_record.metadata is not None
    assert parent_record.metadata["request_counts"]["failed"] >= 1
    assert "backends_tried" not in parent_record.metadata

    final_output = await storage.get_json(parent_record.response_gcs_uri)
    assert final_output["status"] == "FAILED"
    assert final_output["request_counts"]["failed"] >= 1
    # Each item has its own metadata
    for item in final_output["results"]:
        assert "metadata" in item
        assert "backends_tried" in item["metadata"]


@pytest.mark.asyncio
async def test_decomposed_batch_marks_failed_when_one_subrequest_fails(mock_storage, mock_queues, backend_clients, routing_engine):
    """Test that a decomposed batch is marked as FAILED in BigQuery and GCS if one sub-request fails."""
    tracker, storage = mock_storage
    producer, consumer, q1, q2, q3 = mock_queues

    splitter = BatchSplitter(tracker, storage, producer)
    reassembler = BatchReassembler(tracker, storage)
    primary_worker = PrimaryRequestWorker(tracker, storage, routing_engine, splitter)
    batch_worker = BatchSubRequestWorker(tracker, storage, routing_engine, reassembler)

    # 3-item batch targeting gemini-flex (forces decomposing)
    items = [
        {"custom_id": f"item_{i}", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "gemini-2.0-flash", "messages": [{"role": "user", "content": f"Q{i}"}]}}
        for i in range(3)
    ]
    parent_env = AsyncRequestEnvelope(
        request_id="batch_decomp_failure_test",
        request_type=RequestType.BATCH,
        target_backend="gemini-flex",
        model="gemini-2.0-flash",
        payload={"requests": items},
    )
    await tracker.register_request(parent_env)
    await primary_worker.process_envelope(parent_env)

    # Pull sub-requests from secondary queue
    sub_envelopes = []
    while not q2.empty():
        sub_envelopes.append(await q2.get())
    assert len(sub_envelopes) == 3

    # Item 0: succeeds
    await batch_worker.process_sub_request(sub_envelopes[0])

    # Item 1: fails across all backends
    for client in backend_clients.values():
        client.configure_failure(status_code=500, message="Item 1 backend error", count=5)
    await batch_worker.process_sub_request(sub_envelopes[1])
    for client in backend_clients.values():
        client.reset()

    # Item 2: succeeds
    await batch_worker.process_sub_request(sub_envelopes[2])

    # Verify parent batch status is FAILED because item 1 failed
    parent_record = await tracker.get_request_status("batch_decomp_failure_test")
    assert parent_record.status == RequestStatusEnum.FAILED
    assert parent_record.response_gcs_uri is not None

    # Verify parent metadata has request_counts and not collated sub-request traces
    assert parent_record.metadata is not None
    assert "request_counts" in parent_record.metadata
    assert parent_record.metadata["request_counts"]["total"] == 3
    assert parent_record.metadata["request_counts"]["completed"] == 2
    assert parent_record.metadata["request_counts"]["failed"] == 1
    assert "backends_tried" not in parent_record.metadata
    assert "failover_trace" not in parent_record.metadata

    final_output = await storage.get_json(parent_record.response_gcs_uri)
    assert final_output["status"] == "FAILED"
    assert final_output["request_counts"]["total"] == 3
    assert final_output["request_counts"]["completed"] == 2
    assert final_output["request_counts"]["failed"] == 1

    # Check results array - each item has its own metadata
    assert final_output["results"][0]["response"] is not None
    assert final_output["results"][0]["metadata"] is not None
    assert "backends_tried" in final_output["results"][0]["metadata"]

    assert final_output["results"][1]["response"] is None
    assert final_output["results"][1]["error"]["code"] == 500
    assert final_output["results"][1]["metadata"] is not None
    assert "backends_tried" in final_output["results"][1]["metadata"]

    assert final_output["results"][2]["response"] is not None
    assert final_output["results"][2]["metadata"] is not None


@pytest.mark.asyncio
async def test_decomposed_batch_individual_failover_success(mock_storage, mock_queues, backend_clients, routing_engine):
    """Test that decomposed batch sub-requests go through failover and succeed if fallback backend is available."""
    tracker, storage = mock_storage
    producer, consumer, q1, q2, q3 = mock_queues

    splitter = BatchSplitter(tracker, storage, producer)
    reassembler = BatchReassembler(tracker, storage)
    primary_worker = PrimaryRequestWorker(tracker, storage, routing_engine, splitter)
    batch_worker = BatchSubRequestWorker(tracker, storage, routing_engine, reassembler)

    items = [
        {"custom_id": "c1", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "gemini-2.0-flash", "messages": [{"role": "user", "content": "Q1"}]}},
        {"custom_id": "c2", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "gemini-2.0-flash", "messages": [{"role": "user", "content": "Q2"}]}},
    ]
    parent_env = AsyncRequestEnvelope(
        request_id="batch_decomp_fallback_success",
        request_type=RequestType.BATCH,
        target_backend="gemini-flex",
        model="gemini-2.0-flash",
        payload={"requests": items},
    )
    await tracker.register_request(parent_env)
    await primary_worker.process_envelope(parent_env)

    # Primary for sub-requests (gemini-flex) is configured to fail with 500
    # Secondary candidate (gcp-provisioned-gemini) is healthy
    backend_clients["gemini-flex"].configure_failure(status_code=500, message="Gemini Flex saturated", count=10)

    sub_envelopes = []
    while not q2.empty():
        sub_envelopes.append(await q2.get())

    for sub_env in sub_envelopes:
        await batch_worker.process_sub_request(sub_env)

    parent_record = await tracker.get_request_status("batch_decomp_fallback_success")
    assert parent_record.status == RequestStatusEnum.COMPLETED
    assert parent_record.backend_service_id == "gcp-provisioned-gemini"

    # Parent metadata has counts only
    assert parent_record.metadata is not None
    assert parent_record.metadata["request_counts"]["completed"] == 2
    assert "backends_tried" not in parent_record.metadata

    final_output = await storage.get_json(parent_record.response_gcs_uri)
    assert final_output["status"] == "COMPLETED"
    assert final_output["request_counts"]["completed"] == 2
    assert final_output["request_counts"]["failed"] == 0

    # Individual items have their own failover trace showing fallback from gemini-flex to gcp-provisioned-gemini
    for item in final_output["results"]:
        assert item["metadata"] is not None
        assert "backends_tried" in item["metadata"]
        assert item["metadata"]["backends_tried"][0]["backend_service_id"] == "gemini-flex"
        assert item["metadata"]["backends_tried"][-1]["backend_service_id"] == "gcp-provisioned-gemini"
        assert item["metadata"]["backends_tried"][-1]["success"] is True

"""Failure scenario tests specifically focusing on decomposed batch sub-request processing, worker crashes, and partial error aggregation."""

import pytest
from asyncgw.backends.mock_backend import MockBackend
from asyncgw.batch.reassembler import BatchReassembler
from asyncgw.batch.splitter import BatchSplitter
from asyncgw.models.request import AsyncRequestEnvelope, RequestType
from asyncgw.models.response import RequestStatusEnum
from asyncgw.workers.batch_worker import BatchSubRequestWorker
from asyncgw.workers.primary_worker import PrimaryRequestWorker


@pytest.mark.asyncio
async def test_batch_decomposition_with_partial_worker_failure(mock_storage, mock_queues, backend_clients, routing_engine):
    """Simulate a batch decomposed into 5 items where item #2 fails/crashes."""
    tracker, storage = mock_storage
    producer, consumer, q1, q2, q3 = mock_queues

    splitter = BatchSplitter(tracker, storage, producer)
    reassembler = BatchReassembler(tracker, storage)
    primary_worker = PrimaryRequestWorker(tracker, storage, routing_engine, splitter)
    batch_worker = BatchSubRequestWorker(tracker, storage, routing_engine, reassembler)

    # 1. Create a 5-item batch targeting gemini-flex (forces decomposing)
    items = [
        {"custom_id": f"item_{i}", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "gemini-2.0-flash", "messages": [{"role": "user", "content": f"Query {i}"}]}}
        for i in range(5)
    ]
    parent_env = AsyncRequestEnvelope(
        request_id="batch_chaos_5_items",
        request_type=RequestType.BATCH,
        target_backend="gemini-flex",
        model="gemini-2.0-flash",
        payload={"requests": items},
    )
    await tracker.register_request(parent_env)

    # 2. Primary worker splits and publishes to secondary queue
    await primary_worker.process_envelope(parent_env)
    assert q2.qsize() == 5

    # 3. Pull sub-requests and simulate worker processing
    sub_envelopes = []
    while not q2.empty():
        sub_envelopes.append(await q2.get())

    assert len(sub_envelopes) == 5

    # Process items: simulate worker crashing or backend 500 error on item with sequence 2
    for env in sub_envelopes:
        seq = env.sequence_number
        if seq == 2:
            # Simulate failure on item 2 across all failover candidates
            for client in backend_clients.values():
                client.configure_failure(status_code=500, message="Worker node crashed", count=5)
            await batch_worker.process_sub_request(env)
            for client in backend_clients.values():
                client.reset()
        else:
            await batch_worker.process_sub_request(env)

    # 4. Verify sub-request records in BigQuery
    sub_records = await tracker.get_batch_sub_requests("batch_chaos_5_items")
    assert len(sub_records) == 5
    assert sub_records[0].status == RequestStatusEnum.COMPLETED
    assert sub_records[1].status == RequestStatusEnum.COMPLETED
    assert sub_records[2].status == RequestStatusEnum.FAILED # Failed item
    assert sub_records[3].status == RequestStatusEnum.COMPLETED
    assert sub_records[4].status == RequestStatusEnum.COMPLETED

    # 5. Verify parent batch was properly reassembled in GCS
    parent_record = await tracker.get_request_status("batch_chaos_5_items")
    assert parent_record.status == RequestStatusEnum.COMPLETED
    assert parent_record.response_gcs_uri is not None

    final_output = await storage.get_json(parent_record.response_gcs_uri)
    assert final_output["id"] == "batch_chaos_5_items"
    assert final_output["request_counts"]["total"] == 5
    assert final_output["request_counts"]["completed"] == 4
    assert final_output["request_counts"]["failed"] == 1

    # Verify strict sequence preservation (0, 1, 2, 3, 4)
    results = final_output["results"]
    assert len(results) == 5
    assert results[0]["custom_id"] == "item_0"
    assert results[0]["response"] is not None

    assert results[1]["custom_id"] == "item_1"
    assert results[1]["response"] is not None

    assert results[2]["custom_id"] == "item_2"
    assert results[2]["response"] is None
    assert results[2]["error"]["code"] == 500
    assert "Worker node crashed" in results[2]["error"]["message"]

    assert results[3]["custom_id"] == "item_3"
    assert results[3]["response"] is not None

    assert results[4]["custom_id"] == "item_4"
    assert results[4]["response"] is not None

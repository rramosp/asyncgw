"""Unit and integration tests for Batch Sub-Request Worker."""

import pytest
from asyncgw.batch.reassembler import BatchReassembler
from asyncgw.models.request import AsyncRequestEnvelope, RequestType
from asyncgw.models.response import RequestStatusEnum
from asyncgw.workers.batch_worker import BatchSubRequestWorker


@pytest.mark.asyncio
async def test_batch_worker_processes_sub_requests_and_reassembles(mock_storage, routing_engine):
    tracker, storage = mock_storage
    reassembler = BatchReassembler(tracker, storage)
    worker = BatchSubRequestWorker(tracker, storage, routing_engine, reassembler)

    parent_id = "batch_end_to_end_test"
    total_items = 2

    # 1. Register parent in BigQuery
    parent_env = AsyncRequestEnvelope(
        request_id=parent_id,
        request_type=RequestType.BATCH,
        total_items=total_items,
        model="gemini-2.0-flash",
    )
    await tracker.register_request(parent_env)

    # 2. Sub-requests
    sub1 = AsyncRequestEnvelope(
        request_id=f"{parent_id}_0",
        parent_request_id=parent_id,
        sequence_number=0,
        total_items=total_items,
        custom_id="cust_1",
        request_type=RequestType.BATCH_SUB_REQUEST,
        model="gemini-2.0-flash",
        payload={"messages": [{"role": "user", "content": "Query 1"}]},
    )
    sub2 = AsyncRequestEnvelope(
        request_id=f"{parent_id}_1",
        parent_request_id=parent_id,
        sequence_number=1,
        total_items=total_items,
        custom_id="cust_2",
        request_type=RequestType.BATCH_SUB_REQUEST,
        model="gemini-2.0-flash",
        payload={"messages": [{"role": "user", "content": "Query 2"}]},
    )
    await tracker.register_batch_sub_requests([sub1, sub2])

    # Process first sub-request
    await worker.process_sub_request(sub1)
    status_sub1 = await tracker.get_request_status(f"{parent_id}_0", sequence_number=0)
    assert status_sub1.status == RequestStatusEnum.COMPLETED

    # Parent should still be pending/processing
    parent_status1 = await tracker.get_request_status(parent_id)
    assert parent_status1.status != RequestStatusEnum.COMPLETED

    # Process second sub-request (triggers reassembly)
    await worker.process_sub_request(sub2)
    status_sub2 = await tracker.get_request_status(f"{parent_id}_1", sequence_number=1)
    assert status_sub2.status == RequestStatusEnum.COMPLETED

    # Parent batch should now be COMPLETED in BigQuery
    parent_status2 = await tracker.get_request_status(parent_id)
    assert parent_status2.status == RequestStatusEnum.COMPLETED
    assert parent_status2.response_gcs_uri is not None

    # Verify GCS final output
    final_output = await storage.get_json(parent_status2.response_gcs_uri)
    assert final_output["id"] == parent_id
    assert len(final_output["results"]) == 2
    assert final_output["results"][0]["custom_id"] == "cust_1"
    assert final_output["results"][1]["custom_id"] == "cust_2"

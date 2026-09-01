"""Unit and integration tests for Batch Splitter and Batch Reassembler."""

import pytest
from asyncgw.batch.reassembler import BatchReassembler
from asyncgw.workers.batch_worker import BatchSubRequestWorker
from asyncgw.batch.splitter import BatchSplitter
from asyncgw.models.request import AsyncRequestEnvelope, BatchItem, RequestType
from asyncgw.models.response import RequestStatusEnum


@pytest.mark.asyncio
async def test_batch_splitting_and_sequence_numbering(mock_storage, mock_queues):
    tracker, storage = mock_storage
    producer, consumer, q1, q2, q3 = mock_queues

    splitter = BatchSplitter(tracker, storage, producer)

    items = [
        {"custom_id": "item_0", "method": "POST", "url": "/v1/chat/completions", "body": {"messages": [{"role": "user", "content": "Question 0"}]}},
        {"custom_id": "item_1", "method": "POST", "url": "/v1/chat/completions", "body": {"messages": [{"role": "user", "content": "Question 1"}]}},
        {"custom_id": "item_2", "method": "POST", "url": "/v1/chat/completions", "body": {"messages": [{"role": "user", "content": "Question 2"}]}},
    ]
    parent_env = AsyncRequestEnvelope(
        request_id="parent_batch_123",
        request_type=RequestType.BATCH,
        model="gemini-2.0-flash",
        payload={"requests": items},
    )

    sub_envs = await splitter.split_and_enqueue(parent_env)

    assert len(sub_envs) == 3
    assert q2.qsize() == 3  # 3 items published to secondary queue

    for idx, env in enumerate(sub_envs):
        assert env.parent_request_id == "parent_batch_123"
        assert env.sequence_number == idx
        assert env.custom_id == f"item_{idx}"
        assert env.total_items == 3

    # Check BigQuery entries
    sub_records = await tracker.get_batch_sub_requests("parent_batch_123")
    assert len(sub_records) == 3
    assert [r.sequence_number for r in sub_records] == [0, 1, 2]


@pytest.mark.asyncio
async def test_batch_reassembly_strict_sequence_order(mock_storage):
    tracker, storage = mock_storage
    reassembler = BatchReassembler(tracker, storage)

    parent_id = "batch_test_reassemble"
    total_items = 3

    # Register parent
    parent_env = AsyncRequestEnvelope(
        request_id=parent_id,
        request_type=RequestType.BATCH,
        total_items=total_items,
    )
    await tracker.register_request(parent_env)

    # Register sub-requests
    sub_envs = [
        AsyncRequestEnvelope(
            request_id=f"{parent_id}_{i}",
            parent_request_id=parent_id,
            sequence_number=i,
            total_items=total_items,
            custom_id=f"custom_{i}",
            request_type=RequestType.BATCH_SUB_REQUEST,
        )
        for i in range(total_items)
    ]
    await tracker.register_batch_sub_requests(sub_envs)

    # Complete items out-of-order (simulate concurrent worker completions: 2, 0, 1)
    # Item 2
    await reassembler.save_sub_request_part(
        parent_request_id=parent_id,
        sequence_number=2,
        custom_id="custom_2",
        result_data={"choices": [{"message": {"content": "Answer 2"}}]},
    )
    await tracker.mark_completed(
        request_id=f"{parent_id}_2",
        response_gcs_uri=reassembler.get_part_gcs_path(parent_id, 2),
        response_status_code=200,
        response_content_length=50,
        elapsed_seconds=0.1,
        backend_service_id="gemini-flex",
        sequence_number=2,
    )

    # Incomplete: should return None
    res1 = await reassembler.try_reassemble_batch(parent_id)
    assert res1 is None

    # Item 0
    await reassembler.save_sub_request_part(
        parent_request_id=parent_id,
        sequence_number=0,
        custom_id="custom_0",
        result_data={"choices": [{"message": {"content": "Answer 0"}}]},
    )
    await tracker.mark_completed(
        request_id=f"{parent_id}_0",
        response_gcs_uri=reassembler.get_part_gcs_path(parent_id, 0),
        response_status_code=200,
        response_content_length=50,
        elapsed_seconds=0.1,
        backend_service_id="gemini-flex",
        sequence_number=0,
    )

    # Still incomplete
    res2 = await reassembler.try_reassemble_batch(parent_id)
    assert res2 is None

    # Item 1 (Final item completes)
    await reassembler.save_sub_request_part(
        parent_request_id=parent_id,
        sequence_number=1,
        custom_id="custom_1",
        result_data={"choices": [{"message": {"content": "Answer 1"}}]},
    )
    await tracker.mark_completed(
        request_id=f"{parent_id}_1",
        response_gcs_uri=reassembler.get_part_gcs_path(parent_id, 1),
        response_status_code=200,
        response_content_length=50,
        elapsed_seconds=0.1,
        backend_service_id="gemini-flex",
        sequence_number=1,
    )

    # Now all items complete -> reassembly succeeds
    final_batch = await reassembler.try_reassemble_batch(parent_id)
    assert final_batch is not None
    assert final_batch.status == RequestStatusEnum.COMPLETED
    assert len(final_batch.results) == 3

    # Check strict sequence ordering in output: 0, 1, 2
    assert final_batch.results[0].custom_id == "custom_0"
    assert final_batch.results[0].response["body"]["choices"][0]["message"]["content"] == "Answer 0"
    assert final_batch.results[1].custom_id == "custom_1"
    assert final_batch.results[1].response["body"]["choices"][0]["message"]["content"] == "Answer 1"
    assert final_batch.results[2].custom_id == "custom_2"
    assert final_batch.results[2].response["body"]["choices"][0]["message"]["content"] == "Answer 2"

    # Check parent status in BigQuery
    parent_status = await tracker.get_request_status(parent_id)
    assert parent_status.status == RequestStatusEnum.COMPLETED
    assert parent_status.response_gcs_uri is not None

@pytest.mark.asyncio
async def test_batch_sub_requests_status_transitions_not_stuck_in_pending(mock_storage, mock_queues, routing_engine):
    """Verify that sub-requests created from batch decomposition transition out of PENDING to COMPLETED."""
    tracker, storage = mock_storage
    producer, consumer, q1, q2, q3 = mock_queues
    reassembler = BatchReassembler(tracker, storage)
    worker = BatchSubRequestWorker(tracker, storage, routing_engine, reassembler)

    parent_id = "batch_status_trans_test"
    total_items = 2

    parent_env = AsyncRequestEnvelope(
        request_id=parent_id,
        request_type=RequestType.BATCH,
        total_items=total_items,
        model="gemini-2.0-flash",
    )
    await tracker.register_request(parent_env)

    sub_envs = [
        AsyncRequestEnvelope(
            request_id=f"{parent_id}_{i}",
            parent_request_id=parent_id,
            sequence_number=i,
            total_items=total_items,
            custom_id=f"item_{i}",
            request_type=RequestType.BATCH_SUB_REQUEST,
            model="gemini-2.0-flash",
            payload={"messages": [{"role": "user", "content": f"Q {i}"}]},
        )
        for i in range(total_items)
    ]
    await tracker.register_batch_sub_requests(sub_envs)

    # Initial state: sub-requests must be PENDING
    init_subs = await tracker.get_batch_sub_requests(parent_id)
    assert len(init_subs) == 2
    assert all(s.status == RequestStatusEnum.PENDING for s in init_subs)

    # Process first sub-request through BatchSubRequestWorker
    await worker.process_sub_request(sub_envs[0])

    # Check that first sub-request is COMPLETED (NOT pending)
    mid_subs = await tracker.get_batch_sub_requests(parent_id)
    assert len(mid_subs) == 2
    assert mid_subs[0].status == RequestStatusEnum.COMPLETED
    assert mid_subs[0].parent_request_id == parent_id
    assert mid_subs[1].status == RequestStatusEnum.PENDING

    # Process second sub-request
    await worker.process_sub_request(sub_envs[1])

    # Check that all sub-requests are COMPLETED and parent is COMPLETED
    final_subs = await tracker.get_batch_sub_requests(parent_id)
    assert len(final_subs) == 2
    assert all(s.status == RequestStatusEnum.COMPLETED for s in final_subs)

    final_parent = await tracker.get_request_status(parent_id)
    assert final_parent.status == RequestStatusEnum.COMPLETED


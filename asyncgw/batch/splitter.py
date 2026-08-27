"""Batch splitter breaking bulk requests into indexed individual queries."""

import asyncio
import logging
from typing import List, Optional

from asyncgw.models.request import AsyncRequestEnvelope, BatchItem, RequestType
from asyncgw.queue.base import BaseQueueProducer
from asyncgw.storage.base import BaseBlobStorage, BaseRequestTracker

logger = logging.getLogger(__name__)


class BatchSplitter:
    """Splits OpenAI batch requests into individual sub-requests and publishes them to the secondary Pub/Sub queue."""

    def __init__(
        self,
        request_tracker: BaseRequestTracker,
        blob_storage: BaseBlobStorage,
        queue_producer: BaseQueueProducer,
    ):
        self.request_tracker = request_tracker
        self.blob_storage = blob_storage
        self.queue_producer = queue_producer

    async def extract_batch_items(
        self, parent_envelope: AsyncRequestEnvelope
    ) -> List[BatchItem]:
        """Extract list of BatchItem from inline payload or GCS input file."""
        payload = parent_envelope.payload
        items: List[BatchItem] = []

        # 1. Check if inline requests provided
        if "requests" in payload and isinstance(payload["requests"], list):
            for r in payload["requests"]:
                if isinstance(r, dict):
                    items.append(BatchItem(**r))
                elif isinstance(r, BatchItem):
                    items.append(r)
            return items

        # 2. Check input_file_id or raw_input_gcs_uri
        file_uri = (
            parent_envelope.raw_input_gcs_uri
            or payload.get("input_file_id")
            or payload.get("input_gcs_uri")
        )
        if file_uri:
            json_lines = await self.blob_storage.get_jsonl(file_uri)
            for idx, line in enumerate(json_lines):
                custom_id = line.get("custom_id", f"req_{idx}")
                method = line.get("method", "POST")
                url = line.get("url", "/v1/chat/completions")
                body = line.get("body", line)
                items.append(BatchItem(custom_id=custom_id, method=method, url=url, body=body))
            return items

        return items

    async def split_and_enqueue(
        self, parent_envelope: AsyncRequestEnvelope
    ) -> List[AsyncRequestEnvelope]:
        """Break batch into individual sub-requests, register in BigQuery, and enqueue on secondary Pub/Sub."""
        items = await self.extract_batch_items(parent_envelope)
        if not items:
            logger.warning(f"Batch request {parent_envelope.request_id} has 0 items to process.")
            return []

        total_items = len(items)
        sub_envelopes: List[AsyncRequestEnvelope] = []

        for seq, item in enumerate(items):
            sub_env = AsyncRequestEnvelope(
                request_id=f"{parent_envelope.request_id}_{seq}",
                parent_request_id=parent_envelope.request_id,
                sequence_number=seq,
                total_items=total_items,
                custom_id=item.custom_id,
                request_type=RequestType.BATCH_SUB_REQUEST,
                model=item.body.get("model", parent_envelope.model),
                payload=item.body,
                created_at=parent_envelope.created_at,
                expires_at=parent_envelope.expires_at,
                max_wait_seconds=parent_envelope.max_wait_seconds,
                client_id=parent_envelope.client_id,
                priority=parent_envelope.priority,
                tags=parent_envelope.tags.copy() if parent_envelope.tags else {},
                routing_strategy=parent_envelope.routing_strategy,
                target_backend=parent_envelope.target_backend,
            )
            sub_envelopes.append(sub_env)

        # 1. Register all sub-requests in BigQuery tracker
        await self.request_tracker.register_batch_sub_requests(sub_envelopes)
        logger.info(
            f"Registered {total_items} sub-requests in BigQuery for parent batch {parent_envelope.request_id}"
        )

        # 2. Publish each sub-request to the secondary Pub/Sub queue
        for sub_env in sub_envelopes:
            await self.queue_producer.publish_batch_item(sub_env)

        logger.info(
            f"Published {total_items} sub-requests to batch-items Pub/Sub for {parent_envelope.request_id}"
        )
        return sub_envelopes

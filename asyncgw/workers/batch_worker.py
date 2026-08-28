"""Batch Sub-Request Worker consuming individual queries and triggering batch reassembly."""

import asyncio
from datetime import datetime, timezone
import json
import logging
import time
from typing import Optional, Callable, Awaitable, Any

from asyncgw.backends.base import BackendExecutionResult, BaseLLMBackend
from asyncgw.batch.reassembler import BatchReassembler
from asyncgw.config import BackendConfig
from asyncgw.models.request import AsyncRequestEnvelope
from asyncgw.router.engine import RoutingEngine
from asyncgw.storage.base import BaseBlobStorage, BaseRequestTracker

logger = logging.getLogger(__name__)


class BatchSubRequestWorker:
    """Processes broken-down batch sub-requests from the secondary Pub/Sub queue."""

    def __init__(
        self,
        request_tracker: BaseRequestTracker,
        blob_storage: BaseBlobStorage,
        routing_engine: RoutingEngine,
        batch_reassembler: BatchReassembler,
        config_reloader: Optional[Callable[[], Awaitable[Any]]] = None,
    ):
        self.request_tracker = request_tracker
        self.blob_storage = blob_storage
        self.routing_engine = routing_engine
        self.batch_reassembler = batch_reassembler
        self.config_reloader = config_reloader

    async def process_sub_request(self, envelope: AsyncRequestEnvelope) -> None:
        """Process an individual sub-request from a decomposed batch."""
        if self.config_reloader is not None:
            try:
                await self.config_reloader()
            except Exception as e:
                logger.debug(f"Error checking config reloader: {e}")
        parent_id = envelope.parent_request_id or envelope.request_id
        seq = envelope.sequence_number if envelope.sequence_number is not None else 0

        logger.info(
            f"Processing batch sub-request {envelope.request_id} (parent: {parent_id}, seq: {seq}, model: {envelope.model})"
        )

        # 1. Check deadline
        if envelope.is_expired():
            logger.warning(f"Batch sub-request {envelope.request_id} (seq: {seq}) timed out before execution.")
            timeout_meta = {
                **(envelope.tags or {}),
                "error": f"Sub-request exceeded maximum wait deadline ({envelope.max_wait_seconds}s)",
            }
            await self.batch_reassembler.save_sub_request_part(
                parent_request_id=parent_id,
                sequence_number=seq,
                custom_id=envelope.custom_id,
                result_data={},
                is_error=True,
                status_code=408,
                error_message=f"Sub-request exceeded maximum wait deadline ({envelope.max_wait_seconds}s)",
                metadata=timeout_meta,
            )
            await self.request_tracker.mark_timed_out(
                request_id=envelope.request_id,
                error_message=f"Sub-request exceeded maximum wait deadline ({envelope.max_wait_seconds}s)",
                sequence_number=seq,
            )
            await self.batch_reassembler.try_reassemble_batch(parent_id)
            return

        # 2. Mark PROCESSING
        decision = self.routing_engine.route_request(envelope)
        strat_obj = self.routing_engine.strategies_map.get(decision.strategy_id)
        strategy_name = strat_obj.name if strat_obj else decision.strategy_id
        init_policy_info = {
            "strategy_id": decision.strategy_id,
            "strategy_name": strategy_name,
            "selection_reason": decision.reason,
            "preference_order": [b.id for b in decision.all_candidate_backends],
        }
        init_metadata = {
            **(envelope.tags or {}),
            "routing_policy": init_policy_info,
            "strategy_id": decision.strategy_id,
            "selection_reason": decision.reason,
            "backends_tried": [],
            "failover_trace": [],
        }

        await self.request_tracker.mark_processing(
            request_id=envelope.request_id,
            backend_service_id=decision.primary_backend.id,
            backend_endpoint=decision.primary_backend.endpoint_url,
            sequence_number=seq,
            metadata=init_metadata,
        )

        # 3. Execute with failover
        async def _call_backend(client: BaseLLMBackend, cfg: BackendConfig) -> BackendExecutionResult:
            return await client.execute_online(envelope)

        result, served_backend = await self.routing_engine.execute_with_failover(
            envelope, _call_backend
        )

        routing_meta = getattr(result, "routing_metadata", {}) or {}
        final_metadata = {
            **(envelope.tags or {}),
            **routing_meta,
        }

        # 4. Save partial part and update sub-request status
        if result.success:
            part_uri = await self.batch_reassembler.save_sub_request_part(
                parent_request_id=parent_id,
                sequence_number=seq,
                custom_id=envelope.custom_id,
                result_data=result.response_data,
                is_error=False,
                status_code=result.status_code,
                metadata=final_metadata,
            )
            await self.request_tracker.mark_completed(
                request_id=envelope.request_id,
                response_gcs_uri=part_uri,
                response_status_code=result.status_code,
                response_content_length=result.content_length,
                elapsed_seconds=result.elapsed_seconds,
                backend_service_id=served_backend.id,
                content_tokens=result.content_tokens,
                sequence_number=seq,
                metadata=final_metadata,
            )
            logger.info(f"Sub-request {envelope.request_id} (seq {seq}) completed via {served_backend.id}")
        else:
            part_uri = await self.batch_reassembler.save_sub_request_part(
                parent_request_id=parent_id,
                sequence_number=seq,
                custom_id=envelope.custom_id,
                result_data={},
                is_error=True,
                status_code=result.status_code,
                error_message=result.error_message,
                metadata=final_metadata,
            )
            await self.request_tracker.mark_failed(
                request_id=envelope.request_id,
                error_message=result.error_message or f"Backend failed with code {result.status_code}",
                response_status_code=result.status_code,
                elapsed_seconds=result.elapsed_seconds,
                backend_service_id=served_backend.id,
                sequence_number=seq,
                metadata=final_metadata,
            )
            logger.warning(f"Sub-request {envelope.request_id} (seq {seq}) failed: {result.error_message}")

        # 5. Check if all sub-requests are completed and reassemble if ready
        await self.batch_reassembler.try_reassemble_batch(parent_id)

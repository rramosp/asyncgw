"""Primary Request Worker processing online requests and dispatching batch jobs."""

import asyncio
from datetime import datetime, timezone
import json
import logging
import time
from typing import Optional

from asyncgw.backends.base import BackendExecutionResult, BaseLLMBackend
from asyncgw.batch.splitter import BatchSplitter
from asyncgw.config import BackendConfig
from asyncgw.models.request import AsyncRequestEnvelope, RequestType
from asyncgw.models.response import RequestStatusEnum
from asyncgw.router.engine import RoutingEngine
from asyncgw.storage.base import BaseBlobStorage, BaseRequestTracker

logger = logging.getLogger(__name__)


class PrimaryRequestWorker:
    """Consumes primary requests from Pub/Sub, enforces deadlines, routes to backends or breaks down batches."""

    def __init__(
        self,
        request_tracker: BaseRequestTracker,
        blob_storage: BaseBlobStorage,
        routing_engine: RoutingEngine,
        batch_splitter: BatchSplitter,
    ):
        self.request_tracker = request_tracker
        self.blob_storage = blob_storage
        self.routing_engine = routing_engine
        self.batch_splitter = batch_splitter

    async def process_envelope(self, envelope: AsyncRequestEnvelope) -> None:
        """Process a single incoming request envelope."""
        logger.info(f"Processing request {envelope.request_id} (type: {envelope.request_type}, model: {envelope.model})")

        # 1. Check user deadline / max wait time
        if envelope.is_expired():
            logger.warning(
                f"Request {envelope.request_id} exceeded maximum wait time ({envelope.max_wait_seconds}s). Expiring."
            )
            error_data = {
                "error": {
                    "message": f"Request exceeded maximum wait deadline of {envelope.max_wait_seconds}s",
                    "type": "timeout_error",
                    "code": 408,
                    "request_id": envelope.request_id,
                }
            }
            gcs_path = f"responses/{envelope.request_id}.json"
            gcs_uri = await self.blob_storage.save_json(gcs_path, error_data)
            await self.request_tracker.mark_timed_out(
                request_id=envelope.request_id,
                error_message=f"Request exceeded maximum wait time ({envelope.max_wait_seconds}s)",
            )
            return

        # 2. Check if request is a Batch request
        if envelope.request_type == RequestType.BATCH:
            await self._handle_batch_request(envelope)
            return

        # 3. Handle Single Online Request (Chat, Completion, Embedding)
        await self._handle_single_request(envelope)

    async def _handle_single_request(self, envelope: AsyncRequestEnvelope) -> None:
        # Route and execute with failover
        decision = self.routing_engine.route_request(envelope)
        await self.request_tracker.mark_processing(
            request_id=envelope.request_id,
            backend_service_id=decision.primary_backend.id,
            backend_endpoint=decision.primary_backend.endpoint_url,
        )

        async def _call_backend(client: BaseLLMBackend, cfg: BackendConfig) -> BackendExecutionResult:
            return await client.execute_online(envelope)

        result, served_backend = await self.routing_engine.execute_with_failover(
            envelope, _call_backend
        )

        gcs_path = f"responses/{envelope.request_id}.json"

        if result.success:
            if isinstance(result.response_data, dict):
                result.response_data["backend_service_id"] = served_backend.id
                result.response_data.pop("backend_batch_service_mode", None)

            # Store successful response in GCS
            gcs_uri = await self.blob_storage.save_json(gcs_path, result.response_data)
            await self.request_tracker.mark_completed(
                request_id=envelope.request_id,
                response_gcs_uri=gcs_uri,
                response_status_code=result.status_code,
                response_content_length=result.content_length,
                elapsed_seconds=result.elapsed_seconds,
                backend_service_id=served_backend.id,
                content_tokens=result.content_tokens,
                backend_batch_service_mode=None,
            )
            logger.info(
                f"Successfully completed request {envelope.request_id} via {served_backend.id} in {result.elapsed_seconds:.2f}s"
            )
        else:
            # Check if failure was due to timeout
            if result.status_code == 408 or "TIMEOUT" in str(result.error_message):
                error_payload = {
                    "error": {
                        "message": result.error_message or "Request timed out",
                        "type": "timeout_error",
                        "code": 408,
                    },
                    "backend_service_id": served_backend.id,
                }
                gcs_uri = await self.blob_storage.save_json(gcs_path, error_payload)
                await self.request_tracker.mark_timed_out(
                    request_id=envelope.request_id,
                    error_message=result.error_message or "Request timed out",
                )
            else:
                error_payload = {
                    "error": {
                        "message": result.error_message or "Internal backend failure",
                        "type": "backend_error",
                        "code": result.status_code,
                    },
                    "backend_service_id": served_backend.id,
                }
                gcs_uri = await self.blob_storage.save_json(gcs_path, error_payload)
                await self.request_tracker.mark_failed(
                    request_id=envelope.request_id,
                    error_message=result.error_message or f"Backend returned status {result.status_code}",
                    response_status_code=result.status_code,
                    elapsed_seconds=result.elapsed_seconds,
                    backend_service_id=served_backend.id,
                    backend_batch_service_mode=None,
                )
            logger.warning(
                f"Request {envelope.request_id} failed with code {result.status_code}: {result.error_message}"
            )

    async def _handle_batch_request(self, envelope: AsyncRequestEnvelope) -> None:
        decision = self.routing_engine.route_request(envelope)
        logger.info(
            f"Batch request {envelope.request_id} routing strategy: {decision.strategy_id} "
            f"(requires breakdown: {decision.requires_batch_breakdown})"
        )

        await self.request_tracker.mark_processing(
            request_id=envelope.request_id,
            backend_service_id=decision.primary_backend.id,
            backend_endpoint=decision.primary_backend.endpoint_url,
        )

        if decision.requires_batch_breakdown:
            # Backend does not support native batch; break down into individual sub-requests
            logger.info(f"Breaking down batch request {envelope.request_id} into individual queries...")
            sub_envelopes = await self.batch_splitter.split_and_enqueue(envelope)
            if not sub_envelopes:
                await self.request_tracker.mark_failed(
                    request_id=envelope.request_id,
                    error_message="Empty batch or invalid batch payload",
                    backend_service_id=decision.primary_backend.id,
                    backend_batch_service_mode="decomposed",
                )
        else:
            # Backend supports native bulk batch execution
            items = await self.batch_splitter.extract_batch_items(envelope)
            
            async def _call_batch_backend(client: BaseLLMBackend, cfg: BackendConfig) -> BackendExecutionResult:
                return await client.execute_batch(envelope, items)

            result, served_backend = await self.routing_engine.execute_with_failover(
                envelope, _call_batch_backend
            )

            gcs_path = f"responses/{envelope.request_id}.json"
            if result.success:
                if isinstance(result.response_data, dict):
                    result.response_data["backend_service_id"] = served_backend.id
                    result.response_data["backend_batch_service_mode"] = "native"

                gcs_uri = await self.blob_storage.save_json(gcs_path, result.response_data)
                await self.request_tracker.mark_completed(
                    request_id=envelope.request_id,
                    response_gcs_uri=gcs_uri,
                    response_status_code=result.status_code,
                    response_content_length=result.content_length,
                    elapsed_seconds=result.elapsed_seconds,
                    backend_service_id=served_backend.id,
                    backend_batch_service_mode="native",
                )
            else:
                error_payload = {
                    "error": {
                        "message": result.error_message,
                        "type": "batch_execution_error",
                        "code": result.status_code,
                    },
                    "backend_service_id": served_backend.id,
                    "backend_batch_service_mode": "native",
                }
                gcs_uri = await self.blob_storage.save_json(gcs_path, error_payload)
                await self.request_tracker.mark_failed(
                    request_id=envelope.request_id,
                    error_message=result.error_message or "Batch execution failed",
                    response_status_code=result.status_code,
                    elapsed_seconds=result.elapsed_seconds,
                    backend_service_id=served_backend.id,
                    backend_batch_service_mode="native",
                )

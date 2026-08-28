"""Primary Request Worker processing online requests and dispatching batch jobs."""

import asyncio
from datetime import datetime, timezone
import json
import logging
import time
from typing import Optional, Callable, Awaitable, Any

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
        config_reloader: Optional[Callable[[], Awaitable[Any]]] = None,
    ):
        self.request_tracker = request_tracker
        self.blob_storage = blob_storage
        self.routing_engine = routing_engine
        self.batch_splitter = batch_splitter
        self.config_reloader = config_reloader

    async def process_envelope(self, envelope: AsyncRequestEnvelope) -> None:
        """Process a single incoming request envelope."""
        if self.config_reloader is not None:
            try:
                await self.config_reloader()
            except Exception as e:
                logger.debug(f"Error checking config reloader: {e}")
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
        }
        init_metadata.pop("backends_tried", None)
        init_metadata.pop("failover_trace", None)

        await self.request_tracker.mark_processing(
            request_id=envelope.request_id,
            backend_service_id=decision.primary_backend.id,
            backend_endpoint=decision.primary_backend.endpoint_url,
            metadata=init_metadata,
        )

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
                metadata=final_metadata,
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
                    metadata=final_metadata,
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
                    metadata=final_metadata,
                )
            logger.warning(
                f"Request {envelope.request_id} failed with code {result.status_code}: {result.error_message}"
            )

    async def _handle_batch_request(self, envelope: AsyncRequestEnvelope) -> None:
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
        }
        init_metadata.pop("backends_tried", None)
        init_metadata.pop("failover_trace", None)

        logger.info(
            f"Batch request {envelope.request_id} routing strategy: {decision.strategy_id} "
            f"(requires breakdown: {decision.requires_batch_breakdown})"
        )

        await self.request_tracker.mark_processing(
            request_id=envelope.request_id,
            backend_service_id=decision.primary_backend.id,
            backend_endpoint=decision.primary_backend.endpoint_url,
            metadata=init_metadata,
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
                    metadata=init_metadata,
                )
        else:
            # Backend supports native bulk batch execution
            items = await self.batch_splitter.extract_batch_items(envelope)
            if not items:
                await self.request_tracker.mark_failed(
                    request_id=envelope.request_id,
                    error_message="Empty batch or invalid batch payload",
                    backend_service_id=decision.primary_backend.id,
                    backend_batch_service_mode="native",
                    metadata=init_metadata,
                )
                return

            start_time = time.time()
            results = []
            completed_count = 0
            failed_count = 0
            total_tokens = 0
            last_served_backend = decision.primary_backend
            all_backends_tried = []

            for seq, item in enumerate(items):
                item_env = AsyncRequestEnvelope(
                    request_id=f"{envelope.request_id}_{seq}",
                    parent_request_id=envelope.request_id,
                    sequence_number=seq,
                    total_items=len(items),
                    custom_id=item.custom_id,
                    request_type=RequestType.BATCH_SUB_REQUEST,
                    model=item.body.get("model", envelope.model),
                    payload=item.body,
                    created_at=envelope.created_at,
                    expires_at=envelope.expires_at,
                    max_wait_seconds=envelope.max_wait_seconds,
                    client_id=envelope.client_id,
                    priority=envelope.priority,
                    tags=envelope.tags.copy() if envelope.tags else {},
                    routing_strategy=envelope.routing_strategy,
                    target_backend=envelope.target_backend,
                )

                async def _call_backend(client: BaseLLMBackend, cfg: BackendConfig) -> BackendExecutionResult:
                    return await client.execute_online(item_env)

                item_res, served_item_backend = await self.routing_engine.execute_with_failover(
                    item_env, _call_backend
                )
                last_served_backend = served_item_backend
                routing_meta = getattr(item_res, "routing_metadata", {}) or {}
                item_metadata = {
                    **(item_env.tags or {}),
                    **routing_meta,
                }

                if item_res.success:
                    completed_count += 1
                    if item_res.content_tokens:
                        total_tokens += item_res.content_tokens
                    results.append({
                        "id": f"batch_req_{seq}",
                        "custom_id": item.custom_id,
                        "response": {"status_code": item_res.status_code, "body": item_res.response_data},
                        "error": None,
                        "metadata": item_metadata,
                    })
                else:
                    failed_count += 1
                    results.append({
                        "id": f"batch_req_{seq}",
                        "custom_id": item.custom_id,
                        "response": None,
                        "error": {"code": item_res.status_code, "message": item_res.error_message},
                        "metadata": item_metadata,
                    })

            elapsed = time.time() - start_time
            now_ts = int(time.time())
            final_status = RequestStatusEnum.COMPLETED if failed_count == 0 else RequestStatusEnum.FAILED

            batch_out = {
                "id": envelope.request_id,
                "object": "batch",
                "endpoint": "/v1/chat/completions",
                "status": final_status.value,
                "backend_service_id": last_served_backend.id,
                "backend_batch_service_mode": "native",
                "created_at": int(envelope.created_at.timestamp()) if envelope.created_at else now_ts,
                "completed_at": now_ts,
                "request_counts": {
                    "total": len(items),
                    "completed": completed_count,
                    "failed": failed_count,
                },
                "output_file_id": f"responses/{envelope.request_id}.json",
                "results": results,
            }

            gcs_path = f"responses/{envelope.request_id}.json"
            gcs_uri = await self.blob_storage.save_json(gcs_path, batch_out)
            content_len = len(json.dumps(batch_out))

            final_metadata = {
                **(envelope.tags or {}),
                "routing_policy": init_policy_info,
                "strategy_id": decision.strategy_id,
                "selection_reason": decision.reason,
                "request_counts": batch_out["request_counts"],
                "total_items": len(items),
                "completed_items": completed_count,
                "failed_items": failed_count,
            }
            final_metadata.pop("backends_tried", None)
            final_metadata.pop("failover_trace", None)

            if final_status == RequestStatusEnum.COMPLETED:
                await self.request_tracker.mark_completed(
                    request_id=envelope.request_id,
                    response_gcs_uri=gcs_uri,
                    response_status_code=200,
                    response_content_length=content_len,
                    elapsed_seconds=elapsed,
                    backend_service_id=last_served_backend.id,
                    backend_batch_service_mode="native",
                    content_tokens=total_tokens,
                    metadata=final_metadata,
                )
                logger.info(f"Native batch {envelope.request_id} completed successfully via {last_served_backend.id}")
            else:
                await self.request_tracker.mark_failed(
                    request_id=envelope.request_id,
                    error_message=f"Batch failed: {failed_count}/{len(items)} items failed",
                    response_status_code=500,
                    elapsed_seconds=elapsed,
                    backend_service_id=last_served_backend.id,
                    backend_batch_service_mode="native",
                    metadata=final_metadata,
                    response_gcs_uri=gcs_uri,
                )
                logger.warning(f"Native batch {envelope.request_id} failed: {failed_count}/{len(items)} items failed")

"""FastAPI Application providing OpenAI-compatible Asynchronous HTTP endpoints and Admin APIs."""

from datetime import datetime, timedelta, timezone
import json
import logging
from typing import Any, Dict, List, Optional
import uuid

from fastapi import Body, Depends, FastAPI, Header, HTTPException, Query, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from asyncgw.backends.base import BaseLLMBackend
from asyncgw.backends.gcp_provisioned import GCPProvisionedBackend
from asyncgw.backends.gemini_flex import GeminiFlexBackend
from asyncgw.backends.health import BackendHealthStatus, HealthMonitor
from asyncgw.backends.mock_backend import MockBackend
from asyncgw.backends.openai_client import OpenAIBackend
from asyncgw.batch.reassembler import BatchReassembler
from asyncgw.batch.splitter import BatchSplitter
from asyncgw.config import (
    AsyncGWConfig,
    BackendConfig,
    GatewaySettings,
    PoliciesConfig,
    load_asyncgw_config,
    load_backends_config,
    load_policies_config,
    save_backends_config,
)
from asyncgw.models.request import (
    AsyncRequestEnvelope,
    BatchRequest,
    ChatCompletionRequest,
    CompletionRequest,
    EmbeddingRequest,
    RequestType,
)
from asyncgw.models.response import (
    AsyncSubmitResponse,
    BatchAggregatedResponse,
    RequestStatusEnum,
    RequestStatusResponse,
)
from asyncgw.queue.base import BaseQueueConsumer, BaseQueueProducer
from asyncgw.queue.memory_queue import InMemoryQueueConsumer, InMemoryQueueProducer
from asyncgw.queue.pubsub import PubSubQueueConsumer, PubSubQueueProducer
from asyncgw.router.engine import RoutingEngine
from asyncgw.storage.base import BaseBlobStorage, BaseRequestTracker
from asyncgw.storage.bigquery import BigQueryRequestTracker
from asyncgw.storage.gcs import GCSBlobStorage
from asyncgw.storage.memory_mock import InMemoryBlobStorage, InMemoryRequestTracker

logger = logging.getLogger(__name__)



def create_backend_client(b_cfg: BackendConfig, environment_mode: str = "mock") -> BaseLLMBackend:
    """Factory to instantiate backend client corresponding to backend configuration."""
    if b_cfg.endpoint_url.startswith("mock://") or environment_mode == "mock":
        return MockBackend(b_cfg)
    elif "openai.com" in b_cfg.endpoint_url:
        return OpenAIBackend(b_cfg)
    elif "provisioned" in b_cfg.id.lower():
        return GCPProvisionedBackend(b_cfg)
    else:
        return GeminiFlexBackend(b_cfg)


def create_app(
    settings: Optional[GatewaySettings] = None,
    backends: Optional[List[BackendConfig]] = None,
    policies: Optional[PoliciesConfig] = None,
    asyncgw_config: Optional[AsyncGWConfig] = None,
    request_tracker: Optional[BaseRequestTracker] = None,
    blob_storage: Optional[BaseBlobStorage] = None,
    queue_producer: Optional[BaseQueueProducer] = None,
    queue_consumer: Optional[BaseQueueConsumer] = None,
) -> FastAPI:
    """Factory creating and wiring the Asynchronous Gateway FastAPI app."""

    settings = settings or GatewaySettings()
    backends = backends if backends is not None else load_backends_config(settings.backends_config_path)
    policies = policies if policies is not None else load_policies_config(settings.policies_config_path)
    asyncgw_config = asyncgw_config if asyncgw_config is not None else load_asyncgw_config(settings.asyncgw_config_path)

    # Initialize storage & queues based on environment mode
    if request_tracker is None:
        if settings.environment_mode == "gcp":
            request_tracker = BigQueryRequestTracker(settings)
        else:
            request_tracker = InMemoryRequestTracker()

    if blob_storage is None:
        if settings.environment_mode == "gcp":
            blob_storage = GCSBlobStorage(settings)
        else:
            blob_storage = InMemoryBlobStorage(bucket_name=settings.gcs_bucket_name)

    if queue_producer is None:
        if settings.environment_mode == "gcp":
            queue_producer = PubSubQueueProducer(settings)
        else:
            import asyncio
            q1 = asyncio.Queue()
            q2 = asyncio.Queue()
            q3 = asyncio.Queue()
            queue_producer = InMemoryQueueProducer(q1, q2, q3)
            if queue_consumer is None:
                queue_consumer = InMemoryQueueConsumer(q1, q2, q3)

    # Build backend clients map
    backend_clients: Dict[str, BaseLLMBackend] = {}
    for b_cfg in backends:
        backend_clients[b_cfg.id] = create_backend_client(b_cfg, settings.environment_mode)

    health_monitor = HealthMonitor(backends)
    routing_engine = RoutingEngine(backends, policies, health_monitor, backend_clients)
    batch_splitter = BatchSplitter(request_tracker, blob_storage, queue_producer)
    batch_reassembler = BatchReassembler(request_tracker, blob_storage)

    app = FastAPI(
        title="Google Cloud Asynchronous LLM Gateway",
        description="OpenAPI compatible asynchronous gateway for bulk and queued LLM inference with policy routing and GCP storage",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Store state on app
    app.state.settings = settings
    app.state.backends = backends
    app.state.policies = policies
    app.state.asyncgw_config = asyncgw_config
    app.state.request_tracker = request_tracker
    app.state.blob_storage = blob_storage
    app.state.queue_producer = queue_producer
    app.state.queue_consumer = queue_consumer
    app.state.backend_clients = backend_clients
    app.state.health_monitor = health_monitor
    app.state.routing_engine = routing_engine
    app.state.batch_splitter = batch_splitter
    app.state.batch_reassembler = batch_reassembler

    @app.on_event("startup")
    async def startup_event():
        try:
            await app.state.request_tracker.initialize()
            await app.state.blob_storage.initialize()
            await app.state.queue_producer.initialize()
            logger.info("Async Gateway components initialized successfully.")
        except Exception as e:
            logger.warning(f"Storage/queue initialization encountered warning: {e}")

    # -------------------------------------------------------------
    # Helper to calculate request expiration deadline
    # -------------------------------------------------------------
    def _compute_deadline(
        now: datetime, max_wait_sec: Optional[int]
    ) -> tuple[Optional[datetime], int]:
        cfg_timeouts = policies.global_timeouts
        if max_wait_sec is None:
            max_wait_sec = cfg_timeouts.default_max_wait_seconds
        else:
            max_wait_sec = max(
                cfg_timeouts.min_wait_seconds,
                min(max_wait_sec, cfg_timeouts.absolute_max_wait_seconds),
            )
        expires_at = now + timedelta(seconds=max_wait_sec)
        return expires_at, max_wait_sec

    # -------------------------------------------------------------
    # OpenAI Chat Completions Endpoint (Asynchronous)
    # -------------------------------------------------------------
    @app.post(
        "/v1/chat/completions",
        status_code=status.HTTP_202_ACCEPTED,
        response_model=AsyncSubmitResponse,
        summary="Submit Asynchronous Chat Completion Request",
        tags=["Inference"],
    )
    async def chat_completions(
        req: ChatCompletionRequest,
        x_max_wait_seconds: Optional[int] = Header(None, alias="X-Max-Wait-Seconds"),
        x_routing_override: Optional[str] = Header(None, alias="X-Routing-Override"),
    ):
        now = datetime.now(timezone.utc)
        req_id = f"req_{uuid.uuid4().hex}"
        max_wait = req.max_wait_seconds or x_max_wait_seconds
        expires_at, effective_max_wait = _compute_deadline(now, max_wait)

        envelope = AsyncRequestEnvelope(
            request_id=req_id,
            request_type=RequestType.CHAT_COMPLETION,
            model=req.model,
            payload=req.model_dump(),
            created_at=now,
            expires_at=expires_at,
            max_wait_seconds=effective_max_wait,
            priority=req.priority or "normal",
            tags=req.tags or {},
            target_backend=req.routing_override or x_routing_override,
        )

        decision = app.state.routing_engine.route_request(envelope)
        strat_obj = app.state.routing_engine.strategies_map.get(decision.strategy_id)
        strategy_name = strat_obj.name if strat_obj else decision.strategy_id
        envelope.tags = {
            **(req.tags or {}),
            "routing_policy": {
                "strategy_id": decision.strategy_id,
                "strategy_name": strategy_name,
                "selection_reason": decision.reason,
                "preference_order": [b.id for b in decision.all_candidate_backends],
            },
            "strategy_id": decision.strategy_id,
            "selection_reason": decision.reason,
            "backends_tried": [],
            "failover_trace": [],
        }

        # 1. Register in BigQuery with PENDING status
        await app.state.request_tracker.register_request(envelope)

        # 2. Publish to Pub/Sub primary queue
        await app.state.queue_producer.publish_request(envelope)

        return AsyncSubmitResponse(
            request_id=req_id,
            status=RequestStatusEnum.PENDING,
            created_at=now,
            status_url=f"/v1/requests/{req_id}",
            response_url=f"/v1/requests/{req_id}/response",
            max_wait_seconds=effective_max_wait,
            model=req.model,
            message="Chat completion request enqueued for asynchronous processing",
        )

    # -------------------------------------------------------------
    # Legacy Completions Endpoint (Asynchronous)
    # -------------------------------------------------------------
    @app.post(
        "/v1/completions",
        status_code=status.HTTP_202_ACCEPTED,
        response_model=AsyncSubmitResponse,
        summary="Submit Asynchronous Text Completion Request",
        tags=["Inference"],
    )
    async def text_completions(
        req: CompletionRequest,
        x_max_wait_seconds: Optional[int] = Header(None, alias="X-Max-Wait-Seconds"),
    ):
        now = datetime.now(timezone.utc)
        req_id = f"req_{uuid.uuid4().hex}"
        max_wait = req.max_wait_seconds or x_max_wait_seconds
        expires_at, effective_max_wait = _compute_deadline(now, max_wait)

        envelope = AsyncRequestEnvelope(
            request_id=req_id,
            request_type=RequestType.COMPLETION,
            model=req.model,
            payload=req.model_dump(),
            created_at=now,
            expires_at=expires_at,
            max_wait_seconds=effective_max_wait,
            priority=req.priority or "normal",
            tags=req.tags or {},
        )

        decision = app.state.routing_engine.route_request(envelope)
        strat_obj = app.state.routing_engine.strategies_map.get(decision.strategy_id)
        strategy_name = strat_obj.name if strat_obj else decision.strategy_id
        envelope.tags = {
            **(req.tags or {}),
            "routing_policy": {
                "strategy_id": decision.strategy_id,
                "strategy_name": strategy_name,
                "selection_reason": decision.reason,
                "preference_order": [b.id for b in decision.all_candidate_backends],
            },
            "strategy_id": decision.strategy_id,
            "selection_reason": decision.reason,
            "backends_tried": [],
            "failover_trace": [],
        }

        await app.state.request_tracker.register_request(envelope)
        await app.state.queue_producer.publish_request(envelope)

        return AsyncSubmitResponse(
            request_id=req_id,
            status=RequestStatusEnum.PENDING,
            created_at=now,
            status_url=f"/v1/requests/{req_id}",
            response_url=f"/v1/requests/{req_id}/response",
            max_wait_seconds=effective_max_wait,
            model=req.model,
            message="Text completion request enqueued for asynchronous processing",
        )

    # -------------------------------------------------------------
    # Embeddings Endpoint (Asynchronous)
    # -------------------------------------------------------------
    @app.post(
        "/v1/embeddings",
        status_code=status.HTTP_202_ACCEPTED,
        response_model=AsyncSubmitResponse,
        summary="Submit Asynchronous Embeddings Request",
        tags=["Inference"],
    )
    async def embeddings(
        req: EmbeddingRequest,
        x_max_wait_seconds: Optional[int] = Header(None, alias="X-Max-Wait-Seconds"),
    ):
        now = datetime.now(timezone.utc)
        req_id = f"req_{uuid.uuid4().hex}"
        max_wait = req.max_wait_seconds or x_max_wait_seconds
        expires_at, effective_max_wait = _compute_deadline(now, max_wait)

        envelope = AsyncRequestEnvelope(
            request_id=req_id,
            request_type=RequestType.EMBEDDING,
            model=req.model,
            payload=req.model_dump(),
            created_at=now,
            expires_at=expires_at,
            max_wait_seconds=effective_max_wait,
        )

        await app.state.request_tracker.register_request(envelope)
        await app.state.queue_producer.publish_request(envelope)

        return AsyncSubmitResponse(
            request_id=req_id,
            status=RequestStatusEnum.PENDING,
            created_at=now,
            status_url=f"/v1/requests/{req_id}",
            response_url=f"/v1/requests/{req_id}/response",
            max_wait_seconds=effective_max_wait,
            model=req.model,
            message="Embedding request enqueued for asynchronous processing",
        )

    # -------------------------------------------------------------
    # OpenAI Batch API Endpoint (Asynchronous)
    # -------------------------------------------------------------
    @app.post(
        "/v1/batches",
        status_code=status.HTTP_202_ACCEPTED,
        response_model=AsyncSubmitResponse,
        summary="Submit Batch Request (OpenAI Batch API Compatible)",
        tags=["Batch"],
    )
    async def submit_batch(
        req: BatchRequest,
        x_max_wait_seconds: Optional[int] = Header(None, alias="X-Max-Wait-Seconds"),
        x_routing_override: Optional[str] = Header(None, alias="X-Routing-Override"),
    ):
        now = datetime.now(timezone.utc)
        batch_id = f"batch_{uuid.uuid4().hex}"
        max_wait = req.max_wait_seconds or x_max_wait_seconds
        expires_at, effective_max_wait = _compute_deadline(now, max_wait)

        total_items = len(req.requests) if req.requests else 1
        model_name = "batch-model"
        if req.requests and len(req.requests) > 0 and "model" in req.requests[0].body:
            model_name = req.requests[0].body["model"]

        target_backend = (
            req.metadata.get("target_backend")
            or req.metadata.get("routing_override")
            or x_routing_override
            if req.metadata
            else x_routing_override
        )
        routing_strategy = req.metadata.get("routing_strategy") if req.metadata else None

        envelope = AsyncRequestEnvelope(
            request_id=batch_id,
            request_type=RequestType.BATCH,
            model=model_name,
            total_items=total_items,
            payload=req.model_dump(),
            created_at=now,
            expires_at=expires_at,
            max_wait_seconds=effective_max_wait,
            raw_input_gcs_uri=req.input_file_id,
            tags=req.metadata or {},
            target_backend=target_backend,
            routing_strategy=routing_strategy,
        )

        decision = app.state.routing_engine.route_request(envelope)
        strat_obj = app.state.routing_engine.strategies_map.get(decision.strategy_id)
        strategy_name = strat_obj.name if strat_obj else decision.strategy_id
        envelope.tags = {
            **(req.metadata or {}),
            "routing_policy": {
                "strategy_id": decision.strategy_id,
                "strategy_name": strategy_name,
                "selection_reason": decision.reason,
                "preference_order": [b.id for b in decision.all_candidate_backends],
            },
            "strategy_id": decision.strategy_id,
            "selection_reason": decision.reason,
        }

        await app.state.request_tracker.register_request(envelope)
        await app.state.queue_producer.publish_request(envelope)

        return AsyncSubmitResponse(
            request_id=batch_id,
            batch_id=batch_id,
            status=RequestStatusEnum.PENDING,
            created_at=now,
            status_url=f"/v1/batches/{batch_id}",
            response_url=f"/v1/batches/{batch_id}/output",
            max_wait_seconds=effective_max_wait,
            model=model_name,
            total_items=total_items,
            message="Batch request accepted and enqueued for asynchronous processing",
        )

    # -------------------------------------------------------------
    # Request Polling Status Endpoint
    # -------------------------------------------------------------
    @app.get(
        "/v1/requests/{request_id}",
        response_model=RequestStatusResponse,
        summary="Poll Status and Metadata of an Asynchronous Request",
        tags=["Status & Results"],
    )
    async def get_request_status(request_id: str):
        status_res = await app.state.request_tracker.get_request_status(request_id)
        if not status_res:
            raise HTTPException(status_code=404, detail=f"Request '{request_id}' not found.")

        # Ensure backend_batch_service_mode only appears for batch requests
        is_batch = status_res.request_type == RequestType.BATCH.value or request_id.startswith("batch_") or (status_res.total_items and status_res.total_items > 1)
        if is_batch:
            sub_reqs = await app.state.request_tracker.get_batch_sub_requests(request_id)
            if sub_reqs:
                status_res.total_items = len(sub_reqs)
                status_res.completed_items = sum(1 for s in sub_reqs if s.status == RequestStatusEnum.COMPLETED)
                status_res.failed_items = sum(1 for s in sub_reqs if s.status in [RequestStatusEnum.FAILED, RequestStatusEnum.TIMED_OUT])
                status_res.backend_batch_service_mode = "decomposed"
            elif not status_res.backend_batch_service_mode:
                status_res.backend_batch_service_mode = "native"
            if status_res.metadata is not None:
                status_res.metadata.pop("backends_tried", None)
                status_res.metadata.pop("failover_trace", None)
        else:
            status_res.backend_batch_service_mode = None

        return status_res

    # -------------------------------------------------------------
    # Request Response Retrieval Endpoint
    # -------------------------------------------------------------
    @app.get(
        "/v1/requests/{request_id}/response",
        summary="Retrieve Completed Response Payload from Storage",
        tags=["Status & Results"],
    )
    async def get_request_response(request_id: str):
        status_res = await app.state.request_tracker.get_request_status(request_id)
        if not status_res:
            raise HTTPException(status_code=404, detail=f"Request '{request_id}' not found.")

        if status_res.status == RequestStatusEnum.PENDING or status_res.status == RequestStatusEnum.PROCESSING:
            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED,
                content={
                    "request_id": request_id,
                    "status": status_res.status.value,
                    "message": "Response is not ready yet. Please continue polling.",
                    "started_at": status_res.started_at.isoformat() if status_res.started_at else None,
                },
            )

        if status_res.status == RequestStatusEnum.TIMED_OUT:
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail=status_res.error_message or "Request timed out before completion.",
            )

        if status_res.status == RequestStatusEnum.FAILED:
            # Check if this was a batch request with response_gcs_uri
            is_batch = status_res.request_type == RequestType.BATCH.value or request_id.startswith("batch_") or (status_res.total_items and status_res.total_items > 1)
            if is_batch and status_res.response_gcs_uri:
                try:
                    data = await app.state.blob_storage.get_json(status_res.response_gcs_uri)
                    if isinstance(data, dict):
                        if status_res.backend_service_id and (not data.get("backend_service_id") or data.get("backend_service_id") == "gateway_batch_reassembler"):
                            data["backend_service_id"] = status_res.backend_service_id
                        if not data.get("backend_batch_service_mode"):
                            sub_reqs = await app.state.request_tracker.get_batch_sub_requests(request_id)
                            data["backend_batch_service_mode"] = "decomposed" if sub_reqs else "native"
                        _format_batch_results_response(data, status_res)
                    return JSONResponse(status_code=200, content=data)
                except Exception:
                    pass

            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={
                    "error": {
                        "message": status_res.error_message or "Execution failed on backend",
                        "type": "gateway_execution_error",
                        "status_code": status_res.response_status_code or 500,
                    },
                    "backend_service_id": status_res.backend_service_id,
                },
            )

        # COMPLETED
        if status_res.response_gcs_uri:
            try:
                data = await app.state.blob_storage.get_json(status_res.response_gcs_uri)
                if isinstance(data, dict):
                    if status_res.backend_service_id and (not data.get("backend_service_id") or data.get("backend_service_id") == "gateway_batch_reassembler"):
                        data["backend_service_id"] = status_res.backend_service_id

                    is_batch = status_res.request_type == RequestType.BATCH.value or request_id.startswith("batch_") or (status_res.total_items and status_res.total_items > 1)
                    if is_batch:
                        if not data.get("backend_batch_service_mode"):
                            sub_reqs = await app.state.request_tracker.get_batch_sub_requests(request_id)
                            data["backend_batch_service_mode"] = "decomposed" if sub_reqs else "native"
                        _format_batch_results_response(data, status_res)
                    else:
                        data.pop("backend_batch_service_mode", None)
                return JSONResponse(status_code=200, content=data)
            except Exception as e:
                logger.error(f"Error fetching response blob: {e}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to fetch response payload from storage: {str(e)}",
                )

        raise HTTPException(
            status_code=500,
            detail="Request marked completed but response URI is missing.",
        )

    # -------------------------------------------------------------
    # Helper to format batch results response according to config
    # -------------------------------------------------------------
    def _format_batch_results_response(
        data: Dict[str, Any],
        status_res: RequestStatusResponse,
    ) -> Dict[str, Any]:
        cfg = getattr(app.state, "asyncgw_config", None)
        max_limit = cfg.max_batch_items_in_api if cfg and hasattr(cfg, "max_batch_items_in_api") else 100

        results_list = data.get("results")
        if isinstance(results_list, list):
            total_items = len(results_list)
            returned_items_list = results_list[:max_limit]
            data["results"] = returned_items_list
            data["total_items"] = total_items
            data["returned_items"] = len(returned_items_list)
        else:
            total_items = data.get("total_items") or status_res.total_items or 0
            data["total_items"] = total_items
            data["returned_items"] = 0
            data["results"] = []


        req_id = status_res.request_id
        if settings.environment_mode == "gcp" and status_res.response_gcs_uri and not status_res.response_gcs_uri.startswith("gs://mock-"):
            data["results_uri"] = status_res.response_gcs_uri
        elif settings.environment_mode == "gcp":
            data["results_uri"] = status_res.response_gcs_uri or f"gs://{settings.gcs_bucket_name}/responses/{req_id}.json"
        else:
            data["results_uri"] = f"/v1/batches/{req_id}/download"

        return data

    # -------------------------------------------------------------
    # Batch Status & Output Endpoints (OpenAI Compatible)
    # -------------------------------------------------------------
    @app.get(
        "/v1/batches/{batch_id}",
        response_model=RequestStatusResponse,
        summary="Get Batch Status (OpenAI Batch API Compatible)",
        tags=["Batch"],
    )
    async def get_batch_status(batch_id: str):
        status_res = await app.state.request_tracker.get_request_status(batch_id)
        if not status_res:
            raise HTTPException(status_code=404, detail=f"Batch '{batch_id}' not found.")
        
        # Aggregate sub-request counts if available
        sub_reqs = await app.state.request_tracker.get_batch_sub_requests(batch_id)
        if sub_reqs:
            status_res.total_items = len(sub_reqs)
            status_res.completed_items = sum(1 for s in sub_reqs if s.status == RequestStatusEnum.COMPLETED)
            status_res.failed_items = sum(1 for s in sub_reqs if s.status in [RequestStatusEnum.FAILED, RequestStatusEnum.TIMED_OUT])
            status_res.backend_batch_service_mode = "decomposed"
            if status_res.metadata is not None:
                status_res.metadata["request_counts"] = {
                    "total": status_res.total_items,
                    "completed": status_res.completed_items,
                    "failed": status_res.failed_items,
                }
                status_res.metadata["total_items"] = status_res.total_items
                status_res.metadata["completed_items"] = status_res.completed_items
                status_res.metadata["failed_items"] = status_res.failed_items
                status_res.metadata.pop("backends_tried", None)
                status_res.metadata.pop("failover_trace", None)
        else:
            if not status_res.backend_batch_service_mode:
                status_res.backend_batch_service_mode = "native"
            if status_res.metadata is not None:
                status_res.metadata.pop("backends_tried", None)
                status_res.metadata.pop("failover_trace", None)

        return status_res

    @app.get(
        "/v1/batches/{batch_id}/output",
        summary="Get Batch Aggregated Output (OpenAI Batch API Compatible)",
        tags=["Batch"],
    )
    async def get_batch_output(batch_id: str):
        status_res = await app.state.request_tracker.get_request_status(batch_id)
        if not status_res:
            raise HTTPException(status_code=404, detail=f"Batch '{batch_id}' not found.")

        if status_res.status in [RequestStatusEnum.PENDING, RequestStatusEnum.PROCESSING]:
            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED,
                content={
                    "batch_id": batch_id,
                    "status": status_res.status.value,
                    "message": "Batch is still processing.",
                },
            )

        if status_res.response_gcs_uri:
            data = await app.state.blob_storage.get_json(status_res.response_gcs_uri)
            if isinstance(data, dict):
                if status_res.backend_service_id and (not data.get("backend_service_id") or data.get("backend_service_id") == "gateway_batch_reassembler"):
                    data["backend_service_id"] = status_res.backend_service_id
                if not data.get("backend_batch_service_mode"):
                    sub_reqs = await app.state.request_tracker.get_batch_sub_requests(batch_id)
                    data["backend_batch_service_mode"] = "decomposed" if sub_reqs else "native"
                _format_batch_results_response(data, status_res)
            return JSONResponse(status_code=200, content=data)

        raise HTTPException(
            status_code=404, detail=f"Batch output for '{batch_id}' not found."
        )

    @app.get(
        "/v1/batches/{batch_id}/download",
        summary="Download Complete Batch Output JSON",
        tags=["Batch"],
    )
    @app.get(
        "/v1/requests/{batch_id}/download",
        summary="Download Complete Batch Output JSON (Alias)",
        tags=["Batch"],
    )
    async def download_batch_output(batch_id: str):
        status_res = await app.state.request_tracker.get_request_status(batch_id)
        if not status_res:
            raise HTTPException(status_code=404, detail=f"Batch '{batch_id}' not found.")

        if status_res.status in [RequestStatusEnum.PENDING, RequestStatusEnum.PROCESSING]:
            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED,
                content={
                    "batch_id": batch_id,
                    "status": status_res.status.value,
                    "message": "Batch is still processing.",
                },
            )

        if status_res.response_gcs_uri:
            data = await app.state.blob_storage.get_json(status_res.response_gcs_uri)
            return Response(
                content=json.dumps(data, indent=2),
                media_type="application/json",
                headers={
                    "Content-Disposition": f'attachment; filename="{batch_id}_results.json"'
                },
            )

        raise HTTPException(
            status_code=404, detail=f"Batch output for '{batch_id}' not found."
        )

    @app.get(
        "/v1/requests/{request_id}/items",
        summary="List Decomposed Sub-Requests for a Batch",
        tags=["Batch", "Status & Results"],
    )
    @app.get(
        "/v1/batches/{request_id}/items",
        summary="List Decomposed Sub-Requests for a Batch (Alias)",
        tags=["Batch"],
    )
    async def get_batch_sub_requests_endpoint(request_id: str):
        sub_reqs = await app.state.request_tracker.get_batch_sub_requests(request_id)
        return {
            "parent_request_id": request_id,
            "total_items": len(sub_reqs),
            "items": [s.model_dump() for s in sub_reqs],
        }

    # -------------------------------------------------------------
    # Admin & Management APIs
    # -------------------------------------------------------------
    @app.get(
        "/v1/admin/backends",
        summary="List all configured backend services and live health status",
        tags=["Admin"],
    )
    async def list_backends():
        health_statuses = app.state.health_monitor.get_all_statuses()
        results = []
        for b in app.state.backends:
            h = health_statuses.get(b.id)
            results.append({
                "config": b.model_dump(),
                "health": h.model_dump() if h else {"is_healthy": True},
            })
        return {"backends": results}

    @app.get(
        "/v1/admin/backends/{backend_id}",
        summary="Get details and health status for a specific backend service",
        tags=["Admin"],
    )
    async def get_backend(backend_id: str):
        target = next((b for b in app.state.backends if b.id == backend_id), None)
        if not target:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Backend '{backend_id}' not found",
            )
        health = app.state.health_monitor.get_all_statuses().get(backend_id)
        return {
            "config": target.model_dump(),
            "health": health.model_dump() if health else {"is_healthy": True},
        }

    @app.post(
        "/v1/admin/backends",
        summary="Register a new LLM backend service",
        tags=["Admin"],
        status_code=status.HTTP_201_CREATED,
    )
    async def create_backend(backend_in: BackendConfig):
        if any(b.id == backend_in.id for b in app.state.backends):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Backend with ID '{backend_in.id}' already exists",
            )

        app.state.backends.append(backend_in)
        client = create_backend_client(backend_in, app.state.settings.environment_mode)
        app.state.backend_clients[backend_in.id] = client
        app.state.health_monitor.update_backends(app.state.backends)
        app.state.routing_engine.update_config(
            app.state.backends, app.state.policies, app.state.backend_clients
        )

        if hasattr(app.state, "local_fleet") and app.state.local_fleet and getattr(app.state.local_fleet, "routing_engine", None):
            app.state.local_fleet.routing_engine.update_config(
                app.state.backends, app.state.policies, app.state.backend_clients
            )

        try:
            save_backends_config(app.state.backends, app.state.settings.backends_config_path)
        except Exception as e:
            logger.warning(f"Failed to persist backends configuration: {e}")

        return {
            "message": f"Backend '{backend_in.id}' registered successfully",
            "backend": backend_in.model_dump(),
        }

    @app.put(
        "/v1/admin/backends/{backend_id}",
        summary="Update an existing LLM backend service",
        tags=["Admin"],
    )
    async def update_backend(backend_id: str, backend_in: BackendConfig):
        target_idx = next(
            (i for i, b in enumerate(app.state.backends) if b.id == backend_id), None
        )
        if target_idx is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Backend '{backend_id}' not found",
            )

        if backend_in.id != backend_id and any(
            b.id == backend_in.id for b in app.state.backends
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cannot rename: backend with ID '{backend_in.id}' already exists",
            )

        if backend_in.id != backend_id:
            app.state.backend_clients.pop(backend_id, None)

        app.state.backends[target_idx] = backend_in
        client = create_backend_client(backend_in, app.state.settings.environment_mode)
        app.state.backend_clients[backend_in.id] = client
        app.state.health_monitor.update_backends(app.state.backends)
        app.state.routing_engine.update_config(
            app.state.backends, app.state.policies, app.state.backend_clients
        )

        if hasattr(app.state, "local_fleet") and app.state.local_fleet and getattr(app.state.local_fleet, "routing_engine", None):
            app.state.local_fleet.routing_engine.update_config(
                app.state.backends, app.state.policies, app.state.backend_clients
            )

        try:
            save_backends_config(app.state.backends, app.state.settings.backends_config_path)
        except Exception as e:
            logger.warning(f"Failed to persist backends configuration: {e}")

        return {
            "message": f"Backend '{backend_in.id}' updated successfully",
            "backend": backend_in.model_dump(),
        }

    @app.delete(
        "/v1/admin/backends/{backend_id}",
        summary="Delete an LLM backend service",
        tags=["Admin"],
    )
    async def delete_backend(backend_id: str):
        target_idx = next(
            (i for i, b in enumerate(app.state.backends) if b.id == backend_id), None
        )
        if target_idx is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Backend '{backend_id}' not found",
            )

        deleted = app.state.backends.pop(target_idx)
        app.state.backend_clients.pop(backend_id, None)
        app.state.health_monitor.update_backends(app.state.backends)
        app.state.routing_engine.update_config(
            app.state.backends, app.state.policies, app.state.backend_clients
        )

        if hasattr(app.state, "local_fleet") and app.state.local_fleet and getattr(app.state.local_fleet, "routing_engine", None):
            app.state.local_fleet.routing_engine.update_config(
                app.state.backends, app.state.policies, app.state.backend_clients
            )

        try:
            save_backends_config(app.state.backends, app.state.settings.backends_config_path)
        except Exception as e:
            logger.warning(f"Failed to persist backends configuration: {e}")

        return {
            "message": f"Backend '{backend_id}' deleted successfully",
            "deleted_id": backend_id,
        }

    @app.post(
        "/v1/admin/backends/{backend_id}/probe",
        summary="Trigger immediate health check probe on a backend",
        tags=["Admin"],
    )
    async def probe_backend(backend_id: str):
        try:
            h_status = await app.state.health_monitor.probe_backend(backend_id)
            return {"backend_id": backend_id, "status": h_status.model_dump()}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.get(
        "/v1/admin/policies",
        summary="Get active routing policies and rules",
        tags=["Admin"],
    )
    async def get_policies():
        return app.state.policies.model_dump()

    @app.put(
        "/v1/admin/policies",
        summary="Update routing policies",
        tags=["Admin"],
    )
    async def update_policies(policies_update: PoliciesConfig):
        app.state.policies = policies_update
        app.state.routing_engine.update_config(app.state.backends, policies_update)
        return {"message": "Policies updated successfully", "policies": policies_update.model_dump()}

    @app.get(
        "/v1/admin/info",
        summary="Get gateway environment and deployment configuration",
        tags=["Admin"],
    )
    @app.get(
        "/v1/admin/infra",
        summary="Get complete GCP infrastructure, Artifact Registry containers, and Cloud Run worker trigger metadata",
        tags=["Admin"],
    )
    async def get_system_info():
        settings = app.state.settings
        project_id = settings.project_id
        region = settings.location
        dev_mode = settings.environment_mode == "mock"
        repo_name = "asyncgw-docker"
        repo_uri = f"{region}-docker.pkg.dev/{project_id}/{repo_name}"
        gateway_image = f"{repo_uri}/asyncgw-gateway:latest"
        worker_image = f"{repo_uri}/asyncgw-worker:latest"

        return {
            "environment_mode": settings.environment_mode,
            "dev_mode": dev_mode,
            "project_id": project_id,
            "location": region,
            "region": region,
            "gcs_bucket_name": settings.gcs_bucket_name,
            "gcs_retention_days": settings.gcs_retention_days,
            "bq_dataset": settings.bq_dataset,
            "bq_table": settings.bq_table,
            "pubsub_topic_requests": settings.pubsub_topic_requests,
            "pubsub_subscription_requests": settings.pubsub_subscription_requests,
            "pubsub_topic_batch_items": settings.pubsub_topic_batch_items,
            "pubsub_subscription_batch_items": settings.pubsub_subscription_batch_items,
            "pubsub_dlq_topic": settings.pubsub_dlq_topic,
            "artifact_registry": {
                "repository": repo_name,
                "location": region,
                "format": "DOCKER",
                "repository_url": repo_uri,
                "console_url": f"https://console.cloud.google.com/artifacts/docker/{project_id}/{region}/{repo_name}?project={project_id}",
                "images": [
                    {
                        "name": "asyncgw-gateway",
                        "tag": "latest",
                        "full_image_uri": gateway_image,
                        "dockerfile": "Dockerfile.gateway",
                        "base_image": "python:3.11-slim",
                        "role": "API Gateway, Swagger OpenAPI, Web UI Dashboard & Queue Dispatcher",
                        "entrypoint": "python -m asyncgw.main gateway",
                        "exposed_ports": "8080 (HTTP), 8000",
                        "target_compute": "asyncgw-gateway (Cloud Run Service)",
                        "description": "FastAPI gateway providing OpenAI-compatible asynchronous endpoints (/v1/chat/completions, /v1/batches), Swagger docs, interactive Web UI, and Pub/Sub queue publishing.",
                    },
                    {
                        "name": "asyncgw-worker",
                        "tag": "latest",
                        "full_image_uri": worker_image,
                        "dockerfile": "Dockerfile.worker",
                        "base_image": "python:3.11-slim",
                        "role": "Async Inference Engine, Batch Splitter/Reassembler & Queue Consumer",
                        "entrypoint": "python -m asyncgw.main worker-all | worker-primary | worker-batch",
                        "exposed_ports": "8080 (Health probe)",
                        "target_compute": "asyncgw-worker-fleet (Service), asyncgw-job-primary (Job), asyncgw-job-batch (Job)",
                        "description": "Asynchronous execution worker fleet. Subscribes to Pub/Sub queues, routes inference to Vertex AI Gemini Provisioned / Flex / OpenAI backends, decomposes batches into sub-requests, and reassembles ordered JSON results in GCS.",
                    },
                ],
            },
            "cloud_run": {
                "services": [
                    {
                        "name": "asyncgw-worker-fleet",
                        "service_type": "Cloud Run Service (Continuous)",
                        "role": "Continuous Background Worker Fleet",
                        "image": worker_image,
                        "ingress": "INGRESS_TRAFFIC_INTERNAL_ONLY (Internal)",
                        "trigger_type": "Continuous Pub/Sub Streaming Pull",
                        "trigger_badge": "Pub/Sub Pull Stream",
                        "trigger_badge_color": "emerald",
                        "trigger_details": "Subscribes directly to 'asyncgw-requests-sub' and 'asyncgw-batch-items-sub' via Streaming Pull with zero idle delay. CPU allocation is always on (cpu_idle = false) to prevent cold starts during message arrival.",
                        "trigger_flow": "Pub/Sub message arrival -> Worker streaming pull loop receives envelope immediately -> Validates TTL/deadline -> Dispatches to LLM backend -> Saves response to GCS -> Updates BigQuery",
                        "scaling": "min: 1 instance, max: 50 instances (auto-scales with queue backlog)",
                        "resources": "4 vCPU, 4 GiB RAM (cpu_idle = false)",
                        "command": "python -m asyncgw.main worker-all",
                        "service_account": f"asyncgw-worker-sa@{project_id}.iam.gserviceaccount.com",
                    },
                    {
                        "name": "asyncgw-gateway",
                        "service_type": "Cloud Run Service",
                        "role": "API Gateway & Web UI Dashboard",
                        "image": gateway_image,
                        "ingress": "INGRESS_TRAFFIC_ALL (Public)",
                        "trigger_type": "Synchronous HTTP / HTTPS REST Requests",
                        "trigger_badge": "HTTP / REST Trigger",
                        "trigger_badge_color": "cyan",
                        "trigger_details": "Triggered by incoming HTTP client requests (POST /v1/chat/completions, POST /v1/batches) and Web Dashboard access on port 8080.",
                        "trigger_flow": "Client POST request -> Writes PENDING state to BigQuery -> Enqueues envelope to Pub/Sub -> Returns HTTP 202 Accepted",
                        "scaling": "min: 1 instance, max: 20 instances",
                        "resources": "2 vCPU, 2 GiB RAM",
                        "command": "python -m asyncgw.main gateway",
                        "service_account": f"asyncgw-gateway-sa@{project_id}.iam.gserviceaccount.com",
                    },
                ],
                "jobs": [
                    {
                        "name": "asyncgw-job-primary",
                        "service_type": "Cloud Run Job",
                        "role": "Scheduled / Event-Driven Primary Queue Worker",
                        "image": worker_image,
                        "trigger_type": "Cloud Scheduler (Cron) / Eventarc / Manual Run",
                        "trigger_badge": "Cloud Scheduler / Eventarc",
                        "trigger_badge_color": "blue",
                        "trigger_details": "Executed via Cloud Scheduler (e.g. cron schedule), Eventarc queue depth threshold alert, or 'gcloud run jobs execute asyncgw-job-primary' to burst-drain primary request envelopes.",
                        "trigger_flow": "Trigger Signal -> Spawns 5 parallel container tasks -> Each task drains and processes pending requests from 'asyncgw-requests-topic' -> Terminates on queue empty",
                        "tasks": "5 parallel tasks (task_count = 5)",
                        "resources": "2 vCPU, 2 GiB RAM per task",
                        "command": "python -m asyncgw.main worker-primary",
                        "service_account": f"asyncgw-worker-sa@{project_id}.iam.gserviceaccount.com",
                    },
                    {
                        "name": "asyncgw-job-batch",
                        "service_type": "Cloud Run Job",
                        "role": "Parallel Decomposed Batch Worker",
                        "image": worker_image,
                        "trigger_type": "Batch Event Signal / Cloud Scheduler / Manual Run",
                        "trigger_badge": "Batch Event / Scheduler",
                        "trigger_badge_color": "indigo",
                        "trigger_details": "Triggered on large batch submissions via Eventarc notification, scheduled cron intervals, or 'gcloud run jobs execute asyncgw-job-batch' to process decomposed batch items across parallel tasks.",
                        "trigger_flow": "Batch Decomposed -> Sub-requests published to 'asyncgw-batch-items-topic' -> Spawns 10 parallel container tasks -> Items processed concurrently -> Reassembler triggers on completion",
                        "tasks": "10 parallel tasks (task_count = 10)",
                        "resources": "2 vCPU, 2 GiB RAM per task",
                        "command": "python -m asyncgw.main worker-batch",
                        "service_account": f"asyncgw-worker-sa@{project_id}.iam.gserviceaccount.com",
                    },
                ],
            },
        }

    @app.get(
        "/v1/admin/requests",
        summary="List recent requests with status filtering",
        tags=["Admin"],
    )
    async def list_recent_requests(
        limit: int = Query(50, ge=1, le=500),
        status: Optional[RequestStatusEnum] = Query(None),
    ):
        items = await app.state.request_tracker.list_recent_requests(limit=limit, status=status)
        return {"total": len(items), "requests": [i.model_dump() for i in items]}

    @app.get(
        "/v1/admin/stats",
        summary="Get system metrics and request counts",
        tags=["Admin"],
    )
    async def get_system_stats():
        all_reqs = await app.state.request_tracker.list_recent_requests(limit=500)
        status_counts = {}
        for s in RequestStatusEnum:
            status_counts[s.value] = sum(1 for r in all_reqs if r.status == s)

        return {
            "total_requests_tracked": len(all_reqs),
            "status_breakdown": status_counts,
            "backends_count": len(app.state.backends),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # -------------------------------------------------------------
    # Health Probe
    # -------------------------------------------------------------
    @app.get("/healthz", summary="Liveness and Readiness Probe", tags=["System"])
    async def healthz():
        return {
            "status": "healthy",
            "environment_mode": settings.environment_mode,
            "project_id": settings.project_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    return app

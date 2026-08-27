"""Main CLI entrypoint for Asynchronous LLM Gateway services and workers."""

import argparse
import asyncio
from datetime import datetime, timezone
import logging
import os
import sys

from fastapi import FastAPI
import uvicorn

from asyncgw.backends.base import BaseLLMBackend
from asyncgw.backends.gcp_provisioned import GCPProvisionedBackend
from asyncgw.backends.gemini_flex import GeminiFlexBackend
from asyncgw.backends.health import HealthMonitor
from asyncgw.backends.mock_backend import MockBackend
from asyncgw.backends.openai_client import OpenAIBackend
from asyncgw.batch.reassembler import BatchReassembler
from asyncgw.batch.splitter import BatchSplitter
from asyncgw.config import GatewaySettings, load_backends_config, load_policies_config
from asyncgw.gateway.api import create_app
from asyncgw.queue.memory_queue import InMemoryQueueConsumer, InMemoryQueueProducer
from asyncgw.queue.pubsub import PubSubQueueConsumer, PubSubQueueProducer
from asyncgw.router.engine import RoutingEngine
from asyncgw.storage.bigquery import BigQueryRequestTracker
from asyncgw.storage.gcs import GCSBlobStorage
from asyncgw.storage.memory_mock import InMemoryBlobStorage, InMemoryRequestTracker
from asyncgw.ui.app import create_ui_app
from asyncgw.workers.batch_worker import BatchSubRequestWorker
from asyncgw.workers.primary_worker import PrimaryRequestWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("asyncgw")


def run_gateway():
    """Run the FastAPI Gateway server with integrated Web UI and in-memory background workers."""
    settings = GatewaySettings()
    logger.info(f"Starting Async Gateway API & Web UI Dashboard on http://{settings.api_host}:{settings.api_port}")
    app = create_ui_app(create_app(settings))

    # In local mock mode, auto-start in-memory workers so queued requests are processed
    if settings.environment_mode == "mock" and isinstance(getattr(app.state, "queue_consumer", None), InMemoryQueueConsumer):
        @app.on_event("startup")
        async def _start_local_workers():
            primary_worker = PrimaryRequestWorker(
                request_tracker=app.state.request_tracker,
                blob_storage=app.state.blob_storage,
                routing_engine=app.state.routing_engine,
                batch_splitter=app.state.batch_splitter,
            )
            batch_worker = BatchSubRequestWorker(
                request_tracker=app.state.request_tracker,
                blob_storage=app.state.blob_storage,
                routing_engine=app.state.routing_engine,
                batch_reassembler=app.state.batch_reassembler,
            )
            await app.state.queue_consumer.consume_requests(primary_worker.process_envelope)
            await app.state.queue_consumer.consume_batch_items(batch_worker.process_sub_request)
            logger.info("Local background in-memory worker fleet started automatically.")

    uvicorn.run(app, host=settings.api_host, port=settings.api_port)


async def _run_primary_worker_async():
    settings = GatewaySettings()
    logger.info(f"Starting Primary Request Worker for topic {settings.pubsub_topic_requests}...")

    backends = load_backends_config(settings.backends_config_path)
    policies = load_policies_config(settings.policies_config_path)

    request_tracker = BigQueryRequestTracker(settings) if settings.environment_mode == "gcp" else InMemoryRequestTracker()
    blob_storage = GCSBlobStorage(settings) if settings.environment_mode == "gcp" else InMemoryBlobStorage(bucket_name=settings.gcs_bucket_name)
    queue_producer = PubSubQueueProducer(settings) if settings.environment_mode == "gcp" else InMemoryQueueProducer()
    queue_consumer = PubSubQueueConsumer(settings) if settings.environment_mode == "gcp" else InMemoryQueueConsumer()

    await request_tracker.initialize()
    await blob_storage.initialize()
    await queue_producer.initialize()
    await queue_consumer.initialize()

    backend_clients = {}
    for b in backends:
        if b.endpoint_url.startswith("mock://") or settings.environment_mode == "mock":
            backend_clients[b.id] = MockBackend(b)
        elif "openai.com" in b.endpoint_url:
            backend_clients[b.id] = OpenAIBackend(b)
        elif "provisioned" in b.id.lower():
            backend_clients[b.id] = GCPProvisionedBackend(b)
        else:
            backend_clients[b.id] = GeminiFlexBackend(b)

    health_monitor = HealthMonitor(backends)
    routing_engine = RoutingEngine(backends, policies, health_monitor, backend_clients)
    batch_splitter = BatchSplitter(request_tracker, blob_storage, queue_producer)

    worker = PrimaryRequestWorker(
        request_tracker=request_tracker,
        blob_storage=blob_storage,
        routing_engine=routing_engine,
        batch_splitter=batch_splitter,
    )

    await queue_consumer.consume_requests(worker.process_envelope)
    logger.info("Primary Request Worker is actively listening for requests.")


async def _run_batch_worker_async():
    settings = GatewaySettings()
    logger.info(f"Starting Batch Sub-Request Worker for topic {settings.pubsub_topic_batch_items}...")

    backends = load_backends_config(settings.backends_config_path)
    policies = load_policies_config(settings.policies_config_path)

    request_tracker = BigQueryRequestTracker(settings) if settings.environment_mode == "gcp" else InMemoryRequestTracker()
    blob_storage = GCSBlobStorage(settings) if settings.environment_mode == "gcp" else InMemoryBlobStorage(bucket_name=settings.gcs_bucket_name)
    queue_consumer = PubSubQueueConsumer(settings) if settings.environment_mode == "gcp" else InMemoryQueueConsumer()

    await request_tracker.initialize()
    await blob_storage.initialize()
    await queue_consumer.initialize()

    backend_clients = {}
    for b in backends:
        if b.endpoint_url.startswith("mock://") or settings.environment_mode == "mock":
            backend_clients[b.id] = MockBackend(b)
        elif "openai.com" in b.endpoint_url:
            backend_clients[b.id] = OpenAIBackend(b)
        elif "provisioned" in b.id.lower():
            backend_clients[b.id] = GCPProvisionedBackend(b)
        else:
            backend_clients[b.id] = GeminiFlexBackend(b)

    health_monitor = HealthMonitor(backends)
    routing_engine = RoutingEngine(backends, policies, health_monitor, backend_clients)
    batch_reassembler = BatchReassembler(request_tracker, blob_storage)

    worker = BatchSubRequestWorker(
        request_tracker=request_tracker,
        blob_storage=blob_storage,
        routing_engine=routing_engine,
        batch_reassembler=batch_reassembler,
    )

    await queue_consumer.consume_batch_items(worker.process_sub_request)
    logger.info("Batch Sub-Request Worker is actively listening for sub-requests.")


async def _run_all_workers_async():
    await _run_primary_worker_async()
    await _run_batch_worker_async()


def run_worker_service(mode: str = "worker-all"):
    """Run continuous worker service with an integrated HTTP health probe server for Cloud Run."""
    settings = GatewaySettings()
    app = FastAPI(title=f"AsyncGW Worker ({mode})", docs_url=None, redoc_url=None)

    @app.on_event("startup")
    async def _start_workers():
        if mode in ["worker-primary", "worker-all"]:
            await _run_primary_worker_async()
        if mode in ["worker-batch", "worker-all"]:
            await _run_batch_worker_async()
        logger.info(f"Background worker processes initialized and listening for '{mode}'.")

    @app.get("/healthz")
    @app.get("/")
    async def health():
        return {
            "status": "healthy",
            "service": f"asyncgw-{mode}",
            "environment_mode": settings.environment_mode,
            "project_id": settings.project_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    logger.info(f"Starting Worker Fleet Health listener on http://{settings.api_host}:{settings.api_port} (Mode: {mode})")
    uvicorn.run(app, host=settings.api_host, port=settings.api_port)


def main():
    parser = argparse.ArgumentParser(description="Asynchronous LLM Gateway CLI")
    parser.add_argument(
        "service",
        nargs="?",
        default="gateway",
        choices=["gateway", "worker-primary", "worker-batch", "worker-all", "ui"],
        help="Service or worker mode to run (default: gateway)",
    )
    args = parser.parse_args()

    if args.service == "gateway":
        run_gateway()
    elif args.service in ["worker-primary", "worker-batch", "worker-all"]:
        run_worker_service(args.service)
    elif args.service == "ui":
        from asyncgw.ui.app import run_ui
        run_ui()


if __name__ == "__main__":
    main()

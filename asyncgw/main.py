"""Main CLI entrypoint for Asynchronous LLM Gateway services and workers."""

import argparse
import asyncio
from datetime import datetime, timezone
import logging
import os
import sys
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import JSONResponse
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


class WorkerFleetManager:
    """Persistent lifecycle manager for background workers and queue consumers."""

    def __init__(self, settings: GatewaySettings, mode: str = "worker-all"):
        self.settings = settings
        self.mode = mode
        self.request_tracker = None
        self.blob_storage = None
        self.queue_producer = None
        self.queue_consumer = None
        self.routing_engine = None
        self.batch_splitter = None
        self.batch_reassembler = None
        self.primary_worker = None
        self.batch_worker = None
        self.is_running = False
        self.last_error: Optional[str] = None

    async def initialize(self):
        logger.info(
            f"Initializing WorkerFleet (Mode: {self.mode}, Env: {self.settings.environment_mode}, Project: {self.settings.project_id})..."
        )
        backends = load_backends_config(self.settings.backends_config_path)
        policies = load_policies_config(self.settings.policies_config_path)

        self.request_tracker = (
            BigQueryRequestTracker(self.settings)
            if self.settings.environment_mode == "gcp"
            else InMemoryRequestTracker()
        )
        self.blob_storage = (
            GCSBlobStorage(self.settings)
            if self.settings.environment_mode == "gcp"
            else InMemoryBlobStorage(bucket_name=self.settings.gcs_bucket_name)
        )
        self.queue_producer = (
            PubSubQueueProducer(self.settings)
            if self.settings.environment_mode == "gcp"
            else InMemoryQueueProducer()
        )
        self.queue_consumer = (
            PubSubQueueConsumer(self.settings)
            if self.settings.environment_mode == "gcp"
            else InMemoryQueueConsumer()
        )

        await self.request_tracker.initialize()
        await self.blob_storage.initialize()
        await self.queue_producer.initialize()
        await self.queue_consumer.initialize()

        backend_clients = {}
        for b in backends:
            if b.endpoint_url.startswith("mock://") or self.settings.environment_mode == "mock":
                backend_clients[b.id] = MockBackend(b)
            elif "openai.com" in b.endpoint_url:
                backend_clients[b.id] = OpenAIBackend(b)
            elif "provisioned" in b.id.lower():
                backend_clients[b.id] = GCPProvisionedBackend(b)
            else:
                backend_clients[b.id] = GeminiFlexBackend(b)

        health_monitor = HealthMonitor(backends)
        self.routing_engine = RoutingEngine(backends, policies, health_monitor, backend_clients)
        self.batch_splitter = BatchSplitter(
            self.request_tracker, self.blob_storage, self.queue_producer
        )
        self.batch_reassembler = BatchReassembler(self.request_tracker, self.blob_storage)

        self.primary_worker = PrimaryRequestWorker(
            request_tracker=self.request_tracker,
            blob_storage=self.blob_storage,
            routing_engine=self.routing_engine,
            batch_splitter=self.batch_splitter,
        )
        self.batch_worker = BatchSubRequestWorker(
            request_tracker=self.request_tracker,
            blob_storage=self.blob_storage,
            routing_engine=self.routing_engine,
            batch_reassembler=self.batch_reassembler,
        )

    async def start(self):
        try:
            await self.initialize()

            if self.mode in ["worker-primary", "worker-all"]:
                logger.info(
                    f"Starting Primary Request Worker listening on subscription '{self.settings.pubsub_subscription_requests}'..."
                )
                await self.queue_consumer.consume_requests(self.primary_worker.process_envelope)

            if self.mode in ["worker-batch", "worker-all"]:
                logger.info(
                    f"Starting Batch Sub-Request Worker listening on subscription '{self.settings.pubsub_subscription_batch_items}'..."
                )
                await self.queue_consumer.consume_batch_items(self.batch_worker.process_sub_request)

            self.is_running = True
            logger.info(f"WorkerFleetManager started successfully and is actively listening (Mode: {self.mode}).")
        except Exception as e:
            self.last_error = str(e)
            logger.error(f"Failed to start WorkerFleetManager: {e}", exc_info=True)
            raise

    async def stop(self):
        self.is_running = False
        if self.queue_consumer:
            await self.queue_consumer.stop()
        logger.info("WorkerFleetManager stopped.")


def run_gateway():
    """Run the FastAPI Gateway server with integrated Web UI and in-memory background workers."""
    settings = GatewaySettings()
    logger.info(f"Starting Async Gateway API & Web UI Dashboard on http://{settings.api_host}:{settings.api_port}")
    app = create_ui_app(create_app(settings))

    # In local mock mode, auto-start in-memory workers so queued requests are processed
    if settings.environment_mode == "mock" and isinstance(getattr(app.state, "queue_consumer", None), InMemoryQueueConsumer):
        fleet = WorkerFleetManager(settings, mode="worker-all")
        app.state.local_fleet = fleet

        @app.on_event("startup")
        async def _start_local_workers():
            fleet.request_tracker = app.state.request_tracker
            fleet.blob_storage = app.state.blob_storage
            fleet.queue_producer = app.state.queue_producer
            fleet.queue_consumer = app.state.queue_consumer
            fleet.routing_engine = app.state.routing_engine
            fleet.batch_splitter = app.state.batch_splitter
            fleet.batch_reassembler = app.state.batch_reassembler

            fleet.primary_worker = PrimaryRequestWorker(
                request_tracker=app.state.request_tracker,
                blob_storage=app.state.blob_storage,
                routing_engine=app.state.routing_engine,
                batch_splitter=app.state.batch_splitter,
            )
            fleet.batch_worker = BatchSubRequestWorker(
                request_tracker=app.state.request_tracker,
                blob_storage=app.state.blob_storage,
                routing_engine=app.state.routing_engine,
                batch_reassembler=app.state.batch_reassembler,
            )
            await app.state.queue_consumer.consume_requests(fleet.primary_worker.process_envelope)
            await app.state.queue_consumer.consume_batch_items(fleet.batch_worker.process_sub_request)
            fleet.is_running = True
            logger.info("Local in-memory background worker fleet started automatically.")

    uvicorn.run(app, host=settings.api_host, port=settings.api_port)


def run_worker_service(mode: str = "worker-all"):
    """Run continuous worker service with an integrated HTTP health probe server for Cloud Run."""
    settings = GatewaySettings()
    app = FastAPI(title=f"AsyncGW Worker ({mode})", docs_url=None, redoc_url=None)
    fleet = WorkerFleetManager(settings, mode=mode)
    app.state.fleet = fleet

    @app.on_event("startup")
    async def _start_workers():
        try:
            await fleet.start()
        except Exception as e:
            logger.error(f"Error during worker startup: {e}")

    @app.on_event("shutdown")
    async def _stop_workers():
        try:
            await fleet.stop()
        except Exception as e:
            logger.warning(f"Error during worker shutdown: {e}")

    @app.get("/healthz")
    @app.get("/")
    async def health():
        return {
            "status": "healthy" if fleet.is_running else ("error" if fleet.last_error else "initializing"),
            "service": f"asyncgw-{mode}",
            "environment_mode": settings.environment_mode,
            "project_id": settings.project_id,
            "is_running": fleet.is_running,
            "error": fleet.last_error,
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

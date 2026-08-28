"""Main CLI entrypoint for Asynchronous LLM Gateway services and workers."""

import argparse
import asyncio
from datetime import datetime, timezone
import logging
import os
from pathlib import Path
import sys
import time
from typing import Dict, Optional

from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn

from asyncgw.backends.base import BaseLLMBackend
from asyncgw.backends.factory import create_backend_client
from asyncgw.backends.gcp_provisioned import GCPProvisionedBackend
from asyncgw.backends.gemini_flex import GeminiFlexBackend
from asyncgw.backends.health import HealthMonitor
from asyncgw.backends.mock_backend import MockBackend
from asyncgw.backends.openai_client import OpenAIBackend
from asyncgw.batch.reassembler import BatchReassembler
from asyncgw.batch.splitter import BatchSplitter
from asyncgw.config import (
    BackendConfig,
    GatewaySettings,
    PoliciesConfig,
    load_backends_config,
    load_policies_config,
    save_backends_config,
    save_policies_config,
)
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
    """Persistent lifecycle manager for background workers and queue consumers with automatic live config reloading."""

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
        self.backend_clients: Dict[str, BaseLLMBackend] = {}
        self.health_monitor: Optional[HealthMonitor] = None
        self.is_running = False
        self.last_error: Optional[str] = None
        self._last_backends_mtime: float = 0.0
        self._last_policies_mtime: float = 0.0
        self._last_storage_version: float = 0.0
        self._config_watcher_task: Optional[asyncio.Task] = None

    def _get_file_mtime(self, file_path: str) -> float:
        try:
            path = Path(file_path)
            if not path.is_absolute():
                base_dir = Path(__file__).resolve().parent.parent
                path = base_dir / path
            if path.exists():
                return path.stat().st_mtime
        except Exception:
            pass
        return 0.0

    async def check_and_reload_config(self, force: bool = False) -> bool:
        """Check for modified config files (locally or in Cloud Storage) and live-reload in-memory routing engine."""
        should_reload = force
        loaded_remote_backends = None
        loaded_remote_policies = None

        # 1. Check Cloud Storage (GCS) if available for remote updates
        if self.blob_storage is not None:
            try:
                if await self.blob_storage.exists("system/config/version.json"):
                    ver_data = await self.blob_storage.get_json("system/config/version.json")
                    remote_version = float(ver_data.get("version", 0.0))
                    if remote_version > self._last_storage_version or force:
                        logger.info(
                            f"WorkerFleetManager: Remote configuration update detected in storage (v={remote_version})"
                        )
                        if await self.blob_storage.exists("system/config/backends.json"):
                            b_data = await self.blob_storage.get_json("system/config/backends.json")
                            loaded_remote_backends = [BackendConfig(**b) for b in b_data.get("backends", [])]
                            try:
                                save_backends_config(loaded_remote_backends, self.settings.backends_config_path)
                            except Exception as fe:
                                logger.debug(f"Local backends file save note: {fe}")
                        if await self.blob_storage.exists("system/config/policies.json"):
                            p_data = await self.blob_storage.get_json("system/config/policies.json")
                            loaded_remote_policies = PoliciesConfig(**p_data.get("policies", {}))
                            try:
                                save_policies_config(loaded_remote_policies, self.settings.policies_config_path)
                            except Exception as fe:
                                logger.debug(f"Local policies file save note: {fe}")
                        self._last_storage_version = max(remote_version, self._last_storage_version)
                        should_reload = True
            except Exception as e:
                logger.debug(f"Storage config sync check note: {e}")

        # 2. Check local file modification times
        curr_backends_mtime = self._get_file_mtime(self.settings.backends_config_path)
        curr_policies_mtime = self._get_file_mtime(self.settings.policies_config_path)

        if curr_backends_mtime != self._last_backends_mtime or curr_policies_mtime != self._last_policies_mtime:
            should_reload = True

        if not should_reload:
            return False

        try:
            backends = loaded_remote_backends if loaded_remote_backends is not None else load_backends_config(self.settings.backends_config_path)
            policies = loaded_remote_policies if loaded_remote_policies is not None else load_policies_config(self.settings.policies_config_path)

            new_clients = {}
            for b in backends:
                new_clients[b.id] = create_backend_client(b, self.settings.environment_mode)
            self.backend_clients = new_clients

            if self.health_monitor:
                self.health_monitor.update_backends(backends)

            if self.routing_engine:
                self.routing_engine.update_config(backends, policies, self.backend_clients)

            self._last_backends_mtime = curr_backends_mtime
            self._last_policies_mtime = curr_policies_mtime
            logger.info(
                f"WorkerFleetManager: Successfully live-reloaded configuration ({len(backends)} backends, default policy: '{policies.default_policy}')."
            )
            return True
        except Exception as e:
            logger.error(f"WorkerFleetManager: Failed to reload configuration: {e}", exc_info=True)
            return False

    async def _config_watcher_loop(self):
        """Background loop that periodically checks for configuration changes."""
        while self.is_running:
            try:
                await asyncio.sleep(2.0)
                await self.check_and_reload_config()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Config watcher loop note: {e}")

    async def initialize(self):
        logger.info(
            f"Initializing WorkerFleet (Mode: {self.mode}, Env: {self.settings.environment_mode}, Project: {self.settings.project_id})..."
        )
        backends = load_backends_config(self.settings.backends_config_path)
        policies = load_policies_config(self.settings.policies_config_path)

        self._last_backends_mtime = self._get_file_mtime(self.settings.backends_config_path)
        self._last_policies_mtime = self._get_file_mtime(self.settings.policies_config_path)
        self._last_storage_version = 0.0

        if self.settings.environment_mode == "gcp":
            self.request_tracker = BigQueryRequestTracker(self.settings)
            self.blob_storage = GCSBlobStorage(self.settings)
            self.queue_producer = PubSubQueueProducer(self.settings)
            self.queue_consumer = PubSubQueueConsumer(self.settings)
        else:
            self.request_tracker = InMemoryRequestTracker()
            self.blob_storage = InMemoryBlobStorage(bucket_name=self.settings.gcs_bucket_name)
            q1 = asyncio.Queue()
            q2 = asyncio.Queue()
            q3 = asyncio.Queue()
            self.queue_producer = InMemoryQueueProducer(q1, q2, q3)
            self.queue_consumer = InMemoryQueueConsumer(q1, q2, q3)

        await self.request_tracker.initialize()
        await self.blob_storage.initialize()
        await self.queue_producer.initialize()
        await self.queue_consumer.initialize()

        # Check and sync with remote blob storage configuration on startup
        await self.check_and_reload_config(force=False)

        self.backend_clients = {}
        for b in backends:
            self.backend_clients[b.id] = create_backend_client(b, self.settings.environment_mode)

        self.health_monitor = HealthMonitor(backends)
        self.routing_engine = RoutingEngine(backends, policies, self.health_monitor, self.backend_clients)
        self.batch_splitter = BatchSplitter(
            self.request_tracker, self.blob_storage, self.queue_producer
        )
        self.batch_reassembler = BatchReassembler(self.request_tracker, self.blob_storage)

        self.primary_worker = PrimaryRequestWorker(
            request_tracker=self.request_tracker,
            blob_storage=self.blob_storage,
            routing_engine=self.routing_engine,
            batch_splitter=self.batch_splitter,
            config_reloader=self.check_and_reload_config,
        )
        self.batch_worker = BatchSubRequestWorker(
            request_tracker=self.request_tracker,
            blob_storage=self.blob_storage,
            routing_engine=self.routing_engine,
            batch_reassembler=self.batch_reassembler,
            config_reloader=self.check_and_reload_config,
        )

    async def start(self):
        try:
            await self.initialize()
            self.is_running = True
            self._config_watcher_task = asyncio.create_task(self._config_watcher_loop())

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

            logger.info(f"WorkerFleetManager started successfully and is actively listening (Mode: {self.mode}).")
        except Exception as e:
            self.last_error = str(e)
            logger.error(f"Failed to start WorkerFleetManager: {e}", exc_info=True)
            raise

    async def stop(self):
        self.is_running = False
        if self._config_watcher_task and not self._config_watcher_task.done():
            self._config_watcher_task.cancel()
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
                config_reloader=fleet.check_and_reload_config,
            )
            fleet.batch_worker = BatchSubRequestWorker(
                request_tracker=app.state.request_tracker,
                blob_storage=app.state.blob_storage,
                routing_engine=app.state.routing_engine,
                batch_reassembler=app.state.batch_reassembler,
                config_reloader=fleet.check_and_reload_config,
            )
            fleet.is_running = True
            fleet._config_watcher_task = asyncio.create_task(fleet._config_watcher_loop())
            await app.state.queue_consumer.consume_requests(fleet.primary_worker.process_envelope)
            await app.state.queue_consumer.consume_batch_items(fleet.batch_worker.process_sub_request)
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

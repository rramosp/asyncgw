"""Health check and availability monitor for LLM backend services."""

import asyncio
from datetime import datetime, timezone
import logging
from typing import Dict, List, Optional
import httpx
from pydantic import BaseModel, Field

from asyncgw.config import BackendConfig

logger = logging.getLogger(__name__)


class BackendHealthStatus(BaseModel):
    backend_id: str
    is_healthy: bool = True
    consecutive_failures: int = 0
    last_check_time: Optional[datetime] = None
    last_latency_ms: Optional[float] = None
    last_status_code: Optional[int] = None
    last_error: Optional[str] = None


class HealthMonitor:
    """Monitors backend health and availability according to configured rules."""

    def __init__(self, backends: List[BackendConfig]):
        self.backends_map: Dict[str, BackendConfig] = {b.id: b for b in backends}
        self.health_statuses: Dict[str, BackendHealthStatus] = {
            b.id: BackendHealthStatus(backend_id=b.id, is_healthy=True)
            for b in backends
        }
        self._lock = asyncio.Lock()
        self._monitoring_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    def update_backends(self, backends: List[BackendConfig]) -> None:
        """Update backend configurations and initialize new health status if needed."""
        self.backends_map = {b.id: b for b in backends}
        for b in backends:
            if b.id not in self.health_statuses:
                self.health_statuses[b.id] = BackendHealthStatus(backend_id=b.id, is_healthy=True)

    def is_backend_available(self, backend_id: str) -> bool:
        """Check if a backend is configured active and currently passing health checks."""
        config = self.backends_map.get(backend_id)
        if not config or not config.is_active:
            return False
        status = self.health_statuses.get(backend_id)
        if not status:
            return True
        return status.is_healthy

    async def probe_backend(self, backend_id: str) -> BackendHealthStatus:
        """Execute a single health probe against a backend service."""
        config = self.backends_map.get(backend_id)
        if not config:
            raise ValueError(f"Unknown backend ID: {backend_id}")

        health_cfg = config.health_check
        if not health_cfg:
            # If no health check defined, default to healthy
            async with self._lock:
                status = self.health_statuses[backend_id]
                status.is_healthy = True
                status.last_check_time = datetime.now(timezone.utc)
                return status

        # Handle mock backend schema
        if config.endpoint_url.startswith("mock://"):
            async with self._lock:
                status = self.health_statuses[backend_id]
                status.is_healthy = True
                status.last_check_time = datetime.now(timezone.utc)
                status.last_latency_ms = 1.0
                status.last_status_code = 200
                status.consecutive_failures = 0
                return status

        start_time = asyncio.get_running_loop().time()
        now = datetime.now(timezone.utc)
        headers = {}
        if config.auth.type == "api_key" and config.auth.api_key_value:
            headers[config.auth.header_name] = f"{config.auth.header_prefix or ''}{config.auth.api_key_value}"

        try:
            async with httpx.AsyncClient(timeout=health_cfg.timeout_seconds) as client:
                res = await client.request(
                    method=health_cfg.method,
                    url=health_cfg.endpoint_url,
                    headers=headers,
                )
                latency = (asyncio.get_running_loop().time() - start_time) * 1000.0

                async with self._lock:
                    status = self.health_statuses[backend_id]
                    status.last_check_time = now
                    status.last_latency_ms = latency
                    status.last_status_code = res.status_code

                    if res.status_code == health_cfg.expected_status or res.is_success:
                        status.is_healthy = True
                        status.consecutive_failures = 0
                        status.last_error = None
                    else:
                        status.consecutive_failures += 1
                        status.last_error = f"HTTP {res.status_code}: {res.text[:200]}"
                        if status.consecutive_failures >= health_cfg.max_consecutive_failures:
                            status.is_healthy = False
                            logger.warning(
                                f"Backend {backend_id} marked UNHEALTHY after "
                                f"{status.consecutive_failures} failures. Error: {status.last_error}"
                            )
                    return status

        except Exception as e:
            latency = (asyncio.get_running_loop().time() - start_time) * 1000.0
            async with self._lock:
                status = self.health_statuses[backend_id]
                status.last_check_time = now
                status.last_latency_ms = latency
                status.consecutive_failures += 1
                status.last_error = str(e)
                if status.consecutive_failures >= health_cfg.max_consecutive_failures:
                    status.is_healthy = False
                    logger.warning(
                        f"Backend {backend_id} marked UNHEALTHY due to exception: {e}"
                    )
                return status

    def record_execution_outcome(
        self, backend_id: str, success: bool, status_code: int, error: Optional[str] = None
    ) -> None:
        """Record real traffic outcome (circuit breaker / reactive health tracking)."""
        if backend_id not in self.health_statuses:
            return
        status = self.health_statuses[backend_id]
        if success:
            status.consecutive_failures = 0
            status.is_healthy = True
        else:
            if status_code in [429, 500, 502, 503, 504]:
                status.consecutive_failures += 1
                cfg = self.backends_map.get(backend_id)
                threshold = (
                    cfg.health_check.max_consecutive_failures
                    if cfg and cfg.health_check
                    else 3
                )
                if status.consecutive_failures >= threshold:
                    status.is_healthy = False
                    status.last_error = f"Circuit breaker tripped on status {status_code}: {error}"
                    logger.warning(f"Circuit breaker tripped for {backend_id}: {status.last_error}")

    async def probe_all(self) -> Dict[str, BackendHealthStatus]:
        """Probe all backends concurrently."""
        tasks = [self.probe_backend(b_id) for b_id in self.backends_map.keys()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return self.health_statuses

    def get_all_statuses(self) -> Dict[str, BackendHealthStatus]:
        return dict(self.health_statuses)

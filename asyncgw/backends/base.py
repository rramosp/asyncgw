"""Base interface for LLM backends."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from asyncgw.config import BackendConfig
from asyncgw.models.request import AsyncRequestEnvelope, BatchItem


class BackendExecutionResult:
    """Standardized result returned from backend execution."""

    def __init__(
        self,
        success: bool,
        status_code: int,
        response_data: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
        elapsed_seconds: float = 0.0,
        content_length: int = 0,
        content_tokens: Optional[int] = None,
        raw_headers: Optional[Dict[str, str]] = None,
        routing_metadata: Optional[Dict[str, Any]] = None,
    ):
        self.success = success
        self.status_code = status_code
        self.response_data = response_data or {}
        self.error_message = error_message
        self.elapsed_seconds = elapsed_seconds
        self.content_length = content_length
        self.content_tokens = content_tokens
        self.raw_headers = raw_headers or {}
        self.routing_metadata = routing_metadata or {}


class BaseLLMBackend(ABC):
    """Abstract interface for interacting with downstream LLM inference providers."""

    def __init__(self, config: BackendConfig):
        self.config = config
        self.backend_id = config.id
        self.name = config.name

    @abstractmethod
    async def execute_online(
        self, envelope: AsyncRequestEnvelope
    ) -> BackendExecutionResult:
        """Execute a single online inference request (chat, completion, embeddings)."""
        pass

    @abstractmethod
    async def execute_batch(
        self, envelope: AsyncRequestEnvelope, items: List[BatchItem]
    ) -> BackendExecutionResult:
        """Execute a batch request on backends supporting native batch."""
        pass

    @abstractmethod
    async def check_health(self) -> bool:
        """Probe the backend health/availability endpoint."""
        pass

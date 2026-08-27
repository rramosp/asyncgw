"""Abstract interfaces for storage: Request Tracker (BigQuery) and Blob Storage (GCS)."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from asyncgw.models.request import AsyncRequestEnvelope
from asyncgw.models.response import RequestStatusEnum, RequestStatusResponse


class BaseRequestTracker(ABC):
    """Abstract interface for request lifecycle tracking (BigQuery)."""

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize datasets, tables, and schema partitions if not exist."""
        pass

    @abstractmethod
    async def register_request(self, envelope: AsyncRequestEnvelope) -> None:
        """Register a new request in PENDING state."""
        pass

    @abstractmethod
    async def register_batch_sub_requests(
        self, envelopes: List[AsyncRequestEnvelope]
    ) -> None:
        """Bulk register sub-requests broken down from a batch."""
        pass

    @abstractmethod
    async def mark_processing(
        self,
        request_id: str,
        backend_service_id: str,
        backend_endpoint: Optional[str] = None,
        sequence_number: Optional[int] = None,
    ) -> None:
        """Mark request as PROCESSING."""
        pass

    @abstractmethod
    async def mark_completed(
        self,
        request_id: str,
        response_gcs_uri: str,
        response_status_code: int,
        response_content_length: int,
        elapsed_seconds: float,
        backend_service_id: str,
        content_tokens: Optional[int] = None,
        sequence_number: Optional[int] = None,
        backend_batch_service_mode: Optional[str] = None,
    ) -> None:
        """Mark request as COMPLETED."""
        pass

    @abstractmethod
    async def mark_failed(
        self,
        request_id: str,
        error_message: str,
        response_status_code: Optional[int] = None,
        elapsed_seconds: Optional[float] = None,
        backend_service_id: Optional[str] = None,
        sequence_number: Optional[int] = None,
        backend_batch_service_mode: Optional[str] = None,
    ) -> None:
        """Mark request as FAILED."""
        pass

    @abstractmethod
    async def mark_timed_out(
        self,
        request_id: str,
        error_message: str = "Request exceeded user-specified maximum wait time",
        sequence_number: Optional[int] = None,
    ) -> None:
        """Mark request as TIMED_OUT."""
        pass

    @abstractmethod
    async def get_request_status(
        self, request_id: str, sequence_number: Optional[int] = None
    ) -> Optional[RequestStatusResponse]:
        """Fetch current status for a given request."""
        pass

    @abstractmethod
    async def get_batch_sub_requests(
        self, parent_request_id: str
    ) -> List[RequestStatusResponse]:
        """Fetch all sub-requests for a batch."""
        pass

    @abstractmethod
    async def list_recent_requests(
        self, limit: int = 50, status: Optional[RequestStatusEnum] = None
    ) -> List[RequestStatusResponse]:
        """Fetch recent requests for admin dashboard."""
        pass


class BaseBlobStorage(ABC):
    """Abstract interface for object storage (GCS)."""

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize bucket and retention lifecycle rules."""
        pass

    @abstractmethod
    async def save_json(
        self, path: str, data: Dict[str, Any], content_type: str = "application/json"
    ) -> str:
        """Save JSON object to storage and return its URI (gs://...)."""
        pass

    @abstractmethod
    async def get_json(self, path_or_uri: str) -> Dict[str, Any]:
        """Read JSON object from storage path or URI."""
        pass

    @abstractmethod
    async def save_jsonl(self, path: str, lines: List[Dict[str, Any]]) -> str:
        """Save JSONL file to storage and return its URI."""
        pass

    @abstractmethod
    async def get_jsonl(self, path_or_uri: str) -> List[Dict[str, Any]]:
        """Read JSONL file from storage."""
        pass

    @abstractmethod
    async def exists(self, path_or_uri: str) -> bool:
        """Check if an object exists."""
        pass

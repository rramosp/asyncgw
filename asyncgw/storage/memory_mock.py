"""In-memory and file-based mock storage for testing and local dev mode."""

import asyncio
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional

from asyncgw.models.request import AsyncRequestEnvelope, RequestType
from asyncgw.models.response import RequestStatusEnum, RequestStatusResponse
from asyncgw.storage.base import BaseBlobStorage, BaseRequestTracker

logger = logging.getLogger(__name__)


class InMemoryRequestTracker(BaseRequestTracker):
    """In-memory request tracker storing records in Python dictionaries."""

    def __init__(self):
        self.records: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        pass

    def _get_key(self, request_id: str, sequence_number: Optional[int]) -> str:
        if sequence_number is not None:
            return f"{request_id}:{sequence_number}"
        return request_id

    async def register_request(self, envelope: AsyncRequestEnvelope) -> None:
        async with self._lock:
            key = self._get_key(envelope.request_id, envelope.sequence_number)
            self.records[key] = {
                "request_id": envelope.request_id,
                "parent_request_id": envelope.parent_request_id,
                "sequence_number": envelope.sequence_number,
                "total_items": envelope.total_items or 1,
                "custom_id": envelope.custom_id,
                "request_type": envelope.request_type.value,
                "status": RequestStatusEnum.PENDING.value,
                "model": envelope.model,
                "max_wait_seconds": envelope.max_wait_seconds,
                "created_at": envelope.created_at,
                "expires_at": envelope.expires_at,
                "started_at": None,
                "completed_at": None,
                "elapsed_seconds": None,
                "backend_service_id": envelope.target_backend,
                "backend_endpoint": None,
                "response_status_code": None,
                "response_content_length": None,
                "response_gcs_uri": None,
                "error_message": None,
                "retry_count": envelope.retry_count,
                "content_tokens": None,
                "metadata_json": json.dumps(envelope.tags or {}),
            }

    async def register_batch_sub_requests(
        self, envelopes: List[AsyncRequestEnvelope]
    ) -> None:
        async with self._lock:
            for env in envelopes:
                key = self._get_key(env.request_id, env.sequence_number)
                self.records[key] = {
                    "request_id": env.request_id,
                    "parent_request_id": env.parent_request_id,
                    "sequence_number": env.sequence_number,
                    "total_items": env.total_items or 1,
                    "custom_id": env.custom_id,
                    "request_type": env.request_type.value,
                    "status": RequestStatusEnum.PENDING.value,
                    "model": env.model,
                    "max_wait_seconds": env.max_wait_seconds,
                    "created_at": env.created_at,
                    "expires_at": env.expires_at,
                    "started_at": None,
                    "completed_at": None,
                    "elapsed_seconds": None,
                    "backend_service_id": env.target_backend,
                    "backend_endpoint": None,
                    "response_status_code": None,
                    "response_content_length": None,
                    "response_gcs_uri": None,
                    "error_message": None,
                    "retry_count": env.retry_count,
                    "content_tokens": None,
                    "metadata_json": json.dumps(env.tags or {}),
                }

    async def mark_processing(
        self,
        request_id: str,
        backend_service_id: str,
        backend_endpoint: Optional[str] = None,
        sequence_number: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        async with self._lock:
            key = self._get_key(request_id, sequence_number)
            if key in self.records:
                self.records[key]["status"] = RequestStatusEnum.PROCESSING.value
                self.records[key]["started_at"] = datetime.now(timezone.utc)
                self.records[key]["backend_service_id"] = backend_service_id
                self.records[key]["backend_endpoint"] = backend_endpoint
                if metadata is not None:
                    existing = {}
                    if self.records[key].get("metadata_json"):
                        try:
                            existing = json.loads(self.records[key]["metadata_json"])
                        except Exception:
                            pass
                    existing.update(metadata)
                    self.records[key]["metadata_json"] = json.dumps(existing)

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
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        async with self._lock:
            key = self._get_key(request_id, sequence_number)
            if key in self.records:
                self.records[key]["status"] = RequestStatusEnum.COMPLETED.value
                self.records[key]["completed_at"] = datetime.now(timezone.utc)
                self.records[key]["response_gcs_uri"] = response_gcs_uri
                self.records[key]["response_status_code"] = response_status_code
                self.records[key]["response_content_length"] = response_content_length
                self.records[key]["elapsed_seconds"] = elapsed_seconds
                self.records[key]["backend_service_id"] = backend_service_id
                self.records[key]["content_tokens"] = content_tokens or 0
                if backend_batch_service_mode is not None:
                    self.records[key]["backend_batch_service_mode"] = backend_batch_service_mode
                if metadata is not None:
                    existing = {}
                    if self.records[key].get("metadata_json"):
                        try:
                            existing = json.loads(self.records[key]["metadata_json"])
                        except Exception:
                            pass
                    if backend_batch_service_mode is not None:
                        existing.pop("backends_tried", None)
                        existing.pop("failover_trace", None)
                    existing.update(metadata)
                    if backend_batch_service_mode is not None:
                        existing.pop("backends_tried", None)
                        existing.pop("failover_trace", None)
                    self.records[key]["metadata_json"] = json.dumps(existing)

    async def mark_failed(
        self,
        request_id: str,
        error_message: str,
        response_status_code: Optional[int] = None,
        elapsed_seconds: Optional[float] = None,
        backend_service_id: Optional[str] = None,
        sequence_number: Optional[int] = None,
        backend_batch_service_mode: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        response_gcs_uri: Optional[str] = None,
    ) -> None:
        async with self._lock:
            key = self._get_key(request_id, sequence_number)
            if key in self.records:
                self.records[key]["status"] = RequestStatusEnum.FAILED.value
                self.records[key]["completed_at"] = datetime.now(timezone.utc)
                self.records[key]["error_message"] = error_message
                self.records[key]["response_status_code"] = response_status_code or 500
                self.records[key]["elapsed_seconds"] = elapsed_seconds or 0.0
                if response_gcs_uri is not None:
                    self.records[key]["response_gcs_uri"] = response_gcs_uri
                if backend_service_id:
                    self.records[key]["backend_service_id"] = backend_service_id
                if backend_batch_service_mode is not None:
                    self.records[key]["backend_batch_service_mode"] = backend_batch_service_mode
                if metadata is not None:
                    existing = {}
                    if self.records[key].get("metadata_json"):
                        try:
                            existing = json.loads(self.records[key]["metadata_json"])
                        except Exception:
                            pass
                    if backend_batch_service_mode is not None:
                        existing.pop("backends_tried", None)
                        existing.pop("failover_trace", None)
                    existing.update(metadata)
                    if backend_batch_service_mode is not None:
                        existing.pop("backends_tried", None)
                        existing.pop("failover_trace", None)
                    self.records[key]["metadata_json"] = json.dumps(existing)

    async def mark_timed_out(
        self,
        request_id: str,
        error_message: str = "Request exceeded user-specified maximum wait time",
        sequence_number: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        async with self._lock:
            key = self._get_key(request_id, sequence_number)
            if key in self.records:
                self.records[key]["status"] = RequestStatusEnum.TIMED_OUT.value
                self.records[key]["completed_at"] = datetime.now(timezone.utc)
                self.records[key]["error_message"] = error_message
                if metadata is not None:
                    existing = {}
                    if self.records[key].get("metadata_json"):
                        try:
                            existing = json.loads(self.records[key]["metadata_json"])
                        except Exception:
                            pass
                    existing.update(metadata)
                    self.records[key]["metadata_json"] = json.dumps(existing)

    async def get_request_status(
        self, request_id: str, sequence_number: Optional[int] = None
    ) -> Optional[RequestStatusResponse]:
        async with self._lock:
            key = self._get_key(request_id, sequence_number)
            record = self.records.get(key)
            if not record:
                return None
            return self._record_to_status_response(record)

    async def get_batch_sub_requests(
        self, parent_request_id: str
    ) -> List[RequestStatusResponse]:
        async with self._lock:
            sub_reqs = []
            for r in self.records.values():
                if r.get("parent_request_id") == parent_request_id:
                    sub_reqs.append(self._record_to_status_response(r))
            sub_reqs.sort(key=lambda x: (x.sequence_number if x.sequence_number is not None else 0))
            return sub_reqs

    async def list_recent_requests(
        self, limit: int = 50, status: Optional[RequestStatusEnum] = None
    ) -> List[RequestStatusResponse]:
        async with self._lock:
            items = list(self.records.values())
            if status:
                items = [i for i in items if i["status"] == status.value]
            items.sort(key=lambda x: x["created_at"], reverse=True)
            return [self._record_to_status_response(i) for i in items[:limit]]

    def _record_to_status_response(self, row: Dict[str, Any]) -> RequestStatusResponse:
        metadata = {}
        if row.get("metadata_json"):
            try:
                metadata = json.loads(row["metadata_json"])
            except Exception:
                pass

        req_type = row.get("request_type")
        batch_mode = row.get("backend_batch_service_mode")
        # Ensure backend_batch_service_mode only appears if the request was a batch
        if req_type != RequestType.BATCH.value and req_type != "batch":
            batch_mode = None

        return RequestStatusResponse(
            request_id=row["request_id"],
            parent_request_id=row.get("parent_request_id"),
            sequence_number=row.get("sequence_number"),
            total_items=row.get("total_items") or 1,
            status=RequestStatusEnum(row["status"]),
            model=row.get("model"),
            request_type=req_type,
            created_at=row.get("created_at"),
            started_at=row.get("started_at"),
            completed_at=row.get("completed_at"),
            elapsed_seconds=row.get("elapsed_seconds"),
            backend_service_id=row.get("backend_service_id"),
            backend_batch_service_mode=batch_mode,
            response_status_code=row.get("response_status_code"),
            response_content_length=row.get("response_content_length"),
            response_gcs_uri=row.get("response_gcs_uri"),
            error_message=row.get("error_message"),
            retry_count=row.get("retry_count") or 0,
            content_tokens=row.get("content_tokens"),
            metadata=metadata,
        )


class InMemoryBlobStorage(BaseBlobStorage):
    """In-memory mock for Google Cloud Storage."""

    def __init__(self, bucket_name: str = "mock-asyncgw-bucket"):
        self.bucket_name = bucket_name
        self.blobs: Dict[str, str] = {}
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        pass

    def _normalize_key(self, path_or_uri: str) -> str:
        prefix = f"gs://{self.bucket_name}/"
        if path_or_uri.startswith(prefix):
            return path_or_uri[len(prefix):]
        if path_or_uri.startswith("gs://"):
            parts = path_or_uri.replace("gs://", "").split("/", 1)
            if len(parts) == 2:
                return parts[1]
        return path_or_uri.lstrip("/")

    async def save_json(
        self, path: str, data: Dict[str, Any], content_type: str = "application/json"
    ) -> str:
        async with self._lock:
            key = self._normalize_key(path)
            self.blobs[key] = json.dumps(data, indent=2)
            return f"gs://{self.bucket_name}/{key}"

    async def get_json(self, path_or_uri: str) -> Dict[str, Any]:
        async with self._lock:
            key = self._normalize_key(path_or_uri)
            if key not in self.blobs:
                raise FileNotFoundError(f"Blob not found in memory: {path_or_uri}")
            return json.loads(self.blobs[key])

    async def save_jsonl(self, path: str, lines: List[Dict[str, Any]]) -> str:
        async with self._lock:
            key = self._normalize_key(path)
            self.blobs[key] = "\n".join(json.dumps(line) for line in lines) + "\n"
            return f"gs://{self.bucket_name}/{key}"

    async def get_jsonl(self, path_or_uri: str) -> List[Dict[str, Any]]:
        async with self._lock:
            key = self._normalize_key(path_or_uri)
            if key not in self.blobs:
                raise FileNotFoundError(f"Blob not found in memory: {path_or_uri}")
            content = self.blobs[key]
            results = []
            for line in content.strip().split("\n"):
                if line.strip():
                    results.append(json.loads(line.strip()))
            return results

    async def exists(self, path_or_uri: str) -> bool:
        async with self._lock:
            key = self._normalize_key(path_or_uri)
            return key in self.blobs

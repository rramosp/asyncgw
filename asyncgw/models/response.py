"""Response data models for OpenAI-compatible LLM Gateway."""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union
import uuid
from pydantic import BaseModel, Field


class RequestStatusEnum(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"


class AsyncSubmitResponse(BaseModel):
    """Immediate response returned to client when a request or batch is queued."""

    request_id: str = Field(description="Unique request ID for polling and retrieving results")
    status: RequestStatusEnum = RequestStatusEnum.PENDING
    created_at: datetime
    status_url: str = Field(description="URL to poll for request status")
    response_url: Optional[str] = Field(
        default=None, description="URL to retrieve results once completed"
    )
    max_wait_seconds: Optional[int] = None
    model: str
    message: str = "Request accepted and enqueued for asynchronous processing"
    batch_id: Optional[str] = None
    total_items: Optional[int] = 1


class RequestStatusResponse(BaseModel):
    """Detailed status response for a single request or batch."""

    request_id: str
    parent_request_id: Optional[str] = None
    sequence_number: Optional[int] = None
    total_items: Optional[int] = 1
    completed_items: Optional[int] = None
    failed_items: Optional[int] = None

    status: RequestStatusEnum
    model: Optional[str] = None
    request_type: Optional[str] = None

    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    elapsed_seconds: Optional[float] = None

    backend_service_id: Optional[str] = None
    backend_batch_service_mode: Optional[str] = Field(
        default=None,
        description="Batch execution mode ('native' or 'decomposed'), only present for batch requests",
    )
    response_status_code: Optional[int] = None
    response_content_length: Optional[int] = None
    response_gcs_uri: Optional[str] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    content_tokens: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None


class Usage(BaseModel):
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


class ChoiceMessage(BaseModel):
    role: str = "assistant"
    content: Optional[str] = None
    tool_calls: Optional[List[Dict[str, Any]]] = None


class Choice(BaseModel):
    index: int = 0
    message: Optional[ChoiceMessage] = None
    text: Optional[str] = None  # for legacy completions
    finish_reason: Optional[str] = "stop"


class ChatCompletionResponse(BaseModel):
    id: str = Field(default_factory=lambda: f"chatcmpl-{uuid.uuid4().hex[:12]}")
    object: str = "chat.completion"
    created: int = Field(default_factory=lambda: int(datetime.utcnow().timestamp()))
    model: str
    choices: List[Choice]
    usage: Optional[Usage] = None
    system_fingerprint: Optional[str] = None


class BatchOutputItem(BaseModel):
    id: str
    custom_id: str
    response: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None


class BatchAggregatedResponse(BaseModel):
    id: str
    object: str = "batch"
    endpoint: str = "/v1/chat/completions"
    status: RequestStatusEnum
    backend_service_id: Optional[str] = None
    backend_batch_service_mode: Optional[str] = Field(
        default=None,
        description="Batch execution mode ('native' or 'decomposed')",
    )
    created_at: int
    in_progress_at: Optional[int] = None
    completed_at: Optional[int] = None
    failed_at: Optional[int] = None
    expires_at: Optional[int] = None
    request_counts: Dict[str, int] = Field(
        default_factory=lambda: {"total": 0, "completed": 0, "failed": 0}
    )
    output_file_id: Optional[str] = None
    error_file_id: Optional[str] = None
    output_gcs_uri: Optional[str] = None
    total_items: Optional[int] = None
    returned_items: Optional[int] = None
    results_uri: Optional[str] = None
    results: Optional[List[BatchOutputItem]] = None



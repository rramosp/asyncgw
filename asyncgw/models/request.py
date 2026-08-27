"""Request data models for OpenAI-compatible LLM Gateway."""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Union
import uuid
from pydantic import BaseModel, Field


class RequestType(str, Enum):
    CHAT_COMPLETION = "chat.completion"
    COMPLETION = "text.completion"
    EMBEDDING = "embeddings"
    BATCH = "batch"
    BATCH_SUB_REQUEST = "batch.sub_request"


class ChatMessage(BaseModel):
    role: str = Field(description="Role of the author: system, user, assistant, tool, function")
    content: Optional[Union[str, List[Dict[str, Any]]]] = Field(
        default=None, description="Message content as string or multimodal content parts"
    )
    name: Optional[str] = None
    tool_calls: Optional[List[Dict[str, Any]]] = None
    tool_call_id: Optional[str] = None


class ChatCompletionRequest(BaseModel):
    model: str = Field(description="Model identifier, e.g. gemini-2.0-flash, gpt-4o, etc.")
    messages: List[ChatMessage] = Field(description="List of conversation messages")
    temperature: Optional[float] = 1.0
    top_p: Optional[float] = 1.0
    n: Optional[int] = 1
    stream: Optional[bool] = False
    stop: Optional[Union[str, List[str]]] = None
    max_tokens: Optional[int] = None
    presence_penalty: Optional[float] = 0.0
    frequency_penalty: Optional[float] = 0.0
    response_format: Optional[Dict[str, Any]] = None
    tools: Optional[List[Dict[str, Any]]] = None
    tool_choice: Optional[Union[str, Dict[str, Any]]] = None
    user: Optional[str] = None

    # Gateway routing & deadline parameters
    max_wait_seconds: Optional[int] = Field(
        default=None, description="Maximum wait time in seconds before expiring request"
    )
    priority: Optional[str] = Field(default="normal", description="Priority tier: low, normal, high")
    tags: Optional[Dict[str, str]] = Field(default_factory=dict)
    routing_override: Optional[str] = Field(
        default=None, description="Direct target backend override if allowed"
    )


class CompletionRequest(BaseModel):
    model: str
    prompt: Union[str, List[str]]
    max_tokens: Optional[int] = 16
    temperature: Optional[float] = 1.0
    top_p: Optional[float] = 1.0
    n: Optional[int] = 1
    stop: Optional[Union[str, List[str]]] = None
    max_wait_seconds: Optional[int] = None
    priority: Optional[str] = "normal"
    tags: Optional[Dict[str, str]] = Field(default_factory=dict)


class EmbeddingRequest(BaseModel):
    model: str
    input: Union[str, List[str], List[int], List[List[int]]]
    encoding_format: Optional[str] = "float"
    dimensions: Optional[int] = None
    user: Optional[str] = None
    max_wait_seconds: Optional[int] = None


class BatchItem(BaseModel):
    custom_id: str = Field(description="User-provided identifier per request in batch")
    method: str = Field(default="POST", description="HTTP method, typically POST")
    url: str = Field(description="Relative URL: /v1/chat/completions, /v1/embeddings, etc.")
    body: Dict[str, Any] = Field(description="OpenAI compatible request body")


class BatchRequest(BaseModel):
    input_file_id: Optional[str] = Field(
        default=None, description="GCS URI or storage ID of JSONL batch input file"
    )
    endpoint: str = Field(
        default="/v1/chat/completions",
        description="The endpoint to be used for all requests in the batch",
    )
    completion_window: Optional[str] = Field(
        default="24h", description="Time frame in which the batch should be processed"
    )
    metadata: Optional[Dict[str, str]] = Field(default_factory=dict)
    requests: Optional[List[BatchItem]] = Field(
        default=None, description="Inline list of batch requests for direct submission"
    )
    max_wait_seconds: Optional[int] = Field(
        default=None, description="Maximum wait time for entire batch in seconds"
    )


class AsyncRequestEnvelope(BaseModel):
    """Unified envelope passed over Pub/Sub queues and stored in BigQuery."""

    request_id: str = Field(default_factory=lambda: f"req_{uuid.uuid4().hex}")
    parent_request_id: Optional[str] = None
    sequence_number: Optional[int] = None
    total_items: Optional[int] = 1
    custom_id: Optional[str] = None

    request_type: RequestType = RequestType.CHAT_COMPLETION
    model: str = "default-llm"
    payload: Dict[str, Any] = Field(default_factory=dict)

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: Optional[datetime] = None
    max_wait_seconds: Optional[int] = None

    client_id: Optional[str] = None
    priority: str = "normal"
    tags: Dict[str, str] = Field(default_factory=dict)
    routing_strategy: Optional[str] = None
    target_backend: Optional[str] = None

    retry_count: int = 0
    raw_input_gcs_uri: Optional[str] = None

    def is_expired(self) -> bool:
        """Check if request has surpassed its maximum wait deadline."""
        if not self.expires_at:
            return False
        return datetime.now(timezone.utc) > self.expires_at

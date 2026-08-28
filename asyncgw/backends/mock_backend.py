"""Mock LLM Backend for deterministic testing, offline simulation, and chaos testing."""

import asyncio
from datetime import datetime
import json
import logging
import time
from typing import Any, Dict, List, Optional
import uuid

from asyncgw.backends.base import BackendExecutionResult, BaseLLMBackend
from asyncgw.config import BackendConfig
from asyncgw.models.request import AsyncRequestEnvelope, BatchItem

logger = logging.getLogger(__name__)


class MockBackend(BaseLLMBackend):
    """High-fidelity mock LLM backend with failure injection and latency controls."""

    def __init__(self, config: BackendConfig):
        super().__init__(config)
        self.simulated_latency_seconds: float = 0.05
        self.should_fail: bool = False
        self.failure_status_code: int = 500
        self.failure_error_message: str = "Simulated backend internal error"
        self.fail_count_remaining: int = 0
        self.health_ok: bool = True
        self.calls_count: int = 0

    def configure_failure(
        self,
        status_code: int = 500,
        message: str = "Simulated error",
        count: int = 1,
    ) -> None:
        """Configure transient or permanent failure simulation."""
        self.should_fail = True
        self.failure_status_code = status_code
        self.failure_error_message = message
        self.fail_count_remaining = count

    def reset(self) -> None:
        self.should_fail = False
        self.fail_count_remaining = 0
        self.health_ok = True
        self.calls_count = 0

    async def execute_online(
        self, envelope: AsyncRequestEnvelope
    ) -> BackendExecutionResult:
        self.calls_count += 1
        if self.simulated_latency_seconds > 0:
            await asyncio.sleep(self.simulated_latency_seconds)

        if self.should_fail and self.fail_count_remaining > 0:
            self.fail_count_remaining -= 1
            if self.fail_count_remaining <= 0:
                self.should_fail = False
            return BackendExecutionResult(
                success=False,
                status_code=self.failure_status_code,
                error_message=self.failure_error_message,
                elapsed_seconds=self.simulated_latency_seconds,
            )

        # Generate realistic OpenAI ChatCompletionResponse
        model = envelope.model or "mock-model-v1"
        messages = envelope.payload.get("messages", [])
        last_msg = messages[-1].get("content", "") if messages else "Hello from Mock Backend"

        response_text = f"Mock response to prompt: {str(last_msg)[:100]}"
        response_payload = {
            "id": f"chatcmpl-mock-{uuid.uuid4().hex[:8]}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": response_text,
                    },
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": len(str(last_msg).split()),
                "completion_tokens": len(response_text.split()),
                "total_tokens": len(str(last_msg).split()) + len(response_text.split()),
            },
        }

        content_str = json.dumps(response_payload)
        return BackendExecutionResult(
            success=True,
            status_code=200,
            response_data=response_payload,
            elapsed_seconds=self.simulated_latency_seconds,
            content_length=len(content_str),
            content_tokens=response_payload["usage"]["total_tokens"],
        )

    async def execute_batch(
        self, envelope: AsyncRequestEnvelope, items: List[BatchItem]
    ) -> BackendExecutionResult:
        if not self.config.capabilities.supports_batch:
            return BackendExecutionResult(
                success=False,
                status_code=400,
                error_message="Backend does not support native batch processing",
            )

        start_time = time.time()
        results = []
        for seq, item in enumerate(items):
            item_env = AsyncRequestEnvelope(
                request_id=f"{envelope.request_id}_{seq}",
                parent_request_id=envelope.request_id,
                sequence_number=seq,
                custom_id=item.custom_id,
                model=item.body.get("model", envelope.model),
                payload=item.body,
            )
            res = await self.execute_online(item_env)
            item_meta = {
                "backend_service_id": self.config.id,
                "elapsed_seconds": res.elapsed_seconds,
            }
            results.append({
                "id": f"batch_req_{seq}",
                "custom_id": item.custom_id,
                "response": {"status_code": res.status_code, "body": res.response_data} if res.success else None,
                "error": {"code": res.status_code, "message": res.error_message} if not res.success else None,
                "metadata": item_meta,
            })

        elapsed = time.time() - start_time
        failed_count = sum(1 for r in results if r.get("error") is not None)
        status_str = "COMPLETED" if failed_count == 0 else "FAILED"
        batch_out = {
            "id": envelope.request_id,
            "object": "batch",
            "status": status_str,
            "backend_service_id": self.config.id,
            "backend_batch_service_mode": "native",
            "results": results,
        }
        return BackendExecutionResult(
            success=(failed_count == 0),
            status_code=200 if failed_count == 0 else 500,
            error_message=None if failed_count == 0 else f"Batch failed: {failed_count}/{len(items)} items failed",
            response_data=batch_out,
            elapsed_seconds=elapsed,
            content_length=len(json.dumps(batch_out)),
        )

    async def check_health(self) -> bool:
        return self.health_ok

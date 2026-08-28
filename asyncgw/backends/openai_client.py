"""OpenAI Direct Backend Client."""

import asyncio
from datetime import datetime
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional
import httpx

from asyncgw.backends.base import BackendExecutionResult, BaseLLMBackend
from asyncgw.config import BackendConfig
from asyncgw.models.request import AsyncRequestEnvelope, BatchItem

logger = logging.getLogger(__name__)


class OpenAIBackend(BaseLLMBackend):
    """Client for OpenAI Direct API."""

    def __init__(self, config: BackendConfig):
        super().__init__(config)
        self.api_key = (
            config.auth.api_key_value
            or (os.getenv(config.auth.secret_env) if config.auth.secret_env else None)
            or os.getenv("OPENAI_API_KEY", "mock-openai-key")
        )

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

    async def execute_online(
        self, envelope: AsyncRequestEnvelope
    ) -> BackendExecutionResult:
        start_time = time.time()
        endpoint_url = f"{self.config.endpoint_url.rstrip('/')}/chat/completions"
        if envelope.request_type.value == "text.completion":
            endpoint_url = f"{self.config.endpoint_url.rstrip('/')}/completions"
        elif envelope.request_type.value == "embeddings":
            endpoint_url = f"{self.config.endpoint_url.rstrip('/')}/embeddings"

        payload = envelope.payload.copy()
        if "model" not in payload and envelope.model:
            payload["model"] = envelope.model

        # Filter out internal gateway control fields
        for key in ["max_wait_seconds", "priority", "tags", "routing_override"]:
            payload.pop(key, None)

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                res = await client.post(endpoint_url, json=payload, headers=self._get_headers())
                elapsed = time.time() - start_time

                if res.is_success:
                    data = res.json()
                    tokens = data.get("usage", {}).get("total_tokens", 0)
                    return BackendExecutionResult(
                        success=True,
                        status_code=res.status_code,
                        response_data=data,
                        elapsed_seconds=elapsed,
                        content_length=len(res.text),
                        content_tokens=tokens,
                    )
                else:
                    return BackendExecutionResult(
                        success=False,
                        status_code=res.status_code,
                        error_message=f"OpenAI API Error ({res.status_code}): {res.text[:400]}",
                        elapsed_seconds=elapsed,
                    )
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"OpenAI backend execution exception: {e}")
            return BackendExecutionResult(
                success=False,
                status_code=500,
                error_message=str(e),
                elapsed_seconds=elapsed,
            )

    async def execute_batch(
        self, envelope: AsyncRequestEnvelope, items: List[BatchItem]
    ) -> BackendExecutionResult:
        # Submit batch to OpenAI or execute sequential/concurrent items
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
            if res.success:
                results.append({
                    "id": f"batch_req_{seq}",
                    "custom_id": item.custom_id,
                    "response": {"status_code": 200, "body": res.response_data},
                    "error": None,
                    "metadata": item_meta,
                })
            else:
                results.append({
                    "id": f"batch_req_{seq}",
                    "custom_id": item.custom_id,
                    "response": None,
                    "error": {"code": res.status_code, "message": res.error_message},
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
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.get(
                    f"{self.config.endpoint_url.rstrip('/')}/models",
                    headers=self._get_headers(),
                )
                return res.is_success
        except Exception:
            return False

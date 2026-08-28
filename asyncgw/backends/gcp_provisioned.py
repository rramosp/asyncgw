"""GCP Provisioned Throughput Backend (Vertex AI Gemini Dedicated Endpoints)."""

import asyncio
from datetime import datetime, timezone
import json
import logging
import time
from typing import Any, Dict, List, Optional
import httpx

from asyncgw.backends.base import BackendExecutionResult, BaseLLMBackend
from asyncgw.config import BackendConfig
from asyncgw.models.request import AsyncRequestEnvelope, BatchItem

logger = logging.getLogger(__name__)


class GCPProvisionedBackend(BaseLLMBackend):
    """Client for Google Cloud Vertex AI Provisioned Throughput Gemini endpoints."""

    def __init__(self, config: BackendConfig):
        super().__init__(config)
        self._auth_token: Optional[str] = None
        self._token_expiry: float = 0

    async def _get_auth_token(self) -> str:
        """Obtain GCP Bearer token using Application Default Credentials."""
        now = time.time()
        if self._auth_token and now < self._token_expiry:
            return self._auth_token

        def _fetch_token():
            import google.auth
            from google.auth.transport.requests import Request

            credentials, _ = google.auth.default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            credentials.refresh(Request())
            return credentials.token, credentials.expiry

        try:
            token, expiry = await asyncio.to_thread(_fetch_token)
            self._auth_token = token
            # Expire cache slightly earlier
            self._token_expiry = time.time() + 3000
            return self._auth_token
        except Exception as e:
            logger.warning(f"Could not obtain GCP ADC token (falling back to unauthenticated/mock): {e}")
            return "mock-adc-token"

    def _convert_openai_to_gemini(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Convert OpenAI ChatCompletion request format to Vertex AI Gemini format."""
        messages = payload.get("messages", [])
        system_instruction = None
        contents = []

        for msg in messages:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            if role == "system":
                system_instruction = {"parts": [{"text": str(content)}]}
            else:
                gemini_role = "model" if role in ["assistant", "model"] else "user"
                if isinstance(content, str):
                    parts = [{"text": content}]
                elif isinstance(content, list):
                    parts = []
                    for p in content:
                        if isinstance(p, dict) and p.get("type") == "text":
                            parts.append({"text": p.get("text", "")})
                        else:
                            parts.append({"text": str(p)})
                else:
                    parts = [{"text": str(content)}]
                contents.append({"role": gemini_role, "parts": parts})

        gemini_body: Dict[str, Any] = {"contents": contents}
        if system_instruction:
            gemini_body["systemInstruction"] = system_instruction

        generation_config: Dict[str, Any] = {}
        if "temperature" in payload:
            generation_config["temperature"] = payload["temperature"]
        if "top_p" in payload:
            generation_config["topP"] = payload["top_p"]
        if "max_tokens" in payload and payload["max_tokens"] is not None:
            generation_config["maxOutputTokens"] = payload["max_tokens"]
        if "stop" in payload and payload["stop"]:
            stop = payload["stop"]
            generation_config["stopSequences"] = [stop] if isinstance(stop, str) else stop

        if generation_config:
            gemini_body["generationConfig"] = generation_config

        return gemini_body

    def _convert_gemini_to_openai(
        self, gemini_resp: Dict[str, Any], model: str
    ) -> Dict[str, Any]:
        """Convert Vertex AI Gemini response back to OpenAI ChatCompletion format."""
        candidates = gemini_resp.get("candidates", [])
        choices = []
        for idx, cand in enumerate(candidates):
            content_parts = cand.get("content", {}).get("parts", [])
            text = "".join(p.get("text", "") for p in content_parts)
            finish_reason = cand.get("finishReason", "STOP").lower()
            if finish_reason == "stop":
                finish_reason = "stop"
            elif finish_reason == "max_tokens":
                finish_reason = "length"

            choices.append({
                "index": idx,
                "message": {"role": "assistant", "content": text},
                "finish_reason": finish_reason,
            })

        usage_meta = gemini_resp.get("usageMetadata", {})
        prompt_tokens = usage_meta.get("promptTokenCount", 0)
        completion_tokens = usage_meta.get("candidatesTokenCount", 0)
        total_tokens = usage_meta.get("totalTokenCount", prompt_tokens + completion_tokens)

        return {
            "id": f"chatcmpl-gemini-{int(time.time()*1000)}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": choices if choices else [{"index": 0, "message": {"role": "assistant", "content": ""}, "finish_reason": "stop"}],
            "usage": {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
            },
        }

    async def execute_online(
        self, envelope: AsyncRequestEnvelope
    ) -> BackendExecutionResult:
        start_time = time.time()
        model_name = envelope.model or "gemini-2.0-flash"
        gemini_payload = self._convert_openai_to_gemini(envelope.payload)

        # Build Vertex AI endpoint URL: :generateContent
        base_url = self.config.endpoint_url.rstrip("/")
        if "{PROJECT_ID}" in base_url or "${PROJECT_ID}" in base_url:
            base_url = base_url.replace("{PROJECT_ID}", "demo-project").replace("${PROJECT_ID}", "demo-project")
        
        endpoint = f"{base_url}/{model_name}:generateContent"

        token = await self._get_auth_token()
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                res = await client.post(endpoint, json=gemini_payload, headers=headers)
                elapsed = time.time() - start_time

                if res.is_success:
                    gemini_data = res.json()
                    openai_compat = self._convert_gemini_to_openai(gemini_data, model_name)
                    content_str = json.dumps(openai_compat)
                    tokens = openai_compat.get("usage", {}).get("total_tokens", 0)
                    return BackendExecutionResult(
                        success=True,
                        status_code=res.status_code,
                        response_data=openai_compat,
                        elapsed_seconds=elapsed,
                        content_length=len(content_str),
                        content_tokens=tokens,
                    )
                else:
                    return BackendExecutionResult(
                        success=False,
                        status_code=res.status_code,
                        error_message=f"Vertex AI Provisioned Error ({res.status_code}): {res.text[:500]}",
                        elapsed_seconds=elapsed,
                    )
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"GCP Provisioned execution exception: {e}")
            return BackendExecutionResult(
                success=False,
                status_code=500,
                error_message=str(e),
                elapsed_seconds=elapsed,
            )

    async def execute_batch(
        self, envelope: AsyncRequestEnvelope, items: List[BatchItem]
    ) -> BackendExecutionResult:
        # Native batch execution for Vertex AI batch jobs or simulated high-throughput bulk
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
            item_res = await self.execute_online(item_env)
            item_meta = {
                "backend_service_id": self.config.id,
                "elapsed_seconds": item_res.elapsed_seconds,
            }
            if item_res.success:
                results.append({
                    "id": f"batch_req_{seq}",
                    "custom_id": item.custom_id,
                    "response": {"status_code": 200, "body": item_res.response_data},
                    "error": None,
                    "metadata": item_meta,
                })
            else:
                results.append({
                    "id": f"batch_req_{seq}",
                    "custom_id": item.custom_id,
                    "response": None,
                    "error": {"code": item_res.status_code, "message": item_res.error_message},
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
        return True

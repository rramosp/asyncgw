"""Gemini FLEX Backend (Vertex AI Pay-as-you-go on-demand endpoints)."""

from asyncgw.backends.gcp_provisioned import GCPProvisionedBackend
from asyncgw.backends.base import BackendExecutionResult
from asyncgw.config import BackendConfig
from asyncgw.models.request import AsyncRequestEnvelope, BatchItem
from typing import List


class GeminiFlexBackend(GCPProvisionedBackend):
    """Client for Google Cloud Vertex AI Gemini on-demand (FLEX) endpoints.
    
    Inherits OpenAI-Gemini payload conversions from GCPProvisionedBackend.
    Does not support native batch execution (requires breakdown into individual items).
    """

    def __init__(self, config: BackendConfig):
        super().__init__(config)

    async def execute_batch(
        self, envelope: AsyncRequestEnvelope, items: List[BatchItem]
    ) -> BackendExecutionResult:
        # Flex endpoints do not support bulk batch in a single call.
        return BackendExecutionResult(
            success=False,
            status_code=400,
            error_message="Gemini FLEX does not support native bulk batch; requests must be broken down by gateway",
        )

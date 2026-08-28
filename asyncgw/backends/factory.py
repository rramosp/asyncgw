"""Factory to instantiate LLM backend clients based on BackendConfig."""

from asyncgw.backends.base import BaseLLMBackend
from asyncgw.backends.gcp_provisioned import GCPProvisionedBackend
from asyncgw.backends.gemini_flex import GeminiFlexBackend
from asyncgw.backends.mock_backend import MockBackend
from asyncgw.backends.openai_client import OpenAIBackend
from asyncgw.config import BackendConfig


def create_backend_client(b_cfg: BackendConfig, environment_mode: str = "mock") -> BaseLLMBackend:
    """Factory to instantiate backend client corresponding to backend configuration."""
    if b_cfg.endpoint_url.startswith("mock://") or environment_mode == "mock":
        return MockBackend(b_cfg)
    elif "openai.com" in b_cfg.endpoint_url:
        return OpenAIBackend(b_cfg)
    elif "provisioned" in b_cfg.id.lower():
        return GCPProvisionedBackend(b_cfg)
    else:
        return GeminiFlexBackend(b_cfg)

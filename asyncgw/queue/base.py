"""Abstract interface for queuing layer (Pub/Sub)."""

from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator, Callable, Dict, Optional
from asyncgw.models.request import AsyncRequestEnvelope


class BaseQueueProducer(ABC):
    """Abstract interface for producing requests to Pub/Sub topics."""

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize topics if not exist."""
        pass

    @abstractmethod
    async def publish_request(self, envelope: AsyncRequestEnvelope) -> str:
        """Publish a request envelope to the primary requests topic."""
        pass

    @abstractmethod
    async def publish_batch_item(self, envelope: AsyncRequestEnvelope) -> str:
        """Publish an individual batch sub-request envelope to the batch items topic."""
        pass

    @abstractmethod
    async def publish_dlq(self, envelope: AsyncRequestEnvelope, reason: str) -> str:
        """Publish failed/poison envelope to Dead Letter Queue."""
        pass


class BaseQueueConsumer(ABC):
    """Abstract interface for consuming requests from Pub/Sub subscriptions."""

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize subscriptions if not exist."""
        pass

    @abstractmethod
    async def consume_requests(
        self, callback: Callable[[AsyncRequestEnvelope], Any]
    ) -> None:
        """Consume messages from the primary requests queue."""
        pass

    @abstractmethod
    async def consume_batch_items(
        self, callback: Callable[[AsyncRequestEnvelope], Any]
    ) -> None:
        """Consume messages from the batch items queue."""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Stop listening and close connections."""
        pass

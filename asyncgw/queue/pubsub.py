"""Google Cloud Pub/Sub producer and consumer implementations."""

import asyncio
from concurrent.futures import TimeoutError
import json
import logging
from typing import Any, Callable, Dict, Optional

from asyncgw.config import GatewaySettings
from asyncgw.models.request import AsyncRequestEnvelope
from asyncgw.queue.base import BaseQueueConsumer, BaseQueueProducer

logger = logging.getLogger(__name__)


class PubSubQueueProducer(BaseQueueProducer):
    """Produces async request envelopes to Google Cloud Pub/Sub topics."""

    def __init__(self, settings: GatewaySettings):
        self.settings = settings
        self.project_id = settings.project_id
        self._publisher = None

    def _get_publisher(self):
        if self._publisher is None:
            from google.cloud import pubsub_v1
            self._publisher = pubsub_v1.PublisherClient()
        return self._publisher

    async def initialize(self) -> None:
        """Create topics if they do not already exist."""
        from google.api_core.exceptions import AlreadyExists
        publisher = self._get_publisher()

        topics = [
            self.settings.pubsub_topic_requests,
            self.settings.pubsub_topic_batch_items,
            self.settings.pubsub_dlq_topic,
        ]

        def _sync_create():
            for t_name in topics:
                topic_path = publisher.topic_path(self.project_id, t_name)
                try:
                    publisher.create_topic(request={"name": topic_path})
                    logger.info(f"Created Pub/Sub topic: {topic_path}")
                except AlreadyExists:
                    logger.debug(f"Pub/Sub topic already exists: {topic_path}")
                except Exception as te:
                    logger.debug(f"Pub/Sub topic check note: {te}")

        await asyncio.to_thread(_sync_create)

    def _resolve_topic_path(self, topic_name: str) -> str:
        if topic_name.startswith("projects/"):
            return topic_name
        return self._get_publisher().topic_path(self.project_id, topic_name)

    async def publish_request(self, envelope: AsyncRequestEnvelope) -> str:
        """Publish a request to the primary requests topic."""
        topic_path = self._resolve_topic_path(self.settings.pubsub_topic_requests)
        return await self._publish(topic_path, envelope)

    async def publish_batch_item(self, envelope: AsyncRequestEnvelope) -> str:
        """Publish an individual batch item to the batch items topic."""
        topic_path = self._resolve_topic_path(self.settings.pubsub_topic_batch_items)
        return await self._publish(topic_path, envelope)

    async def publish_dlq(self, envelope: AsyncRequestEnvelope, reason: str) -> str:
        """Publish to Dead Letter Queue."""
        topic_path = self._resolve_topic_path(self.settings.pubsub_dlq_topic)
        attributes = {"dlq_reason": reason, "request_id": envelope.request_id}
        return await self._publish(topic_path, envelope, extra_attributes=attributes)

    async def _publish(
        self,
        topic_path: str,
        envelope: AsyncRequestEnvelope,
        extra_attributes: Optional[Dict[str, str]] = None,
    ) -> str:
        publisher = self._get_publisher()
        data = envelope.model_dump_json().encode("utf-8")

        attributes = {
            "request_id": envelope.request_id,
            "request_type": envelope.request_type.value,
            "model": envelope.model,
            "priority": envelope.priority,
        }
        if envelope.parent_request_id:
            attributes["parent_request_id"] = envelope.parent_request_id
        if envelope.sequence_number is not None:
            attributes["sequence_number"] = str(envelope.sequence_number)
        if extra_attributes:
            attributes.update(extra_attributes)

        def _sync_pub():
            future = publisher.publish(topic_path, data, **attributes)
            return future.result(timeout=10.0)

        msg_id = await asyncio.to_thread(_sync_pub)
        logger.debug(f"Published message {msg_id} to {topic_path} for request {envelope.request_id}")
        return msg_id


class PubSubQueueConsumer(BaseQueueConsumer):
    """Consumes requests from Google Cloud Pub/Sub subscriptions."""

    def __init__(self, settings: GatewaySettings):
        self.settings = settings
        self.project_id = settings.project_id
        self._subscriber = None
        self._running = False
        self._futures = []

    def _get_subscriber(self):
        if self._subscriber is None:
            from google.cloud import pubsub_v1
            self._subscriber = pubsub_v1.SubscriberClient()
        return self._subscriber

    async def initialize(self) -> None:
        """Create subscriptions if they do not exist."""
        from google.api_core.exceptions import AlreadyExists
        subscriber = self._get_subscriber()

        subs = [
            (self.settings.pubsub_subscription_requests, self.settings.pubsub_topic_requests),
            (self.settings.pubsub_subscription_batch_items, self.settings.pubsub_topic_batch_items),
        ]

        def _sync_create():
            for sub_name, topic_name in subs:
                sub_path = subscriber.subscription_path(self.project_id, sub_name)
                topic_path = subscriber.topic_path(self.project_id, topic_name)
                try:
                    subscriber.create_subscription(
                        request={
                            "name": sub_path,
                            "topic": topic_path,
                            "ack_deadline_seconds": 60,
                        }
                    )
                    logger.info(f"Created Pub/Sub subscription: {sub_path}")
                except AlreadyExists:
                    logger.debug(f"Subscription already exists: {sub_path}")
                except Exception as se:
                    logger.debug(f"Pub/Sub subscription check note: {se}")

        await asyncio.to_thread(_sync_create)

    def _resolve_subscription_path(self, sub_name: str) -> str:
        if sub_name.startswith("projects/"):
            return sub_name
        return self._get_subscriber().subscription_path(self.project_id, sub_name)

    async def consume_requests(
        self, callback: Callable[[AsyncRequestEnvelope], Any]
    ) -> None:
        sub_path = self._resolve_subscription_path(self.settings.pubsub_subscription_requests)
        await self._start_streaming_pull(sub_path, callback)

    async def consume_batch_items(
        self, callback: Callable[[AsyncRequestEnvelope], Any]
    ) -> None:
        sub_path = self._resolve_subscription_path(self.settings.pubsub_subscription_batch_items)
        await self._start_streaming_pull(sub_path, callback)

    async def _start_streaming_pull(
        self, subscription_path: str, callback: Callable[[AsyncRequestEnvelope], Any]
    ) -> None:
        subscriber = self._get_subscriber()
        loop = asyncio.get_running_loop()

        def _msg_callback(message):
            try:
                payload_str = message.data.decode("utf-8")
                envelope = AsyncRequestEnvelope.model_validate_json(payload_str)
                # Dispatch async callback into event loop
                fut = asyncio.run_coroutine_threadsafe(callback(envelope), loop)
                fut.result(timeout=120)  # Wait for worker processing
                message.ack()
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
                message.nack()

        streaming_pull_future = subscriber.subscribe(subscription_path, callback=_msg_callback)
        self._futures.append(streaming_pull_future)
        self._running = True
        logger.info(f"Started streaming pull on {subscription_path}")

    async def stop(self) -> None:
        self._running = False
        for f in self._futures:
            f.cancel()
        if self._subscriber:
            self._subscriber.close()

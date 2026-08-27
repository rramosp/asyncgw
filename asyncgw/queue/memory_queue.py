"""In-memory queue implementation for testing and local development."""

import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional

from asyncgw.models.request import AsyncRequestEnvelope
from asyncgw.queue.base import BaseQueueConsumer, BaseQueueProducer

logger = logging.getLogger(__name__)


class InMemoryQueueProducer(BaseQueueProducer):
    """In-memory queue producer."""

    def __init__(
        self,
        requests_queue: asyncio.Queue,
        batch_items_queue: asyncio.Queue,
        dlq_queue: asyncio.Queue,
    ):
        self.requests_queue = requests_queue
        self.batch_items_queue = batch_items_queue
        self.dlq_queue = dlq_queue

    async def initialize(self) -> None:
        pass

    async def publish_request(self, envelope: AsyncRequestEnvelope) -> str:
        await self.requests_queue.put(envelope)
        return f"mem_msg_req_{envelope.request_id}"

    async def publish_batch_item(self, envelope: AsyncRequestEnvelope) -> str:
        await self.batch_items_queue.put(envelope)
        return f"mem_msg_sub_{envelope.request_id}_{envelope.sequence_number}"

    async def publish_dlq(self, envelope: AsyncRequestEnvelope, reason: str) -> str:
        await self.dlq_queue.put((envelope, reason))
        return f"mem_msg_dlq_{envelope.request_id}"


class InMemoryQueueConsumer(BaseQueueConsumer):
    """In-memory queue consumer."""

    def __init__(
        self,
        requests_queue: asyncio.Queue,
        batch_items_queue: asyncio.Queue,
        dlq_queue: asyncio.Queue,
    ):
        self.requests_queue = requests_queue
        self.batch_items_queue = batch_items_queue
        self.dlq_queue = dlq_queue
        self._running = False
        self._tasks: List[asyncio.Task] = []

    async def initialize(self) -> None:
        pass

    async def consume_requests(
        self, callback: Callable[[AsyncRequestEnvelope], Any]
    ) -> None:
        self._running = True

        async def _worker_loop():
            while self._running:
                try:
                    envelope = await asyncio.wait_for(self.requests_queue.get(), timeout=0.5)
                    try:
                        await callback(envelope)
                    except Exception as e:
                        logger.error(f"Error in request consumer callback: {e}", exc_info=True)
                        await self.dlq_queue.put((envelope, str(e)))
                    finally:
                        self.requests_queue.task_done()
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break

        task = asyncio.create_task(_worker_loop())
        self._tasks.append(task)

    async def consume_batch_items(
        self, callback: Callable[[AsyncRequestEnvelope], Any]
    ) -> None:
        self._running = True

        async def _batch_worker_loop():
            while self._running:
                try:
                    envelope = await asyncio.wait_for(self.batch_items_queue.get(), timeout=0.5)
                    try:
                        await callback(envelope)
                    except Exception as e:
                        logger.error(f"Error in batch item consumer callback: {e}", exc_info=True)
                        await self.dlq_queue.put((envelope, str(e)))
                    finally:
                        self.batch_items_queue.task_done()
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break

        task = asyncio.create_task(_batch_worker_loop())
        self._tasks.append(task)

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

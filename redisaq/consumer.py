"""
Consumer module for redisaq

Implements the Consumer class for consuming jobs from Redis Streams.
"""

import asyncio
import json
import logging
import uuid
from asyncio import Task
from typing import Optional, List

import aioredis
import orjson

from redisaq.common import TopicOperator
from redisaq.constants import APPLICATION_PREFIX
from redisaq.keys import TopicKeys


class Consumer(TopicOperator):
    """Consumer for processing jobs from Redis Streams."""

    def __init__(
        self,
        topic: str,
        redis_url: str = "redis://localhost:6379/0",
        group_name: str = "default_group",
        consumer_name: Optional[str] = "default_consumer",
        heartbeat_interval: float = 5.0,
        heartbeat_ttl: float = 10.0,
        deserializer=None,
        debug=False,
        logger=None,
    ):
        self.topic = topic
        self.redis_url = redis_url
        self.group_name = group_name
        self.consumer_name = consumer_name or str(uuid.uuid4())
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_ttl = heartbeat_ttl
        self.redis: Optional[aioredis.Redis] = None
        self.partitions: List[int] = []
        self.is_consuming = False
        self._topic_keys = TopicKeys(self.topic)
        self.pubsub: Optional[aioredis.client.PubSub] = None
        self.deserializer = deserializer or orjson
        self.logger = logger or logging.getLogger(f"{APPLICATION_PREFIX}.{consumer_name}.{self.consumer_name}")
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)
        self._heartbeat_task: Optional[Task] = None
        self._ready_task: Optional[Task] = None
        self._rebalance_event = asyncio.Event()
        self._consumer_count = 0
        self._partition_count = 0
        self._is_ready = False

    async def connect(self) -> None:
        """Connect to Redis and initialize consumer group."""
        if self.redis is None:
            self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)

        if self.redis is None:
            raise ValueError("Redis is not connected!")

        await self._create_consumer_group_for_topic()

        num_partitions = await self.get_num_partitions()
        for partition in range(num_partitions):
            if not self._topic_keys.has_partition(partition):
                self._topic_keys.add_partition(partition)

            await self._create_consumer_group_for_partition(partition)

        self.pubsub = self.redis.pubsub()
        await self.pubsub.subscribe(self._topic_keys.rebalance_channel)

    async def close(self) -> None:
        """Close Redis connection and pubsub."""
        if self.pubsub:
            await self.pubsub.unsubscribe(self._topic_keys.rebalance_channel)
            await self.pubsub.close()
            self.pubsub = None
        if self.redis:
            await self.redis.close()
            self.redis = None

    async def register_consumer(self) -> None:
        """Register consumer in the group."""
        if self.redis is None:
            raise RuntimeError("Redis is not connected! Please run connect() function first!")

        self._heartbeat_task = asyncio.create_task(self.heartbeat())
        self._ready_task = asyncio.create_task(self.set_ready())

    async def get_active_consumers(self) -> List[str]:
        """Get list of active consumers."""
        if self.redis is None:
            raise RuntimeError("Redis is not connected! Please run connect() function first!")

        consumers = []
        async for key in self.redis.scan_iter(f"{self._topic_keys.consumer_group_keys.consumer_key}:*"):
            consumer_id = key.decode().split(":")[1]
            consumers.append(consumer_id)
        return consumers

    async def get_ready_consumers(self) -> List[str]:
        """Get list of ready consumers."""
        if self.redis is None:
            raise RuntimeError("Redis is not connected! Please run connect() function first!")

        consumers = []
        async for key in self.redis.scan_iter(f"{self._topic_keys.consumer_group_keys.consumer_ready_key}:*"):
            consumer_id = key.decode().split(":")[1]
            consumers.append(consumer_id)
        return consumers

    async def update_partitions(self) -> None:
        """Update assigned partitions for this consumer."""
        if self.redis is None:
            raise RuntimeError("Redis is not connected! Please run connect() function first!")

        num_partitions = await self.get_num_partitions()
        consumers = await self.get_active_consumers()
        consumer_count = len(consumers)
        if consumer_count == 0:
            self.partitions = []
            return

        partitions_per_consumer = max(1, num_partitions // consumer_count)
        consumer_index = consumers.index(self.consumer_name) if self.consumer_name in consumers else 0
        start = consumer_index * partitions_per_consumer
        end = start + partitions_per_consumer if consumer_index < consumer_count - 1 else num_partitions
        self.partitions = list(range(start, end))
        self.logger.info(f"Consumer {self.consumer_name} assigned partitions: {self.partitions}")
        self._is_ready = True

    async def signal_rebalance(self) -> None:
        """Signal a rebalance event."""
        if self.redis is None:
            raise RuntimeError("Redis is not connected! Please run connect() function first!")

        await self.redis.publish(self._topic_keys.rebalance_channel, "rebalance")
        self.logger.info(f"Consumer {self.consumer_name} signaled rebalance")

    async def remove_ready(self) -> None:
        """Set consumer as ready after rebalance."""
        if self.redis is None:
            raise RuntimeError("Redis is not connected! Please run connect() function first!")

        self._is_ready = False
        consumer_ready_key = f"{self._topic_keys.consumer_group_keys.consumer_ready_key}:{self.consumer_name}"
        await self.redis.delete(consumer_ready_key)
        self.logger.info(f"Consumer {self.consumer_name} marked as ready")

    async def set_ready(self) -> None:
        """Send periodic ready heartbeat to indicate consumer is alive."""
        if self.redis is None:
            raise RuntimeError("Redis is not connected! Please run connect() function first!")

        while True:
            if self._is_ready:
                try:
                    consumer_ready_key = f"{self._topic_keys.consumer_group_keys.consumer_ready_key}:{self.consumer_name}"
                    await self.redis.set(name=consumer_ready_key, value=self.consumer_name, ex=int(self.heartbeat_ttl))
                    await asyncio.sleep(self.heartbeat_interval)
                except Exception as e:
                    self.logger.error(f"Error sending ready heartbeat: {e}")

            await asyncio.sleep(self.heartbeat_interval)

    async def all_consumers_ready(self) -> bool:
        """Check if all consumers are ready."""
        if self.redis is None:
            raise RuntimeError("Redis is not connected! Please run connect() function first!")

        consumers = await self.get_active_consumers()
        ready_consumers = await self.get_ready_consumers()
        return set(consumers) == set(ready_consumers) and len(consumers) > 0

    async def wait_for_all_ready(self) -> bool:
        """Wait until all consumers are ready"""
        while True:
            if await self.all_consumers_ready():
                return True

            await asyncio.sleep(0.1)

    async def heartbeat(self) -> None:
        """Send periodic heartbeat to indicate consumer is alive."""
        if self.redis is None:
            raise RuntimeError("Redis is not connected! Please run connect() function first!")

        while True:
            try:
                await self._do_heartbeat()
                await asyncio.sleep(self.heartbeat_interval)
            except Exception as e:
                self.logger.error(f"Error sending heartbeat: {e}")
                break

    async def start(self) -> None:
        """Start the consumer: register, rebalance, and wait for all ready."""
        await self.connect()

        if self.redis is None:
            raise RuntimeError("Redis is not connected! Please run connect() function first!")

        # Register consumer
        await self.register_consumer()
        self.logger.info(f"Consumer {self.consumer_name} registered in group {self.group_name}")

        # Signal rebalance to other consumers
        await self.signal_rebalance()

        self.logger.info(f"Preparing for consuming...")
        await asyncio.sleep(5)

        await self._do_rebalance()

        tasks = [
            self._consume(),
            self._detect_changes(),
            self._wait_for_rebalance(),
        ]
        await asyncio.gather(*tasks)

    async def _consume(self):
        """Consume jobs from assigned partitions."""
        if self.redis is None:
            raise RuntimeError("Redis is not connected! Please run connect() function first!")

        while True:
            if not self.is_consuming:
                return  # Not ready to consume

            try:
                streams = {self._topic_keys.partition_keys[p].stream_key: ">" for p in self.partitions}
                messages = await self.redis.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams=streams,
                    count=1,
                    block=1000
                )
                if not messages:
                    return None

                stream, [(msg_id, msg)] = messages[0]
                job = {
                    "id": msg["id"],
                    "topic": msg["topic"],
                    "payload": msg["payload"],
                    "created_at": json.loads(msg["created_at"])
                }
                await self.redis.xack(stream, self.group_name, msg_id)
                return job

            except Exception as e:
                self.logger.error(f"Error consuming job: {e}", exc_info=e)
                return None

    async def _create_consumer_group_for_partition(self, partition: int):
        try:
            await self.redis.xgroup_create(
                self._topic_keys.partition_keys[partition].stream_key,
                self.group_name,
                id="0",
                mkstream=True,
            )
        except aioredis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def _create_consumer_group_for_topic(self):
        if self._topic_keys.consumer_group_keys is None:
            self._topic_keys.set_consumer_group(self.group_name)

        await self.redis.sadd(self._topic_keys.consumer_group_key, self.group_name)

    async def _do_heartbeat(self):
        consumers_key = f"{self._topic_keys.consumer_group_keys.consumer_key}:{self.consumer_name}"
        await self.redis.set(name=consumers_key, value=self.consumer_name, ex=int(self.heartbeat_ttl))

    async def _detect_changes(self):
        # Check for rebalance signal via pubsub
        while True:
            # detect rebalance via pub/sub
            if self.pubsub:
                message = await self.pubsub.get_message(timeout=0.01)
                if message and message["type"] == "message" and message["data"] == "rebalance":
                    self._rebalance_event.set()
                    await asyncio.sleep(0.1)
                    continue

            # detect rebalance via consumer count change
            consumers = await self.get_active_consumers()
            if len(consumers) != self._consumer_count:
                self.logger.info(f"Consumer count change {self._consumer_count} -> {len(consumers)}")
                self._rebalance_event.set()
                self._consumer_count = len(consumers)
                await asyncio.sleep(0.1)
                continue

            # detect rebalance via partition count change
            partition_count = await self.get_num_partitions()
            if partition_count != self._partition_count:
                self.logger.info(f"Partition count change {self._partition_count} -> {partition_count}")
                self._rebalance_event.set()
                self._partition_count = partition_count
                await asyncio.sleep(0.1)
                continue

            await asyncio.sleep(0.1)

    async def _wait_for_rebalance(self):
        while True:
            await self._rebalance_event.wait()
            await self._do_rebalance()

    async def _do_rebalance(self):
        self.logger.info(f"Consumer {self.consumer_name} pausing for rebalance")
        self.is_consuming = False
        await self.remove_ready()
        await self.update_partitions()
        await self.wait_for_all_ready()
        self.logger.info(f"Consumer {self.consumer_name} starting consumption")
        self.is_consuming = True

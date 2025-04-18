"""
Consumer module for redisaq

Implements the Consumer class for consuming jobs from Redis Streams.
"""

import asyncio
import logging
import uuid
from asyncio import Task
from typing import Optional, List, Dict, Union

import aioredis
import orjson
from redis import ResponseError

from redisaq.common import TopicOperator
from redisaq.constants import APPLICATION_PREFIX
from redisaq.keys import TopicKeys
from redisaq.models import BatchCallback, SingleCallback, Message


class Consumer(TopicOperator):
    """Consumer for processing jobs from Redis Streams."""

    def __init__(
        self,
        topic: str,
        redis_url: str = "redis://localhost:6379/0",
        group_name: str = "default_group",
        consumer_name: Optional[str] = "default_consumer",
        batch_size: int = 10,
        heartbeat_interval: float = 3.0,
        heartbeat_ttl: float = 12.0,
        serializer=None,
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
        self.batch_size = batch_size
        self.callback: Optional[Union[SingleCallback, BatchCallback]] = None
        self.pubsub: Optional[aioredis.client.PubSub] = None
        self.deserializer = deserializer or orjson
        self.serializer = serializer or orjson
        self.logger = logger or logging.getLogger(f"{APPLICATION_PREFIX}.{consumer_name}.{self.consumer_name}")
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)
        self._is_consuming = False
        self._topic_keys = TopicKeys(self.topic)
        self._heartbeat_task: Optional[Task] = None
        self._rebalance_event = asyncio.Event()
        self._stopped_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._consumer_count = -1
        self._partition_count = -1
        self._is_ready = False
        self._is_start = False

    async def connect(self) -> None:
        """Connect to Redis and initialize consumer group."""
        if self.redis is None:
            self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)

        self.logger.info(f"Connected to redis at {self.redis_url}")

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

    async def get_consumers(self) -> Dict[str, bool]:
        """Get list of consumers."""
        if self.redis is None:
            raise RuntimeError("Redis is not connected! Please run connect() function first!")

        consumers = []
        keys = []
        values = []
        async for key in self.redis.scan_iter(f"{self._topic_keys.consumer_group_keys.consumer_key}:*"):
            keys.append(key)
            consumer_id = key.split(":")[-1]
            consumers.append(consumer_id)

        if keys:
            values = await self.redis.mget(*keys)

        return {k: self.deserializer.loads(v) for k, v in zip(consumers, values)}

    async def update_partitions(self) -> None:
        """Update assigned partitions for this consumer."""
        if self.redis is None:
            raise RuntimeError("Redis is not connected! Please run connect() function first!")

        num_partitions = await self.get_num_partitions()
        consumers = list((await self.get_consumers()).keys())
        consumer_count = len(consumers)
        if consumer_count == 0:
            self.partitions = []
            return

        partitions_per_consumer = max(1, (num_partitions - 1) // consumer_count + 1)
        consumer_index = consumers.index(self.consumer_name) if self.consumer_name in consumers else 0
        start = consumer_index * partitions_per_consumer
        end = start + partitions_per_consumer if consumer_index < consumer_count - 1 else num_partitions
        self.partitions = list(range(start, end))
        self.logger.info(f"Assigned partitions: {self.partitions}")
        self._is_ready = True

    async def signal_rebalance(self) -> None:
        """Signal a rebalance event."""
        if self.redis is None:
            raise RuntimeError("Redis is not connected! Please run connect() function first!")

        await self.redis.publish(self._topic_keys.rebalance_channel, "rebalance")
        self.logger.info(f"Fire rebalance signal")

    async def remove_ready(self) -> None:
        """Set consumer as not ready before rebalance."""
        if self.redis is None:
            raise RuntimeError("Redis is not connected! Please run connect() function first!")

        self._is_ready = False
        await self._do_heartbeat()
        self.logger.info(f"Marked as unready")

    async def all_consumers_ready(self) -> bool:
        """Check if all consumers are ready."""
        if self.redis is None:
            raise RuntimeError("Redis is not connected! Please run connect() function first!")

        all_consumers = await self.get_consumers()
        active_consumers = []
        ready_consumers = []
        for consumer, is_ready in all_consumers.items():
            active_consumers.append(consumer)
            if is_ready:
                ready_consumers.append(consumer)

        return set(active_consumers) == set(ready_consumers) and len(active_consumers) > 0

    async def wait_for_all_ready(self) -> bool:
        """Wait until all consumers are ready"""
        while self._is_start:
            if await self.all_consumers_ready():
                return True

            await asyncio.sleep(0.1)

        return False

    async def heartbeat(self) -> None:
        """Send periodic heartbeat to indicate consumer is alive."""
        if self.redis is None:
            raise RuntimeError("Redis is not connected! Please run connect() function first!")

        while self._is_start:
            try:
                await self._do_heartbeat()
                await asyncio.sleep(self.heartbeat_interval)
            except Exception as e:
                self.logger.error(f"Error sending heartbeat: {e}", exc_info=e)

    async def consume(self, callback: SingleCallback) -> None:
        """Consume single message"""
        if self.callback is not None:
            raise ValueError("Consumer is running! Can't consuming!")

        self.callback = callback

        self._is_start = True

        await self._prepare_for_consume()

        tasks = [
            self._do_consume(is_batch=False),
            self._detect_changes(),
            self._wait_for_rebalance(),
        ]
        await asyncio.gather(*tasks)
        self.logger.info(f"Stopped!")
        self._stopped_event.set()

    async def _consume(self):
        try:
            pending_messages = await self.get_pending_messages(count=1)
            if len(pending_messages) > 0:
                for msg_id, pending_message in pending_messages:
                    try:
                        message = Message.from_dict(pending_message)
                        await self.callback(message)
                    except Exception as e:
                        self.logger.error(f"Error processing pending message {msg_id}", exc_info=e)
                    finally:
                        if pending_message.stream:
                            await self.redis.xack(pending_message.stream, self.group_name, msg_id)
            else:
                streams = {self._topic_keys.partition_keys[p].stream_key: ">" for p in self.partitions}
                messages = await self.redis.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams=streams,
                    count=1,
                    block=1000
                )
                if not messages:
                    await asyncio.sleep(0.1)
                    return

                stream, [(msg_id, msg)] = messages[0]
                message = Message.from_dict(msg)
                try:
                    await self.callback(message)
                except Exception as e:
                    self.logger.error(f"Error processing message: {msg}", exc_info=e)
                finally:
                    await self.redis.xack(stream, self.group_name, msg_id)
        except Exception as e:
            self.logger.error(f"Error consuming message: {e}", exc_info=e)
        finally:
            pass

    async def consume_batch(self, callback: BatchCallback, batch_size: int = None) -> None:
        """Consume messages by batch"""
        if self.callback is not None:
            raise ValueError("Consumer is running! Can't consuming!")

        self.callback = callback
        self.batch_size = batch_size or self.batch_size
        self._is_start = True

        await self._prepare_for_consume()

        tasks = [
            self._do_consume(is_batch=True),
            self._detect_changes(),
            self._wait_for_rebalance(),
        ]
        await asyncio.gather(*tasks)
        self.logger.info(f"Stopped!")
        self._stopped_event.set()

    async def _consume_batch(self):
        try:
            pending_messages = await self.get_pending_messages(count=self.batch_size)
            if len(pending_messages) > 0:
                try:
                    messages = []
                    for msg_id, pending_message in pending_messages:
                        messages.append(Message.from_dict(pending_message))

                    await self.callback(messages)
                except Exception as e:
                    self.logger.error(f"Error processing batch messages", exc_info=e)
                finally:
                    for msg_id, pending_message in pending_messages:
                        if pending_message.stream:
                            await self.redis.xack(pending_message.stream, self.group_name, msg_id)
            else:
                streams = {self._topic_keys.partition_keys[p].stream_key: ">" for p in self.partitions}
                result = await self.redis.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams=streams,
                    count=self.batch_size,
                    block=1000
                )
                if not result:
                    await asyncio.sleep(0.1)
                    return

                all_messages = []
                for stream, messages in result:
                    for (msg_id, msg) in messages:
                        message = Message.from_dict(msg)
                        message.stream = stream
                        all_messages.append(message)

                try:
                    await self.callback(all_messages)
                except Exception as e:
                    self.logger.error(f"Error processing batch messages", exc_info=e)
                finally:
                    for stream, messages in result:
                        for (msg_id, msg) in messages:
                            await self.redis.xack(stream, self.group_name, msg_id)
        except Exception as e:
            self.logger.error(f"Error consuming job: {e}", exc_info=e)
        finally:
            pass

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
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def _create_consumer_group_for_topic(self):
        if self._topic_keys.consumer_group_keys is None:
            self._topic_keys.set_consumer_group(self.group_name)

        await self.redis.sadd(self._topic_keys.consumer_group_key, self.group_name)

    async def _do_heartbeat(self):
        if self.redis is None:
            raise RuntimeError("Redis is not connected! Please run connect() function first!")

        consumers_key = f"{self._topic_keys.consumer_group_keys.consumer_key}:{self.consumer_name}"
        await self.redis.set(name=consumers_key, value=self.serializer.dumps(self._is_ready),
                             ex=int(self.heartbeat_ttl))

    async def _detect_changes(self):
        # Check for rebalance signal via pubsub
        while self._is_start:
            # detect rebalance via pub/sub
            if self.pubsub:
                message = await self.pubsub.get_message(timeout=0.01)
                if message and message["type"] == "message" and message["data"] == "rebalance":
                    self.logger.info("New consumer joined!")
                    self._rebalance_event.set()

            # detect rebalance via consumer count change
            consumers = await self.get_consumers()
            if len(consumers) != self._consumer_count:
                if self._consumer_count != -1:
                    self.logger.info(f"Consumer count change {self._consumer_count} -> {len(consumers)}")
                    self._rebalance_event.set()
                self._consumer_count = len(consumers)

            # detect rebalance via partition count change
            partition_count = await self.get_num_partitions()
            if partition_count != self._partition_count:
                if self._partition_count != -1:
                    self.logger.info(f"Partition count change {self._partition_count} -> {partition_count}")
                    self._rebalance_event.set()
                self._partition_count = partition_count

            await asyncio.sleep(0.1)

    async def _wait_for_rebalance(self):
        while self._is_start:
            await self._rebalance_event.wait()
            await self._do_rebalance()
            self._rebalance_event.clear()

    async def _do_rebalance(self):
        self.logger.info(f"Pausing for rebalance")
        self._is_consuming = False
        await self.remove_ready()
        self.logger.info("Wait for stop consuming...")
        async with self._lock:
            await self.update_partitions()
            await self.wait_for_all_ready()
            self.logger.info(f"Starting consumption")
            await asyncio.sleep(0.5)
            self._is_consuming = True

    async def stop(self):
        await self.close()
        self._stopped_event.clear()
        self._is_start = False
        self.logger.info(f"Stopping...")
        await self._stopped_event.wait()

    async def _prepare_for_consume(self):
        await self.connect()

        if self.redis is None:
            raise RuntimeError("Redis is not connected! Please run connect() function first!")

        # Register consumer
        await self.register_consumer()
        self.logger.info(f"Registered in group {self.group_name}")

        self.logger.info(f"Preparing for consuming...")
        await self.signal_rebalance()

    async def get_pending_messages(self, count: int):
        return []

    async def _do_consume(self, is_batch: bool):
        """Consume jobs from assigned partitions."""
        if self.redis is None:
            raise RuntimeError("Redis is not connected! Please run connect() function first!")

        last_is_consuming = self._is_consuming
        while self._is_start:
            if not self._is_consuming:
                await asyncio.sleep(0.1)
                continue
            else:
                if not last_is_consuming:
                    self.logger.info("Change from paused to resumed. Starting...")
                    await asyncio.sleep(3)
                    if not self._is_consuming:
                        last_is_consuming = self._is_consuming
                        continue

                last_is_consuming = self._is_consuming

            if is_batch:
                await self._consume_batch()
            else:
                await self._consume()

        self.logger.info("Stopped consuming!")
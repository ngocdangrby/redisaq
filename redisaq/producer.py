"""
Producer module for redisaq

Implements the Producer class for enqueuing messages to Redis Streams.
"""

import asyncio
import hashlib
import logging
import time

import orjson
import uuid
from typing import Optional, List, Dict

import aioredis

from redisaq.common import TopicOperator
from redisaq.constants import APPLICATION_PREFIX
from redisaq.keys import TopicKeys
from redisaq.models import Message
from redisaq.utils import APPLICATION_METADATA_TOPICS


class Producer(TopicOperator):
    """Producer for enqueuing messages to Redis Streams."""
    def __init__(
        self,
        topic: str,
        redis_url: str = "redis://localhost:6379/0",
        init_partitions: int = 1,
        maxlen: Optional[int] = None,
        approximate: bool = True,
        serializer=None,
        debug=False,
        logger=None,
    ):
        self.topic = topic
        self.redis_url = redis_url
        self.maxlen = maxlen
        self.approximate = approximate
        self.redis: Optional[aioredis.Redis] = None
        self._init_partitions = init_partitions
        self._topic_keys = TopicKeys(self.topic)
        self._last_partition_enqueue = -1
        self.serializer = serializer or orjson
        self.logger = logger or logging.getLogger(f"{APPLICATION_PREFIX}.producer")
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)

    async def request_partition_increase(self, num_partitions: int) -> None:
        """Request an increase in the number of partitions."""
        if self.redis is None:
            raise RuntimeError("Redis not connected. Please run connect() function first")

        try:
            current = await self.get_num_partitions()
            if num_partitions > current:
                await self.redis.set(self._topic_keys.partition_key, num_partitions)
                self.logger.info(f"Set partitions for {self.topic} to {num_partitions}")
        except Exception as e:
            self.logger.error(f"Error increasing partitions: {e}", exc_info=e)
            raise

    async def connect(self) -> None:
        """Connect to Redis."""
        if self.redis is None:
            self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)

        if self.redis is None:
            raise ValueError("Redis is not connected!")

        await self._create_topic_if_not_exist()
        self.logger.info(f"Connected to topic {self.topic}!")
        await self._create_partitions()

    async def close(self) -> None:
        """Close Redis connection."""
        if self.redis:
            await self.redis.close()
            self.redis = None

    async def _enqueue(
        self,
        message: Message,
        maxlen: Optional[int] = None,
        approximate: Optional[bool] = None
    ) -> str:
        """Enqueue a single message to the specified partition."""
        if self.redis is None:
            raise RuntimeError("Redis not connected. Please run connect() function first")

        try:
            message = await self._process_message(message)
            final_maxlen = maxlen if maxlen is not None else self.maxlen
            final_approximate = approximate if approximate is not None else self.approximate

            msg_dict = message.to_dict()
            msg_dict['payload'] = self.serializer.dumps(msg_dict['payload'])
            await self.redis.xadd(
                name=self._topic_keys.partition_keys[message.partition].stream_key,
                fields=msg_dict,
                maxlen=final_maxlen,
                approximate=final_approximate
            )
            self._last_partition_enqueue = message.partition
            self.logger.debug(
                f"Enqueued message {message.msg_id} to {self._topic_keys.partition_keys[message.partition].stream_key}")
            return message.msg_id
        except Exception as e:
            self.logger.error(f"Error enqueuing message: {e}", exc_info=e)
            raise

    async def enqueue(
        self,
        payload: Dict,
        timeout: float = 0,
        partition_key: str = "",
        maxlen: Optional[int] = None,
        approximate: Optional[bool] = None
    ) -> str:
        """Enqueue a single message to the specified partition."""
        return await self._enqueue(
            maxlen=maxlen,
            approximate=approximate,
            message=Message(topic=self.topic, payload=payload, timeout=timeout, partition_key=partition_key)
        )

    async def _batch_enqueue(
        self,
        messages: List[Message],
        maxlen: Optional[int] = None,
        approximate: Optional[bool] = None
    ) -> List[str]:
        """Enqueue multiple messages to the topic."""
        if self.redis is None:
            raise RuntimeError("Redis not connected. Please run connect() function first")

        try:
            job_ids = []
            partitions = []
            final_maxlen = maxlen if maxlen is not None else self.maxlen
            final_approximate = approximate if approximate is not None else self.approximate
            async with self.redis.pipeline() as pipe:
                for message in messages:
                    message = await self._process_message(message)

                    msg_dict = message.to_dict()
                    msg_dict['payload'] = self.serializer.dumps(msg_dict['payload'])
                    # noinspection PyAsyncCall
                    pipe.xadd(
                        name=self._topic_keys.partition_keys[message.partition].stream_key,
                        fields=msg_dict,
                        maxlen=final_maxlen,
                        approximate=final_approximate
                    )
                    self._last_partition_enqueue = message.partition

                    job_ids.append(message.msg_id)
                    partitions.append(message.partition)

                await pipe.execute()

            # Log streams based on saved partitions
            for job_id, partition in zip(job_ids, partitions):
                self.logger.debug(f"Enqueued message {job_id} to {self._topic_keys.partition_keys[partition].stream_key}")

            return job_ids
        except Exception as e:
            self.logger.error(f"Error batch enqueuing messages: {e}", exc_info=e)
            raise

    async def batch_enqueue(
        self,
        payloads: List[Dict],
        timeout: float = 0,
        partition_key: str = "",
        maxlen: Optional[int] = None,
        approximate: Optional[bool] = None
    ) -> List[str]:
        messages = [Message(topic=self.topic, payload=payload, timeout=timeout, partition_key=partition_key) for payload
                    in payloads]
        return await self._batch_enqueue(
            approximate=approximate,
            maxlen=maxlen,
            messages=messages,
        )

    async def _create_topic_if_not_exist(self):
        added = await self.redis.sadd(APPLICATION_METADATA_TOPICS, self.topic)
        if added:
            self.logger.info(f"New topic {self.topic} is created!")

    async def _create_partitions(self):
        partitions = await self.get_num_partitions()
        if self._init_partitions > partitions:
            await self.request_partition_increase(self._init_partitions)

    async def _process_message(self, message: Message):
        job_id = str(uuid.uuid4())
        message.msg_id = job_id
        # Detect partition
        msg_partition = message.get_partition()
        if msg_partition is None:
            if message.partition_key:
                msg_partition = int(hashlib.md5(str(message.payload[message.partition_key]).encode()).hexdigest(),
                                    16) % await self.get_num_partitions()
            else:
                msg_partition = (self._last_partition_enqueue + 1) % await self.get_num_partitions()

        if not self._topic_keys.has_partition(msg_partition):
            self._topic_keys.add_partition(msg_partition)

        message.partition = msg_partition
        message.enqueued_at = int(time.time())

        return message

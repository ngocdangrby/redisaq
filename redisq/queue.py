import asyncio
import json
import logging
import uuid
from typing import List, Optional, cast
import aioredis
from aioredis import Redis

from .models import Job

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Queue:
    def __init__(self, redis_url: str = "redis://localhost:6379", prefix: str = "jobs"):
        self.redis_url = redis_url
        self.prefix = prefix
        self.redis: Optional[Redis] = None
        self.dead_letter_stream = f"{self.prefix}:dead_letter"

    async def connect(self):
        if self.redis is None:
            self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
        return self.redis

    async def close(self):
        if self.redis:
            await self.redis.close()
            self.redis = None

    async def get_num_partitions(self, topic: str) -> int:
        await self.connect()
        num_partitions = await cast(Redis, self.redis).get(f"{self.prefix}:partitions:{topic}")
        return int(num_partitions) if num_partitions else 0

    async def set_num_partitions(self, topic: str, num_partitions: int):
        await self.connect()
        await cast(Redis, self.redis).set(f"{self.prefix}:partitions:{topic}", num_partitions)
        logger.debug(f"Set {topic} partitions to {num_partitions}")

    async def enqueue(self, topic: str, job: Job) -> str:
        await self.connect()
        num_partitions = await self.get_num_partitions(topic)
        if num_partitions == 0:
            await self.set_num_partitions(topic, 1)
            num_partitions = 1
        if job.id is None:
            job.id = str(uuid.uuid4())
        partition = hash(job.id) % num_partitions
        stream = f"{self.prefix}:{topic}:{partition}"
        try:
            message_id = await cast(Redis, self.redis).xadd(stream, job.to_dict())
            logger.debug(f"Enqueued job {job.id} to {stream}")
            return job.id
        except Exception as e:
            logger.error(f"Error enqueueing job {job.id}: {e}")
            raise

    async def batch_enqueue(self, topic: str, jobs: List[Job]) -> List[str]:
        await self.connect()
        num_partitions = await self.get_num_partitions(topic)
        if num_partitions == 0:
            await self.set_num_partitions(topic, 1)
            num_partitions = 1
        job_ids = []
        for job in jobs:
            if job.id is None:
                job.id = str(uuid.uuid4())
            partition = hash(job.id) % num_partitions
            stream = f"{self.prefix}:{topic}:{partition}"
            try:
                message_id = await cast(Redis, self.redis).xadd(stream, job.to_dict())
                job_ids.append(job.id)
                logger.debug(f"Enqueued job {job.id} to {stream}")
            except Exception as e:
                logger.error(f"Error batch enqueueing job {job.id}: {e}")
                raise
        return job_ids
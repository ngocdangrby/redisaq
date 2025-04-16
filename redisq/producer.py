import asyncio
import json
import uuid
from typing import Any, Optional, List
import aioredis
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Producer:
    def __init__(
        self,
        topic: str,
        redis_url: str = "redis://localhost:6379",
        prefix: str = "redisq",
        maxlen: Optional[int] = None,
        approximate: bool = True
    ):
        self.topic = topic
        self.redis_url = redis_url
        self.prefix = prefix
        self.maxlen = maxlen
        self.approximate = approximate
        self.redis = None

    async def connect(self):
        if self.redis is None:
            self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
        return self.redis

    async def close(self):
        if self.redis:
            await self.redis.close()
            self.redis = None

    async def enqueue(
        self,
        payload: Any,
        maxlen: Optional[int] = None,
        approximate: Optional[bool] = None
    ) -> str:
        await self.connect()
        try:
            job_id = str(uuid.uuid4())
            partition = await self.redis.incr(f"{self.prefix}:stats:{self.topic}:jobs") % await self.get_num_partitions()
            stream = f"{self.prefix}:{self.topic}:{partition}"
            final_maxlen = maxlen if maxlen is not None else self.maxlen
            final_approximate = approximate if approximate is not None else self.approximate
            await self.redis.xadd(
                stream,
                {
                    "id": job_id,
                    "topic": self.topic,
                    "payload": json.dumps(payload),
                    "created_at": json.dumps({"sec": int(asyncio.get_event_loop().time()), "usec": 0})
                },
                maxlen=final_maxlen,
                approximate=final_approximate
            )
            logger.debug(f"Enqueued job {job_id} to {stream}")
            return job_id
        except Exception as e:
            logger.error(f"Error enqueuing job: {e}", exc_info=e)
            raise

    async def batch_enqueue(
        self,
        payloads: List[Any],
        maxlen: Optional[int] = None,
        approximate: Optional[bool] = None
    ) -> List[str]:
        await self.connect()
        try:
            job_ids = []
            final_maxlen = maxlen if maxlen is not None else self.maxlen
            final_approximate = approximate if approximate is not None else self.approximate
            async with self.redis.pipeline() as pipe:
                for payload in payloads:
                    job_id = str(uuid.uuid4())
                    partition = await self.redis.incr(f"{self.prefix}:stats:{self.topic}:jobs") % await self.get_num_partitions()
                    stream = f"{self.prefix}:{self.topic}:{partition}"
                    pipe.xadd(
                        stream,
                        {
                            "id": job_id,
                            "topic": self.topic,
                            "payload": json.dumps(payload),
                            "created_at": json.dumps({"sec": int(asyncio.get_event_loop().time()), "usec": 0})
                        },
                        maxlen=final_maxlen,
                        approximate=final_approximate
                    )
                    job_ids.append(job_id)
                await pipe.execute()
            for job_id, stream in zip(job_ids, [f"{self.prefix}:{self.topic}:{i % await self.get_num_partitions()}" for i in range(len(job_ids))]):
                logger.debug(f"Enqueued job {job_id} to {stream}")
            return job_ids
        except Exception as e:
            logger.error(f"Error batch enqueuing jobs: {e}", exc_info=e)
            raise

    async def get_num_partitions(self) -> int:
        await self.connect()
        try:
            num_partitions = await self.redis.get(f"{self.prefix}:partitions:{self.topic}")
            if num_partitions is None:
                await self.redis.set(f"{self.prefix}:partitions:{self.topic}", 1)
                return 1
            return int(num_partitions)
        except Exception as e:
            logger.error(f"Error getting partitions: {e}", exc_info=e)
            raise

    async def request_partition_increase(self, num_partitions: int):
        await self.connect()
        try:
            current = await self.get_num_partitions()
            if num_partitions > current:
                await self.redis.set(f"{self.prefix}:partitions:{self.topic}", num_partitions)
                logger.info(f"Set partitions for {self.topic} to {num_partitions}")
        except Exception as e:
            logger.error(f"Error increasing partitions: {e}", exc_info=e)
            raise
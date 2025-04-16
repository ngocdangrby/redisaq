import asyncio
import json
import logging
from typing import Callable, List, Optional, Awaitable
import aioredis
from redisq.models import Job

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Consumer:
    def __init__(
        self,
        topic: str,
        group: str,
        consumer_id: str,
        process_job: Callable[[Job], Awaitable[None]],
        redis_url: str = "redis://localhost:6379",
        prefix: str = "redisq",
        heartbeat_ttl: int = 10,
        heartbeat_interval: int = 5
    ):
        self.topic = topic
        self.group = group
        self.consumer_id = consumer_id
        self.process_job = process_job
        self.redis_url = redis_url
        self.prefix = prefix
        self.heartbeat_ttl = heartbeat_ttl
        self.heartbeat_interval = heartbeat_interval
        self.redis = None
        self.running = False
        self.partitions: List[int] = []
        self.paused = False

    async def connect(self):
        if self.redis is None:
            self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
        return self.redis

    async def close(self):
        if self.redis:
            await self.redis.close()
            self.redis = None

    async def start(self):
        await self.connect()
        self.running = True
        try:
            await self.register()
            await asyncio.gather(
                self.heartbeat(),
                self.consume(),
                self.monitor_workers()
            )
        finally:
            await self.stop()

    async def stop(self):
        self.running = False
        await self.close()

    async def register(self):
        await self.connect()
        try:
            num_partitions = await self.get_num_partitions()
            for partition in range(num_partitions):
                stream = f"{self.prefix}:{self.topic}:{partition}"
                try:
                    await self.redis.xgroup_create(stream, self.group, id="0", mkstream=True)
                except Exception as e:
                    if "BUSYGROUP" not in str(e):
                        raise
            logger.info(f"Consumer {self.consumer_id} registered for topic {self.topic}, group {self.group}")
            await self.assign_partitions()
        except Exception as e:
            logger.error(f"Error registering consumer: {e}", exc_info=e)
            raise

    async def heartbeat(self):
        await self.connect()
        while self.running:
            try:
                await self.redis.setex(
                    f"{self.prefix}:worker:{self.topic}:{self.group}:{self.consumer_id}",
                    self.heartbeat_ttl,
                    "1"
                )
                await asyncio.sleep(self.heartbeat_interval)
            except Exception as e:
                logger.error(f"Error in heartbeat: {e}", exc_info=e)

    async def assign_partitions(self):
        await self.connect()
        try:
            workers = await self.redis.keys(f"{self.prefix}:worker:{self.topic}:{self.group}:*")
            num_partitions = await self.get_num_partitions()
            worker_count = len(workers)
            partitions_per_worker = max(1, num_partitions // max(1, worker_count))
            worker_index = sorted([w.split(":")[-1] for w in workers]).index(self.consumer_id)
            start = worker_index * partitions_per_worker
            end = start + partitions_per_worker if worker_index < worker_count - 1 else num_partitions
            self.partitions = list(range(start, min(end, num_partitions)))
            logger.info(f"Consumer {self.consumer_id} assigned partitions {self.partitions}")
        except Exception as e:
            logger.error(f"Error assigning partitions: {e}", exc_info=e)
            raise

    async def get_num_partitions(self) -> int:
        await self.connect()
        try:
            num_partitions = await self.redis.get(f"{self.prefix}:partitions:{self.topic}")
            if num_partitions is None:
                return 1
            return int(num_partitions)
        except Exception as e:
            logger.error(f"Error getting partitions: {e}", exc_info=e)
            raise

    async def consume(self):
        await self.connect()
        while self.running:
            try:
                if self.paused:
                    await asyncio.sleep(1)
                    continue
                for partition in self.partitions:
                    stream = f"{self.prefix}:{self.topic}:{partition}"
                    jobs = await self.redis.xreadgroup(
                        self.group,
                        self.consumer_id,
                        {stream: ">"},
                        count=1,
                        block=1000
                    )
                    if jobs:
                        for stream, messages in jobs:
                            for msg_id, msg in messages:
                                job = Job(
                                    id=msg["id"],
                                    topic=msg["topic"],
                                    payload=json.loads(msg["payload"]),
                                    created_at=json.loads(msg["created_at"])
                                )
                                try:
                                    await self.process_job(job)
                                    await self.redis.xack(stream, self.group, msg_id)
                                except Exception as e:
                                    logger.error(f"Error processing job {job.id}: {e}", exc_info=e)
                                    await self.redis.xadd(
                                        f"{self.prefix}:dead_letter",
                                        {"job_id": job.id, "error": str(e), "payload": json.dumps(job.payload)}
                                    )
                                    await self.redis.xack(stream, self.group, msg_id)
            except Exception as e:
                logger.error(f"Error in consume: {e}", exc_info=e)
                await asyncio.sleep(1)

    async def monitor_workers(self):
        await self.connect()
        while self.running:
            try:
                current_workers = await self.redis.keys(f"{self.prefix}:worker:{self.topic}:{self.group}:*")
                if not self.partitions or len(current_workers) != await self.get_worker_count():
                    await self.assign_partitions()
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error monitoring workers: {e}", exc_info=e)

    async def get_worker_count(self) -> int:
        await self.connect()
        try:
            workers = await self.redis.keys(f"{self.prefix}:worker:{self.topic}:{self.group}:*")
            return len(workers)
        except Exception as e:
            logger.error(f"Error getting worker count: {e}", exc_info=e)
            raise
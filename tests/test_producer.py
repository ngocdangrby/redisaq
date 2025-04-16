import asyncio
import pytest
from redisq import Producer

@pytest.mark.asyncio
async def test_enqueue():
    producer = Producer(redis_url="redis://localhost:6379")
    await producer.enqueue("test_topic", {"data": "test"})
    await producer.close()

@pytest.mark.asyncio
async def test_batch_enqueue():
    producer = Producer(redis_url="redis://localhost:6379")
    payloads = [{"data": "test1"}, {"data": "test2"}]
    job_ids = await producer.batch_enqueue("test_topic", payloads)
    assert len(job_ids) == 2
    assert all(isinstance(job_id, str) for job_id in job_ids)
    await producer.close()
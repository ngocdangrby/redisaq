import asyncio
import pytest
from redisq import Consumer, Job

async def process_job(job: Job):
    pass

@pytest.mark.asyncio
async def test_consumer_init():
    consumer = Consumer("test_topic", process_job, "redis://localhost:6379", "test_consumer")
    assert consumer.topic == "test_topic"
    assert consumer.consumer_id == "test_consumer"

@pytest.mark.asyncio
async def test_register():
    consumer = Consumer("test_topic", process_job, "redis://localhost:6379", "test_consumer")
    await consumer.register()
    await consumer.close()
"""
Tests for Consumer class
"""
import asyncio
import json

import pytest
import pytest_asyncio

from redisaq import Job
from redisaq.producer import Producer
from redisaq.consumer import Consumer


@pytest_asyncio.fixture
async def producer(redis_client):
    """Provide a Producer instance for tests."""
    producer = Producer(
        topic="test_topic",
        redis_url="redis://localhost:6379",
        prefix="redisaq_test"  # Use redisaq_test for testing
    )
    await producer.connect()
    yield producer
    await producer.close()


@pytest_asyncio.fixture
async def consumer(redis_client):
    async def processing_job(job: Job):
        print(f"Processed message {json.dumps(job.to_dict())}")

    """Provide a Consumer instance for tests."""
    consumer = Consumer(
        group="test_group",
        topic="test_topic",
        consumer_id="consumer_test_1",
        redis_url="redis://localhost:6379",
        process_job=processing_job,
        prefix="redisaq_test"  # Use redisaq_test for testing
    )
    await consumer.connect()
    yield consumer
    await consumer.close()


@pytest.mark.asyncio
async def test_consume_job(producer: Producer, consumer: Consumer, redis_client):
    """Test consuming a message from the stream."""
    stream = f"{consumer.prefix}:test_topic:0"
    before_jobs = await redis_client.xrange(stream)
    assert len(before_jobs) == 0, "Init message must be empty"
    await producer.enqueue({"data": "test"})

    await consumer.start()
    await asyncio.sleep(1)
    after_jobs = await redis_client.xrange(stream)
    assert len(after_jobs) == 0, "After consume, message count must be empty"


@pytest.mark.asyncio
async def test_heartbeat(producer, consumer, redis_client):
    """Test consumer heartbeat."""
    await producer.enqueue({"data": "test"})
    await consumer.consume()
    heartbeat_key = f"{consumer.prefix}:heartbeat:{consumer.topic}"
    heartbeat = await redis_client.get(heartbeat_key)
    assert heartbeat is not None


@pytest.mark.asyncio
async def test_dead_letter_queue(producer, consumer, redis_client):
    """Test dead letter queue functionality."""
    await producer.enqueue({"data": "test"})
    await consumer.consume()
    dlq_stream = f"{consumer.prefix}:dlq:{consumer.topic}"
    dlq_jobs = await redis_client.xrange(dlq_stream)
    assert len(dlq_jobs) == 1


@pytest.mark.asyncio
async def test_partition_assignment(producer, consumer, redis_client):
    """Test partition assignment for consumer."""
    await producer.request_partition_increase(2)
    await producer.batch_enqueue([{"data": "job1"}, {"data": "job2"}])
    stream0 = f"{consumer.prefix}:test_topic:0"
    stream1 = f"{consumer.prefix}:test_topic:1"
    jobs0 = await redis_client.xrange(stream0)
    jobs1 = await redis_client.xrange(stream1)
    assert len(jobs0) + len(jobs1) == 2

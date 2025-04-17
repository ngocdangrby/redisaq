from typing import AsyncIterator
from unittest.mock import AsyncMock

import aioredis
import fakeredis.aioredis
import orjson
import pytest
import pytest_asyncio
from redis import asyncio as redis

from redisaq.errors import PartitionKeyError
from redisaq.keys import TopicKeys
from redisaq.models import Message
from redisaq.producer import Producer
from redisaq.utils import APPLICATION_METADATA_TOPICS


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[redis.Redis]:
    async with fakeredis.FakeAsyncRedis(decode_responses=True) as client:
        yield client
        await client.close()


@pytest.fixture
def producer(redis_client, monkeypatch):
    # Patch aioredis.from_url to return fake_redis
    monkeypatch.setattr("aioredis.from_url", AsyncMock(return_value=redis_client))
    return Producer(topic="test_topic", redis_url="redis://localhost:6379/0", init_partitions=2)


@pytest.mark.asyncio
async def test_producer_initialization(producer):
    assert producer.topic == "test_topic"
    assert producer.redis_url == "redis://localhost:6379/0"
    assert producer._init_partitions == 2
    assert producer.maxlen is None
    assert producer.approximate is True
    assert isinstance(producer._topic_keys, TopicKeys)
    assert producer._last_partition_enqueue == -1
    assert producer.serializer == orjson


@pytest.mark.asyncio
async def test_connect_success(producer, redis_client):
    await producer.connect()
    assert producer.redis == redis_client
    # Check if topic was added to metadata
    topics = await redis_client.smembers(APPLICATION_METADATA_TOPICS)
    assert "test_topic" in topics
    # Check partition initialization
    partitions = await redis_client.get(producer._topic_keys.partition_key)
    assert int(partitions) == 2


@pytest.mark.asyncio
async def test_connect_already_connected(producer, redis_client):
    await producer.connect()
    redis_before = producer.redis
    await producer.connect()
    assert producer.redis == redis_before  # Should not reconnect


@pytest.mark.asyncio
async def test_close(producer, redis_client):
    await producer.connect()
    await producer.close()
    assert producer.redis is None


@pytest.mark.asyncio
async def test_get_num_partitions(producer, redis_client):
    await producer.connect()
    await redis_client.set(producer._topic_keys.partition_key, "3")
    num_partitions = await producer.get_num_partitions()
    assert num_partitions == 3


@pytest.mark.asyncio
async def test_get_num_partitions_no_redis(producer):
    with pytest.raises(RuntimeError, match="Redis not connected"):
        await producer.get_num_partitions()


@pytest.mark.asyncio
async def test_request_partition_increase(producer, redis_client):
    await producer.connect()
    await redis_client.set(producer._topic_keys.partition_key, "1")
    await producer.request_partition_increase(3)
    partitions = await redis_client.get(producer._topic_keys.partition_key)
    assert int(partitions) == 3


@pytest.mark.asyncio
async def test_enqueue_message(producer, redis_client):
    await producer.connect()
    await redis_client.set(producer._topic_keys.partition_key, "2")
    payload = {"data": "test"}

    # wrong partition key
    with pytest.raises(PartitionKeyError):
        await producer.enqueue(payload, timeout=10, partition_key="key")

    msg_id = await producer.enqueue(payload, timeout=10, partition_key="data")
    assert isinstance(msg_id, str)

    # Check if message was enqueued in Redis stream
    stream_key = producer._topic_keys.partition_keys[producer._last_partition_enqueue].stream_key
    messages = await redis_client.xrange(stream_key)
    assert len(messages) == 1
    assert messages[0][1]["msg_id"] == msg_id
    assert producer._last_partition_enqueue in [0, 1]


@pytest.mark.asyncio
async def test_enqueue_no_redis(producer):
    with pytest.raises(RuntimeError, match="Redis not connected"):
        await producer.enqueue({"data": "test"})


@pytest.mark.asyncio
async def test_batch_enqueue(producer, redis_client):
    await producer.connect()
    await redis_client.set(producer._topic_keys.partition_key, "2")
    payloads = [{"data": "test1"}, {"data": "test2"}]
    job_ids = await producer.batch_enqueue(payloads, timeout=10, partition_key="data")
    assert len(job_ids) == 2
    assert all(isinstance(job_id, str) for job_id in job_ids)
    # Check streams for enqueued messages
    for partition in set(producer._topic_keys.partition_keys.keys()):
        stream_key = producer._topic_keys.partition_keys[partition].stream_key
        messages = await redis_client.xrange(stream_key)
        assert any(msg[1]["msg_id"] in job_ids for msg in messages)


@pytest.mark.asyncio
async def test_process_message(producer, redis_client):
    await producer.connect()
    await redis_client.set(producer._topic_keys.partition_key, "2")
    message = Message(topic="test_topic", payload={"key": "value"}, partition_key="key")
    processed_message = await producer._process_message(message)
    assert isinstance(processed_message.msg_id, str)
    assert processed_message.partition in [0, 1]
    assert isinstance(processed_message.enqueued_at, int)


@pytest.mark.asyncio
async def test_create_topic_if_not_exist(producer, redis_client):
    await producer.connect()
    topics = await redis_client.smembers(APPLICATION_METADATA_TOPICS)
    assert "test_topic" in topics


@pytest.mark.asyncio
async def test_create_partitions(producer, redis_client):
    await producer.connect()
    await redis_client.set(producer._topic_keys.partition_key, "1")
    await producer._create_partitions()
    partitions = await redis_client.get(producer._topic_keys.partition_key)
    assert int(partitions) == 2

"""
Test cases for topic reconsumption functionality in redisaq.

This module tests the ability to reconsume messages from a topic
using different consumer groups and from different positions in the stream.
"""

import asyncio
from typing import Any, Dict, List

import pytest

from redisaq import Consumer, Message, Producer

# Test configurations
TEST_TOPIC = "test_reconsume_topic"
TEST_GROUP_1 = "test_group_1"
TEST_GROUP_2 = "test_group_2"
REDIS_URL = "redis://localhost:6379/0"


@pytest.fixture
async def producer(mock_aioredis_from_url):
    """Create a producer instance for testing."""
    prod = Producer(
        topic=TEST_TOPIC, redis_url=REDIS_URL, maxlen=1000, init_partitions=8
    )
    await prod.connect()
    yield prod
    await prod.close()


@pytest.fixture
async def consumer_group1(mock_aioredis_from_url):
    """Create first consumer group instance."""
    cons = Consumer(
        topic=TEST_TOPIC,
        group_name=TEST_GROUP_1,
        consumer_name="consumer1",
        redis_url=REDIS_URL,
        batch_size=5,
    )
    await cons.connect()
    yield cons
    await cons.close()


@pytest.fixture
async def consumer_group2(mock_aioredis_from_url):
    """Create second consumer group instance for reconsumption."""
    cons = Consumer(
        topic=TEST_TOPIC,
        group_name=TEST_GROUP_2,
        consumer_name="consumer2",
        redis_url=REDIS_URL,
        batch_size=5,
    )
    await cons.connect()
    yield cons
    await cons.close()


async def process_messages(messages: List[Message]) -> List[Dict[str, Any]]:
    """Process messages and return their payloads."""
    return [msg.payload for msg in messages]


@pytest.mark.asyncio
async def test_multiple_group_consumption(
    producer: Producer, consumer_group1: Consumer, consumer_group2: Consumer
):
    """
    Test that multiple consumer groups can consume the same messages
    from different positions in the stream.
    """
    # 1. Produce test messages
    test_messages = [{"id": i, "data": f"test_message_{i}"} for i in range(10)]
    msg_ids = await producer.batch_enqueue(test_messages)
    assert len(msg_ids) == 10, "Failed to produce all test messages"

    # 2. First consumer group processes messages
    processed_group1 = []

    async def process_group1(messages: List[Message]):
        nonlocal processed_group1
        processed_group1.extend(await process_messages(messages))

    await consumer_group1.connect()
    task = asyncio.create_task(
        consumer_group1.consume_batch(process_group1, batch_size=10)
    )
    await asyncio.sleep(2)
    assert consumer_group1.partitions == list(range(8))
    await asyncio.sleep(10)
    consumer_group1._is_start = False
    await task

    # Process with first consumer group

    assert len(processed_group1) == 10, "Group 1 should process all messages"

    # 3. Second consumer group processes same messages
    processed_group2 = []

    async def process_group2(messages: List[Message]):
        nonlocal processed_group2
        processed_group2.extend(await process_messages(messages))

    # Process with second consumer group
    await consumer_group2.connect()
    task = asyncio.create_task(
        consumer_group2.consume_batch(process_group2, batch_size=10)
    )
    await asyncio.sleep(2)
    assert consumer_group2.partitions == list(range(8))
    await asyncio.sleep(10)
    consumer_group2._is_start = False
    await task

    assert len(processed_group2) == 10, "Group 2 should process all messages"

    # 4. Verify both groups received same messages in same order
    assert (
        processed_group1 == processed_group2
    ), "Both groups should receive identical messages"

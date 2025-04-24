import asyncio

import pytest
import pytest_asyncio

from redisaq import Producer
from redisaq.consumer import Consumer


# Fixture to create a Consumer instance with fakeredis
@pytest_asyncio.fixture
async def consumer(redis_client):
    topic = "test_topic"
    redis_url = "redis://localhost:6379/1"
    group_name = "test_group"
    consumer_name = "test_consumer"

    consumer = Consumer(
        topic=topic,
        redis_url=redis_url,
        group_name=group_name,
        consumer_name=consumer_name,
        batch_size=2,
        heartbeat_interval=0.1,
        heartbeat_ttl=1.0,
        debug=True,
    )
    # Assign redis_client to consumer
    consumer.redis = redis_client
    consumer.pubsub = redis_client.pubsub()
    await consumer.pubsub.subscribe(consumer._topic_keys.rebalance_channel)
    yield consumer
    await consumer.close()


@pytest.mark.asyncio
async def test_connect(consumer, redis_client, mock_aioredis_from_url):
    """Test the connect method."""
    consumer.redis = None
    consumer.pubsub = None
    await consumer.connect()
    mock_aioredis_from_url.assert_called_once_with(
        consumer.redis_url, decode_responses=True
    )
    assert consumer.redis is not None
    assert consumer.pubsub is not None
    assert id(consumer.redis) == id(redis_client)
    # Verify the group_name was added to the Redis set
    members = await consumer.redis.smembers(consumer._topic_keys.consumer_group_key)
    assert consumer.group_name in members


@pytest.mark.asyncio
async def test_close(consumer):
    """Test the close method."""
    await consumer.close()
    assert consumer.pubsub is None
    assert consumer.redis is None


@pytest.mark.asyncio
async def test_register_consumer(consumer, mock_aioredis_from_url):
    """Test the register_consumer method."""
    await consumer.connect()
    await consumer.register_consumer()
    assert consumer._heartbeat_task is not None
    consumer._is_start = True

    # Check if heartbeat has run at least once
    await asyncio.sleep(0.3)
    consumers_key = f"{consumer._topic_keys.consumer_group_keys.consumer_key}:{consumer.consumer_name}"
    value = await consumer.redis.get(consumers_key)
    assert value is not None
    assert consumer.deserializer(value) is False  # _is_ready defaults to False
    consumer._is_start = False


@pytest.mark.asyncio
async def test_get_consumers(consumer, mock_aioredis_from_url):
    """Test the get_consumers method."""
    # Add fake consumers to Redis
    await consumer.connect()
    consumers_key1 = (
        f"{consumer._topic_keys.consumer_group_keys.consumer_key}:consumer1"
    )
    consumers_key2 = (
        f"{consumer._topic_keys.consumer_group_keys.consumer_key}:consumer2"
    )
    await consumer.redis.set(consumers_key1, consumer.serializer(True))
    await consumer.redis.set(consumers_key2, consumer.serializer(False))

    consumers = await consumer.get_consumers()
    assert consumers == {"consumer1": True, "consumer2": False}


@pytest.mark.asyncio
async def test_update_partitions(consumer, mock_aioredis_from_url):
    """Test the update_partitions method."""

    async def get_num_partitions():
        return 4

    # Mock get_num_partitions
    consumer.get_num_partitions = get_num_partitions
    await consumer.connect()

    # Add consumer to Redis
    consumers_key = f"{consumer._topic_keys.consumer_group_keys.consumer_key}:{consumer.consumer_name}"
    await consumer.redis.set(consumers_key, consumer.serializer(True))

    await consumer.update_partitions()
    assert consumer.partitions == [0, 1, 2, 3]  # Single consumer gets all partitions
    assert consumer._is_ready is True


@pytest.mark.asyncio
async def test_signal_rebalance(consumer, mock_aioredis_from_url):
    """Test the signal_rebalance method."""
    await consumer.signal_rebalance()
    # Check if pubsub received the message

    is_exist = False
    for _ in range(2):
        message = await consumer.pubsub.get_message(timeout=1.0)
        if not (
            message and message["type"] == "message" and message["data"] == "rebalance"
        ):
            continue

        is_exist = True
        assert message["type"] == "message"
        assert message["data"] == "rebalance"

    assert is_exist is True


@pytest.mark.asyncio
async def test_remove_ready(consumer, mock_aioredis_from_url):
    """Test the remove_ready method."""
    await consumer.connect()
    consumer._is_ready = True
    await consumer.remove_ready()
    assert consumer._is_ready is False
    consumers_key = f"{consumer._topic_keys.consumer_group_keys.consumer_key}:{consumer.consumer_name}"
    value = await consumer.redis.get(consumers_key)
    assert consumer.deserializer(value) is False


@pytest.mark.asyncio
async def test_all_consumers_ready(consumer, mock_aioredis_from_url):
    """Test the all_consumers_ready method."""
    # Add consumers to Redis
    await consumer.connect()

    consumers_key1 = (
        f"{consumer._topic_keys.consumer_group_keys.consumer_key}:consumer1"
    )
    consumers_key2 = (
        f"{consumer._topic_keys.consumer_group_keys.consumer_key}:consumer2"
    )
    await consumer.redis.set(consumers_key1, consumer.serializer(True))
    await consumer.redis.set(consumers_key2, consumer.serializer(True))

    result = await consumer.all_consumers_ready()
    assert result is True

    await consumer.redis.set(consumers_key2, consumer.serializer(False))
    result = await consumer.all_consumers_ready()
    assert result is False


@pytest.mark.asyncio
async def test_wait_for_all_ready(consumer, mock_aioredis_from_url):
    """Test the wait_for_all_ready method."""
    await consumer.connect()
    consumer._is_start = True
    # Add consumer to Redis
    consumers_key = f"{consumer._topic_keys.consumer_group_keys.consumer_key}:{consumer.consumer_name}"
    await consumer.redis.set(consumers_key, consumer.serializer(False))

    async def set_ready():
        await asyncio.sleep(0.1)
        await consumer.redis.set(consumers_key, consumer.serializer(True))

    # Run wait_for_all_ready and set_ready concurrently
    task = asyncio.create_task(consumer.wait_for_all_ready())
    await asyncio.create_task(set_ready())
    result = await task
    assert result is True


@pytest.mark.asyncio
async def test_heartbeat(consumer, mock_aioredis_from_url):
    """Test the heartbeat method."""
    await consumer.connect()
    consumer._is_start = True
    await consumer.register_consumer()
    consumers_key = f"{consumer._topic_keys.consumer_group_keys.consumer_key}:{consumer.consumer_name}"

    # Wait for a few heartbeat cycles
    await asyncio.sleep(0.3)
    consumer._is_start = False
    await consumer._heartbeat_task

    value = await consumer.redis.get(consumers_key)
    assert value is not None
    assert consumer.deserializer(value) is False  # _is_ready remains False


@pytest.mark.asyncio
async def test_do_rebalance(consumer, mock_aioredis_from_url):
    """Test the _do_rebalance method."""

    async def get_num_partitions():
        return 4

    await consumer.connect()
    consumer._is_consuming = True
    consumer.get_num_partitions = get_num_partitions
    consumers_key = f"{consumer._topic_keys.consumer_group_keys.consumer_key}:{consumer.consumer_name}"
    await consumer.redis.set(consumers_key, consumer.serializer(True))

    await consumer._do_rebalance()
    assert consumer._is_consuming is True
    assert consumer.partitions == [0, 1, 2, 3]

    consumers_key = f"{consumer._topic_keys.consumer_group_keys.consumer_key}:{consumer.consumer_name}2"
    await consumer.redis.set(consumers_key, consumer.serializer(True))

    await consumer._do_rebalance()
    assert consumer._is_consuming is True
    assert consumer.partitions == [0, 1]


@pytest.mark.asyncio
async def test_wait_for_rebalance(consumer, mock_aioredis_from_url):
    """Test the _wait_for_rebalance method."""
    await consumer.connect()
    consumer._is_start = True
    consumer._rebalance_event.set()  # Set event to bypass wait()

    task = asyncio.create_task(consumer._wait_for_rebalance())
    await asyncio.sleep(0.1)  # Wait for _do_rebalance to be called
    consumer._is_start = False
    await task

    assert consumer._rebalance_event.is_set() is False  # Ensure clear() was called


@pytest.mark.asyncio
async def test_consume(consumer, mock_aioredis_from_url):
    """Test the consume method."""

    async def callback(message):
        pass

    async def get_num_partitions():
        return 1

    consumer._is_start = True
    consumer.get_num_partitions = get_num_partitions
    await consumer.connect()
    consumers_key = f"{consumer._topic_keys.consumer_group_keys.consumer_key}:{consumer.consumer_name}"
    await consumer.redis.set(consumers_key, consumer.serializer(True))

    task = asyncio.create_task(consumer.consume(callback))
    await asyncio.sleep(0.2)
    consumer._is_start = False
    await task


@pytest.mark.asyncio
async def test_read_single_message(consumer, mock_aioredis_from_url):
    """Test read single message method."""

    producer = Producer(
        topic=consumer.topic,
        redis_url="redis://localhost:6379/1",  # URL is mocked
        init_partitions=2,
    )
    await producer.connect()

    async def callback(message):
        pass

    async def get_num_partitions():
        return 2

    consumer._is_start = True
    consumer.get_num_partitions = get_num_partitions
    await consumer.connect()
    consumers_key = f"{consumer._topic_keys.consumer_group_keys.consumer_key}:{consumer.consumer_name}"
    await consumer.redis.set(consumers_key, consumer.serializer(True))
    consumer.partitions = [0, 1]
    await producer.enqueue({"data": "1", "id": 1}, partition_key="id")
    await producer.enqueue({"data": "11", "id": 1}, partition_key="id")
    await producer.enqueue({"data": "2", "id": 2}, partition_key="id")
    await producer.enqueue({"data": "22", "id": 2}, partition_key="id")

    # first message from stream with partition = 0
    assert consumer.last_read_partition_index == -1
    message_streams = await consumer._read_messages_from_streams(count=1)
    assert consumer.last_read_partition_index == 0
    assert message_streams is not None
    assert len(message_streams) == 1
    message_stream = message_streams[0]
    assert message_stream[0] == consumer._topic_keys.partition_keys[0].stream_key
    messages = message_stream[1]
    assert len(messages) == 1
    _, message = messages[0]
    assert isinstance(message["payload"], str)
    assert consumer.deserializer(message["payload"])["id"] == 2
    assert consumer.deserializer(message["payload"])["data"] == "2"

    # first message from stream with partition = 1
    message_streams = await consumer._read_messages_from_streams(count=1)
    assert consumer.last_read_partition_index == 1
    assert message_streams is not None
    assert len(message_streams) == 1
    message_stream = message_streams[0]
    assert message_stream[0] == consumer._topic_keys.partition_keys[1].stream_key
    messages = message_stream[1]
    assert len(messages) == 1
    _, message = messages[0]
    assert isinstance(message["payload"], str)
    assert consumer.deserializer(message["payload"])["id"] == 1
    assert consumer.deserializer(message["payload"])["data"] == "1"

    # second message from stream with partition = 0
    message_streams = await consumer._read_messages_from_streams(count=1)
    assert consumer.last_read_partition_index == 0
    assert message_streams is not None
    assert len(message_streams) == 1
    message_stream = message_streams[0]
    assert message_stream[0] == consumer._topic_keys.partition_keys[0].stream_key
    messages = message_stream[1]
    assert len(messages) == 1
    _, message = messages[0]
    assert isinstance(message["payload"], str)
    assert consumer.deserializer(message["payload"])["id"] == 2
    assert consumer.deserializer(message["payload"])["data"] == "22"

    # second message from stream with partition = 1
    message_streams = await consumer._read_messages_from_streams(count=1)
    assert consumer.last_read_partition_index == 1
    assert message_streams is not None
    assert len(message_streams) == 1
    message_stream = message_streams[0]
    assert message_stream[0] == consumer._topic_keys.partition_keys[1].stream_key
    messages = message_stream[1]
    assert len(messages) == 1
    _, message = messages[0]
    assert isinstance(message["payload"], str)
    assert consumer.deserializer(message["payload"])["id"] == 1
    assert consumer.deserializer(message["payload"])["data"] == "11"


async def test_read_batch_messages(consumer, mock_aioredis_from_url):
    """Test read batch messages method."""

    producer = Producer(
        topic=consumer.topic,
        redis_url="redis://localhost:6379/1",  # URL is mocked
        init_partitions=2,
    )
    await producer.connect()

    async def callback(message):
        pass

    async def get_num_partitions():
        return 2

    consumer._is_start = True
    consumer.get_num_partitions = get_num_partitions
    await consumer.connect()
    consumers_key = f"{consumer._topic_keys.consumer_group_keys.consumer_key}:{consumer.consumer_name}"
    await consumer.redis.set(consumers_key, consumer.serializer(True))
    consumer.partitions = [0, 1]
    await producer.enqueue({"data": "1", "id": 1}, partition_key="id")
    await producer.enqueue({"data": "11", "id": 1}, partition_key="id")
    await producer.enqueue({"data": "2", "id": 2}, partition_key="id")
    await producer.enqueue({"data": "22", "id": 2}, partition_key="id")

    # messages from stream with partition = 0
    assert consumer.last_read_partition_index == -1
    message_streams = await consumer._read_messages_from_streams(count=2)
    assert consumer.last_read_partition_index == 0
    assert message_streams is not None
    assert len(message_streams) == 1
    message_stream = message_streams[0]
    assert message_stream[0] == consumer._topic_keys.partition_keys[0].stream_key
    messages = message_stream[1]
    assert len(messages) == 2
    for _, message in messages:
        assert isinstance(message["payload"], str)
        assert consumer.deserializer(message["payload"])["id"] == 2

    # messages from stream with partition = 1
    message_streams = await consumer._read_messages_from_streams(count=2)
    assert consumer.last_read_partition_index == 1
    assert message_streams is not None
    assert len(message_streams) == 1
    message_stream = message_streams[0]
    assert message_stream[0] == consumer._topic_keys.partition_keys[1].stream_key
    messages = message_stream[1]
    assert len(messages) == 2
    for _, message in messages:
        assert isinstance(message["payload"], str)
        assert consumer.deserializer(message["payload"])["id"] == 1

    # messages from stream with partition = 0
    message_streams = await consumer._read_messages_from_streams(count=2)
    assert consumer.last_read_partition_index == 0
    assert message_streams is not None
    assert len(message_streams) == 0

    # messages from stream with partition = 1
    message_streams = await consumer._read_messages_from_streams(count=2)
    assert consumer.last_read_partition_index == 1
    assert message_streams is not None
    assert len(message_streams) == 0


@pytest.mark.asyncio
async def test_consume_batch(consumer, mock_aioredis_from_url):
    """Test the consume_batch method."""

    async def callback(messages):
        pass

    async def get_num_partitions():
        return 1

    consumer._is_start = True
    consumer.get_num_partitions = get_num_partitions
    await consumer.connect()
    consumers_key = f"{consumer._topic_keys.consumer_group_keys.consumer_key}:{consumer.consumer_name}"
    await consumer.redis.set(consumers_key, consumer.serializer(True))

    task = asyncio.create_task(consumer.consume_batch(callback, batch_size=5))
    await asyncio.sleep(0.2)
    consumer._is_start = False
    await task

    assert consumer.batch_size == 5


@pytest.mark.asyncio
async def test_do_consume_single(consumer, mock_aioredis_from_url):
    """Test the _do_consume method in single message mode."""
    consumer._is_start = True
    consumer._is_consuming = True
    consumer.partitions = [0]

    async def callback(message):
        pass

    async def get_pending_messages(count):
        return []

    consumer.callback = callback
    consumer.get_pending_messages = get_pending_messages
    await consumer.connect()

    task = asyncio.create_task(consumer._do_consume(is_batch=False))
    await asyncio.sleep(0.1)
    consumer._is_start = False
    await task


@pytest.mark.asyncio
async def test_do_consume_batch(consumer, mock_aioredis_from_url):
    """Test the _do_consume method in batch mode."""
    consumer._is_start = True
    consumer._is_consuming = True
    consumer.partitions = [0]

    async def callback(messages):
        pass

    async def get_pending_messages(count):
        return []

    consumer.callback = callback
    consumer.get_pending_messages = get_pending_messages
    await consumer.connect()

    task = asyncio.create_task(consumer._do_consume(is_batch=True))
    await asyncio.sleep(0.1)
    consumer._is_start = False
    await task

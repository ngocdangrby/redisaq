import asyncio
import logging
import time
import uuid
from typing import List

import pytest
from redisaq.consumer import Consumer
from redisaq.models import Message
from redisaq.producer import Producer


@pytest.mark.asyncio
class TestProducerConsumerIntegration:
    def setup_method(self):
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger("test_producer_consumer")
        # Test configuration
        self.topic = f"test_topic_{uuid.uuid4()}"  # Unique topic for each test
        self.group_name = "test_group"
        self.consumer_name = "test_consumer"
        self.producer = None
        self.consumer = None
        self.received_messages = []

    async def teardown_method(self):
        # Clean up Producer
        if self.producer:
            await self.producer.close()
        # Clean up Consumer
        if self.consumer:
            await self.consumer.stop()

    async def message_callback(self, messages: List[Message]):
        """Callback to collect received messages."""
        self.received_messages.extend(messages)
        self.logger.info(f"Received {len(messages)} messages")

    async def single_message_callback(self, message: Message):
        """Callback to collect received messages."""
        self.received_messages.extend([message])
        self.logger.info(f"Received 1 message")

    async def test_producer_consumer_single_message(self, redis_client, mock_aioredis_from_url):
        """Test that a single message sent by producer is received by consumer."""
        # Initialize Producer
        self.producer = Producer(
            topic=self.topic,
            redis_url="redis://localhost:6379/0",  # URL is mocked
            init_partitions=2,
            debug=True,
            logger=self.logger
        )
        await self.producer.connect()

        # Initialize Consumer
        self.consumer = Consumer(
            topic=self.topic,
            redis_url="redis://localhost:6379/0",  # URL is mocked
            group_name=self.group_name,
            consumer_name=self.consumer_name,
            batch_size=10,
            debug=True,
            logger=self.logger
        )
        await self.consumer.connect()

        # Enqueue a single message
        payload = {"data": "test_message", "id": 1}
        msg_id = await self.producer.enqueue(payload=payload, partition_key="id")

        # Start consumer in the background
        consumer_task = asyncio.create_task(
            self.consumer.consume(callback=self.single_message_callback)
        )

        # Wait for the message to be received or timeout
        timeout = 10  # seconds
        start_time = time.time()
        while len(self.received_messages) < 1:
            if time.time() - start_time > timeout:
                pytest.fail("Timeout waiting for message")
            await asyncio.sleep(0.1)

        # Verify the received message
        assert len(self.received_messages) == 1
        received_message = self.received_messages[0]
        assert received_message.msg_id == msg_id
        assert received_message.topic == self.topic
        assert received_message.payload == payload
        assert received_message.partition_key == "id"
        assert received_message.partition in [0, 1]  # Should be in one of the partitions
        assert received_message.enqueued_at is not None

    async def test_producer_consumer_batch_messages(self, redis_client, mock_aioredis_from_url):
        """Test that multiple messages sent by producer are received by consumer."""
        # Initialize Producer
        self.producer = Producer(
            topic=self.topic,
            redis_url="redis://localhost:6379/0",  # URL is mocked
            init_partitions=2,
            debug=True,
            logger=self.logger
        )
        await self.producer.connect()

        # Initialize Consumer
        self.consumer = Consumer(
            topic=self.topic,
            redis_url="redis://localhost:6379/0",  # URL is mocked
            group_name=self.group_name,
            consumer_name=self.consumer_name,
            batch_size=10,
            debug=True,
            logger=self.logger
        )
        await self.consumer.connect()

        # Enqueue multiple messages
        payloads = [
            {"data": f"test_message_{i}", "id": i} for i in range(5)
        ]
        msg_ids = await self.producer.batch_enqueue(payloads=payloads, partition_key="id")

        # Start consumer in the background
        consumer_task = asyncio.create_task(
            self.consumer.consume_batch(self.message_callback, batch_size=10)
        )

        # Wait for all messages to be received or timeout
        timeout = 20  # seconds
        start_time = asyncio.get_event_loop().time()
        while len(self.received_messages) < len(payloads):
            if asyncio.get_event_loop().time() - start_time > timeout:
                pytest.fail("Timeout waiting for messages")
            await asyncio.sleep(0.1)

        # Stop the consumer
        #await self.consumer.stop()
        #consumer_task.cancel()
        #try:
        #    await consumer_task
        #except asyncio.CancelledError:
        #    pass

        # Verify the received messages
        assert len(self.received_messages) == len(payloads)
        received_msg_ids = [msg.msg_id for msg in self.received_messages]
        assert set(received_msg_ids) == set(msg_ids)
        for msg, expected_payload in zip(self.received_messages, payloads):
            assert msg.topic == self.topic
            assert msg.payload == expected_payload
            assert msg.partition_key == "id"
            assert msg.partition in [0, 1]
            assert msg.enqueued_at is not None

    async def test_producer_consumer_partition_distribution(self, redis_client, mock_aioredis_from_url):
        """Test that messages are distributed across partitions correctly."""
        # Initialize Producer
        self.producer = Producer(
            topic=self.topic,
            redis_url="redis://localhost:6379/0",  # URL is mocked
            init_partitions=2,
            debug=True,
            logger=self.logger
        )
        await self.producer.connect()

        # Initialize Consumer
        self.consumer = Consumer(
            topic=self.topic,
            redis_url="redis://localhost:6379/0",  # URL is mocked
            group_name=self.group_name,
            consumer_name=self.consumer_name,
            batch_size=10,
            debug=True,
            logger=self.logger
        )
        await self.consumer.connect()

        # Enqueue messages with different partition keys
        payloads = [
            {"data": f"test_message_{i}", "id": i} for i in range(10)
        ]
        msg_ids = await self.producer.batch_enqueue(payloads=payloads, partition_key="id")

        # Start consumer in the background
        consumer_task = asyncio.create_task(
            self.consumer.consume_batch(self.message_callback, batch_size=10)
        )

        # Wait for all messages to be received or timeout
        timeout = 5  # seconds
        start_time = asyncio.get_event_loop().time()
        while len(self.received_messages) < len(payloads):
            if asyncio.get_event_loop().time() - start_time > timeout:
                pytest.fail("Timeout waiting for messages")
            await asyncio.sleep(0.1)

        # Stop the consumer
        await self.consumer.stop()
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

        # Verify partition distribution
        partitions = [msg.partition for msg in self.received_messages]
        assert len(set(partitions)) > 1, "Messages should be distributed across multiple partitions"
        assert all(p in [0, 1] for p in partitions), "Partitions should be 0 or 1"
        assert len(self.received_messages) == len(payloads)
        received_msg_ids = [msg.msg_id for msg in self.received_messages]
        assert set(received_msg_ids) == set(msg_ids)
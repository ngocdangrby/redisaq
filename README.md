# redisaq

`redisaq` is a Python library for distributed job queuing and processing using Redis Streams. It provides a robust, scalable solution for handling distributed workloads with features like consumer groups, automatic partition rebalancing, and fault tolerance.

## Installation
Install `redisaq` from PyPI:

```bash
pip install redisaq
```

## Features

### Producer
- **Message Handling**:
  - Single message enqueuing: `enqueue(payload)`
  - Batch operations: `batch_enqueue(payloads)`
  - Custom partition key support for message routing
  - Configurable message timeouts
- **Stream Management**:
  - Configurable stream length (`maxlen`) and trimming behavior (`approximate`)
  - Dynamic partition scaling with `request_partition_increase()`
  - Automatic partition key hashing for load distribution
  - Custom serialization support (default: `orjson`)

### Consumer
- **Message Processing**:
  - Support for both single and batch message processing
  - Configurable batch size
  - Asynchronous message handling with custom callbacks
  - Automatic message acknowledgment
- **Fault Tolerance**:
  - Heartbeat mechanism (configurable interval and TTL)
  - Automatic crash detection
  - Graceful consumer registration and deregistration
  - Partition rebalancing on consumer changes
- **Group Management**:
  - Consumer group support with `XREADGROUP`
  - Dynamic partition assignment
  - Automatic consumer group creation
  - Message tracking and acknowledgment

### Advanced Features
- **Scalability**:
  - Multi-partition support for horizontal scaling
  - Dynamic partition count adjustment
  - Efficient round-robin partition assignment
- **Reliability**:
  - Built-in error handling and retries
  - Dead-letter queue support
  - Message persistence via Redis Streams
- **Monitoring**:
  - Detailed logging with configurable levels
  - Consumer and producer status tracking
  - Partition assignment monitoring

### Technical Details
- **Async Support**: Built with `asyncio` for non-blocking operations
- **Redis Integration**: Uses Redis Streams with `aioredis`
- **Type Safety**: Full type hints support
- **Customization**: Configurable serialization/deserialization
- **Namespace Management**: Automatic key prefixing and organization

**Warning**: Unbounded streams (`maxlen=None`) can consume significant Redis memory. Set `maxlen` (e.g., 1000) to limit stream size in production.

## Usage

### Basic Producer-Consumer Example

```python
from redisaq import Producer, Consumer
import asyncio

async def process_message(message):
    print(f"Processing message {message.msg_id}: {message.payload}")
    await asyncio.sleep(1)  # Simulate work

async def main():
    # Initialize producer with topic and max stream length
    producer = Producer(
        topic="notifications",
        maxlen=1000,
        redis_url="redis://localhost:6379/0"
    )
    await producer.connect()

    # Send some messages
    await producer.batch_enqueue([
        {"type": "email", "to": "user1@example.com", "subject": "Hello"},
        {"type": "sms", "to": "+1234567890", "text": "Hi there"}
    ])

    # Initialize consumer
    consumer = Consumer(
        topic="notifications",
        group_name="notification_processors",
        batch_size=10,
        heartbeat_interval=3.0,
        redis_url="redis://localhost:6379/0"
    )
    
    # Connect and start processing
    await consumer.connect()
    await consumer.process(process_message)

    # Cleanup
    await producer.close()
    await consumer.close()

if __name__ == "__main__":
    asyncio.run(main())
```

### Advanced Usage

#### Partition Key Routing
```python
from redisaq import Producer, Message

async def send_notifications():
    producer = Producer(topic="notifications", init_partitions=3)
    await producer.connect()

    # Messages with same user_id will go to same partition
    message = Message(
        topic="notifications",
        payload={"user_id": "123", "content": "Hello"},
        partition_key="user_id"
    )
    await producer.enqueue(message)
```

#### Batch Processing
```python
from redisaq import Consumer, BatchCallback
from typing import List

async def process_batch(messages: List[Message]):
    print(f"Processing batch of {len(messages)} messages")
    for msg in messages:
        # Process each message in the batch
        print(f"Message {msg.msg_id}: {msg.payload}")

consumer = Consumer(
    topic="notifications",
    batch_size=10,  # Process up to 10 messages at once
    heartbeat_interval=3.0
)

await consumer.process_batch(process_batch)
```

#### Custom Serialization
```python
import msgpack
from redisaq import Producer, Consumer

# Custom serializer/deserializer
def msgpack_serializer(data):
    return msgpack.packb(data).decode('utf-8')

def msgpack_deserializer(data):
    return msgpack.unpackb(data.encode('utf-8'))

# Use custom serialization
producer = Producer(
    topic="data",
    serializer=msgpack_serializer
)

consumer = Consumer(
    topic="data",
    deserializer=msgpack_deserializer
)
```

### FastAPI Example

See [`examples/fastapi`](examples/fastapi) for a full-featured FastAPI integration.

## Examples
- **Basic Example**: Demonstrates batch job production, consumption, rebalancing, and reconsumption. See [examples/basic/README.md](examples/basic/README.md).
- **FastAPI Integration**: Shows how to integrate `redisaq` with a FastAPI application for job submission and processing. See [examples/fastapi/README.md](examples/fastapi/README.md).

## Running Tests
```bash
poetry run pytest
```

## Contributing
- Report issues or suggest features via GitHub Issues.
- Submit pull requests with clear descriptions.

## License
MIT
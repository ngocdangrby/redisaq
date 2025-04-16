# redisq

`redisq` is a Python library for distributed job queuing and processing using Redis Streams. It supports consumer groups, partition rebalancing, reconsumption, heartbeats, crash detection, and efficient batch job production.

## Features
- **Producer**:
  - Enqueue individual jobs with `enqueue(payload)` or batches with `batch_enqueue(payloads)` to a specified topic.
  - Configurable stream length (`maxlen`) and trimming behavior (`approximate`) via `XADD`.
  - Dynamic partition scaling for load distribution.
  - Uses `redisq` prefix by default for streams and keys (e.g., `redisq:send_email:0`).
- **Consumer**:
  - Process jobs in consumer groups using `XREADGROUP` and `XACK`.
  - Self-assign partitions with round-robin strategy.
  - Pause/rebalance/resume on consumer join or crash.
  - Heartbeats (TTL 10s, interval 5s) for crash detection.
  - Dead-letter queue for failed jobs (`redisq:dead_letter`).
  - Asynchronous `process_job` function for non-blocking job handling.
- **Reconsumption**: Create a new consumer group to reprocess all jobs in streams.
- **Async**: Built with `asyncio` for non-blocking I/O.
- **Redis-Compatible**: Uses Redis Streams for persistence and coordination.

**Warning**: Unbounded streams (`maxlen=None`) can consume significant Redis memory. Set `maxlen` (e.g., 1000) to limit stream size in production.

## Installation
```bash
poetry install
```

## Examples
- **Basic Example**: Demonstrates batch job production, consumption, rebalancing, and reconsumption. See [examples/basic/README.md](examples/basic/README.md).
- **FastAPI Integration**: Shows how to integrate `redisq` with a FastAPI application for job submission and monitoring. See [examples/fastapi/README.md](examples/fastapi/README.md).

## Usage
```python
from redisq import Producer, Consumer
import asyncio

async def main():
    # Producer
    producer = Producer(topic="my_topic", maxlen=1000)
    await producer.batch_enqueue([
        {"data": "job1"},
        {"data": "job2"}
    ])

    # Consumer
    async def process_job(job):
        print(f"Processing job {job.id}: {job.payload}")
        await asyncio.sleep(1)

    consumer = Consumer(
        topic="my_topic",
        group="my_group",
        consumer_id="consumer_1",
        process_job=process_job
    )
    await consumer.start()

asyncio.run(main())
```

## Running Tests
```bash
poetry run pytest
```

## Contributing
- Report issues or suggest features via GitHub Issues.
- Submit pull requests with clear descriptions.

## License
MIT
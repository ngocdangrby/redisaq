"""
Basic Example: Redisq Consumer (New Group)

This script runs a consumer for the 'send_email' topic in a new group 'email_group_v2' to
demonstrate reconsumption of jobs. It uses a heartbeat (TTL 10s, interval 5s) for coordination.

Prerequisites:
- Python 3.8+ and dependencies installed (`poetry install`).
- Redis running at redis://localhost:6379.
- Jobs enqueued by producer (examples/basic/producer.py).

How to Run:
1. Start Redis:
   ```bash
   docker-compose up -d
   ```
2. Run Producer to enqueue jobs:
   ```bash
   poetry run python examples/basic/producer.py
   ```
3. Stop producer and existing consumers.
4. Run New Group Consumer:
   ```bash
   poetry run python examples/basic/consumer_new_group.py
   ```
5. Stop with Ctrl+C, then:
   ```bash
   docker-compose down
   ```

Expected Behavior:
- Registers with 'send_email' topic, group 'email_group_v2'.
- Reconsumes all jobs (~1000 per stream) from streams.
- Processes and acknowledges jobs independently of 'email_group'.
"""

import asyncio
import logging
import random
from redisq import Consumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def process_job(job):
    logger.info(f"Consumer new_group processing job {job.id} with payload {job.payload}")
    await asyncio.sleep(random.uniform(0.5, 2.0))  # Simulate processing
    logger.info(f"Consumer new_group completed job {job.id}")

async def main():
    consumer = Consumer(
        topic="send_email",
        group="email_group_v2",
        process_job=process_job,
        consumer_id="consumer_new",
        redis_url="redis://localhost:6379"
    )
    try:
        await consumer.start()
    except KeyboardInterrupt:
        logger.info("Stopping consumer new_group")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
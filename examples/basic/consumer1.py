"""
Basic Consumer Example for redisaq (Consumer 1)

This script runs a consumer in the 'email_group' group for the 'send_email' topic,
processing messages from assigned partitions with heartbeats and rebalancing.

Behavior:
- Registers with group 'email_group', consumer_id 'consumer_1'.
- Uses heartbeats (TTL 10s, interval 5s).
- Processes messages asynchronously, acknowledges with XACK.
- Rebalances partitions when new consumers join.

Prerequisites:
- Python 3.8+ and redisaq installed (`pip install redisaq`).
- Redis running at redis://localhost:6379.
- Producer enqueuing messages (producer.py).

How to Run:
1. Start Redis:
   ```bash
   docker-compose up -d
   ```
2. Run Consumer:
   ```bash
   python consumer1.py
   ```
3. Run Producer (in another terminal):
   ```bash
   python producer.py
   ```
4. Stop with Ctrl+C, then:
   ```bash
   docker-compose down
   ```
"""

import asyncio
import logging
import random

from redisaq import Consumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def process_job(job):
    logger.info(f"Consumer 1 processing job {job.id} with payload {job.payload}")
    await asyncio.sleep(random.uniform(0.5, 1.5))  # Simulate processing
    logger.info(f"Consumer 1 completed job {job.id}")

async def main():
    consumer = Consumer(
        topic="send_email",
        group="email_group",
        consumer_id="consumer_1",
        process_job=process_job,
        redis_url="redis://localhost:6379"
    )
    try:
        await consumer.start()
    except KeyboardInterrupt:
        logger.info("Stopping consumer")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
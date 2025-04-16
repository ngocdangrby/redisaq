"""
Basic Example: Redisq Consumer 3

This script runs a consumer for the 'send_email' topic, processing jobs in group 'email_group'.
It uses a heartbeat (TTL 10s, interval 5s) to coordinate with other consumers.

Prerequisites:
- Python 3.8+ and dependencies installed (`poetry install`).
- Redis running at redis://localhost:6379.
- Producer running (examples/basic/producer.py).

How to Run:
1. Start Redis:
   ```bash
   docker-compose up -d
   ```
2. Run Producer:
   ```bash
   poetry run python examples/basic/producer.py
   ```
3. Run Consumers:
   ```bash
   poetry run python examples/basic/consumer1.py
   poetry run python examples/basic/consumer2.py
   poetry run python examples/basic/consumer3.py
   ```
4. Stop with Ctrl+C.

Expected Behavior:
- Registers with 'send_email' topic, group 'email_group'.
- Uses heartbeats to coordinate with other consumers.
- Pauses, rebalances, and resumes when new consumers join.
- Self-assigns partitions and processes jobs.
- Acknowledges jobs with xack, allowing reconsumption with a new group.
"""

import asyncio
import logging
import random
from redisq import Consumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def process_job(job):
    logger.info(f"Consumer 3 processing job {job.id} with payload {job.payload}")
    await asyncio.sleep(random.uniform(0.5, 2.0))  # Simulate processing
    logger.info(f"Consumer 3 completed job {job.id}")

async def main():
    consumer = Consumer(
        topic="send_email",
        group="email_group",
        process_job=process_job,
        consumer_id="consumer_3",
        redis_url="redis://localhost:6379"
    )
    try:
        await consumer.start()
    except KeyboardInterrupt:
        logger.info("Stopping consumer 3")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
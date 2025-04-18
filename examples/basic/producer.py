"""
Basic Producer Example for redisaq

This script demonstrates a producer that enqueues batches of email messages to the 'send_email' topic,
with dynamic partition scaling and stream length limiting.

Behavior:
- Enqueues batches of 2-5 messages every 0.5-2 seconds.
- Payloads: {"to": "userN@example.com", "subject": "..."}
- Increases partitions (1 to 8) every 5 messages.
- Limits streams to ~1000 messages with maxlen=1000.

Prerequisites:
- Python 3.8+ and redisaq installed (`pip install redisaq`).
- Redis running at redis://localhost:6379.

How to Run:
1. Start Redis:
   ```bash
   docker-compose up -d
   ```
2. Run Consumers (in separate terminals):
   ```bash
   python consumer1.py
   python consumer2.py
   ```
3. Run Producer:
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
import time

from redisaq import Producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    producer = Producer(topic="send_email", redis_url="redis://localhost:6379/0", maxlen=10000)
    await producer.connect()
    job_count = 0
    max_speed = 0

    try:
        start_time = time.time()
        while True:
            batch_size = 1 + random.randint(2, 5)
            payloads = [
                {
                    "to": f"user{i}@example.com",
                    "subject": f"Email {i}",
                    "body": f"Content of email {i}"
                }
                for i in range(job_count + 1, job_count + batch_size + 1)
            ]
            job_id = await producer.enqueue(payloads[0])
            logger.info(f"Enqueued single job: {job_id}")

            job_ids = await producer.batch_enqueue(payloads)
            logger.info(f"Enqueued batch of {len(job_ids)} messages: {job_ids}")
            job_count += batch_size

            if job_count % 30 == 0:
                new_partitions = min(8, job_count // 30)
                await producer.request_partition_increase(new_partitions)
                logger.info(f"Increased partitions to {new_partitions}")

            if job_count % 20 == 0:
                speed = round(job_count/(time.time() - start_time) * 100) / 100
                max_speed = max(max_speed, speed)
                logger.info(f"Speed: {speed} msgs/s {job_count}")

            # if job_count >= 100000:
            #     speed = round(job_count / (time.time() - start_time) * 100) / 100
            #     max_speed = max(max_speed, speed)
            #     logger.info(f"Speed: {speed} msgs/s {job_count}")
            #     break
    except KeyboardInterrupt:
        logger.info("Stopping producer")
    finally:
        await producer.close()
        logger.info(f"Max speed: {max_speed}")

if __name__ == "__main__":
    asyncio.run(main())
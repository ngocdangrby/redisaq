"""
Basic Example: Redisq Producer

This script runs a producer that enqueues batches of jobs to the 'send_email' topic every 0.5-2 seconds.
Every 5 jobs, it increases the number of partitions up to a maximum of 8.

WARNING: By default, Redis Streams are unbounded if maxlen is set to None, which can lead to
significant memory usage in Redis if streams grow indefinitely. In this example, we set maxlen=1000
at initialization to limit each stream to approximately 1000 messages. Use maxlen=None cautiously in
production to avoid excessive memory consumption.

Prerequisites:
- Python 3.8+ and dependencies installed (`poetry install`).
- Redis running at redis://localhost:6379.

How to Run:
1. Start Redis:
   ```bash
   docker-compose up -d
   ```
2. Run Producer:
   ```bash
   poetry run python examples/basic/producer.py
   ```
3. Run Consumers (in separate terminals):
   ```bash
   poetry run python examples/basic/consumer1.py
   poetry run python examples/basic/consumer2.py
   ```
4. Stop with Ctrl+C, then:
   ```bash
   docker-compose down
   ```

Expected Behavior:
- Enqueues batches of 2-5 jobs with payloads like {"to": "userN@example.com", "subject": "..."}.
- Increases partitions (1 to 8) every 5 jobs.
- Limits streams to maxlen=1000 (set at init) using xadd.
- Logs job IDs and partition changes.
"""

import asyncio
import logging
import random
from redisq import Producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    producer = Producer(topic="send_email", redis_url="redis://localhost:6379", maxlen=1000, approximate=True)
    try:
        job_count = 0
        while True:
            batch_size = random.randint(2, 5)
            payloads = [
                {
                    "to": f"user{job_count + i + 1}@example.com",
                    "subject": f"Email {job_count + i + 1}",
                    "body": f"This is email number {job_count + i + 1}"
                }
                for i in range(batch_size)
            ]
            job_ids = await producer.batch_enqueue(payloads)
            logger.info(f"Enqueued batch of {len(job_ids)} jobs: {job_ids}")
            job_count += batch_size
            
            # Every 5 jobs, try to increase partitions
            if job_count >= 5 and job_count % 5 < batch_size:
                current_partitions = await producer.get_num_partitions()
                if current_partitions < 8:
                    await producer.request_partition_increase(current_partitions + 1)
                    logger.info(f"Increased partitions to {current_partitions + 1}")
            
            await asyncio.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        logger.info("Stopping producer")
    finally:
        await producer.close()

if __name__ == "__main__":
    asyncio.run(main())
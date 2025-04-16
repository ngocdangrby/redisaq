"""
FastAPI Integration Example for redisq: Job Consumer

This script runs a consumer for the 'send_email' topic in group 'email_group',
processing jobs enqueued by the FastAPI app (main.py). It runs in a separate process
from the FastAPI app.

Prerequisites:
- Python 3.8+ and dependencies installed (`poetry install`).
- Redis running at redis://localhost:6379.
- FastAPI app enqueuing jobs (main.py).

How to Run:
1. Start Redis:
   ```bash
   docker-compose up -d
   ```
2. Run Consumer:
   ```bash
   poetry run python consumer.py
   ```
3. Run FastAPI App (in another terminal):
   ```bash
   poetry run uvicorn main:app --host 0.0.0.0 --port 8000
   ```
4. Enqueue Jobs:
   ```bash
   curl -X POST http://localhost:8000/jobs -H "Content-Type: application/json" -d '{"jobs": [{"to": "user1@example.com", "subject": "Test", "body": "Hello"}]}'
   ```
5. Stop with Ctrl+C, then:
   ```bash
   docker-compose down
   ```

Expected Behavior:
- Registers with 'send_email' topic, group 'email_group'.
- Uses heartbeats (TTL 10s, interval 5s) for coordination.
- Processes jobs asynchronously, acknowledges with xack.
- Supports reconsumption with a new group.
"""

import asyncio
import logging
from redisq import Consumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def process_job(job):
    logger.info(f"API consumer processing job {job.id} with payload {job.payload}")
    await asyncio.sleep(1)  # Simulate processing
    logger.info(f"API consumer completed job {job.id}")

async def main():
    consumer = Consumer(
        topic="send_email",
        group="email_group",
        consumer_id="api_consumer",
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
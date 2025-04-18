import asyncio
import logging
import random
from typing import List

from redisaq import Consumer, Message

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def process_job(messages: List[Message]):
    logger.info(f"Consumer 2 processing {len(messages)} messages")
    for message in messages:
        logger.info(f"Consumer 2 processing job {message.msg_id} with payload {message.payload}")
        await asyncio.sleep(random.uniform(0.5, 1.5))  # Simulate processing
        logger.info(f"Consumer 2 completed job {message.msg_id}")

async def main():
    consumer = Consumer(
        topic="send_email",
        group_name="email_group",
        consumer_name="consumer_2",
        redis_url="redis://localhost:6379/0",
        batch_size=5,
    )
    try:
        await consumer.consume_batch(callback=process_job)
    except KeyboardInterrupt:
        logger.info("Stopping consumer")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
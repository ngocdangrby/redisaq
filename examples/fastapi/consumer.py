"""
FastAPI Integration Example for redisaq: Email Processing Consumer

This script implements a consumer process that handles email jobs submitted through
the FastAPI application. It demonstrates advanced features of redisaq including
batch processing, priority-based routing, and error handling.

Features:
- Batch message processing
- Priority-based job handling
- Graceful shutdown
- Error handling with dead-letter queue
- Health monitoring via heartbeats

Prerequisites:
- Python 3.8+
- Redis server
- FastAPI app running (main.py)

Usage:
1. Start the consumer:
   ```bash
   python consumer.py
   ```

2. Monitor logs for processing status
3. Use Ctrl+C for graceful shutdown
"""

import asyncio
import logging
from typing import List

from redisaq import Consumer, Message

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def send_email(to: str, subject: str, body: str) -> None:
    """
    Simulate sending an email. In production, replace with actual email sending logic.
    """
    logger.info(f"Sending email to {to}: {subject}")
    await asyncio.sleep(1)  # Simulate email sending
    logger.info(f"Email sent to {to}")


async def process_message(message: Message) -> None:
    """
    Process a single email message.
    Includes error handling and priority-based processing.
    """
    try:
        payload = message.payload
        priority = payload.get("priority", 1)

        # Add priority-based delay for demonstration
        delay = 2 if priority > 1 else 1
        await asyncio.sleep(delay)

        # Send the email
        await send_email(
            to=payload["to"], subject=payload["subject"], body=payload["body"]
        )

        logger.info(f"Processed message {message.msg_id} " f"(priority: {priority})")

    except Exception as e:
        logger.error(f"Error processing message {message.msg_id}: {e}", exc_info=True)
        raise  # Let redisaq handle the error


async def process_batch(messages: List[Message]) -> None:
    """
    Process a batch of email messages.
    Demonstrates concurrent processing with asyncio.
    """
    logger.info(f"Processing batch of {len(messages)} messages")

    # Process messages concurrently
    tasks = [process_message(msg) for msg in messages]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Check for any errors
    errors = [r for r in results if isinstance(r, Exception)]
    if errors:
        logger.error(f"Batch processing had {len(errors)} errors")


async def main() -> None:
    """
    Main consumer loop with graceful shutdown handling.
    """
    consumer = Consumer(
        topic="send_email",
        group_name="email_processors",
        consumer_name="email_worker_1",
        redis_url="redis://localhost:6379/0",
        batch_size=5,  # Process up to 5 messages at once
        heartbeat_interval=3.0,  # Send heartbeat every 3 seconds
    )

    try:
        logger.info("Starting email consumer...")
        await consumer.connect()

        # Use batch processing for better throughput
        await consumer.consume_batch(process_batch)

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    except Exception as e:
        logger.error(f"Consumer error: {e}", exc_info=True)
    finally:
        await consumer.close()
        logger.info("Consumer shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())

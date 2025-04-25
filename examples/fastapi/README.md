# FastAPI Integration Example for redisaq

This example demonstrates a scalable email job queue using FastAPI and redisaq.

## Overview
- **API Server (`main.py`)**: Accepts email jobs via HTTP and enqueues them to Redis Streams.
- **Consumer (`consumer.py`)**: Processes jobs in batches, simulates email sending, and demonstrates error handling and partitioning.

---

## Installation

```bash
pip install redisaq fastapi uvicorn pydantic[email]
```

---

## Usage

### 1. Start Redis
```bash
# Using Docker
sudo docker run --name redisq-redis -p 6379:6379 -d redis
# Or use your local Redis server
redis-server
```

### 2. Start the FastAPI Server
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### 3. Start the Consumer
```bash
python consumer.py
```

---

## API Example

### Submit Jobs
```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "jobs": [
      {"to": "user1@example.com", "subject": "Welcome", "body": "Hello!", "priority": 2},
      {"to": "user2@example.com", "subject": "Update", "body": "News!", "priority": 1}
    ]
  }'
```

### Check Status
```bash
curl http://localhost:8000/status
```

---

## main.py (API Server)
```python
import logging
from typing import Dict, List
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
from redisaq import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Email Queue API", description="API for queuing email jobs using redisaq", version="1.0.0")

producer = Producer(
    topic="send_email",
    redis_url="redis://localhost:6379/0",
    maxlen=10000,
    init_partitions=2,
)

@app.on_event("startup")
async def startup_event():
    await producer.connect()

@app.on_event("shutdown")
async def shutdown_event():
    await producer.close()

class EmailJob(BaseModel):
    to: str
    subject: str
    body: str
    priority: int = 1

class JobBatchRequest(BaseModel):
    jobs: List[EmailJob]

@app.post("/jobs", response_model=Dict[str, List[str]])
async def enqueue_jobs(request: JobBatchRequest):
    try:
        payloads = [job.model_dump() for job in request.jobs]
        job_ids = await producer.batch_enqueue(payloads, partition_key="priority")
        logger.info(f"Enqueued {len(job_ids)} jobs")
        return {"job_ids": job_ids}
    except Exception as e:
        logger.error(f"Error enqueueing jobs: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to enqueue jobs: {str(e)}")

@app.get("/status")
async def get_status():
    try:
        await producer.redis.ping()
        num_partitions = await producer.get_num_partitions()
        return {"status": "healthy", "redis_connected": True, "num_partitions": num_partitions, "topic": producer.topic}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "unhealthy", "redis_connected": False, "error": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, port=8080)
```

---

## consumer.py (Worker)
```python
import asyncio
import logging
from typing import List
from redisaq import Consumer, Message

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

async def send_email(to: str, subject: str, body: str) -> None:
    logger.info(f"Sending email to {to}: {subject}")
    await asyncio.sleep(1)
    logger.info(f"Email sent to {to}")

async def process_message(message: Message) -> None:
    try:
        payload = message.payload
        priority = payload.get("priority", 1)
        delay = 2 if priority > 1 else 1
        await asyncio.sleep(delay)
        await send_email(to=payload["to"], subject=payload["subject"], body=payload["body"])
        logger.info(f"Processed message {message.msg_id} (priority: {priority})")
    except Exception as e:
        logger.error(f"Error processing message {message.msg_id}: {e}", exc_info=True)
        raise

async def process_batch(messages: List[Message]) -> None:
    logger.info(f"Processing batch of {len(messages)} messages")
    tasks = [process_message(msg) for msg in messages]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    errors = [r for r in results if isinstance(r, Exception)]
    if errors:
        logger.error(f"Batch processing had {len(errors)} errors")

async def main() -> None:
    consumer = Consumer(
        topic="send_email",
        group_name="email_processors",
        consumer_name="email_worker_1",
        redis_url="redis://localhost:6379/0",
        batch_size=5,
        heartbeat_interval=3.0
    )
    try:
        logger.info("Starting email consumer...")
        await consumer.connect()
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
```

---

## Notes
- You can run multiple consumers for higher throughput.
- Partitioning is handled automatically; use `partition_key` for routing.
- The consumer demonstrates batch and single-message processing, error handling, and logging.

app = FastAPI()
producer = Producer(
    topic="send_email",
    redis_url="redis://localhost:6379/0",
    maxlen=10000,  # Limit stream size
    init_partitions=2,  # Start with 2 partitions for better distribution
)

@app.post("/jobs", response_model=Dict[str, List[str]])
async def enqueue_jobs(request: JobBatchRequest):
    """
    Submit a batch of email jobs for processing.
    Returns the IDs of the enqueued jobs.
    """
    try:
        # Convert jobs to payloads with priority-based routing
        payloads = [
            {**job.model_dump(), "enqueued_at": None}  # Will be set by producer
            for job in request.jobs
        ]

        # Enqueue jobs with partition routing by priority
        job_ids = await producer.batch_enqueue(
            payloads, partition_key="priority"  # Route by priority
        )

        logger.info(f"Enqueued {len(job_ids)} jobs")
        return {"job_ids": job_ids}

    except Exception as e:
        logger.error(f"Error enqueueing jobs: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to enqueue jobs: {str(e)}")
```

### Processing Layer (`consumer.py`)

```python
from redisaq import Consumer, Message

async def send_email(to: str, subject: str, body: str) -> None:
    logger.info(f"Sending email to {to}: {subject}")
    await asyncio.sleep(1)
    logger.info(f"Email sent to {to}")

async def process_message(message: Message) -> None:
    try:
        payload = message.payload
        priority = payload.get("priority", 1)
        delay = 2 if priority > 1 else 1
        await asyncio.sleep(delay)
        await send_email(
            to=payload["to"], subject=payload["subject"], body=payload["body"]
        )
        logger.info(f"Processed message {message.msg_id} (priority: {priority})")
    except Exception as e:
        logger.error(f"Error processing message {message.msg_id}: {e}", exc_info=True)
        raise

async def process_batch(messages: List[Message]) -> None:
    logger.info(f"Processing batch of {len(messages)} messages")
    tasks = [process_message(msg) for msg in messages]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    errors = [r for r in results if isinstance(r, Exception)]
    if errors:
        logger.error(f"Batch processing had {len(errors)} errors")

consumer = Consumer(
    topic="send_email",
    group_name="email_processors",
    consumer_name="email_worker_1",
    redis_url="redis://localhost:6379/0",
    batch_size=5,
    heartbeat_interval=3.0
)

await consumer.connect()
await consumer.consume_batch(process_batch)
```

### Key Features

#### 1. Message Routing
- Priority-based partition routing
- Configurable batch sizes
- Multiple consumer support

#### 2. Processing
- Concurrent batch processing
- Priority-based handling
- Automatic acknowledgment
- Error handling with retries

#### 3. Monitoring
- Health check endpoint
- Partition information
- Detailed logging
- Consumer heartbeats

## Usage Guide

### 1. Start Redis
```bash
# Using Docker
docker-compose up -d redis

# Or local Redis server
redis-server
```

### 2. Launch Services

1. Start API server:
```bash
# Development mode with auto-reload
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

2. Start consumer:
```bash
python consumer.py
```

### 3. Send Test Jobs

1. Single job:
```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "jobs": [{
      "to": "user@example.com",
      "subject": "Welcome",
      "body": "Hello!",
      "priority": 1
    }]
  }'
```

2. Batch of jobs:
```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "jobs": [
      {
        "to": "user1@example.com",
        "subject": "High Priority",
        "body": "Urgent message",
        "priority": 2
      },
      {
        "to": "user2@example.com",
        "subject": "Normal Priority",
        "body": "Regular update",
        "priority": 1
      }
    ]
  }'
```

### 4. Monitor System

1. Check API health:
```bash
curl http://localhost:8000/status
```

2. Monitor Redis streams:
```bash
# Check stream info
redis-cli xinfo stream redisaq:send_email:0

# View consumer groups
redis-cli xinfo groups redisaq:send_email:0

# Check pending messages
redis-cli xpending redisaq:send_email:0 email_processors
```

3. View logs:
```bash
# API logs
tail -f api.log

# Consumer logs
tail -f consumer.log
```

### 5. Cleanup
```bash
# Stop services
kill $(pgrep -f "uvicorn main:app")
kill $(pgrep -f "python consumer.py")

# Stop Redis
docker-compose down
```

## Error Handling

### API Errors
- Invalid email format: 422 Unprocessable Entity
- Redis connection issues: 500 Internal Server Error
- Invalid request format: 400 Bad Request

### Consumer Errors
- Failed jobs go to dead-letter queue
- Automatic retry for transient failures
- Detailed error logging

## Production Considerations

1. **Configuration**
   - Adjust `maxlen` based on memory requirements
   - Configure appropriate batch sizes
   - Set proper heartbeat intervals

2. **Scaling**
   - Run multiple consumers
   - Increase partition count for better distribution
   - Monitor Redis memory usage

3. **Monitoring**
   - Implement proper logging
   - Set up alerts for failed jobs
   - Monitor consumer health
   ```

## Running the Example
1. **Start Consumer** (in one terminal):
   ```bash
   python consumer.py
   ```

2. **Start FastAPI App** (in another terminal):
   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8000
   ```

3. **Test Endpoints**:
   - Enqueue jobs:
     ```bash
     curl -X POST http://localhost:8000/jobs -H "Content-Type: application/json" -d '{"messages": [{"to": "user1@example.com", "subject": "Test", "body": "Hello"}, {"to": "user2@example.com", "subject": "Test2", "body": "Hi"}]}'
     ```
     Response: `{"job_ids": ["<uuid1>", "<uuid2>"]}`
   - Check status:
     ```bash
     curl http://localhost:8000/status
     ```
     Response: `{"redis": "connected"}`

4. **Stop**:
   - Press `Ctrl+C` in both terminals to stop the app and consumer.
   - Shut down Redis:
     ```bash
     docker-compose down
     ```

## Expected Logs
- **Consumer**:
  ```
  INFO:redisaq:Consumer api_consumer registered for topic send_email, group email_group
  INFO:redisaq:Consumer api_consumer assigned partitions [0]
  INFO:redisaq:API consumer processing job <uuid1> with payload {'to': 'user1@example.com', ...}
  INFO:redisaq:API consumer completed job <uuid1>
  ```
- **FastAPI App**:
  ```
  INFO:uvicorn:127.0.0.1:12345 - "POST /jobs HTTP/1.1" 200 OK
  ```

## Verifying Behavior
Use Redis CLI to inspect:
- **Check Stream**:
  ```bash
  XLEN redisaq:send_email:0
  ```
  Output: ~1000 messages max.
- **View Jobs**:
  ```bash
  XRANGE redisaq:send_email:0 - +
  ```
- **Check Consumer Group**:
  ```bash
  XINFO GROUPS redisaq:send_email:0
  ```
  Output: Shows `email_group`.
- **Check Consumers**:
  ```bash
  XINFO CONSUMERS redisaq:send_email:0 email_group
  ```
  Output: Lists `api_consumer`.
- **Check Dead-Letter**:
  ```bash
  XRANGE redisaq:dead_letter - +
  ```

## Notes
- **Scaling**: Run multiple `consumer.py` instances with different `consumer_id`s (e.g., `api_consumer_2`) to distribute partitions.
- **Reconsumption**: Create a new group (e.g., `email_group_v2`) to reprocess jobs (see `examples/basic/`).
- **Unbounded Streams**: Modify `main.py` to use `maxlen=None`, but monitor memory (`INFO MEMORY`).
- **Monitoring**: Check consumer status via Redis:
  ```bash
  KEYS redisaq:worker:send_email:email_group:*
  ```

This example shows how `redisaq` integrates with FastAPI for job queuing and processing in separate processes.
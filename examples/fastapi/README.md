# FastAPI Integration Example for redisaq

This example demonstrates how to build a scalable email processing system using FastAPI and redisaq. It showcases advanced features like batch processing, priority-based routing, and robust error handling.

## Overview

The example implements an email processing system with two components:

### 1. API Server (`main.py`)
- **Endpoints**:
  - `POST /jobs`: Submit email jobs with priority
  - `GET /status`: Check system health and partitions
- **Features**:
  - Email validation with Pydantic
  - Priority-based message routing
  - Batch job submission
  - Comprehensive error handling
  - Health monitoring

### 2. Message Processor (`consumer.py`)
- **Processing**:
  - Concurrent batch processing
  - Priority-based handling
  - Simulated email sending
- **Features**:
  - Automatic partition assignment
  - Heartbeat monitoring
  - Graceful shutdown
  - Dead-letter queue for failed jobs
  - Detailed logging

## Installation

```bash
# Install dependencies
pip install redisaq fastapi uvicorn pydantic[email]

# Optional: Install development tools
pip install uvicorn[standard] httpx pytest
```

## Implementation Details

### API Layer (`main.py`)

```python
from fastapi import FastAPI
from pydantic import BaseModel, EmailStr
from redisaq import Producer

class EmailJob(BaseModel):
    to: EmailStr
    subject: str
    body: str
    priority: int = 1

class JobBatchRequest(BaseModel):
    jobs: List[EmailJob]

app = FastAPI()
producer = Producer(
    topic="send_email",
    maxlen=10000,
    init_partitions=2
)

@app.post("/jobs")
async def enqueue_jobs(request: JobBatchRequest):
    payloads = [
        {**job.model_dump(), "enqueued_at": None}
        for job in request.jobs
    ]
    job_ids = await producer.batch_enqueue(
        payloads,
        partition_key="priority"
    )
    return {"job_ids": job_ids}
```

### Processing Layer (`consumer.py`)

```python
from redisaq import Consumer, Message

async def process_batch(messages: List[Message]):
    logger.info(f"Processing batch of {len(messages)} messages")
    tasks = [process_message(msg) for msg in messages]
    await asyncio.gather(*tasks, return_exceptions=True)

consumer = Consumer(
    topic="send_email",
    group_name="email_processors",
    batch_size=5,
    heartbeat_interval=3.0
)

await consumer.connect()
await consumer.process_batch(process_batch)
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
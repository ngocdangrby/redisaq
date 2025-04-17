# FastAPI Integration Example for redisaq

This example demonstrates how to integrate `redisaq` with a FastAPI application to enqueue and process jobs asynchronously using Redis Streams, with the consumer running in a separate process.

## Installation
Install `redisaq` from PyPI:

```bash
pip install redisaq
```

Install FastAPI and Uvicorn:

```bash
pip install fastapi uvicorn
```

## Overview
The example simulates an email-sending API:
- **`main.py`**: A FastAPI app with:
  - **POST /jobs**: Enqueues a batch of email jobs to the `send_email` topic, with payloads like `{"to": "user1@example.com", "subject": "..."}`.
  - **GET /status**: Checks Redis connection status.
- **`consumer.py`**: A separate script running a `Consumer` for the `send_email` topic in group `email_group`. It processes jobs asynchronously, uses heartbeats (TTL 10s, interval 5s), and acknowledges jobs with `XACK`. The `process_job` function is asynchronous for non-blocking processing.
- Jobs are limited to ~1000 messages per stream (`maxlen=1000` via `XADD`).

### Key Features
- **Batch Job Submission**: Submit multiple jobs via a single API call using `batch_enqueue`.
- **Separate Consumer Process**: Consumer runs independently, allowing scaling and isolation from the FastAPI app.
- **Consumer Status**: Monitor Redis connectivity via API; consumer status checked via logs or Redis.
- **Stream Limiting**: Streams are capped at ~1000 messages to manage Redis memory.
- **Consumer Groups**: Supports reconsumption by creating a new group (not shown in this example).
- **Heartbeats**: Consumer maintains a heartbeat (`redisaq:worker:send_email:email_group:api_consumer`, TTL 10s, updated every 5s).
- **Dead-Letter Queue**: Failed jobs go to `redisaq:dead_letter`.
- **Async Processing**: Consumer `process_job` function is asynchronous, allowing non-blocking job handling.

**Warning**: Setting `maxlen=None` (unbounded streams) can lead to significant memory usage in Redis. This example uses `maxlen=1000`, but be cautious in production.

## Prerequisites
- Python 3.8+
- Redis running at `redis://localhost:6379`
- Docker (optional, for running Redis via `docker-compose`)

## Setup
1. **Start Redis**:
   Use Docker:
   ```bash
   docker-compose up -d
   ```
   Or run Redis locally:
   ```bash
   redis-server
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
# Basic Example for redisaq

This directory contains a basic example demonstrating the `redisaq` library's capabilities, including batch job production and consumption, consumer groups, reconsumption, partition rebalancing, heartbeats, and crash detection.

## Installation
Install `redisaq` from PyPI:

```bash
pip install redisaq
```

## Overview

The example simulates an email-sending system:
- **`producer.py`**: Enqueues batches of 2-5 jobs to the `send_email` topic every 0.5-2 seconds, with payloads like `{"to": "userN@example.com", "subject": "..."}`. It increases partitions (1 to 8) every 5 jobs and limits streams to approximately 1000 messages using `XADD` with `maxlen=1000`.
- **`consumer1.py`, `consumer2.py`, `consumer3.py`**: Consumers in the `email_group` consumer group, processing jobs from assigned partitions of `send_email`. They use heartbeats (TTL 10s, interval 5s) for coordination, pause/rebalance/resume when new consumers join, and acknowledge jobs with `XACK`.
- **`consumer_new_group.py`**: A consumer in a new group (`email_group_v2`) to demonstrate reconsumption of all jobs from the streams, independent of `email_group`.

### Key Features
- **Batch Production**: Producer supports `batch_enqueue` to add multiple jobs efficiently in a single operation, distributing them across partitions.
- **Consumer Groups**: Consumers read jobs via `XREADGROUP` and acknowledge with `XACK`, enabling reconsumption by creating a new group.
- **Reconsumption**: Jobs remain in streams (limited by `maxlen=1000`), allowing a new group (e.g., `email_group_v2`) to reprocess all jobs from the consume.
- **Partition Rebalancing**: Consumers self-assign partitions (round-robin) and rebalance when new consumers join or crash, with pause/resume to avoid conflicts.
- **Heartbeats**: Each consumer maintains a heartbeat key (`redisaq:worker:send_email:email_group:<consumer_id>`, TTL 10s, updated every 5s) for crash detection.
- **Crash Detection**: If a consumer's heartbeat expires after 10s, others rebalance to take over its partitions.
- **Dead-Letter Queue**: Failed jobs are sent to `redisaq:dead_letter` and acknowledged to avoid reprocessing in the same group.
- **Stream Limiting**: Producer limits streams to ~1000 messages to manage memory, configurable via `maxlen`.
- **Async Processing**: Consumer `process_job` function is asynchronous, allowing non-blocking job handling.

**Warning**: Setting `maxlen=None` (unbounded streams) can lead to significant memory usage in Redis. The example uses `maxlen=1000` to limit streams, but be cautious with unbounded streams in production.

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
1. **Start Consumers** (in separate terminals):
   ```bash
   python consumer1.py
   ```
   ```bash
   python consumer2.py
   ```

2. **Start Producer** (in another terminal):
   ```bash
   python producer.py
   ```

3. **Stop**:
   - Press `Ctrl+C` in each terminal to stop the scripts.
   - Shut down Redis:
     ```bash
     docker-compose down
     ```

### Testing Rebalancing
To test partition rebalancing:
1. While `consumer1.py` and `consumer2.py` are running, consume a third consumer:
   ```bash
   python consumer3.py
   ```
2. Observe logs showing pause, rebalance, and resume:
   - `consumer_1` and `consumer_2` pause consumption.
   - Partitions are reassigned (e.g., 4 partitions, 3 consumers: `[0,1]`, `[2]`, `[3]`).
   - Consumers resume processing.

### Testing Reconsumption
To test reconsumption with a new consumer group:
1. Stop all consumers (`Ctrl+C`).
2. Run the new group consumer:
   ```bash
   python consumer_new_group.py
   ```
3. Observe logs showing all jobs (~1000 per stream) reprocessed in `email_group_v2`.

### Verifying Behavior
Use Redis CLI to inspect the system:
- **Check Stream Length**:
  ```bash
  XLEN redisaq:send_email:0
  ```
  Output: ~1000 messages (due to `maxlen=1000`).
- **View Jobs**:
  ```bash
  XRANGE redisaq:send_email:0 - +
  ```
  Output: Recent jobs in the stream.
- **Check Consumer Groups**:
  ```bash
  XINFO GROUPS redisaq:send_email:0
  ```
  Output: Shows `email_group` and `email_group_v2` (if reconsumption tested).
- **Check Consumers**:
  ```bash
  XINFO CONSUMERS redisaq:send_email:0 email_group
  ```
  Output: Lists `consumer_1`, `consumer_2`, etc.
- **Check Dead-Letter Queue**:
  ```bash
  XRANGE redisaq:dead_letter - +
  ```
  Output: Failed jobs (if any).
- **Monitor Memory**:
  ```bash
  INFO MEMORY
  ```
  Output: Verify memory usage stays reasonable with `maxlen=1000`.

## Expected Logs
- **Producer**:
  ```
  INFO:redisaq:Enqueued batch of 3 jobs: [<uuid1>, <uuid2>, <uuid3>]
  INFO:redisaq:Increased partitions to 5
  ```
- **Consumers**:
  ```
  INFO:redisaq:Consumer consumer_1 registered for topic send_email, group email_group
  INFO:redisaq:Consumer consumer_1 assigned partitions [0, 1]
  INFO:redisaq:Consumer 1 processing job <uuid1> with payload {'to': 'user1@example.com', ...}
  ```
- **Rebalance**:
  ```
  INFO:redisaq:Consumer consumer_1 pausing consumption
  INFO:redisaq:Consumer consumer_1 assigned partitions [0, 1]
  INFO:redisaq:Consumer consumer_1 resuming consumption
  ```
- **Reconsumption**:
  ```
  INFO:redisaq:Consumer consumer_new assigned partitions [0, 1, 2, 3]
  INFO:redisaq:Consumer new_group processing job <uuid1> with payload {'to': 'user1@example.com', ...}
  ```

## Notes
- **Unbounded Streams**: To test unbounded streams, modify `producer.py` to use `maxlen=None`:
  ```python
  producer = Producer(topic="send_email", ..., maxlen=None)
  ```
  Monitor memory usage carefully (`INFO MEMORY`).
- **Dead-Letter Testing**: To simulate failed jobs, modify a consumer's `process_job` to raise an exception.
- **Scalability**: Add more consumers to see dynamic rebalancing.

This example showcases `redisaq`'s robust features for distributed job processing with Redis Streams.
# Basic Example for redisaq

This directory contains a comprehensive example demonstrating the core capabilities of the `redisaq` library, including message production, consumption, consumer groups, partition management, and fault tolerance features.

## Installation

```bash
pip install redisaq
```

## Example Components

### Producer (`producer.py`)
- Simulates an email notification system
- Features:
  - Batch message production (2-5 messages per batch)
  - Dynamic partition scaling (1 to 8 partitions)
  - Stream length management (max 10,000 messages)
  - Configurable message routing

### Multiple Consumers
- **Primary Consumers** (`consumer1.py`, `consumer2.py`, `consumer3.py`):
  - Part of `email_group` consumer group
  - Automatic partition assignment
  - Heartbeat-based health monitoring
  - Graceful rebalancing on consumer changes

- **Reprocessing Consumer** (`consumer_new_group.py`):
  - Demonstrates message reconsumption
  - Uses separate consumer group (`email_group_v2`)
  - Processes all messages independently

## Implementation Details

### Message Flow
1. **Production**:
   ```python
   # Producer sends email notifications
   messages = [
       {"to": "user1@example.com", "subject": "Welcome"},
       {"to": "user2@example.com", "subject": "Updates"}
   ]
   await producer.batch_enqueue(messages)
   ```

2. **Consumption**:
   ```python
   # Consumer processes messages
   async def process_message(message):
       print(f"Sending email to: {message.payload['to']}")
       await asyncio.sleep(1)  # Simulate email sending

   await consumer.process(process_message)
   ```

### Key Features

#### 1. Partition Management
- Dynamic partition scaling
- Automatic partition assignment
- Round-robin load balancing

#### 2. Fault Tolerance
- Heartbeat monitoring (5s interval)
- Automatic crash detection
- Graceful rebalancing
- Dead-letter queue for failed messages

#### 3. Message Processing
- Batch operations support
- Asynchronous processing
- Message acknowledgment
- Stream size management

**Warning**: Setting `maxlen=None` (unbounded streams) can lead to significant memory usage in Redis. The example uses `maxlen=1000` to limit streams, but be cautious with unbounded streams in production.

## Running the Example

### Prerequisites
- Python 3.8+
- Redis server
- Docker (optional)

### 1. Start Redis
Using Docker:
```bash
docker-compose up -d redis
```
Or locally:
```bash
redis-server
```

### 2. Run Components

1. Start the producer:
```bash
python producer.py
```

2. Start multiple consumers (in separate terminals):
```bash
python consumer1.py  # Primary consumer
python consumer2.py  # Additional consumer
python consumer_new_group.py  # Reprocessing consumer
```

### 3. Monitor Output
- Watch the producer logs for message production
- Observe consumer logs for processing and rebalancing
- Check Redis streams using redis-cli:
  ```bash
  redis-cli xinfo stream redisaq:send_email:0
  ```

### 4. Cleanup
```bash
docker-compose down  # If using Docker
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
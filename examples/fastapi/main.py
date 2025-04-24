"""
FastAPI Integration Example for redisaq: Job Queue API

This script implements a FastAPI application that serves as the API layer for an email
processing system using redisaq. It demonstrates how to integrate redisaq with FastAPI
for scalable job queuing.

Endpoints:
- POST /jobs: Submit email jobs for processing
  - Accepts batch job submissions
  - Returns job IDs for tracking
- GET /status: Check system health
  - Monitors Redis connectivity
  - Returns service status

Features:
- Batch job submission
- Input validation with Pydantic
- Redis connection management
- Stream size management (max 10,000 messages)
- Graceful startup/shutdown

Prerequisites:
- Python 3.8+
- Redis server
- Dependencies: redisaq, fastapi, uvicorn

Usage:
1. Start the API server:
   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8000 --reload
   ```

2. Submit jobs:
   ```bash
   curl -X POST http://localhost:8000/jobs \
     -H "Content-Type: application/json" \
     -d '{
       "jobs": [
         {"to": "user1@example.com", "subject": "Welcome", "body": "Hello!"},
         {"to": "user2@example.com", "subject": "Update", "body": "News!"}
       ]
     }'
   ```

3. Check status:
   ```bash
   curl http://localhost:8000/status
   ```
"""

import logging
from typing import Dict, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr

from redisaq import Producer

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Email Queue API",
    description="API for queuing email jobs using redisaq",
    version="1.0.0",
)

# Initialize producer with recommended settings
producer = Producer(
    topic="send_email",
    redis_url="redis://localhost:6379/0",
    maxlen=10000,  # Limit stream size
    init_partitions=2,  # Start with 2 partitions for better distribution
)


@app.on_event("startup")
async def startup_event():
    await producer.connect()


@app.on_event("shutdown")
async def shutdown_event():
    await producer.close()


class EmailJob(BaseModel):
    """Email job payload validation model"""

    to: EmailStr
    subject: str
    body: str
    priority: int = 1  # Optional priority field

    class Config:
        json_schema_extra = {
            "example": {
                "to": "user@example.com",
                "subject": "Welcome",
                "body": "Hello!",
                "priority": 1,
            }
        }


class JobBatchRequest(BaseModel):
    """Batch job submission request"""

    jobs: List[EmailJob]

    class Config:
        json_schema_extra = {
            "example": {
                "jobs": [
                    {
                        "to": "user1@example.com",
                        "subject": "Welcome",
                        "body": "Hello!",
                        "priority": 1,
                    }
                ]
            }
        }


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


@app.get("/status")
async def get_status():
    """
    Check the health status of the system.
    Returns the connection status and partition information.
    """
    try:
        # Check Redis connection
        await producer.redis.ping()

        # Get partition information
        num_partitions = await producer.get_num_partitions()

        return {
            "status": "healthy",
            "redis_connected": True,
            "num_partitions": num_partitions,
            "topic": producer.topic,
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "unhealthy", "redis_connected": False, "error": str(e)}

from typing import AsyncIterator
from unittest.mock import patch

import fakeredis.aioredis
import pytest_asyncio
from redis import asyncio as redis


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[redis.Redis]:
    async with fakeredis.FakeAsyncRedis(decode_responses=True) as client:
        yield client
        await client.close()


@pytest_asyncio.fixture
async def mock_aioredis_from_url(redis_client):
    """Fixture to mock aioredis.from_url for tests that call connect."""
    with patch("aioredis.from_url", return_value=redis_client) as mock_from_url:
        yield mock_from_url

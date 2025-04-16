import asyncio
import pytest
from redisq import Manager

@pytest.mark.asyncio
async def test_create_topic():
    manager = Manager(redis_url="redis://localhost:6379")
    await manager.create_topic("test_topic", num_partitions=2)
    assert "test_topic" in manager.topics
    num_partitions = await manager.queue.get_num_partitions("test_topic")
    assert num_partitions == 2
    await manager.close()

@pytest.mark.asyncio
async def test_delete_topic():
    manager = Manager(redis_url="redis://localhost:6379")
    await manager.create_topic("test_topic")
    await manager.delete_topic("test_topic")
    assert "test_topic" not in manager.topics
    await manager.close()

@pytest.mark.asyncio
async def test_configure_partitions():
    manager = Manager(redis_url="redis://localhost:6379")
    await manager.create_topic("test_topic")
    await manager.configure_partitions("test_topic", 6)
    num_partitions = await manager.queue.get_num_partitions("test_topic")
    assert num_partitions == 6
    await manager.close()

@pytest.mark.asyncio
async def test_stats():
    manager = Manager(redis_url="redis://localhost:6379")
    stats = await manager.stats()
    assert isinstance(stats, dict)
    assert "topics" in stats
    await manager.close()

@pytest.mark.asyncio
async def test_get_dead_letter():
    manager = Manager(redis_url="redis://localhost:6379")
    jobs = await manager.get_dead_letter(count=5)
    assert isinstance(jobs, list)
    await manager.close()
from logging import Logger
from typing import Optional

from aioredis import Redis

from redisaq.keys import TopicKeys


class TopicOperator:
    logger: Logger
    _topic_keys: TopicKeys
    redis: Optional[Redis] = None

    async def get_num_partitions(self) -> int:
        """Get the number of partitions for the topic."""
        if self.redis is None:
            raise RuntimeError(
                "Redis not connected. Please run connect() function first"
            )

        try:
            num_partitions = await self.redis.get(self._topic_keys.partition_key)
            if num_partitions is None:
                await self.redis.set(self._topic_keys.partition_key, 1)
                return 1

            return int(num_partitions)
        except Exception as e:
            self.logger.error(f"Error getting partitions: {e}", exc_info=e)
            raise

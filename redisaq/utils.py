from redisaq.constants import (
    APPLICATION_PREFIX,
    CONSUMER_GROUP_KEY,
    CONSUMER_KEY,
    MESSAGE_KEY,
    METADATA_KEY,
    PARTITION_KEY,
    REBALANCE_CHANNEL_KEY,
    TOPIC_KEY,
)

APPLICATION_METADATA_TOPICS = f"{APPLICATION_PREFIX}:{METADATA_KEY}:{TOPIC_KEY}"


def get_redis_key_for_topic_partition(topic: str) -> str:
    return f"{APPLICATION_PREFIX}:{PARTITION_KEY}:{topic}"


def get_redis_key_for_topic_consumer_group(topic: str) -> str:
    return f"{APPLICATION_PREFIX}:{CONSUMER_GROUP_KEY}:{topic}"


def get_redis_key_for_topic_rebalance_channel(topic: str) -> str:
    return f"{APPLICATION_PREFIX}:{REBALANCE_CHANNEL_KEY}:{topic}"


def get_redis_key_for_topic_consumer_group_consumer(
    topic: str, consumer_group: str
) -> str:
    return f"{APPLICATION_PREFIX}:{topic}:{consumer_group}:{CONSUMER_KEY}"


def get_redis_key_for_topic_partition_messages(topic: str, partition: int) -> str:
    return f"{APPLICATION_PREFIX}:{topic}:{partition}:{MESSAGE_KEY}"

from typing import Dict, Optional

from redisaq.utils import get_redis_key_for_topic_partition, get_redis_key_for_topic_consumer_group, \
    get_redis_key_for_topic_consumer_group_consumer, get_redis_key_for_topic_partition_messages, \
    get_redis_key_for_topic_rebalance_channel


class TopicConsumerGroupKeys:
    def __init__(self, topic: str, consumer_group: str):
        self.consumer_key = get_redis_key_for_topic_consumer_group_consumer(topic=topic, consumer_group=consumer_group)


class TopicPartitionKeys:
    def __init__(self, topic: str, partition: int):
        self.stream_key = get_redis_key_for_topic_partition_messages(
            topic=topic,
            partition=partition
        )


class TopicKeys:
    def __init__(self, topic: str):
        self.topic = topic
        self.partition_key = get_redis_key_for_topic_partition(topic)
        self.rebalance_channel = get_redis_key_for_topic_rebalance_channel(topic)
        self.consumer_group_key = get_redis_key_for_topic_consumer_group(topic)
        self.partition_keys: Dict[int, TopicPartitionKeys] = {}
        self.consumer_group_keys: Optional[TopicConsumerGroupKeys] = None

    def has_partition(self, partition: int) -> bool:
        return partition in self.partition_keys

    def add_partition(self, partition: int):
        self.partition_keys[partition] = TopicPartitionKeys(topic=self.topic, partition=partition)

    def set_consumer_group(self, consumer_group: str):
        self.consumer_group_keys = TopicConsumerGroupKeys(
            topic=self.topic,
            consumer_group=consumer_group,
        )

from datetime import datetime
from typing import Optional, Dict, Any, Callable, Awaitable, List
import uuid

from redisaq.errors import PartitionKeyError


class Message:
    def __init__(
        self,
        topic: str,
        payload: Dict[str, Any],
        partition_key: str = "",
        msg_id: Optional[str] = None,
        created_at: Optional[int] = None,
        enqueued_at: Optional[int] = None,
        timeout: float = 0,
        partition: int = None
    ):
        self.msg_id = msg_id or str(uuid.uuid4())
        self.topic = topic
        self.payload = payload
        self.partition_key = partition_key
        self.created_at = created_at or int(datetime.utcnow().timestamp())
        self.enqueued_at = enqueued_at
        self.timeout = timeout
        self.partition = partition
        self.stream: Optional[str] = None

        if self.partition_key and self.partition_key not in self.payload:
            raise PartitionKeyError(f"partition key `{self.partition_key}` is not in payload")

    def to_dict(self):
        return {
            "msg_id": self.msg_id,
            "topic": self.topic,
            "payload": self.payload,
            "partition_key": self.partition_key,
            "partition": self.partition,
            "created_at": self.created_at,
            "enqueued_at": self.enqueued_at,
            "timeout": self.timeout
        }

    @classmethod
    def from_dict(cls, data: Dict):
        created_at = int(data.get("created_at", datetime.utcnow().timestamp()))
        partition = int(data.get("partition", None)) if data.get("partition", None) is not None else None
        return cls(
            msg_id=data.get("msg_id"),
            topic=data.get("topic", ""),
            payload=data.get("payload", {}),
            partition_key=data.get("partition_key", ""),
            partition=partition,
            created_at=created_at,
            enqueued_at=int(data.get("enqueued_at", None)),
            timeout=float(data.get("timeout", 0))
        )

    def get_partition(self) -> Optional[int]:
        return None


SingleCallback = Callable[[Message], Awaitable]
BatchCallback = Callable[[List[Message]], Awaitable]

import json
from datetime import datetime
from typing import Optional
import uuid

class Job:
    def __init__(
        self,
        id: Optional[str] = None,
        topic: str = "",
        payload: str = "",
        created_at: Optional[datetime] = None,
        timeout: int = 0
    ):
        self.id = id or str(uuid.uuid4())
        self.topic = topic
        self.payload = payload
        self.created_at = created_at or datetime.utcnow()
        self.timeout = timeout

    def to_dict(self):
        return {
            "id": self.id,
            "topic": self.topic,
            "payload": self.payload,
            "created_at": self.created_at.isoformat(),
            "timeout": self.timeout
        }

    @classmethod
    def from_dict(cls, data: dict):
        created_at = datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.utcnow()
        return cls(
            id=data.get("id"),
            topic=data.get("topic", ""),
            payload=data.get("payload", ""),
            created_at=created_at,
            timeout=int(data.get("timeout", 0))
        )
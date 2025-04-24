from typing import Any, Callable

Serializer = Callable[..., str]
Deserializer = Callable[..., Any]

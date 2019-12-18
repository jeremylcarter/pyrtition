from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass()
class TopicMessage:
    producer_name: str
    timestamp: datetime
    data: Any

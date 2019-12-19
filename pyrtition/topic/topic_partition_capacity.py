from dataclasses import dataclass


@dataclass
class TopicPartitionCapacity:
    number: int
    producers: int

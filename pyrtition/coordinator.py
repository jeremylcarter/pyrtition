from queue import Queue
from threading import RLock
from typing import Dict, Callable, Optional

from pyrtition.topic.topic_coordinator import TopicCoordinator
from pyrtition.topic.topic_message import TopicMessage


class Coordinator:
    topics: Dict[str, TopicCoordinator]

    _lock: RLock

    def __init__(self):
        self.topics = dict()
        self._lock = RLock()

    def get_topic(self, topic_name: str) -> Optional[TopicCoordinator]:
        if topic_name in self.topics:
            return self.topics[topic_name]
        return None

    def get_or_create_topic(self, topic_name: str, max_partition_count: int = 4) -> TopicCoordinator:
        if topic_name in self.topics:
            return self.topics[topic_name]
        new_topic: TopicCoordinator
        try:
            self._lock.acquire()
            new_topic = TopicCoordinator(topic_name, max_partition_count)
            self.topics[topic_name] = new_topic
        finally:
            self._lock.release()
        return new_topic

    def publish(self, topic_name, producer_name, value: any, create_if_not_exists: bool = False) -> bool:
        if create_if_not_exists:
            topic = self.get_or_create_topic(topic_name)
        else:
            topic = self.get_topic(topic_name)
        if topic:
            return topic.publish(producer_name, value)
        else:
            return False

    def get_topic_partition_queue(self, topic_name: str, partition: int) -> Queue:
        topic = self.get_or_create_topic(topic_name)
        return topic.get_queue(partition)

    def create_and_start_consuming(self, topic_name: str, max_partition_count: int = 4,
                                   on_message: Callable[[TopicMessage, int, int], None] = None) -> TopicCoordinator:
        topic = self.get_or_create_topic(topic_name, max_partition_count)
        if on_message:
            topic.start_consuming(on_message)
        return topic

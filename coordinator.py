from queue import Queue
from threading import RLock
from typing import Dict

from consumer.threaded_topic_consumer import ThreadedTopicConsumer
from topic.topic_coordinator import TopicCoordinator


class Coordinator:
    topics: Dict[str, TopicCoordinator]

    _lock: RLock

    def __init__(self):
        self.topics = dict()
        self._lock = RLock()

    def get_or_create_topic(self, topic_name: str, max_partition_count: int = 4) -> TopicCoordinator:
        if topic_name in self.topics:
            return self.topics[topic_name]
        try:
            self._lock.acquire()
            new_topic = TopicCoordinator(topic_name, max_partition_count)
            self.topics[topic_name] = new_topic
        finally:
            self._lock.release()

    def publish(self, topic_name, producer_name, value: any) -> bool:
        topic = self.get_or_create_topic(topic_name)
        return topic.publish(producer_name, value)

    def get_topic_partition_queue(self, topic_name: str, partition: int) -> Queue:
        topic = self.get_or_create_topic(topic_name)
        return topic.get_queue(partition)

    def create_threaded_consumer(self, topic_name: str) -> ThreadedTopicConsumer:
        return ThreadedTopicConsumer.from_topic_name(self, topic_name)

from threading import Thread
from typing import Dict, Callable

from consumer.topic_partition_consumer_thread import TopicPartitionConsumerThread
from coordinator import Coordinator
from topic.topic_coordinator import TopicCoordinator
from topic.topic_message import TopicMessage


class ThreadedTopicConsumer:
    _topic: TopicCoordinator
    _threads: Dict[int, TopicPartitionConsumerThread]

    on_message: Callable[[TopicMessage, int], None]

    def __init__(self, topic: TopicCoordinator):
        self._topic = topic
        self._threads = dict()

    @staticmethod
    def from_topic_name(coordinator: Coordinator, topic_name: str):
        return ThreadedTopicConsumer(topic=coordinator.get_or_create_topic(topic_name))

    def start(self):
        if self.on_message is None:
            raise Exception("on_message callback cannot be None")

        for partition_number in self._topic.partitions:
            thread = TopicPartitionConsumerThread(self._topic, partition_number, self.on_message)
            self._threads[partition_number] = thread

        for thread in self._threads.values():
            thread.start()

    def stop(self):
        for thread in self._threads.values():
            thread.stop()
            thread.join()
            del thread

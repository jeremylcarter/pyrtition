from threading import Thread
from typing import Dict, Callable

from pyrtition.consumer.topic_partition_consumer_thread import TopicPartitionConsumerThread
from pyrtition.topic.topic_coordinator import TopicCoordinator
from pyrtition.topic.topic_message import TopicMessage


class ThreadedTopicConsumer:
    _topic: TopicCoordinator
    _threads: Dict[int, TopicPartitionConsumerThread]

    on_message: Callable[[TopicMessage, int, int], None]

    def __init__(self, topic: TopicCoordinator):
        self._topic = topic
        self._threads = dict()

    def start(self):
        for partition in self._topic.partitions.values():
            thread = TopicPartitionConsumerThread(partition, self.on_message)
            self._threads[partition.number] = thread

        for thread in self._threads.values():
            thread.start()

    def stop(self):
        for thread in self._threads.values():
            thread.stop()
            thread.join()
            del thread

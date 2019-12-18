import threading
from queue import Queue
from typing import Callable, cast

from topic.topic_coordinator import TopicCoordinator
from topic.topic_message import TopicMessage


class TopicPartitionConsumerThread(threading.Thread):
    name: str
    partition: int
    _topic: TopicCoordinator
    _queue: Queue
    _on_message: Callable[[TopicMessage, int], None]
    running: bool = False

    def __init__(self, topic: TopicCoordinator, partition_number: int, on_message: Callable[[TopicMessage, int], None]):
        super(TopicPartitionConsumerThread, self).__init__()
        self.setDaemon(True)
        self.name = f"{topic.name}-{partition_number}"
        self._topic = topic
        self._queue = topic.get_queue(partition_number)
        self._on_message = on_message
        self.partition = partition_number
        self.running = False

    def run(self) -> None:
        print(f"Running thread {self.name}")
        self.running = True
        while self.running:
            if not self._queue.empty():
                message = cast(TopicMessage, self._queue.get())
                if message:
                    self._on_message(message, self.partition)

    def stop(self):
        self.running = False

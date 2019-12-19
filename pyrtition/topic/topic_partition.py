from datetime import datetime
from queue import Queue
from threading import RLock
from typing import Set, Any, Optional, Callable

from pyrtition.topic.topic_message import TopicMessage


class TopicPartition:
    name: str
    number: int
    producer_count: int
    producers: Set[str]
    _queue: Queue
    _lock: RLock
    _consumer_thread: Any = None

    def __init__(self, name: str, number: int):
        self.name = name
        self.number = number
        self.producer_count = 0
        self.producers = set()
        self._queue = Queue()
        self._lock = RLock()

    def dequeue(self) -> Optional[TopicMessage]:
        if not self._queue.empty():
            return self._queue.get()

    def is_queue_empty(self) -> bool:
        return self._queue.empty()

    def get_queue(self) -> Queue:
        return self._queue

    def has_producer(self, producer_name):
        return producer_name in self.producers

    def assign_producer(self, producer_name) -> int:
        self._lock.acquire()
        try:
            self.producers.add(producer_name)
            self.producer_count = len(self.producers)
            return self.producer_count
        finally:
            self._lock.release()

    def put_value(self, producer_name: str, data: Any):
        if not self.has_producer(producer_name):
            raise Exception(f"Producer {producer_name} is not a member of this partition")

        message = TopicMessage(producer_name=producer_name, timestamp=datetime.utcnow(), data=data)
        self._queue.put(message)

        # Notify any consumers
        if self._consumer_thread:
            self._consumer_thread.notify()

    def stop_consuming(self):
        if self._consumer_thread:
            self._consumer_thread.stop()

    def start_consuming(self, on_message: Callable[[TopicMessage, int, int], None] = None):
        # We can only consume if we have an on_message callable
        if not on_message:
            return

        from pyrtition.consumer.topic_partition_consumer_thread import TopicPartitionConsumerThread
        consumer_thread = TopicPartitionConsumerThread(self, on_message)
        consumer_thread.start()
        self._consumer_thread = consumer_thread

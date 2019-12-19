import threading
import time
from queue import Queue
from typing import Callable, cast, Optional

from pyrtition.topic.topic_message import TopicMessage
from pyrtition.topic.topic_partition import TopicPartition


class TopicPartitionConsumerThread(threading.Thread):
    number: int
    running: bool = False

    _stop_event: threading.Event
    _signal = threading.Condition
    _topic_partition: TopicPartition
    _queue: Queue
    _on_message: Callable[[TopicMessage, int, int], None] = None
    _thread_id: int

    def __init__(self, topic_partition: TopicPartition,
                 on_message: Optional[Callable[[TopicMessage, int, int], None]] = None):
        super(TopicPartitionConsumerThread, self).__init__()
        self.setDaemon(True)
        self.setName(f"{topic_partition.name}-{topic_partition.number}")
        self._signal = threading.Condition()
        self._topic_partition = topic_partition
        self._queue = topic_partition.get_queue()
        self.number = topic_partition.number
        self.running = False
        if on_message:
            self._on_message = on_message

    def notify(self):
        try:
            with self._signal:
                self._signal.notify()
        except RuntimeError as ex:
            pass

    def run(self) -> None:
        self.thread_id = threading.get_ident()
        print(f"Running thread {self.name} ({self.thread_id})")
        self.running = True

        while self.running:
            self._signal.acquire()
            try:
                while self._queue.empty():
                    self._signal.wait()
                message = cast(TopicMessage, self._queue.get())
                if message and self._on_message:
                    try:
                        self._on_message(message, self.number, self.thread_id)
                    except Exception as ex:
                        # TODO add logging
                        pass
            finally:
                self._signal.release()

    def stop(self):
        self.running = False

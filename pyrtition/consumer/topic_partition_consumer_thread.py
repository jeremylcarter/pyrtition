import threading
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
    _use_signals: bool = False

    def __init__(self, topic_partition: TopicPartition,
                 on_message: Optional[Callable[[TopicMessage, int, int], None]] = None,
                 use_signals: bool = False):
        super(TopicPartitionConsumerThread, self).__init__()
        self.setDaemon(True)
        self.setName(f"{topic_partition.topic_name}-{topic_partition.number}")
        self._use_signals = use_signals
        self._signal = threading.Condition()
        self._topic_partition = topic_partition
        self._queue = topic_partition.get_queue()
        self.number = topic_partition.number
        self.running = False
        if on_message:
            self._on_message = on_message

    def notify(self):
        if self._use_signals:
            try:
                with self._signal:
                    self._signal.notify()
            except RuntimeError as ex:
                # We have tried to notify the signal when it is being re-acquired
                pass

    def run(self) -> None:
        self.thread_id = threading.get_ident()
        self.running = True

        if self._use_signals:
            self.with_with_signals()
        else:
            self.run_without_signals()

    def run_without_signals(self):
        while self.running:
            message = self._queue.get()
            if message and self._on_message:
                try:
                    self._on_message(message, self.number, self.thread_id)
                except Exception as ex:
                    # TODO add logging
                    print(ex)
                    pass
            self._queue.task_done()

    def with_with_signals(self):
        while self.running:
            self._signal.acquire()
            try:
                while self._queue.empty():
                    self._signal.wait()
                message = self._queue.get()
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


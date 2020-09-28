import random
import logging
from queue import Queue
from threading import RLock
from typing import Dict, List, Any, Optional, Callable

from pyrtition.topic.topic_message import TopicMessage
from pyrtition.topic.topic_partition import TopicPartition
from pyrtition.topic.topic_partition_capacity import TopicPartitionCapacity


class TopicCoordinator:
    name: str
    max_partition_count: int

    partitions: Dict[int, TopicPartition]
    _producer_cache: Dict[str, int]
    _lock: RLock

    def __init__(self, name="default", max_partition_count: int = 4):
        self.name = name
        self.max_partition_count = max_partition_count
        self._lock = RLock()
        self.__partition()

    def __partition(self):
        self.partitions = dict()
        self.producer_cache = dict()
        for i in range(self.max_partition_count):
            self.__create_partition(i + 1)

    def __create_partition(self, number: int) -> bool:
        partition = TopicPartition(self.name, number)
        self.partitions[number] = partition
        return True

    def __assign_new_producer_to_partition(self, producer_name: str) -> int:
        self._lock.acquire()
        try:
            next_available_partition = self.__get_next_available_partition()
            if next_available_partition is not None:
                partition = self.partitions[next_available_partition]
                partition.assign_producer(producer_name)
                self.producer_cache[producer_name] = partition.number
                return partition.number
        finally:
            self._lock.release()

    def __get_next_available_partition(self):
        capacities = self.get_capacity()
        try:
            # Sort them in descending order
            capacities.sort(key=lambda c: c.producers)

            # If there is only 1 available then just return it
            if len(capacities) == 1:
                return capacities[0].number

            # Pick the next available zero capacity partition
            next_available_zero_capacity = next((c for c in capacities if c.producers == 0), None)
            if next_available_zero_capacity is not None:
                return next_available_zero_capacity.number

            # Either pick the lowest available partition or a random one
            pick_lowest = random.getrandbits(1)
            if pick_lowest:
                return capacities[0].number
            else:
                # Pick a random low capacity topic
                next_available_random = random.randint(0, 3)
                if len(capacities) <= next_available_random:
                    return capacities[next_available_random].number

            # As a last resort just return the first partition
            return capacities[0].number
        except Exception as ex:
            logging.exception(ex)
            # As a last resort just return the first partition
            return capacities[0].number

    def get_producer_partition(self, producer_name: str) -> int:
        if producer_name in self.producer_cache:
            return self.producer_cache[producer_name]
        raise Exception(f"Producer {producer_name} is not in topic {self.name}")

    def get_or_add_producer_partition(self, producer_name: str) -> int:
        if producer_name in self.producer_cache:
            return self.producer_cache[producer_name]
        else:
            return self.__assign_new_producer_to_partition(producer_name)

    def publish(self, producer_name: str, data: Optional[Any]) -> bool:
        assigned_to = self.get_or_add_producer_partition(producer_name)

        if data is not None and assigned_to > 0:
            self.partitions[assigned_to].put_value(producer_name, data)
            return True
        return False

    def is_queue_empty(self, partition: int) -> bool:
        if self.partitions[partition]:
            return self.partitions[partition].is_queue_empty()

    def dequeue(self, partition: int) -> Any:
        if self.partitions[partition]:
            return self.partitions[partition].dequeue()

    def get_queue(self, partition: int) -> Queue:
        if self.partitions[partition]:
            return self.partitions[partition].get_queue()

    def get_capacity(self) -> List[TopicPartitionCapacity]:
        return list([TopicPartitionCapacity(partition.number, partition.producer_count)
                     for partition in self.partitions.values()])

    def start_consuming(self, on_message: Callable[[TopicMessage, int, int], None] = None, use_signals: bool = False):
        for partition in self.partitions.values():
            partition.start_consuming(on_message, use_signals)

    def stop_consuming(self):
        for partition in self.partitions.values():
            partition.stop_consuming()

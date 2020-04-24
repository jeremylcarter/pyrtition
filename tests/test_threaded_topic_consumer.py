import time
from unittest import TestCase

from pyrtition.topic.topic_coordinator import TopicCoordinator
from pyrtition.topic.topic_message import TopicMessage


class TestThreadedTopicConsumer(TestCase):
    def test_start(self):
        topic_coordinator = TopicCoordinator("test", 4)
        topic_coordinator.start_consuming(on_message, False)

        producers = 10
        for i in range(0, producers):
            topic_coordinator.publish(f"producer-{i}", i)
        for i in range(0, producers):
            topic_coordinator.publish(f"producer-{i}", i)
        time.sleep(1)
        topic_coordinator.stop_consuming()


def on_message(message: TopicMessage, partition_number: int, thread_id: int):
    print(message)
    print(partition_number)
    print(thread_id)

import threading
import time
from unittest import TestCase

from consumer.threaded_topic_consumer import ThreadedTopicConsumer
from topic.topic_coordinator import TopicCoordinator
from topic.topic_message import TopicMessage


class TestThreadedTopicConsumer(TestCase):
    def test_start(self):
        topic_coordinator = TopicCoordinator()
        consumer = ThreadedTopicConsumer(topic_coordinator)
        consumer.on_message = on_message

        producers = 1000
        for i in range(0, producers):
            topic_coordinator.publish(f"producer-{i}", i)

        consumer.start()
        time.sleep(4)
        consumer.stop()


def on_message(message: TopicMessage, partition_number: int):
    print(message)

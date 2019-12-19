from unittest import TestCase

from pyrtition.topic.topic_coordinator import TopicCoordinator


class TestTopicCoordinator(TestCase):

    def test_get_or_add_producer(self):
        topic_coordinator = TopicCoordinator()

        producers = 1000

        for i in range(0, producers):
            topic_coordinator.publish(f"producer-{i}", i)

        capacities = topic_coordinator.get_capacity()
        total_producers = sum(capacity.producers for capacity in capacities)

        dequeued = 0
        for partition in topic_coordinator.partitions.values():
            while not partition.is_queue_empty():
                partition.dequeue()
                dequeued += 1

        self.assertEqual(producers, total_producers)
        self.assertEqual(dequeued, total_producers)

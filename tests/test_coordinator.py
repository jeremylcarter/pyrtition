from unittest import TestCase

from coordinator import Coordinator


class TestCoordinator(TestCase):
    def test_get_or_create_topic(self):
        coordinator = Coordinator()
        coordinator.get_or_create_topic("topic")

        is_in_topics = "topic" in coordinator.topics
        self.assertTrue(is_in_topics)

    def test_publish(self):
        coordinator = Coordinator()
        coordinator.get_or_create_topic("topic")

        is_in_topics = "topic" in coordinator.topics
        self.assertTrue(is_in_topics)

        coordinator.publish("topic", "test", 1)
        assigned = coordinator.topics["topic"].get_producer_partition("test")
        self.assertTrue(assigned > 0)

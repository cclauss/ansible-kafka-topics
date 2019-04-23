#! /usr/bin/env python

import unittest

class TestValidateClass(unittest.TestCase):
    def validate_name_test(self):
        import kafka_topic
        kafka_topic.validate_name("topic.topic")

class TestKafkaClass(unittest.TestCase):


class TestAnsibleClass(unittest.TestCase):

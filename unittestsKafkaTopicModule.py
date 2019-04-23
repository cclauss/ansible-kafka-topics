#! /usr/bin/env python

import unittest
import mock

class TestValidateClass(unittest.TestCase):
    # validate_name
    def validate_name_test(self):
        import kafka_topic
        kafka_topic.validate_name(name="topic.topic")

    def validate_name_fail(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        kafka_topic.validate_name(name="topic:topic")
        mo.fail_module.assert_called()

    # validate_factor
    def validate_factor_test(self):
        import kafka_topic
        kafka_topic.validate_factor(factor=3,part_or_rep="partition")

    def validate_factor_fail(self).
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        kafka_topic.validate_factor(factor=2.1, part_or_rep="partition")
        mo.fail_module.assert_called()

    # validate_broker
    def validate_broker_test(self):
        import kafka_topic
        broker = validate_broker(broker_definition=[localhost:9092])
        self.assertEqual(broker,"localhost:9092")

    def validate_broker_fail(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        broker = validate_broker(broker_definition=[12:9092])
        mo.fail_module.assert_called()

    # validate_ipv4
    def validate_ipv4_test(self):
        import kafka_topic
        broker = kafka_topic.validate_ipv4(broker=[127.0.0.3:9092])
        self.assertEqual(broker, "127.0.0.3:9092")

    def validate_ipv4_fail(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        broker = kafka_topic.validate_ipv4(broker=[localhost:9095])
        mo.fail_module.assert_called()

    # validate_port
    def validate_port_test(self):
        import kafka_topic
        port = kafka_topic.validate_port(port="3454")
        self.assertEqual(port,3454)

    def validate_port_fail(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        port = kafka_topic.validate_port(port="70123")
        mo.fail_module.assert_called()

    # validate_retention_ms
    def validate_retention_ms_test(self):
        import kafka_topic
        retention = kafka_topic.validate_retention_ms(retention="7d")
        self.assertEqual(retention, 604800000)

    def validate_retention_ms_fail(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        retention = kafka_topic.validate_retention_ms(retention="-7d")
        mo.fail_module.assert_called()

class TestKafkaClass(unittest.TestCase):


class TestAnsibleClass(unittest.TestCase):

#! /usr/bin/env python

import unittest
import mock
from confluent_kafka.admin import AdminClient
from ansible.module_utils.basic import AnsibleModule

class TestValidateClass(unittest.TestCase):
    # validate_name
    def test_validate_name(self):
        import kafka_topic
        kafka_topic.validate_name(name="topic.topic")

    def test_validate_name_fail(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        kafka_topic.validate_name(name="topic:topic")
        mo.fail_module.assert_called()

    # validate_factor
    def test_validate_factor(self):
        import kafka_topic
        kafka_topic.validate_factor(factor=3,part_or_rep="partition")

    def test_validate_factor_fail(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        kafka_topic.validate_factor(factor=2.1, part_or_rep="partition")
        mo.fail_module.assert_called()

    # validate_broker
    def test_validate_broker(self):
        import kafka_topic
        broker = validate_broker(broker_definition=["localhost:9092"])
        self.assertEqual(broker,"localhost:9092")

    def test_validate_broker_fail(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        broker = validate_broker(broker_definition=["12:9092"])
        mo.fail_module.assert_called()

    # validate_ipv4
    def test_validate_ipv4(self):
        import kafka_topic
        broker = kafka_topic.validate_ipv4(broker=["127.0.0.3:9092"])
        self.assertEqual(broker, "127.0.0.3:9092")

    def test_validate_ipv4_fail(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        broker = kafka_topic.validate_ipv4(broker=["localhost:9095"])
        mo.fail_module.assert_called()

    # validate_port
    def test_validate_port(self):
        import kafka_topic
        port = kafka_topic.validate_port(port="3454")
        self.assertEqual(port,3454)

    def test_validate_port_fail(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        port = kafka_topic.validate_port(port="70123")
        mo.fail_module.assert_called()

    # validate_retention_ms
    def test_validate_retention_ms(self):
        import kafka_topic
        retention = kafka_topic.validate_retention_ms(retention="7d")
        self.assertEqual(retention, 604800000)

    def test_validate_retention_ms_fail(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        retention = kafka_topic.validate_retention_ms(retention="-7d")
        mo.fail_module.assert_called()

class TestKafkaClass(unittest.TestCase):

    global admin
    admin = AdminClient({'bootstrap.servers':'localhost:9092'})

    # check_topic
    def test_check_topic_true(self):
        import kafka_topic
        topic = kafka_topic.check_topic(topic="foo.two")
        self.assertTrue(topic)

    def test_check_topic_false(self):
        import kafka_topic
        topic = kafka_topic.check_topic(topic="foo")
        self.assertFalse(topic)

    # compare_part_rep
    def test_compare_part_rep_true(self):
        import kafka_topic
        change = kafka_topic.compare_part_rep(topic="foo.two", partitions=3, replication_factor=1)
        self.assertTrue(change)

    def test_compare_part_rep_fail(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        change = kafka_topic.compare_part_rep(topic="foo.two", partitions=3, replication_factor=2)
        mo.fail_module.assert_called()

    # compare_config
    def test_compare_config_true(self):
        import kafka_topic
        change = kafka_topic.compare_config(topic="foo.two", new_config={retention.ms:90001})
        self.assertTrue(change)

    def test_compare_config_false(self):
        import kafka_topic
        change = kafka_topic.compare_config(topic="foo.two", new_config={retention.ms:604800000})


    # modify_config
    def test_modify_config(self):
        import kafka_topic
        kafka_topic.modify_config(topic="foo.two", new_config={retention.ms:604800000})

    def test_modify_config_fail(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        kafka_topic.modify_config(topic="foo.two", new_config={cleanup.policy:True})
        mo.fail_module.assert_called()

    # create_topic
    def test_create_topic(self):
        import kafka_topic
        kafka_topic.create_topic(topic="foobar", partitions=2, replication_factor=2)

    def test_create_topic_fail(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        kafka_topic.create_topic(topic="foobarbar", partitions=0, replication_factor=2)
        mo.fail_module.assert_called()

    # delete_topic
    def test_delete_topic(self):
        import kafka_topic
        kafka_topic.delete_topic(topic="foobar")

    def test_delete_topic_fail(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        kafka_topic.delete_topic(topic="foobarbar")
        mo.fail_module.assert_called()

    # add_config_together
    def test_add_config_together(self):
        import kafka_topic
        kafka_topic.add_config_together(module)

class TestAnsibleClass(unittest.TestCase):
#
#    global module
#    module = AnsibleModule(
#        argument_spec = dict(
#              name=dict(type='str', required=True)
#          )
#    )
#
    # fail_module
    def test_fail_module(self):
        import kafka_topic
        kafka_topic.fail_module(message="Test is successfull")

if __name__ == '__main__':
    unittest.main()



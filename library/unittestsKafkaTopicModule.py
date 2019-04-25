#! /usr/bin/env python

import unittest
import mock
from confluent_kafka.admin import AdminClient
from ansible.module_utils.basic import AnsibleModule
#import config

class TestValidateClass(unittest.TestCase):
    # validate_name
    def test_validate_name_TVF1(self):
        import kafka_topic
        kafka_topic.validate_name(name="topic.topic")

    def test_validate_name_fail_NTVF1(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        self.assertRaises(Exception, kafka_topic.validate_name, name="topic:topic")
        mo.fail_module.assert_called()

    # validate_factor
    def test_validate_factor_TVF2(self):
        import kafka_topic
        kafka_topic.validate_factor(factor=3,part_or_rep="partition")

    def test_validate_factor_fail_NTFV2(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        self.assertRaises(Exception, kafka_topic.validate_factor, factor=2.1, part_or_rep="partition")
        mo.fail_module.assert_called()

    # validate_broker
    def test_validate_broker_TVF3_1(self):
        import kafka_topic
        broker = kafka_topic.validate_broker(broker_definition=["localhost:9092"])
        self.assertEqual(broker,"localhost:9092")

    def test_validate_broker_fail_NTVF3_1(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        self.assertRaises(Exception, kafka_topic.validate_broker, broker_definition=["12:9092"])
        mo.fail_module.assert_called()

    # validate_ipv4
    def test_validate_ipv4_TVF3_2(self):
        import kafka_topic
        broker = kafka_topic.validate_ipv4(broker=["127.0.0.3","9092"])
        self.assertEqual(broker, "127.0.0.3:9092")

    def test_validate_ipv4_fail_NTVF3_2(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        self.assertRaises(Exception, kafka_topic.validate_ipv4, broker=["localhost","9095"])
        mo.fail_module.assert_called()

    # validate_port
    def test_validate_port_TVF3_3(self):
        import kafka_topic
        port = kafka_topic.validate_port(port="3454")
        self.assertEqual(port,3454)

    def test_validate_port_fail_NTVF3_3(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        self.assertRaises(Exception, kafka_topic.validate_port, port="70123")
        mo.fail_module.assert_called()

    # validate_retention_ms
    def test_validate_retention_ms_TVF4(self):
        import kafka_topic
        retention = kafka_topic.validate_retention_ms(retention="7d")
        self.assertEqual(retention, 604800000)

    def test_validate_retention_ms_fail_NTVF4(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        self.assertRaises(Exception, kafka_topic.validate_retention_ms, retention="-7d")
        mo.fail_module.assert_called()

class TestKafkaClass(unittest.TestCase):

    import kafka_topic
    global admin
    admin = AdminClient({'bootstrap.servers':'localhost:9092'})
    kafka_topic.admin = admin

    # check_topic
    def test_check_topic_true_TKF1(self):
        import kafka_topic
        kafka_topic.admin = admin
        topic = kafka_topic.check_topic(topic="foo.two")
        self.assertTrue(topic)

    def test_check_topic_false_NTKF1(self):
        import kafka_topic
        topic = kafka_topic.check_topic(topic="foo")
        self.assertFalse(topic)

    # compare_part_rep
    def test_compare_part_rep_true_TKF2(self):
        import kafka_topic
        change = kafka_topic.compare_part_rep(topic="foo.two", partitions=3, replication_factor=1)
        self.assertTrue(change)

    def test_compare_part_rep_fail_NTKF2(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        self.assertRaises(Exception, kafka_topic.compare_part_rep, topic="foo.two", partitions=3, replication_factor=2)
        mo.fail_module.assert_called()

    # compare_config
    def test_compare_config_true_TKF3(self):
        import kafka_topic
        change = kafka_topic.compare_config(topic="foo.two", new_config={"retention.ms":"90001"})
        self.assertTrue(change)

    def test_compare_config_false_NTKF3(self):
        import kafka_topic
        change = kafka_topic.compare_config(topic="foo.two", new_config={"retention.ms":"604800000"})
        self.assertFalse(change)


    # modify_config
    def test_modify_config_TKF4(self):
        import kafka_topic
        kafka_topic.modify_config(topic="bar.two", new_config={"retention.ms":"604800000"})

    def test_modify_config_fail_NTKF4(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        self.assertRaises(Exception, kafka_topic.modify_config, topic="foo.two", new_config={"cleanup.policy":"True"})
        mo.fail_module.assert_called()


    # modify_part
    def test_modify_part_TKF5(self):
        import kafka_topic
        kafka_topic.modify_part(topic="bar.two", new_part = 3)

    def test_modify_part_fail_NTKF5(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        self.assertRaises(Exception, kafka_topic.modify_part, topic="foo.two", new_part = 2)
        mo.fail_module.assert_called()


    # create_topic
    def test_create_topic_TKF6(self):
        import kafka_topic
        kafka_topic.create_topic(topic="foo", partitions=2, replication_factor=2)

#    def test_create_topic_fail_NTKF6(self):
#        import kafka_topic
#        mo = mock.Mock()
#        mo.fail_module()
#        self.assertRaises(Exception, kafka_topic.create_topic, topic="foo", partitions=0, replication_factor=2)
#        mo.fail_module.assert_called()

    # delete_topic
    def test_delete_topic_TKF7(self):
        import kafka_topic
        kafka_topic.delete_topic(topic="foo.two")

    def test_delete_topic_fail_NTKF7(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        self.assertRaises(Exception, kafka_topic.delete_topic, topic="foobarbar")
        mo.fail_module.assert_called()

    # add_config_together
#    def test_add_config_together_TKF8(self):
#        import kafka_topic
#        self.assertRaises(Exception, kafka_topic.add_config_together, module)

class TestAnsibleClass(unittest.TestCase):
    # fail_module
    def test_fail_module_TAK1(self):
        import kafka_topic
        self.assertRaises(Exception, kafka_topic.fail_module, msg="Test is successfull")

if __name__ == '__main__':
    unittest.main()



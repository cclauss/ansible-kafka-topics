#! /usr/bin/env python

import unittest
import mock
from confluent_kafka.admin import AdminClient
from ansible.module_utils.basic import AnsibleModule
#import config

class TestValidationClass(unittest.TestCase):
    # validate_name
    def test_validate_name_TVF1(self):
        import kafka_topic
        kafka_topic.validate_name(name="topic.topic")

    def test_validate_name_fail_NTVF1(self):
        import kafka_topic
        mo = mock.Mock()
        mo.fail_module()
        self.assertRaises(Exception, kafka_topic.validate_name, name="topic:topic")
        mo.fail_module.assert_not_called()


if __name__ == '__main__':
    unittest.main()



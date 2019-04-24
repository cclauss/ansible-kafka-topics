#! /usr/bin/env python

# Copyright: (c) 2019, Johanna Koch
# This software is made available to you under the terms of the Apache2.0 license.
# Apache License v2.0
# See LICENSE.txt for details.

ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'community'}

DOCUMENTATION = '''
---
module: kafka_topic

author:
  - Johanna Koch (@post.ch)

short_description: manage kafka-topics

version_added: "2.7"

description:
  - create, delete and modify kafka-topics

options:
  name:
    description:
      - Unique name for topic by which it will be identified.
      - Valid characters are: a-z, A-Z, ".", "-", "_"
      - Also, the topic must be conform to the ISVC-Topic-Grammar.
    required: true
    type: str
  state:
    description:
      - If set to "absent", topic will be deleted if present.
      - If set to "present", topic will be created if not present.
    required: true
    type: str
    choices: [ absent, present ]
  partitions:
    description:
      - How many partitions are created for the topic.
      - Partitions can not be set to zero or negative.
    required: true
    type: int
  replication_factor:
    description:
      - How many times each partition for the topic is replicated.
      - The number of replicas can not be more than the number of brokers in the cluster.
      - Replicas can not be set to zero or negative.
      - Once the replicas are set, they can not be changed with Ansible.
    required: true
    type: int
  bootstrap_server:
    description:
      - Kafka-Broker which is a member of the Kafka-Cluster you want to create the topic on.
      - Use the following format: "host:port".
    required: true
    type: list
  cleanup_policy:
    description:
      - Corresponds to the topic-config "cleanup.policy" from Apache Kafka.
      - If set to "delete", old segments will be deleted when their retention time or
        size limits have been reached.
      - If set to "compact", old segments will be compacted when their retention time
        or size limits have been reached.
    default: delete
    type: str
    choices: [ delete, compact ]
  retention:
    description:
      - Corresponds to the topic-config "retention.ms" from Apache Kafka.
      - How long a log will be retained before being discarded.
      - If set to "-1", no time limit is applied.
      - Else use the following format: "%d%h%m%s%ms".
    default: 604800000ms (==7d)
    type: str
'''
EXAMPLES = '''
---
#create new topic
- name: create topic "foo"
  kafka_topic:
    name: foo
    state: present
    partitions: 2
    replication_factor: 2
    bootstrap_server:
      - localhost:9092
      - 10.10.4.5:5678

#modify topic
- name: modify topic "foo"
  kafka_topic:
    name: foo
    state: present
    partitions: 2
    replication_factor: 2
    bootstrap_server:
      - 127.0.0.4:1234
    retention: 2d12h

#delete topic
- name: delete topic "bar"
  kafka_topic:
    name: bar
    state: absent
    partitions: 1
    replication_factor: 1
    bootstrap_server:
      - 143.34.62.87:45078
    cleanup_policy: compact
'''
RETURN = '''
---
name:
  description: name of the targeted topic
  type: string
  returned: always
state:
  description: state of the targeted topic
  type: string
  returned: success
'''

from ansible.module_utils.basic import AnsibleModule
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource

import re
import socket

##########################################
#                                        #
#       INPUT-VALIDATION-FUNCTIONS       #
#                                        #
##########################################

def validate_name(name):
    max_length = 249
    rema = re.match(r"^([a-z][a-z\d-]+(\.[a-z][a-z\d-]+)*|app\.[a-z]{2,})(\.[A-Z][A-Za-z\d]+(\.v[1-9][0-9]*)?)?(-(state|command|event)(\.state|\.command|\.event)*)?(-[a-z][a-z0-9]*)?(-from\.(test|int|prod))?$
", name).group(0)
    if not rema or len(rema) > max_length:
        msg = ("Invalid name for topic." \
             " Valid characters are: a-z, A-Z, 0-9, \".\",\"-\",\"_\"." \
             " Also check out the Topic-Grammar from the ISVC-Project."
            )
        fail_module(msg)


def validate_factor(factor, part_or_rep):
    try:
        factor = int(factor)
    except ValueError:
        msg = ("Value from %s must be an int." \
              " You tried to set %s as factor." \
              %(part_or_rep, factor)
              )
        fail_module(msg)
        if factor <= 0:
            msg = ("Value from %s must be a positive int." \
                  " You tried to set %s as factor." \
                  %(part_or_rep, factor)
                  )
            fail_module(msg)


def validate_broker(broker_definition):
    pass


def validate_ipv4(broker):
    pass


def validate_port(port):
    pass


def validate_retention_ms(retention):
    pass


##########################################
#                                        #
#           KAFKA-FUNCTIONS              #
#                                        #
##########################################

def check_topic(topic):
    pass


def compare_part_rep(topic, partitions, replication_factor).
    pass


def compare_config(topic, new_config):
    pass


def modify_config(topic, new_config):
    pass


def modify_part(topic, new_part):
    pass


def create_topic(topic, partitions, replication_factor):
    pass


def delete_topic(topic):
    pass


def add_config_together(module):
    pass


##########################################
#                                        #
#           ANSIBLE-FUNCTIONS            #
#                                        #
##########################################

def fail_module(msg):
    module.fail_json(msg, **result)


##########################################
#                                        #
#                 MAIN                   #
#                                        #
##########################################








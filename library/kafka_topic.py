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
      - Also supports now IPv6-definitions.
      - Can be set as an environment-variable looking like this:
      - KAFKA_BOOTSTRAP='["host:port","host:port"]'
    required: true
    type: list

  cleanup_policy:
    description:
      - Corresponds to the topic-config "cleanup.policy" from Apache Kafka.
      - If set to "delete", old segments will be deleted when their retention time or
        size limits have been reached.
      - If set to "compact", old segments will be compacted when their retention time
        or size limits have been reached.
    type: str
    choices: [ delete, compact ]
    default: delete
  compression_type:
    description
      - Corresponds to the topic-config "compression.type" from Apache Kafka.
    type: str
    choices: [uncompressed, zstd, lz4, snappy, gzip, producer ]
    default: producer
  delete_retention_ms:
    description:
      - Corresponds to the topic-config "delete.retention.ms" from Apache Kafka.
      - Use the following format: "%d%h%m%s%ms".
    type: str
    default: 86400000
  file_delete_delay_ms:
    description:
      - Corresponds to the topic-config "file.delete.delay.ms" from Apache Kafka.
      - Use the following format: "%d%h%m%s%ms".
    type: str
    default: 60000
  flush_messages:
    description:
      - Corresponds to the topic-config "flush.messages" from Apache Kafka.
    type: int
    default: 9223372036854775807
  flush_ms:
    description:
      - Corresponds to the topic-config "flush.ms" from Apache Kafka.
      - Use the following format: "%d%h%m%s%ms".
    type: str
    default: 9223372036854775807
  follower_replication_throttled_replicas:
    description:
      - Corresponds to the topic-config "follower.replication.throttled.replicas" from Apache Kafka.
    type: list
    default: ""
  index_interval_bytes:
    description:
      - Corresponds to the topic-config "index.interval.bytes" from Apache Kafka.
    type: int
    default: 4096
  leader_replication_throttled_replicas:
    description:
      - Corresponds to the topic-config "leader.replication.throttled.replicas" from Apache Kafka.
    type: list
    default: ""
  max_message_bytes:
    description:
      - Corresponds to the topic-config "max.message.bytes" from Apache Kafka.
    type: int
    default: 1000012
  message_format_version:
    description:
      - Corresponds to the topic-config "message.format.version" from Apache Kafka.
    type: str
    choices: [0.8.0, 0.8.1, 0.8.2, 0.9.0, 0.10.0-IV0, 0.10.0-IV1, 0.10.1-IV0, 0.10.1-IV1, 0.10.1-IV2, 0.10.2-IV0, 0.11.0-IV0, 0.11.0-IV1, 0.11.0-IV2, 1.0-IV0, 1.1-IV0, 2.0-IV0, 2.0-IV1, 2.1-IV0, 2.1-IV1, 2.1-IV2, 2.2-IV0, 2.2-IV1]
    default: 2.2-IV1
  message_timestamp_difference_max_ms:
    description:
      - Corresponds to the topic-config "message.timestamp.difference.max.ms" from Apache Kafka.
      - Use the following format: "%d%h%m%s%ms".
    type: str
    default: 9223372036854775807
  message_timestamp_type:
    description:
      - Corresponds to the topic-config "message.timestamp.type" from Apache Kafka.
    type: str
    choices: [CreateTime, LogAppendTime]
    default: CreateTime
  min_cleanable_dirty_ratio:
    description:
      - Corresponds to the topic-config "min.cleanable.dirty.ratio" from Apache Kafka.
      - Range: 0-1
    type: float
    default: 0.5
  min_compaction_lag_ms:
    description:
      - Corresponds to the topic-config "min.compaction.lag.ms" from Apache Kafka.
      - Use the following format: "%d%h%m%s%ms".
    type: int
    default: 0
  min_insync_replicas:
    description:
      - Corresponds to the topic-config "min.insync.replicas" from Apache Kafka.
      - Must be greater than 0.
    type: int
    default: 1
  preallocate:
    description:
      - Corresponds to the topic-config "preallocate" from Apache Kafka.
    type: boolean
    default: false
  retention_bytes:
    description:
      - Corresponds to the topic-config "retention.bytes" from Apache Kafka.
      - Setting it to "-1" means unlimited bytes-size.
    type: int
    default: -1
  retention_ms:
    description:
      - Corresponds to the topic-config "retention.ms" from Apache Kafka.
      - How long a log will be retained before being discarded.
      - If set to "-1", no time limit is applied.
      - Else use the following format: "%d%h%m%s%ms".
    default: 604800000ms (==7d)
    type: str
  segment_bytes:
    description:
      - Corresponds to the topic-config "segment.bytes" from Apache Kafka.
      - Must be greater than 13.
    type: int
    default: 1073741824
  segment_index_bytes:
    description:
      - Corresponds to the topic-config "segment.index.bytes" from Apache Kafka.
    type: int
    default: 10485760
  segment_jitter_ms:
    description:
      - Corresponds to the topic-config "segment.jitter.ms" from Apache Kafka.
      - Use the following format: "%d%h%m%s%ms".
    type: int
    default: 0
  segment_ms:
    description:
      - Corresponds to the topic-config "segment.ms" from Apache Kafka.
      - Use the following format: "%d%h%m%s%ms".
    type: int
    default: 604800000
  unclean_leader_election_enable:
    description:
      - Corresponds to the topic-config "unclean.leader.election.enable" from Apache Kafka.
    type: boolean
    default: false
  message_downconversion_enable:
    description:
      - Corresponds to the topic-config "message.downconversion.enable" from Apache Kafka.
    type: boolean
    default: true

  sasl_mechansim:
    description:
      - Choose one of these: PLAIN, GSSAPI, SCRAM-SHA-256, SCRAM-SHA-512 and OAUTHBEARER.
      - Can be set as an environment-variable: KAFKA_SASL_MECHANISM.
    type: str
  password:
    description:
      - SASL-Password.
      - Can be set as an environment-variable: KAFKA_PASSWORD.
    type: str
  username:
    description:
      - SASL-username.
      - Can be set as an environment-variable: KAFKA_USER.
    type: str
  use_tls:
    description:
      - If set to true, TLS will be used, else plaintext..
      - Can be set as an environment-variable: KAFKA_USE_TLS.
    type: bool
  ca_location:
    description:
      - Location of your certificate.
      - Can be set as an environment-variable: KAFKA_CA_LOCATION.
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
    retention_ms: 2d12h

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
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource, KafkaException

import re
import socket
import os
import json

import pdb

##########################################
#                                        #
#           KAFKA-FUNCTIONS              #
#                                        #
##########################################

def check_topic(topic):
    # type: (str) -> bool
    """Check if topic exists.

    Keyword arguments:
    topic -- topicname
    """
    topics = admin.list_topics(timeout=5).topics    #type(topics)=dict
    try:
        topics[topic]
    except KeyError:
        return False
    return True


def compare_part_rep(topic, partitions, replication_factor):
    # type: (str, int, int) -> bool
    """Compare partitions and replication-factor in the playbook with the ones actually set.

    Keyword arguments:
    topic -- topicname
    partitions -- number of partitions
    replication_factor -- number of replications

    Return:
    bool -- True if change is needed, else False
    """
    metadata = admin.list_topics()                                    #type(metadata.topics) = dict
    old_part = len(metadata.topics[topic].partitions)                 #access partitions of topic over .partitions-func
    old_rep = len(metadata.topics[topic].partitions[0].replicas)      #type(partitions) = dict, access replicas with partition-id as key over .replicas-func
    if partitions < old_part:
        msg = ("It is not possible to reduce the amount of partitions." \
              " At the moment, there are %s partitions for the topic %s." \
              " You tried to set %s as the new amount of partitions." \
              %(old_part, topic, partitions)
              )
        fail_module(msg)
    if replication_factor != old_rep:
        msg = ("It is not possible to modify the replication_factor." \
              " At the moment, it is set to %s and you tried to set it to %s." \
              %(old_rep, replication_factor)
              )
        fail_module(msg)
    if partitions == old_part:
        return False
    return True


def compare_config(topic, new_config):
    # type: (str, dict) -> bool
    """Compare the defined config in the playbook with the one set at the moment for this topic.

    Keyword arguments:
    topic -- topicname
    new_config -- dictionary with new config and values

    Return:
    bool -- True if change is needed, else False
    """
    resource = [ConfigResource("TOPIC", topic)]
    des = admin.describe_configs(resource)

    y = list(des.values())
    old_conf = y[0].result()

    #iterate trough new-config-dict and compare with old-config-dict, using config as key
    for config, newvalue in new_config.items():
        if str(newvalue) != old_conf[config].value:
            return True

    return False


def modify_config(topic, new_config):
    # type: (str, dict)
    """Modify topic-config.

    Keyword arguments:
    topic -- topicname
    new_config -- dictionary with new config
    """
    resource = [ConfigResource("TOPIC", topic)]
    des = admin.describe_configs(resource)

    y = list(des.values())
    old_conf = y[0].result()

    for config, newvalue in new_config.items():       #iterate trough new-config-dict and set them on topic-resource
        resource[0].set_config(config, newvalue)

    try:
        des = admin.alter_configs(resource)             #alter topic with new config
        y = list(des.values())
        y[0].result()                        #use .result-func for finalizing
    except KafkaException as e:
        msg = ("Failed to finalize config-change for topic %s: %s" \
              %(topic, e)
              )
        fail_module(msg)


def modify_part(topic, new_part):
    # type: (str, int)
    """Modify topic-partition.

    Keyword arguments:
    topic -- topicname
    new_part -- new number of partitions
    """
    new_parts = [NewPartitions(topic, new_part)]

    try:
        fs = admin.create_partitions(new_parts, validate_only=False)
        y = list(fs.values())
        y[0].result()
    except KafkaException as e:
        msg = ("Failed to finalize partition-change for topic %s: %s" \
              %(topic, e)
              )
        fail_module(msg)


def create_topic(topic, partitions, replication_factor, new_conf):
    # type: (str, int, int, dict)
    """Create a new topic, setting partition and replication-factor immediately.

    Keyword arguments:
    topic -- topicname
    partitions -- number of partitions
    replication_factor -- number of replications, which is once set immutable
    new_conf -- dictionary with topic-config, for example containing retention.ms
    """
    topic = [NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor, config=new_conf)]

    try:
        fs = admin.create_topics(topic)
        y = list(fs.values())
        y[0].result()
    except KafkaException as e:
        msg = ("Failed to create topic %s: %s." \
              %(topic, e)
              )
        fail_module(msg)


def delete_topic(topic):
    # type: (str)
    """Delete the specified topic.

    Keyword arguments:
    topic -- topicname
    """
    topic = [topic]

    try:
        fs = admin.delete_topics(topic)
        y = list(fs.values())
        y[0].result()
    except KafkaException as e:
        msg = ("Failed to delete topic %s: %s" \
              %(topic, e)
              )
        fail_module(msg)



##########################################
#                                        #
#       INPUT-VALIDATION-FUNCTIONS       #
#                                        #
##########################################

def validate_name(name):
    # type: (str)
    """Validate name for topic.

    Keyword arguments:
    topic -- topicname
    """
    max_length = 249
    #regex for checking if topicname matches topic-name-grammar set from the ISVC-Project
    rema = re.match(r"^([a-z][a-z\d-]+(\.[a-z][a-z\d-]+)*|app\.[a-z]{2,})(\.[A-Z][A-Za-z\d]+(\.v[1-9][0-9]*)?)?(-(state|command|event)(\.state|\.command|\.event)*)?(-[a-z][a-z0-9]*)?(-from\.(test|int|prod))?$", name)
    if rema:
        rema = rema.group(0)
    if not rema or len(rema) > max_length:
        msg = ("Invalid name for topic." \
              " Valid characters are: a-z, A-Z, 0-9, \".\",\"-\",\"_\"" \
              " and a max-length of %s characters." \
              " Also check out the Topic-Grammar from the ISVC-Project." \
              %(max_length)
              )
        fail_module(msg)

def validate_factor(factor):
    # type: (int)
    """Validate partition-number and replication-number.

    Keyword arguments:
    factor -- quantity of partitions or replications
    """
    if factor <= 0 or type(factor) != int:
        msg = ("Value must be a positive int." \
              " You tried to set %s as factor." \
              %(factor)
              )
        fail_module(msg)

##########################################
#                                        #
#         TOPIC-CONFIG-VALIDATION        #
#                                        #
##########################################

def add_config_together(topic, module):
    # type: (str, AnsibleModule) -> dict
    """Add different topic-configurations together in one dictionary.
    If a topic-config isn't specified, the one already set will be kept.

    Keyword arguments:
    topic -- Topicname
    module -- This Ansiblemodule-object, containing the user-arguments

    Return:
    new_config -- dictionary containing complete topic-configuration
    """
    #retrieve user-set config
    configs = {
        "cleanup.policy":module.params["cleanup_policy"],
        "compression.type":module.params["compression_type"],
        "delete.retention.ms":module.params["delete_retention_ms"],
        "file.delete.delay.ms":module.params["file_delete_delay_ms"],
        "flush.messages":module.params["flush_messages"],
        "flush.ms":module.params["flush_ms"],
        "follower.replication.throttled.replicas":module.params["follower_replication_throttled_replicas"],
        "index.interval.bytes":module.params["index_interval_bytes"],
        "leader.replication.throttled.replicas":module.params["leader_replication_throttled_replicas"],
        "max.message.bytes":module.params["max_message_bytes"],
        "message.format.version":module.params["message_format_version"],
        "message.timestamp.difference.max.ms":module.params["message_timestamp_difference_max_ms"],
        "message.timestamp.type":module.params["message_timestamp_type"],
        "min.cleanable.dirty.ratio":module.params["min_cleanable_dirty_ratio"],
        "min.compaction.lag.ms":module.params["min_compaction_lag_ms"],
        "min.insync.replicas":module.params["min_insync_replicas"],
        "preallocate":module.params["preallocate"],
        "retention.bytes":module.params["retention_bytes"],
        "retention.ms":module.params["retention_ms"],
        "segment.bytes":module.params["segment_bytes"],
        "segment.index.bytes":module.params["segment_index_bytes"],
        "segment.jitter.ms":module.params["segment_jitter_ms"],
        "segment.ms":module.params["segment_ms"],
        "unclean.leader.election.enable":module.params["unclean_leader_election_enable"],
        "message.downconversion.enable":module.params["message_downconversion_enable"]
    }

    resource = [ConfigResource("TOPIC", topic)]
    des = admin.describe_configs(resource)

    y = list(des.values())
    old_conf = y[0].result()

    # because java-bools are all lowercase and get returned as string, convert python-bool to string and lower for comparision
    if configs['preallocate'] is not None:
        configs['preallocate'] = str(configs['preallocate']).lower()

    if configs['unclean.leader.election.enable'] is not None:
        configs['unclean.leader.election.enable'] = str(configs['unclean.leader.election.enable']).lower()

    if configs['message.downconversion.enable'] is not None:
        configs['message.downconversion.enable'] = str(configs['message.downconversion.enable']).lower()

    new_conf = {}
    for conf, value in configs.items():
        if configs[conf] is not None:
            new_conf[conf] = value
        else:
            new_conf[conf] = old_conf[conf].value
    return new_conf

def validate_delete_retention_ms(delete_retention_ms):
    # type: (str)
    """Validate delete_retention_ms and convert to ms.

    Keyword arguments:
    delete_retention_ms -- user configured delete-retention-ms, pattern: %d%h%m%s%ms
    """
    convert_time_ms(delete_retention_ms, "delete_retention_ms")

def validate_file_delete_delay_ms(file_delete_delay_ms):
    # type: (str)
    """Validate file_delete_delay_ms and convert to ms.

    Keyword arguments:
    file_delete_delay_ms -- user configured file-delete-delay-ms, pattern: %d%h%m%s%ms
    """
    convert_time_ms(file_delete_delay_ms, "file_delete_delay_ms")

def validate_flush_ms(flush_ms):
    # type: (str)
    """Validate flush_ms and convert to ms.

    Keyword arguments:
    flush_ms -- user configured flush-ms, pattern: %d%h%m%s%ms
    """
    convert_time_ms(flush_ms, "flush_ms")

def validate_message_timestamp_difference_max_ms(message_timestamp_difference_max_ms):
    # type: (str)
    """Validate message_timestamp_difference_max_ms and convert to ms.

    Keyword arguments:
    message_timestamp_difference_max_ms -- user configured message-timestamp-difference-max-ms, pattern: %d%h%m%s%ms
    """
    convert_time_ms(message_timestamp_difference_max_ms, "message_timestamp_difference_max_ms")

def validate_min_compaction_lag_ms(min_compaction_lag_ms):
    # type: (str)
    """Validate min_compaction_lag_ms and convert to ms.

    Keyword arguments:
    min_compaction_lag_ms -- user configured min-compaction-lag-ms, pattern: %d%h%m%s%ms
    """
    convert_time_ms(min_compaction_lag_ms, "min_compaction_lag_ms")

def validate_retention_ms(retention_ms):
    # type: (str) -> str
    """Validate retention_ms. If -1, return string, else convert to ms.

    Keyword arguments:
    retention_ms -- user configured retention-ms, pattern: %d%h%m%s%ms

    Return:
    retention_ms -- If set to "-1", return it
    """
    if retention_ms == "-1":     #sets retention-time to unlimited
        return retention_ms
    convert_time_ms(retention_ms, "retention_ms")

def validate_segment_jitter_ms(segment_jitter_ms):
    # type: (str)
    """Validate segment_jitter_ms and convert to ms.

    Keyword arguments:
    segment_jitter_ms -- user configured segment-jitter-ms, pattern: %d%h%m%s%ms
    """
    convert_time_ms(segment_jitter_ms, "segment_jitter_ms")

def validate_segment_ms(segment_ms):
    # type: (str)
    """Validate segment_ms and convert to ms.

    Keyword arguments:
    segment_ms -- user configured segment-ms, pattern: %d%h%m%s%ms
    """
    convert_time_ms(segment_ms, "segment_ms")

def convert_time_ms(time_ms,config_type):
    # type: (str,str)
    """Convert user-given time to ms.

    Keyword arguments:
    time_ms -- user-given time as string
    config_type -- for setting config and error-msg
    """
    #try to parse retention_ms with regex into groups, split by timetype
    rema = re.match( r"^(?P<days>\d+d)?(?P<hours>\d+h)?(?P<minutes>\d+m)?(?P<seconds>\d+s)?(?P<miliseconds>\d+ms)?$", time_ms)

    t = rema.span()
    if t[1] == 0:
        msg = ("Could not parse given %s: %s into ms." \
              " Please use the following pattern: %%d%%h%%m%%s%%ms." \
              %(config_type, time_ms)
              )
        fail_module(msg)

    unit_map = {
        "days":[rema.group("days"),86400000],
        "hours":[rema.group("hours"),3600000],
        "minutes":[rema.group("minutes"),60000],
        "seconds":[rema.group("seconds"),1000],
        "miliseconds":[rema.group("miliseconds"),1]
    }

    ms_total = 0

    for unit, value in unit_map.items():
        if value[0] is not None:
            # cut of non-int-char with regex, which just indicates timetype
            value[0] = re.match(r"^\d+",value[0]).group()
            ms_total = ms_total + int(value[0])*value[1]

    if ms_total >= 2**63:
        msg = ("Your chosen %s is way too long." \
              " It can not be over 9'223'372'036'854'775'807 ms." \
              " You set %s as time, which results in %s ms." \
              %(config_type, time_ms, ms_total)
              )
        fail_module(msg)

    module.params[config_type] = ms_total



def validate_index_interval_bytes(index_interval_bytes):
    # type: (str)
    """Validate index_interval_bytes and convert to bytes.

    Keyword arguments:
    index_interval_bytes -- user configured index-interval-bytes, units: KiB, MiB, GiB, TiB, kB, MB, GB, TB
    """
    convert_storage_bytes(index_interval_bytes, "index_interval_bytes")

def validate_max_message_bytes(max_message_bytes):
    # type: (str)
    """Validate max_message_bytes and convert to bytes.

    Keyword arguments:
    max_message_bytes -- user configured max-message-bytes, units: KiB, MiB, GiB, TiB, kB, MB, GB, TB
    """
    convert_storage_bytes(max_message_bytes, "max_message_bytes")

def validate_retention_bytes(retention_bytes):
    # type: (str)
    """Validate retention_bytes and convert to bytes.

    Keyword arguments:
    retention_bytes -- user configured retention_bytes, units: KiB, MiB, GiB, TiB, kB, MB, GB, TB
    """
    if retention_bytes == "-1":     #sets retention-time to unlimited
        return retention_bytes
    convert_storage_bytes(retention_bytes, "retention_bytes")

def validate_segment_bytes(segment_bytes):
    # type: (str)
    """Validate segment_bytes and convert to bytes.

    Keyword arguments:
    segment_bytes -- user configured segment_bytes, units: KiB, MiB, GiB, TiB, kB, MB, GB, TB
    """
    convert_storage_bytes(segment_bytes, "segment_bytes")

def validate_segment_index_bytes(segment_index_bytes):
    # type: (str)
    """Validate segment_index_bytes and convert to bytes.

    Keyword arguments:
    segment_index_bytes -- user configured segment_index_bytes, units: KiB, MiB, GiB, TiB, kB, MB, GB, TB
    """
    convert_storage_bytes(segment_index_bytes, "segment_index_bytes")

def convert_storage_bytes(storage, config_type):
    # type: (str,str)
    """Convert user-given size into bytes and validate size depending on config-type.

    Keyword arguments:
    storage -- user-given storage-size as string
    config_type -- for setting config and error-msg
    """
    # ^((?P<KiB>\d+KiB)|(?P<MiB>\d+MiB)|(?P<GiB>\d+GiB)|(?P<TiB>\d+TiB)|(?P<kB>\d+kB)|(?P<MB>\d+MB)|(?P<GB>\d+GB)|(?P<TB>\d+TB))?$
    rema = re.match(r"^((?P<KiB>\d+KiB)|(?P<MiB>\d+MiB)|(?P<GiB>\d+GiB)|(?P<TiB>\d+TiB)|(?P<kB>\d+kB)|(?P<MB>\d+MB)|(?P<GB>\d+GB)|(?P<TB>\d+TB)|(?P<B>\d+B))?$", storage)

    t = rema.span()
    if t[1] == 0:
        msg = ("Could not parse given %s: %s into bytes." \
              " Please use one of the following units: KiB, MiB, GiB, TiB, kB, MB, GB, TB, B." \
              %(config_type, storage)
              )
        fail_module(msg)

    #map storage to unit and multiplicator
    unit_map = {
        "KiB":[rema.group("KiB"),1024],
        "MiB":[rema.group("MiB"),1048576],
        "GiB":[rema.group("GiB"),1073741824],
        "TiB":[rema.group("TiB"),1099511627776],
        "kB":[rema.group("kB"),1000],
        "MB":[rema.group("MB"),1000000],
        "GB":[rema.group("GB"),1000000000],
        "TB":[rema.group("TB"),1000000000000],
        "B":[rema.group("B"),1]
    }

    #find the one matched storage-unit, and convert to bytes
    for unit, value in unit_map.items():
        if value[0] is not None:
            # cut off non-int-char
            value[0] = re.match(r"^\d+",value[0]).group()
            bytes_total = int(value[0])*value[1]

    # check if total-bytes is in valid range depending on config-type
    if config_type == "retention_bytes":
        if bytes_total >= 2**63:
            msg = ("Your chosen %s is way too long." \
                  " It can not be over 9'223'372'036'854'775'807 bytes." \
                  " You set %s as size, which results in %s bytes." \
                  %(config_type, storage, bytes_total)
                  )
            fail_module(msg)
    else:
        if config_type == "segment_bytes":
            if bytes_total < 14:
                msg = ("Your chosen %s must be at least 14 bytes." \
                      " You set %s as size, which results in %s bytes." \
                      %(config_type, storage, bytes_total)
                      )
                fail_module(msg)
        if bytes_total >= 2**32:
            msg = ("Your chosen %s is way too long." \
                  " It can not be over 4'294'967'295 bytes." \
                  " You set %s as size, which results in %s bytes." \
                  %(config_type, storage, bytes_total)
                  )
            fail_module(msg)
    module.params[config_type] = bytes_total


##########################################
#                                        #
#         ADMIN-CONFIG-VALIDATION        #
#                                        #
##########################################

def validate_broker(broker_definition):
    # type: (list)
    """Validate broker-definition.
    Set broker-list as a string for admin-conf: 'host:port,host:port'.

    Keyword arguments:
    broker_definition -- list containing broker. Pattern per broker: 'host:port'.
    """
    broker_def_list = []
    for broker in broker_definition:
        broker_parts = broker.split(":")
        if len(broker_parts) == 2:
            validate_ipv4(broker_parts)
        if len(broker_parts) > 2:
            validate_ipv6(broker)
        if len(broker_parts) < 2:
            msg = ("Broker-Definition does not seem to be valid: %s" \
                  " Use following pattern per broker: host:port." \
                  %(broker)
                  )
            fail_module(msg)
        broker_def_list.append(broker)
    final_broker_definition = ",".join(broker_def_list)
    module.params['bootstrap_server'] = final_broker_definition

def validate_ipv4(broker):
    # type: (list)
    """Validate IPv4-address, trying to build a tcp-connection to given address.

    Keyword arguments:
    broker -- definition of one broker, as a list: [host,port]

    Return:
    broker -- valid broker as string: 'host:port'
    """
    port = validate_port(broker[1])
    ip = broker[0]
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        #try to make a connection
        sock.connect((ip, port))
        sock.close()
    except socket.error:
        sock.close()
        msg = ("Can not connect to broker: %s" \
              " Please check if the definition is right." \
              %(broker)
              )
        fail_module(msg)

def validate_ipv6(broker):
    # type: (str)
    """Validate IPv6-address, trying to build a tcp-connection to given address.

    Keyword arguments:
    broker -- definition of one broker, as a list: [host,port]

    Return:
    broker -- valid broker as string: 'host:port'
    """
    # split broker-definition in ip-address and port
    ip_port = broker.rsplit(":",1)
    port = validate_port(ip_port[1])
    ip = ip_port[0]
    # remove square bracket from ipv6-definition, eg. [::1]
    ip = ip[1:-1]

    sock_ipv6 = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    try:
        sock_ipv6.connect((ip,port))
        sock_ipv6.close()
    except socket.error:
        sock_ipv6.close()
        msg = ("Can not connect to broker: %s" \
              " Please check if the definition is right." \
              %(broker)
              )
        fail_module(msg)

def validate_port(port):
    # type: (str) -> int
    """Validate port.

    Keyword arguments:
    port -- port for tcp-connection to broker.

    Return:
    port -- port for tcp-connection to broker.
    """
    try:
        port = int(port)
    except ValueError:
        msg = ("Port needs to be int, but got: %s" \
              %(port)
              )
        fail_module(msg)
    if (port <= 1024) or (port > 65535):
        msg = ("Valid Port-Range is: 1024-65534." \
              " But given Port is: %s" \
              %(port)
              )
        fail_module(msg)
    return port

def validate_sasl_mechanism(sasl_mechanism):
    # type: (str)
    """Validate sasl-mechanism.

    Keyword arguments:
    sasl_mechanism -- user-defined sasl_mechanism
    """
    if sasl_mechanism == "PLAIN":
        validate_sasl_PLAIN()
    elif sasl_mechanism == "GSSAPI":
        fail_module("GSSAPI not supported so far")
    elif sasl_mechanism == "SCRAM-SHA-256":
        fail_module("SCRAM-SHA-256 not supported so far")
    elif sasl_mechanism == "SCRAM-SHA-512":
        fail_module("SCRAM-SHA-512 not supported so far")
    elif sasl_mechanism == "OAUTHBEARER":
        fail_module("OAUTHBEARER not supported so far")
    else:
        msg = ("Supported SASL-Mechanisms are: PLAIN,"\
              " GSSAPI, SCRAM-SHA-256, SCRAM-SHA-512,"\
              " and OAUTHBEARER."\
              )
        fail_module(msg)

def validate_sasl_PLAIN():
    # type: ()
    """Validate sasl-mechanism PLAIN, check if mandatory username, password and protocol is set.
    Also check if ca-location is set.
    Set each value in admin_conf.
    """

    if module.params['username'] is None \
    or module.params['password'] is None \
    or module.params['use_tls'] is None:
        msg = ("If you choose PLAIN as sasl_mechanism," \
              " you also need to set: username," \
              " password and use_tls." \
              )
        fail_module(msg)
    admin_conf['sasl.mechanism'] = "PLAIN"
    admin_conf['sasl.password'] = module.params['password']
    admin_conf['sasl.username'] = module.params['username']
    if module.params['use_tls'] == True:
        admin_conf['security.protocol'] = "sasl_ssl"
    else:
        admin_conf['security.protocol'] = "sasl_plaintext"
    if module.params['ca_location'] is not None:
        admin_conf['ssl.ca.location'] = module.params['ca_location']



##########################################
#                                        #
#           ANSIBLE-FUNCTIONS            #
#                                        #
##########################################

def fail_module(msg):
    # type: (str)
    """Fail module properly with error-message.

    Keyword arguments:
    msg -- error-message to print
    """
    module.fail_json(msg=msg, **result)


##########################################
#                                        #
#                 MAIN                   #
#                                        #
##########################################

def main():

    global module
    global result
    global admin
    global admin_conf

    # initialize object AnsibleModule
    module_args = dict(
        name=dict(type='str', required=True),
        state=dict(type='str', required=True, choices=['absent', 'present']),
        partitions=dict(type='int', required=True),
        replication_factor=dict(type='int', required=True),
        bootstrap_server=dict(type='list'),
        cleanup_policy=dict(type='str', choices=['compact', 'delete']),
        compression_type=dict(type='str', choices=['uncompressed', 'zstd', 'lz4', 'snappy', \
                'gzip', 'producer']),
        delete_retention_ms=dict(type='str'),
        file_delete_delay_ms=dict(type='str'),
        flush_messages=dict(type='int'),
        flush_ms=dict(type='str'),
        follower_replication_throttled_replicas=dict(type='list'),
        index_interval_bytes=dict(type='str'),
        leader_replication_throttled_replicas=dict(type='list'),
        max_message_bytes=dict(type='str'),
        message_format_version=dict(type='str', \
            choices=['0.8.0', '0.8.1', '0.8.2', '0.9.0', \
                    '0.10.0-IV0', '0.10.0-IV1', '0.10.1-IV0', \
                    '0.10.1-IV1', '0.10.1-IV2', '0.10.2-IV0', \
                    '0.11.0-IV0', '0.11.0-IV1', '0.11.0-IV2', \
                    '1.0-IV0', '1.1-IV0', '2.0-IV0', '2.0-IV1', \
                    '2.1-IV0', '2.1-IV1', '2.1-IV2', '2.2-IV0', '2.2-IV1']),
        message_timestamp_difference_max_ms=dict(type='str'),
        message_timestamp_type=dict(type='str', choices=['CreateTime', 'LogAppendTime']),
        min_cleanable_dirty_ratio=dict(type='float'),
        min_compaction_lag_ms=dict(type='str'),
        min_insync_replicas=dict(type='int'),
        preallocate=dict(type='bool'),
        retention_bytes=dict(type='str'),
        retention_ms=dict(type='str'),
        segment_bytes=dict(type='str'),
        segment_index_bytes=dict(type='str'),
        segment_jitter_ms=dict(type='str'),
        segment_ms=dict(type='str'),
        unclean_leader_election_enable=dict(type='bool'),
        message_downconversion_enable=dict(type='bool'),
        sasl_mechanism=dict(type='str', choices=['GSSAPI', 'PLAIN', 'SCRAM-SHA-256', \
                'SCRAM-SHA-512', 'OAUTHBEARER']),
        password=dict(type='str', no_log=True),
        username=dict(type='str'),
        use_tls=dict(type='bool'),
        ca_location=dict(type='str')
    )

    result = dict(
        changed=False,
        name='',
        state=''
    )

    module = AnsibleModule(
        argument_spec=module_args,
    )


    # dict of params which can be set by env-var
    env_param = dict(
        sasl_mechanism="KAFKA_SASL_MECHANISM",
        password="KAFKA_PASSWORD",
        username="KAFKA_USER",
        use_tls="KAFKA_USE_TLS",
        ca_location="KAFKA_CA_LOCATION"
    )

    # bootstrap-server can also be an env-var, but must be parsed into a list
    if module.params['bootstrap_server'] is None:
        try:
            module.params['bootstrap_server']=json.loads(os.environ['KAFKA_BOOTSTRAP'])
        except KeyError:
            msg = ("It seems that there is no bootstrap-server definition in"\
                  " the playbook and also not as an environment-variable."\
                  " If you want to use an environment-variable, make sure it's"\
                  " name is 'KAFKA_BOOTSTRAP'."
                  )
            fail_module(msg)

    # loop through env-param-dict and set all params which are set in env
    for key, value in env_param.items():
        if module.params[key] is None:
            module.params[key] = os.environ.get(value)

    # admin-config dictionary for creating adminclient
    admin_conf = {}

    # set topicname as result as soon as possible, for meaningful error-messages
    result['name'] = module.params['name']

    # map param to corresponding validation-function
    # Choice-Parameter are left out because Ansible validates them
    # Child-Parameter like username are left out aswell because
    # they get validated through their parent-param like sasl_mechanism
    params_valid_dict = dict(
        name=validate_name,
        partitions=validate_factor,
        replication_factor=validate_factor,
        bootstrap_server=validate_broker,
        delete_retention_ms=validate_delete_retention_ms,
        file_delete_delay_ms=validate_file_delete_delay_ms,
        flush_ms=validate_flush_ms,
        index_interval_bytes=validate_index_interval_bytes,
        max_message_bytes=validate_max_message_bytes,
        message_timestamp_difference_max_ms=validate_message_timestamp_difference_max_ms,
        min_compaction_lag_ms=validate_min_compaction_lag_ms,
        retention_bytes=validate_retention_bytes,
        retention_ms=validate_retention_ms,
        segment_bytes=validate_segment_bytes,
        segment_index_bytes=validate_segment_index_bytes,
        segment_jitter_ms=validate_segment_jitter_ms,
        segment_ms=validate_segment_ms,
        sasl_mechanism=validate_sasl_mechanism
    )

    # loop through params_valid_dict and validate all params which are set (not none)
    # validate all parameters
    for key in params_valid_dict:
        if module.params[key] is not None:
            # params_valid_dict[key] returns valid-func.
            # Pass as param for the valid-func the user-set param with module.params[key]
            params_valid_dict[key](module.params[key])


    #create admin_conf-dict for connection-params like authentication
    admin_conf['bootstrap.servers'] = module.params['bootstrap_server']

    # after validation, initialize object AdminClient for configuring topics on kafka-broker
    admin = AdminClient(admin_conf)

    # check if topic exists and act according to return-value
    topic_exists = check_topic(module.params['name'])

    # if topic exists and should stay so, compare configuration and modify them if needed
    if topic_exists and (module.params['state'] == "present"):
        result['state'] = "present"
        mod_part = compare_part_rep(module.params['name'], module.params['partitions'], \
                module.params['replication_factor'])
        if mod_part:
            modify_part(module.params['name'], module.params['partitions'])
            result['changed'] = True
        new_conf = add_config_together(module.params['name'], module)
        mod_conf = compare_config(module.params['name'], new_conf)
        if mod_conf:
            modify_config(module.params['name'], new_conf)
            result['changed'] = True

    # if topic exists and should not, delete it
    if topic_exists and (module.params['state'] == "absent"):
        delete_topic(module.params['name'])
        result['changed'] = True
        result['state'] = "absent"

    # if topic does not exist, but should, create and configure
    if not topic_exists and (module.params['state'] == "present"):
        new_conf = add_config_together(module)
        create_topic(module.params['name'], module.params['partitions'], \
                module.params['replication_factor'], new_conf)
        result['changed'] = True
        result['state'] = "present"

    # if topic does not exist and should stay that way, do nothing
    if not topic_exists and (module.params['state'] == "absent"):
        result['state'] = "absent"

    # exit module and print result-dictionary
    module.exit_json(**result)

if __name__ == '__main__':
    main()

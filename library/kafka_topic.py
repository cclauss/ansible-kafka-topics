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
    required: true
    type: int
  bootstrap_server:
    description:
      - Kafka-Broker which is a member of the Kafka-Cluster you want to manage the topic on.
      - Use the following format: "host:port".
      - Also supports now IPv6-definitions.
      - Can be set as an environment-variable looking like this:
      - KAFKA_BOOTSTRAP='["host:port","host:port"]'
    required: true
    type: list
  zookeeper:
    description:
      - Zookeeper which is a member of the Kafka-Cluster you want to manage the topic on.
      - Is only needed if you want to increase or reduce the number of replicas.
    required: false
    type: list
  config:
    description:
      - Configs for the Topic.
    required: false
    type: list

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
    config:
      - retention.ms: 1000

#modify topic
- name: modify topic "foo"
  kafka_topic:
    name: foo
    state: present
    partitions: 2
    replication_factor: 2
    bootstrap_server:
      - 127.0.0.4:1234
    config:
      - preallocate: true

#delete topic
- name: delete topic "bar"
  kafka_topic:
    name: bar
    state: absent
    partitions: 1
    replication_factor: 1
    bootstrap_server:
      - 143.34.62.87:45078
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
from kazoo.client import KazooClient
from confluent_kafka import KafkaError

import re
import socket
import os
import json
import time
import random

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
    try:
        topics = admin.list_topics(timeout=5).topics    # type(topics)=dict
    except KafkaException as e:
        msg = (
            "Can not retrieve topic %s: %s"
            % (topic, e)
        )
        fail_module(msg)
    try:
        topics[topic]
    except KeyError:
        return False
    return True


def compare_part(topic, partitions):
    # type: (str, int) -> bool
    """Compare partitions in the playbook with the ones actually set.

    Keyword arguments:
    topic -- topicname
    partitions -- number of partitions

    Return:
    bool -- True if change is needed, else False
    """
    try:
        metadata = admin.list_topics()                                    # type(metadata.topics) = dict
    except KafkaException as e:
        msg = (
            "Can not get metadata of topic %s: %s"
            % (topic, e)
        )
        fail_module(msg)
    old_part = len(metadata.topics[topic].partitions)                 # access partitions of topic over .partitions-func
    if partitions < old_part:
        msg = (
            "It is not possible to reduce the amount of partitions."
            " At the moment, there are %s partitions for the topic %s."
            " You tried to set %s as the new amount of partitions."
            % (old_part, topic, partitions)
        )
        fail_module(msg)
    if partitions == old_part:
        return False
    diff['before']['partitions'] = old_part
    diff['after']['partitions'] = partitions
    return True


def compare_rep(topic, replication_factor):
    # type: (str, int) -> bool
    """Compare replication-factor in the playbook with the one actually set.

    Keyword arguments:
    topic -- topicname
    replication_factor -- number of replications

    Return:
    bool -- True if change is needed, else False
    """
    try:
        metadata = admin.list_topics()                                    # type(metadata.topics) = dict
    except KafkaException as e:
        msg = (
            "Can not get metadata of topic %s: %s"
            % (topic, e)
        )
        fail_module(msg)
    old_rep = len(metadata.topics[topic].partitions[0].replicas)      #type(partitions) = dict, access replicas with partition-id as key over .replicas-func
    if replication_factor != old_rep:
        if module.params['zookeeper'] is None:
            msg = (
                "For modifying the replication_factor of a topic,"
                " you also need to set the zookeeper-parameter."
                " At the moment, replication_factor is set to %s"
                "  and you tried to set it to %s."
                % (old_rep, replication_factor)
            )
            fail_module(msg)
        diff['before']['replication_factor'] = old_rep
        diff['after']['replication_factor'] = replication_factor
        return True
    # if replication_factor == old_rep:
    return False


def get_topic_config(topic):
    # type: (str) -> dict
    """Get Topic configuration.

    Keyword arguments:
    topic -- topicname

    Return:
    old_conf -- dict containing topic configuration
    """
    resource = [ConfigResource("TOPIC", topic)]
    try:
        des = admin.describe_configs(resource)
    except KafkaException as e:
        msg = (
            "Can not retrieve topic-config from topic %s: %s"
            % (topic, e)
        )

    y = list(des.values())
    old_conf = y[0].result()
    return old_conf


def compare_config(topic, new_config):
    # type: (str, dict) -> bool
    """Compare the defined config in the playbook with the one set at the moment for this topic.

    Keyword arguments:
    topic -- topicname
    new_config -- dictionary with new config and values

    Return:
    bool -- True if change is needed, else False
    """
    old_conf = get_topic_config(topic)
    change = False

    # iterate through old-config
    for config, oldvalue in old_conf.items():
        try:
            # if a config in playbook is different than old-config, return new
            if oldvalue.value != str(new_config[config]):
                diff['before'][config] = oldvalue.value
                diff['after'][config] = new_config[config]
                change = True
        # if config is not set in playbook, catch keyerror.
        except KeyError:
            # Check if config, which is not set in playbook,
            # is set on default.
            if not old_conf[config].is_default:
                # if config is not on default, check if source is
                # 4 == STATIC_BROKER_CONFIG
                # which is as good as default
                if old_conf[config].source != 4:
                    diff['before'][config] = oldvalue.value
                    diff['after'][config] = "defaultvalue"
                    change = True
    return change


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
        msg = (
            "Failed to finalize partition-change for topic %s: %s"
            % (topic, e)
        )
        fail_module(msg)


def modify_rep(topic, partitions, replication_factor, zookeeper):
    # type: (str, int, int, str)
    """Increase number of replicas per partition.

    Keyword arguments:
    topic -- topicname
    partitions -- number of partitions
    replication_factor -- number of replications
    zookeeper -- host:port from a zookeeper which is part of the Kafka-Cluster

    Return:
    consumed -- wether znode is consumed or not
    """
    # Create Connection to zookeeper
    zk = get_zookeeper_connection()

    # Check if there is already a reassign-quest on the node
    # so it won't get overwritten.
    consumed = wait_until_znode_consumed(zk, "/admin/reassign_partitions", 10, 1)
    if not consumed:
        msg = (
            "There is already a reassign-inquiry for replicas in the queue."
            " We need to wait longer and try again for modifying number"
            " of replicas for topic %s."
            % (topic)
        )
        zk.stop()
        fail_module(msg)

    # Get Broker-id-list
    broker_ids = get_broker_ids(zk)

    # Get Partitions and their replica-id and put in a dict
    part_dict={}
    metadata = admin.list_topics()
    for partition in metadata.topics[topic].partitions.values():
        part_dict[partition.id]=partition.replicas

    json_data = create_rep_json(topic, part_dict, broker_ids, replication_factor)

    # Create znode with instructions to increase replicas!
    created = create_znode(zk, "/admin/reassign_partitions", json_data.encode())
    if not created:
        msg = (
            "Failed to create znode on zookeeper for increasing"
            " replication factor of topic %s."
            % (topic)
        )
        zk.stop()
        fail_module(msg)

    # Wait so the node gets consumed and replicas get increased
    consumed = wait_until_znode_consumed(zk, "/admin/reassign_partitions", 10, 1)
    if not consumed:
        msg = (
            "Znode for reassigning replicas on topic %s is still not"
            " consumed. We probably just need to wait a little"
            " longer."
            % (topic)
        )
        zk.stop()
        fail_module(msg)

    # stop the zookeeper-connection!
    zk.stop()

def create_rep_json(topic, part_metadata, broker_ids, replication_factor):
    # type: (dict) -> str
    """Take Partition-Metadata-Dict and create a json-object
    for modifying number of replicas

    Keyword arguments:
    topic -- topicname
    part_metadata -- dict containing partition-metadata
    broker_ids -- broker IDs for distributing replicas
    replication_factor -- number of replications

    Return:
    json_data -- json-object describing partition and their replicas
    """

    # Instruction for increasing replica-number must be a json.
    # First build a template-dict, then convert to json
    data={}
    data['version']=1

    # Temp-List for adding instruction together
    part_list=[]

    # iterate through dict with partitions and replica-ids
    # to generate template-list for json
    for partition, replicas in part_metadata.items():
        # Create List with broker who do not have a replica of partition so far
        diff=[int(i) for i in broker_ids if int(i) not in replicas]

        # if replica-number should get increased, add brokers who do not have a replica so far
        if len(replicas) < replication_factor:
            while len(replicas) < replication_factor:
                rand = random.randint(0,(len(diff)-1))
                # add random broker, hoping for better broker-distribution
                replicas.append(diff.pop(rand))

        # if replica-number should get reduced, remove brokers from the replica-list
        if len(replicas) > replication_factor:
            while len(replicas) > replication_factor:
                rand = random.randint(0,(len(replicas)-1))
                # remove random broker, hoping for better broker-distribution
                replicas.pop(rand)

        # Add everything in a temp dict together...
        part={}
        part['topic']=topic
        part['partition']=partition
        part['replicas']=replicas

        # ...and append to part_list
        part_list.append(part)

    # add template list into template dict for json and create json
    data['partitions'] = part_list
    return(json.dumps(data))


def modify_config(topic, new_config):
    # type: (str, dict)
    """Modify topic-config.

    Keyword arguments:
    topic -- topicname
    new_config -- dictionary with new config
    """
    resource = [ConfigResource("TOPIC", topic)]

    for config, newvalue in new_config.items():       # iterate trough new-config-dict and set them on topic-resource
        resource[0].set_config(config, newvalue)

    try:
        des = admin.alter_configs(resource)             # alter topic with new config
        y = list(des.values())
        y[0].result()                        # use .result-func for finalizing
    except KafkaException as e:
        msg = (
            "Failed to finalize config-change for topic %s: %s"
            % (topic, e)
        )
        fail_module(msg)


def create_topic(topicname, partitions, replication_factor, new_conf):
    # type: (str, int, int, dict)
    """Create a new topic, setting partition and replication-factor immediately.

    Keyword arguments:
    topicname -- topicname
    partitions -- number of partitions
    replication_factor -- number of replications, which is once set only mutable with a zookeeper
    new_conf -- dictionary with topic-config, for example containing retention.ms
    """
    all_valid = False
    # while not sure all topic-configs are valid
    while not all_valid:

        topic = [NewTopic(topicname, num_partitions=partitions, replication_factor=replication_factor, config=new_conf)]

        try:
            # only validate creation of topic
            fs = admin.create_topics(topic, validate_only = True)
            y = list(fs.values())
            y[0].result()
            all_valid = True
        except KafkaException as e:
            # Errorcode == 40 says we want to set a not-supported topic-config
            if e.args[0].code == 40:
                faultiemsg = e.args[0].str()
                faultiemsg = faultiemsg.split(" ")
                # extract which config is not supported
                faultieconf = faultiemsg[-1]
                # remove unsupported config
                new_conf.pop(faultieconf)
                # print warning
                module.warn(
                    "Will not set Topic-config %s, because this Kafka-Cluster"
                    " does no support this config."
                    %(faultieconf)
                )
            else:
                msg = (
                    "For some reason we won't be able to create Topic %s: %s."
                    % (topic, e)
                )
                fail_module(msg)

    # Create Topic for real
    topic = [NewTopic(topicname, num_partitions=partitions, replication_factor=replication_factor, config=new_conf)]
    try:
        fs = admin.create_topics(topic)
        y = list(fs.values())
        y[0].result()
    except KafkaException as e:
        msg = (
            "Failed to create topic %s: %s."
            % (topic, e)
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
        msg = (
            "Failed to delete topic %s: %s"
            % (topic, e)
        )
        fail_module(msg)

##########################################
#                                        #
#          ZOOKEEPER-FUNCTIONS           #
#                                        #
##########################################


def get_zookeeper_connection():
    # type: () -> kazoo.client.KazooClient
    """Create a connection-object to a zookeeper

    Return:
    zk -- Zookeeper-Client
    """
    zk = KazooClient(hosts=module.params['zookeeper'])
    zk.start()
    return zk


def get_broker_ids(zookeeper_client):
    # type: (kazoo.client.KazooClient) -> list
    """Get a list with broker-ids

    Keyword arguments:
    zookeeper_client -- Zookeeper-Client

    Return:
    broker_ids -- List containing broker-ids
    """
    try:
        broker_ids = zookeeper_client.get_children("/brokers/ids")
    except KazooException as e:
        msg = (
            "Failed to get Broker-ids: %s"
            % (e)
        )
        zookeeper_client.stop()
        fail_module(msg)
    return broker_ids


def create_znode(zookeeper_client, znode, data):
    # type: (kazoo.client.KazooClient, str, bytes) -> bool
    """Create a znode on a zookeeper

    Keyword arguments:
    zookeeper_client -- Zookeeper-Client
    znode -- path and name for znode
    data -- data which znode will contain

    Return:
    created -- if znode was successfully created or not
    """
    created = False
    try:
        zookeeper_client.create(znode, data)
    except KazooException as e:
        return created
    created = True
    return created


def wait_until_znode_consumed(zookeeper_client, znode, max_retries, wait_time):
    # type: (kazoo.client.KazooClient, str, int, int)
    """Wait until a znode is consumed or wait-time is over

    Keyword arguments:
    zookeeper_client -- Zookeeper-Client
    znode -- path and name for znode
    max_retries -- how often client checks if znode exists
    wait_time -- how long client waits between checking (in seconds)
    """
    consumed = False
    retries = 0
    while zookeeper_client.exists(znode):
        time.sleep(wait_time)
        retries = retries + 1
        if retries == max_retries:
            return consumed
    consumed = True
    return consumed

##########################################
#                                        #
#       INPUT-VALIDATION-FUNCTIONS       #
#                                        #
##########################################


def validate_name(name):
    return  # for now until we can skip this with a tag
    # type: (str)
    """Validate name for topic.

    Keyword arguments:
    topic -- topicname
    """
    max_length = 249
    # regex for checking if topicname only has valid characters
    rema = re.match(r"^[[A-Za-z\d\.\-\_]+$",name)
    if rema:
        rema = rema.group(0)
    if not rema or len(rema) > max_length:
        msg = (
            "Invalid name for topic."
            " Valid characters are: a-z, A-Z, 0-9, \".\",\"-\",\"_\""
            " and a max-length of %s characters."
            % (max_length)
        )
        fail_module(msg)


def validate_part_factor(factor):
    # type: (int)
    """Validate number of partitions.

    Keyword arguments:
    factor -- quantity of partitions
    """
    if factor <= 0 or type(factor) != int:
        msg = (
            "Number of partitions must be a positive int."
            " You tried to set %s as factor."
            % (factor)
        )
        fail_module(msg)


def validate_rep_factor(factor):
    # type: (int)
    """Validate number of replications.

    Keyword arguments:
    factor -- quantity of replications
    """
    if factor <= 0 or type(factor) != int:
        msg = (
            "Value must be a positive int."
            " You tried to set %s as factor."
            % (factor)
        )
        fail_module(msg)
    if bool(module.params['zookeeper']):
        zk = get_zookeeper_connection()
        broker_quantity = len(get_broker_ids(zk))
        if broker_quantity < factor:
            msg = (
                "You can't create more replicas of a"
                " partition than you have Kafka-broker"
                " in your cluster. You have %s broker"
                " in your cluster and tried to set replication"
                " factor to %s."
                % (broker_quantity, factor)
            )
            fail_module(msg)

##########################################
#                                        #
#         TOPIC-CONFIG-VALIDATION        #
#                                        #
##########################################

def add_config_together(topic, config):
    # type: (dict) -> dict
    """Go over configs, parse the artificial parameters
    in kafka-native parameters.

    Keyword arguments:
    topic -- topicname
    config -- module parameter config

    Return:
    new_config -- containing only kafka-valid configs
    """
    # artificial configs mapping to native config
    alt_configs = {
        "delete.retention.ms": "delete_retention_time",
        "file.delete.delay.ms": "file_delete_delay_time",
        "flush.ms": "flush_time",
        "index.interval.bytes": "index_interval_size",
        "max.compaction.lag.ms": "max_compaction_lag_time",
        "max.message.bytes": "max_message_size",
        "message.timestamp.difference.max.ms": "message_timestamp_difference_max_time",
        "min.compaction.lag.ms": "min_compaction_lag_time",
        "retention.bytes": "retention_size",
        "retention.ms": "retention_time",
        "segment.bytes": "segment_size",
        "segment.index.bytes": "segment_index_size",
        "segment.jitter.ms": "segment_jitter_time",
        "segment.ms": "segment_time",
    }

    # map artificial config to corresponding validation-function
    conf_valid_dict = dict(
        delete_retention_time = validate_delete_retention_time,
        file_delete_delay_time = validate_file_delete_delay_time,
        flush_time = validate_flush_time,
        index_interval_size = validate_index_interval_size,
        max_compaction_lag_time = validate_max_compaction_lag_time,
        max_message_size = validate_max_message_size,
        message_timestamp_difference_max_time = validate_message_timestamp_difference_max_time,
        min_compaction_lag_time = validate_min_compaction_lag_time,
        retention_size = validate_retention_size,
        retention_time = validate_retention_time,
        segment_size = validate_segment_size,
        segment_index_size = validate_segment_index_size,
        segment_jitter_time = validate_segment_jitter_time,
        segment_time = validate_segment_time,
    )

    # if no config set, skip it
    if not bool(config):
        config = {}
        return config

   # n_conf = native conf, a_conf = alternative conf
    for n_conf, a_conf in alt_configs.items():
        # check mutually exlusive configs not both set
        if (n_conf in config) and (a_conf in config):
            msg = (
                "You tried to set mutually exlusive Topic"
                " config parameters: "
                " %s and %s. You can either set one or another,"
                " but not both."
                % (n_conf, a_conf)
            )
            fail_module(msg)
        # validate a_conf and replace key with n_conf
        if a_conf in config:
          # if it looks like a duck and quacks like a duck
          # it probably is a duck
           n_value = conf_valid_dict[a_conf](config[a_conf])
           del config[a_conf]
           config[n_conf] = n_value

    # because java-bools are all lowercase and get returned as string, convert python-bool to string and lower for comparision
    try:
        config['preallocate'] = str(config['preallocate']).lower()
    except KeyError:
        pass

    try:
        config['unclean.leader.election.enable'] = str(config['unclean.leader.election.enable']).lower()
    except KeyError:
        pass

    try:
        config['message.downconversion.enable'] = str(config['message.downconversion.enable']).lower()
    except KeyError:
        pass

    new_conf = {}

    try:
        old_conf = get_topic_config(topic)
        for conf, value in config.items():
 #           if config[conf] is not None:
            try:
                old_conf[conf]
                new_conf[conf] = value
            except KeyError:
                module.warn(
                    "Will not set Topic-config %s, because this Kafka-Cluster"
                    " does no support this config so far."
                    %(conf)
                )
    except KafkaException:
        for conf, value in config.items():
#            if config[conf] is not None:
            new_conf[conf] = value

    return new_conf


def validate_delete_retention_time(delete_retention_time):
    # type: (str) -> int
    """Validate delete_retention_time and convert to ms.

    Keyword arguments:
    delete_retention_time -- user configured delete-retention-ms, pattern: %d%h%m%s%ms

    Return:
    ms_total -- converted config
    """
    return convert_time_ms(delete_retention_time, "delete_retention_time")


def validate_file_delete_delay_time(file_delete_delay_time):
    # type: (str) -> int
    """Validate file_delete_delay_time and convert to ms.

    Keyword arguments:
    file_delete_delay_time -- user configured file-delete-delay-ms, pattern: %d%h%m%s%ms

    Return:
    ms_total -- converted config
    """
    return convert_time_ms(file_delete_delay_time, "file_delete_delay_time")


def validate_flush_time(flush_time):
    # type: (str) -> int
    """Validate flush_time and convert to ms.

    Keyword arguments:
    flush_time -- user configured flush-ms, pattern: %d%h%m%s%ms

    Return:
    ms_total -- converted config
    """
    return convert_time_ms(flush_time, "flush_time")


def validate_max_compaction_lag_time(max_compaction_lag_time):
    # type: (str) -> int
    """Validate max_compaction_lag_time and convert to ms.

    Keyword arguments:
    max_compaction_lag_time -- user configured max-compaction-lag-ms, pattern: %d%h%m%s%ms

    Return:
    ms_total -- converted config
    """
    return convert_time_ms(max_compaction_lag_time, "max_compaction_lag_time")


def validate_message_timestamp_difference_max_time(message_timestamp_difference_max_time):
    # type: (str) -> int
    """Validate message_timestamp_difference_max_time and convert to ms.

    Keyword arguments:
    message_timestamp_difference_max_time -- user configured message-timestamp-difference-max-ms, pattern: %d%h%m%s%ms

    Return:
    ms_total -- converted config
    """
    return convert_time_ms(message_timestamp_difference_max_time, "message_timestamp_difference_max_time")


def validate_min_compaction_lag_time(min_compaction_lag_time):
    # type: (str) -> int
    """Validate min_compaction_lag_time and convert to ms.

    Keyword arguments:
    min_compaction_lag_time -- user configured min-compaction-lag-ms, pattern: %d%h%m%s%ms

    Return:
    ms_total -- converted config
    """
    return convert_time_ms(min_compaction_lag_time, "min_compaction_lag_time")


def validate_retention_time(retention_time):
    # type: (str) -> str
    """Validate retention_time. If -1, return string, else convert to ms.

    Keyword arguments:
    retention_time -- user configured retention-ms, pattern: %d%h%m%s%ms

    Return:
    retention_time -- If set to "-1", return it
    """
    if retention_time == "-1":     # sets retention-time to unlimited
        return retention_time
    return convert_time_ms(retention_time, "retention_time")


def validate_segment_jitter_time(segment_jitter_time):
    # type: (str) -> int
    """Validate segment_jitter_time and convert to ms.

    Keyword arguments:
    segment_jitter_time -- user configured segment-jitter-ms, pattern: %d%h%m%s%ms

    Return:
    ms_total -- converted config
    """
    return convert_time_ms(segment_jitter_time, "segment_jitter_time")


def validate_segment_time(segment_time):
    # type: (str) -> int
    """Validate segment_time and convert to ms.

    Keyword arguments:
    segment_time -- user configured segment-ms, pattern: %d%h%m%s%ms

    Return:
    ms_total -- converted config
    """
    return convert_time_ms(segment_time, "segment_time")


def convert_time_ms(time_ms, config_type):
    # type: (str,str) -> int
    """Convert user-given time to ms.

    Keyword arguments:
    time_ms -- user-given time as string
    config_type -- for setting config and error-msg

    Return:
    ms_time -- converted time
    """
    # try to parse retention_time with regex into groups, split by timetype
    rema = re.match(r"^(?P<days>\d+d)?(?P<hours>\d+h)?(?P<minutes>\d+m)?(?P<seconds>\d+s)?(?P<miliseconds>\d+ms)?$", time_ms)

    t = rema.span()
    if t[1] == 0:
        msg = (
            "Could not parse given %s: %s into ms."
            " Please use the following pattern: %%d%%h%%m%%s%%ms."
            % (config_type, time_ms)
        )
        fail_module(msg)

    unit_map = {
        "days": [rema.group("days"), 86400000],
        "hours": [rema.group("hours"), 3600000],
        "minutes": [rema.group("minutes"), 60000],
        "seconds": [rema.group("seconds"), 1000],
        "miliseconds": [rema.group("miliseconds"), 1]
    }

    ms_total = 0

    for unit, value in unit_map.items():
        if value[0] is not None:
            # cut of non-int-char with regex, which just indicates timetype
            value[0] = re.match(r"^\d+", value[0]).group()
            ms_total = ms_total + int(value[0])*value[1]

    if ms_total >= 2**63:
        msg = (
            "Your chosen %s is way too long."
            " It can not be over 9'223'372'036'854'775'807 ms."
            " You set %s as time, which results in %s ms."
            % (config_type, time_ms, ms_total)
        )
        fail_module(msg)

    return ms_total


def validate_index_interval_size(index_interval_size):
    # type: (str) -> int
    """Validate index_interval_size and convert to bytes.

    Keyword arguments:
    index_interval_size -- user configured index-interval-bytes, units: KiB, MiB, GiB, TiB, kB, MB, GB, TB

    Return:
    bytes_total -- converted config
    """
    return convert_storage_bytes(index_interval_size, "index_interval_size")


def validate_max_message_size(max_message_size):
    # type: (str) -> int
    """Validate max_message_size and convert to bytes.

    Keyword arguments:
    max_message_size -- user configured max-message-bytes, units: KiB, MiB, GiB, TiB, kB, MB, GB, TB

    Return:
    bytes_total -- converted config
    """
    return convert_storage_bytes(max_message_size, "max_message_size")


def validate_retention_size(retention_size):
    # type: (str) -> int
    """Validate retention_size and convert to bytes.

    Keyword arguments:
    retention_size -- user configured retention_size, units: KiB, MiB, GiB, TiB, kB, MB, GB, TB

    Return:
    bytes_total -- converted config
    """
    if retention_size == "-1":     # sets retention-time to unlimited
        return retention_size
    return convert_storage_bytes(retention_size, "retention_size")


def validate_segment_size(segment_size):
    # type: (str) -> int
    """Validate segment_size and convert to bytes.

    Keyword arguments:
    segment_size -- user configured segment_size, units: KiB, MiB, GiB, TiB, kB, MB, GB, TB

    Return:
    bytes_total -- converted config
    """
    return convert_storage_bytes(segment_size, "segment_size")


def validate_segment_index_size(segment_index_size):
    # type: (str) -> int
    """Validate segment_index_size and convert to bytes.

    Keyword arguments:
    segment_index_size -- user configured segment_index_size, units: KiB, MiB, GiB, TiB, kB, MB, GB, TB

    Return:
    bytes_total -- converted config
    """
    return convert_storage_bytes(segment_index_size, "segment_index_size")


def convert_storage_bytes(storage, config_type):
    # type: (str,str) -> int
    """Convert user-given size into bytes and validate size depending on config-type.

    Keyword arguments:
    storage -- user-given storage-size as string
    config_type -- for error-msg

    Return:
    bytes_total -- converted config
    """
    # ^((?P<KiB>\d+KiB)|(?P<MiB>\d+MiB)|(?P<GiB>\d+GiB)|(?P<TiB>\d+TiB)|(?P<kB>\d+kB)|(?P<MB>\d+MB)|(?P<GB>\d+GB)|(?P<TB>\d+TB))?$
    rema = re.match(r"^((?P<KiB>\d+KiB)|(?P<MiB>\d+MiB)|(?P<GiB>\d+GiB)|(?P<TiB>\d+TiB)|(?P<kB>\d+kB)|(?P<MB>\d+MB)|(?P<GB>\d+GB)|(?P<TB>\d+TB)|(?P<B>\d+B))?$", storage)

    t = rema.span()
    if t[1] == 0:
        msg = (
            "Could not parse given %s: %s into bytes."
            " Please use one of the following units: KiB, MiB, GiB, TiB, kB, MB, GB, TB, B."
            % (config_type, storage)
        )
        fail_module(msg)

    # map storage to unit and multiplicator
    unit_map = {
        "KiB": [rema.group("KiB"), 1024],
        "MiB": [rema.group("MiB"), 1048576],
        "GiB": [rema.group("GiB"), 1073741824],
        "TiB": [rema.group("TiB"), 1099511627776],
        "kB": [rema.group("kB"), 1000],
        "MB": [rema.group("MB"), 1000000],
        "GB": [rema.group("GB"), 1000000000],
        "TB": [rema.group("TB"), 1000000000000],
        "B": [rema.group("B"), 1]
    }

    # find the one matched storage-unit, and convert to bytes
    for unit, value in unit_map.items():
        if value[0] is not None:
            # cut off non-int-char
            value[0] = re.match(r"^\d+", value[0]).group()
            bytes_total = int(value[0])*value[1]

    # check if total-bytes is in valid range depending on config-type
    if config_type == "retention_size":
        if bytes_total >= 2**63:
            msg = (
                "Your chosen %s is way too long."
                " It can not be over 9'223'372'036'854'775'807 bytes."
                " You set %s as size, which results in %s bytes."
                % (config_type, storage, bytes_total)
            )
            fail_module(msg)
    else:
        if config_type == "segment_size":
            if bytes_total < 14:
                msg = (
                    "Your chosen %s must be at least 14 bytes."
                    " You set %s as size, which results in %s bytes."
                    % (config_type, storage, bytes_total)
                )
                fail_module(msg)
        if bytes_total >= 2**32:
            msg = (
                "Your chosen %s is way too long."
                " It can not be over 4'294'967'295 bytes."
                " You set %s as size, which results in %s bytes."
                % (config_type, storage, bytes_total)
            )
            fail_module(msg)
    return bytes_total

##########################################
#                                        #
#         ADMIN-CONFIG-VALIDATION        #
#                                        #
##########################################


def validate_broker(broker_definition):
    module.params['bootstrap_server']=validate_server(broker_definition, "broker")

def validate_zookeeper(zookeeper_definition):
    module.params['zookeeper']=validate_server(zookeeper_definition, "zookeeper")


def validate_server(server_definition, servertype):
    # type: (list, str) -> str
    """Validate server-definition.
    Check if connection to defined server is possible and convert to string for further use.

    Keyword arguments:
    server_definition -- list containing server. Pattern per server: 'host:port'.
    servertype -- to differentiate if broker or zookeeper gets tested for meaningful error-messages.

    Return:
    final_server_definition -- connections as a string: 'host:port,host:port'.
    """
    server_def_list = []
    for server in server_definition:
        server_parts = server.split(":")
        if len(server_parts) == 2:
            validate_ipv4(server_parts, servertype)
        if len(server_parts) > 2:
            validate_ipv6(server, servertype)
        if len(server_parts) < 2:
            msg = (
                "%s-Definition does not seem to be valid: %s"
                " Use following pattern per server: host:port."
                % (servertype,server)
            )
            fail_module(msg)
        server_def_list.append(server)
    final_server_definition = ",".join(server_def_list)
    return(final_server_definition)

def validate_ipv4(server, servertype):
    # type: (list)
    """Validate IPv4-address, trying to build a tcp-connection to given address.

    Keyword arguments:
    server -- definition of one server, as a list: [host,port]
    servertype -- to differentiate if broker or zookeeper gets tested for meaningful error-messages.

    Return:
    server -- valid server as string: 'host:port'
    """
    port = validate_port(server[1], server, servertype)
    ip = server[0]
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # try to make a connection
        sock.connect((ip, port))
        sock.close()
    except socket.error:
        sock.close()
        msg = (
            "Can not connect to %s: %s"
            " Please check if the definition is right."
            % (servertype, server)
        )
        fail_module(msg)

def validate_ipv6(server, servertype):
    # type: (str)
    """Validate IPv6-address, trying to build a tcp-connection to given address.

    Keyword arguments:
    server -- definition of one server, as a list: [host,port]
    servertype -- to differentiate if broker or zookeeper gets tested for meaningful error-messages.

    Return:
    server -- valid server as string: 'host:port'
    """
    # split server-definition in ip-address and port
    ip_port = server.rsplit(":",1)
    port = validate_port(ip_port[1], server, servertype)
    ip = ip_port[0]
    # remove square bracket from ipv6-definition, eg. [::1]
    ip = ip[1:-1]

    sock_ipv6 = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    try:
        sock_ipv6.connect((ip, port))
        sock_ipv6.close()
    except socket.error:
        sock_ipv6.close()
        msg = (
            "Can not connect to %s: %s"
            " Please check if the definition is right."
            % (servertype, server)
        )
        fail_module(msg)

def validate_port(port, server, servertype):
    # type: (str) -> int
    """Validate port.

    Keyword arguments:
    port -- port for tcp-connection to server.
    server -- definition of one server, as a list: [host,port] for meaningful error-messages.
    servertype -- to differentiate if broker or zookeeper gets tested for meaningful error-messages.

    Return:
    port -- port for tcp-connection to server.
    """
    try:
        port = int(port)
    except ValueError:
        msg = (
            "Port for %s as %s needs to be int, but got: %s"
            % (server, servertype, port)
        )
        fail_module(msg)
    if (port <= 1024) or (port > 65535):
        msg = (
            "Valid Port-Range is: 1024-65534."
            " But given Port for %s as %s is: %s"
            % (server, servertype, port)
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
        msg = (
            "Supported SASL-Mechanisms are: PLAIN,"
            " GSSAPI, SCRAM-SHA-256, SCRAM-SHA-512,"
            " and OAUTHBEARER."
        )
        fail_module(msg)


def validate_sasl_PLAIN():
    # type: ()
    """Validate sasl-mechanism PLAIN, check if mandatory username, password and protocol is set.
    Also check if ca-location is set.
    Set each value in admin_conf.
    """

    if (module.params['username'] is None or
            module.params['password'] is None or
            module.params['use_tls'] is None):
        msg = (
            "If you choose PLAIN as sasl_mechanism,"
            " you also need to set: username,"
            " password and use_tls."
        )
        fail_module(msg)
    admin_conf['sasl.mechanism'] = "PLAIN"
    admin_conf['sasl.password'] = module.params['password']
    admin_conf['sasl.username'] = module.params['username']
    if (module.params['use_tls'] is True or
            module.params['use_tls'] == 'true' or
            module.params['use_tls'] == 'True'):
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
    global diff

    # initialize object AnsibleModule
    module_args = dict(
        name = dict(type = 'str', required = True),
        state = dict(type = 'str', required = True, choices = ['absent', 'present']),
        partitions = dict(type = 'int', required = True),
        replication_factor = dict(type = 'int', required = True),
        bootstrap_server = dict(type = 'list'),
        zookeeper = dict(type = 'list'),
        config = dict(type = 'dict'),
        sasl_mechanism = dict(
            type = 'str',
            choices = [
                'GSSAPI',
                'PLAIN',
                'SCRAM-SHA-256',
                'SCRAM-SHA-512',
                'OAUTHBEARER'
            ]
        ),
        password = dict(type = 'str', no_log = True),
        username = dict(type = 'str'),
        use_tls = dict(type = 'bool'),
        ca_location = dict(type = 'str')
    )

    result = dict(
        changed=False,
        name='',
        state='',
    )

    diff=dict(
        before=dict(),
        after=dict()
    )

    # example
    # exclusive_module_args = [
    #    ['delete_retention_ms','delete_retention_time'],
    #    ['file_delete_delay_ms','file_delete_delay_time']
    #]
    exclusive_module_args = []

    module = AnsibleModule(
        argument_spec = module_args,
        mutually_exclusive = exclusive_module_args,
        supports_check_mode = True
    )

    # set topicname as result as soon as possible, for meaningful error-messages
    result['name'] = module.params['name']

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
            module.params['bootstrap_server'] = json.loads(os.environ['KAFKA_BOOTSTRAP'].replace("'", "\""))
        except KeyError:
            msg = (
                "It seems that there is no bootstrap-server definition in"
                " the playbook and also not as an environment-variable."
                " If you want to use an environment-variable, make sure it's"
                " name is 'KAFKA_BOOTSTRAP'."
            )
            fail_module(msg)

    # zookeeper can also be an env-var, but must be parsed into a list
    # also, zookeeper is at the moment an optional parameter
    if module.params['zookeeper'] is None:
        try:
            module.params['zookeeper']=json.loads(os.environ['ZOOKEEPER'].replace("'", "\""))
        except KeyError:
            module.warn(
                "There is no zookeeper-parameter provided."
                " Replication-factor can not be verified if valid and"
                " can not be modified."
            )
            pass
#            msg = (
#                "It seems that there is no zookeeper definition in"
#                " the playbook and also not as an environment-variable."
#                " If you want to use an environment-varible, make sure it's"
#                " name is 'ZOOKEEPER'."
#            )
#            # turn line below to a comment to make zookeeper-def optional
#            fail_module(msg)

    # loop through env-param-dict and set all params which are set in env
    for key, value in env_param.items():
        if module.params[key] is None:
            module.params[key] = os.environ.get(value)

    # admin-config dictionary for creating adminclient
    admin_conf = {}

    # map param to corresponding validation-function
    # Choice-Parameter are left out because Ansible validates them
    # Child-Parameter like username are left out aswell because
    # they get validated through their parent-param like sasl_mechanism
    params_valid_dict = dict(
        name = validate_name,
        partitions = validate_part_factor,
        replication_factor = validate_rep_factor,
        bootstrap_server = validate_broker,
        zookeeper = validate_zookeeper,
        sasl_mechanism = validate_sasl_mechanism
    )

    # loop through params_valid_dict and validate all params which are set (not none)
    # validate all parameters
    for key in params_valid_dict:
        if module.params[key] is not None:
            # params_valid_dict[key] returns valid-func.
            # Pass as param for the valid-func the user-set param with module.params[key]
            params_valid_dict[key](module.params[key])

    # create admin_conf-dict for connection-params like authentication
    admin_conf['bootstrap.servers'] = module.params['bootstrap_server']

    # after validation, initialize object AdminClient for configuring topics on kafka-broker
    admin = AdminClient(admin_conf)

    # check if topic exists and act according to return-value
    topic_exists = check_topic(module.params['name'])

    # if topic exists and should stay so, compare configuration and modify them if needed
    if topic_exists and (module.params['state'] == "present"):
        result['state'] = "present"
        mod_part = compare_part(module.params['name'], module.params['partitions'])
        mod_rep = compare_rep(module.params['name'], module.params['replication_factor'])
        new_conf = add_config_together(module.params['name'], module.params['config'])
        mod_conf = compare_config(module.params['name'], new_conf)
        # if checkmode, do nothing, return changes
        if module.check_mode:
            if mod_part:
                result['changed'] = True
            if mod_rep:
                result['changed'] = True
            if mod_conf:
                result['changed'] = True
            result['diff'] = diff
            module.exit_json(**result)
        if mod_part:
            modify_part(
                module.params['name'],
                module.params['partitions']
            )
            result['changed'] = True
        if mod_rep:
            modify_rep(
                module.params['name'],
                module.params['partitions'],
                module.params['replication_factor'],
                module.params['zookeeper']
            )
            result['changed'] = True
        if mod_conf:
            modify_config(module.params['name'], new_conf)
            result['changed'] = True
        result['diff'] = diff

    # if topic exists and should not, delete it
    if topic_exists and (module.params['state'] == "absent"):
        if module.check_mode:
            result['changed'] = True
            result['diff'] = diff
            module.exit_json(**result)
        delete_topic(module.params['name'])
        result['changed'] = True
        result['state'] = "absent"
        diff['before']['state']='present'
        diff['after']['state']='absent'
        result['diff'] = diff

    # if topic does not exist, but should, create and configure
    if not topic_exists and (module.params['state'] == "present"):
        new_conf = add_config_together(module.params['name'], module.params['config'])
        if module.check_mode:
            result['changed'] = True
            result['diff'] = diff
            module.exit_json(**result)
        create_topic(
            module.params['name'],
            module.params['partitions'],
            module.params['replication_factor'],
            new_conf
        )
        result['changed'] = True
        result['state'] = "present"
        diff['before']['state']='absent'
        diff['after']['state']='present'
        result['diff'] = diff

    # if topic does not exist and should stay that way, do nothing
    if not topic_exists and (module.params['state'] == "absent"):
        result['state'] = "absent"

    # exit module and print result-dictionary
    module.exit_json(**result)

if __name__ == '__main__':
    main()

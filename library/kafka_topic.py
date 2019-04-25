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

# validate name for topic
# param: name = topicname, type: str
# no return
def validate_name(name):
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

# validate partition-number and replication-number
# param: factor = number for partitions or replication, type:int
# param: part_or_rep = which gets validated for error-message if needed, type: str
# no return
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

# validate broker-definition
# param: broker_definition, type:list, pattern per broker: 'host:port'
# returns brokers as a string with following pattern: 'host:port,host:port'
def validate_broker(broker_definition):
    broker_def_list = []
    for broker in broker_definition:
        broker_parts = broker.split(":")
        if len(broker_parts) == 2:
            broker = validate_ipv4(broker_parts)
        if len(broker_parts) > 2:
            msg = ("It seems you tried so set an IPv6-Address: %s" \
                  " We do not support that so far - please set" \
                  " an IPv4-Address." \
                  %(broker)
                  )
            fail_module(msg)
        if len(broker_parts) < 2:
            msg = ("Broker-Definition does not seem to be valid: %s" \
                  " Use following pattern per broker: host:port." \
                  %(broker)
                  )
            fail_module(msg)
        broker_def_list.append(broker)
    final_broker_definition = ",".join(broker_def_list)
    return final_broker_definition

# validate ipv4-address, trying to build a tcp-connection to given address
# param: broker = one broker-definition, type: list, pattern: [host,port]
# return: broker, type: str, pattern: 'host:port'
def validate_ipv4(broker):
    port = validate_port(broker[1])
    ip = broker[0]
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        #try to make a connection
        sock.connect((ip,port))
        sock.close()
    except socket.error:
        sock.close()
        msg = ("Can not connect to broker: %s" \
              " Please check if the definition is right." \
              %(broker)
              )
        fail_module(msg)
    return str(ip)+":"+str(port)

# validate port
# param: port, type: str
# return: port, type: int
def validate_port(port):
    try:
        port = int(port)
    except ValueError:
        msg = ("Port needs to be int, but got: %s" \
              %(port)
              )
        fail_module(msg)
    if (port <= 0) or (port > 65535):
        msg = ("Valid Port-Range is: 1-65535." \
              " But given Port is: %s" \
              %(port)
              )
        fail_module(msg)
    return port

# validate retention time and convert to ms
# param: retention = retention-time, type: str, pattern: %d%h%m%s%ms
# return: retention-time in ms unless set to unlimited, type: int or string
def validate_retention_ms(retention):
    if retention == "-1":     #sets retention-time to unlimited
        return retention

    #try to parse retention with regex into groups, split by timetype
    rema = re.match( r"(?P<days>\d+d)?(?P<hours>\d+h)?(?P<minutes>\d+m)?(?P<seconds>\d+s)?(?P<miliseconds>\d+m)?",retention)

    t = rema.span()
    if t[1] == 0:
        msg = ("Could not parse given retention-time: %s into ms." \
              " Please use the following pattern: %%d%%h%%m%%s%%ms." \
              %(retention)
              )
        fail_module(msg)

    days = rema.group("days")
    hours = rema.group("hours")
    minutes = rema.group("minutes")
    seconds = rema.group("seconds")
    miliseconds = rema.group("miliseconds")

    timetype = [days, hours, minutes, seconds, miliseconds]
    multiplier = [86400000,3600000,60000,1000,1]
    ms_total = 0
    i = 0

    for t_type in timetype:     #convert to ms and add together
        if t_type is not None:
            ms_total = ms_total + (int(t_type[:-1])*multiplier[i])     #[:-1] cuts of last char (which indicates timetype and is not an int)
        i = i+1

    if (ms_total >= 2**63):
        msg = ("Your chosen retention-time is way too long." \
              " Retention-time can not be over 2^63ms." \
              " You set %s as retention, which results in %s ms." \
              %(retention, ms_total)
              )
        fail_module(msg)

    return ms_total


##########################################
#                                        #
#           KAFKA-FUNCTIONS              #
#                                        #
##########################################

# check if topic exists
# param: topic = topicname, type: str
# return: True if topic exists, False if not, type: bool
def check_topic(topic):
    topics = admin.list_topics(timeout=5).topics    #type(topics)=dict
    try:
        topics[topic]
    except KeyError:
        return False
    return True


# compare the defined partitions and replication-factor in the playbook with the actually set
# param: topic = topicname, type: str
# param: partitions, type: int
# param: replication_factor, type: int
# return: True if change is needed, False if no change needed, type: bool
def compare_part_rep(topic, partitions, replication_factor):
    metadata = admin.list_topics()                                    #type(metadata.topics) = dict
    old_part = len(metadata.topics[topic].partitions)                 #access partitions of topic over .partitions-func
    old_rep = len(metadata.topics[topic].partitions[0].replicas)      #type(partitions) = dict, access replicas with partition-id as key over .replicas-func
    if partitions < old_part:
        msg = ("It is not possible to reduce the amount of partitions." \
              " At the moment, there are %s partitions for the topic %s." \
              " You tried to set %s as the new amount of partitions." \
              %(old_part,topic,partitions)
              )
        fail_module(msg)
    if replication_factor != old_rep:
        msg = ("It is not possible to modify the replication_factor." \
              " At the moment, it is set to %s and you tried to set it to %s." \
              %(old_rep,replication_factor)
              )
        fail_module(msg)
    if partitions == old_part:
        return False
    return True


# compare the defined config in the playbook with the one set at the moment for this topic
# param: topic = topicname, type: str
# param: new_config = dictionary with new config and values, type: dict
# return: True if change is needed, False if no change is needed, type: bool
def compare_config(topic, new_config):
    resource = [ConfigResource("TOPIC", topic)]
    des = admin.describe_configs(resource)

    y = list(des.values())
    old_conf = y[0].result()

    for config, newvalue in new_config.items():       #iterate trough new-config-dict and compare with old-config-dict, using config as key
        if newvalue != old_conf[config].value:
            return True

    return False



# modify config
# param: topic = topicname, type: str
# param: new_config = dictionary with new config and values, type: dict
# return: no return
def modify_config(topic, new_config):
    resource = [ConfigResource("TOPIC", topic)]
    des = admin.describe_configs(resource)

    y = list(des.values())
    old_conf = y[0].result()

    for config, newvalue in new_config.items():       #iterate trough new-config-dict and set them on topic-resource
        resource[0].set_config(config,newvalue)

    des = admin.alter_configs(resource)             #alter topic with new config
    y = list(des.values())

    try:
        conf = y[0].result()                        #use .result-func for finalizing
    except Exception:
        msg = ("Failed to finalize config-change for topic %s" \
              %(topic)
              )
        fail_module(msg)


# modify partition
# param: topic = topicname, type: str
# param: new_part = int representing new number of partitions
# return: no return
def modify_part(topic, new_part):
    new_parts = [NewPartitions(topic, new_part)]
    fs = admin.create_partitions(new_parts, validate_only=False)

    y = list(fs.values())

    try:
        conf = y[0].result()
    except Exception:
        msg = ("Failed to finalize partition-change for topic %s" \
              %(topic)
              )
        fail_module(msg)


# create a new topic, setting partition and replication-factor right now
# param: topic = topicname, type: str
# param: partitions = number of partitions, type: int
# param: replication_factor = number of replicatons, which is from then on immutable, type: int
# return: no return
def create_topic(topic, partitions, replication_factor):
    topic = [NewTopic(topic, num_partitions=partitions,  replication_factor=replication_factor)]
    fs = admin.create_topics(topic)

    y = list(fs.values())

    try:
        new_topic = y[0].result()
    except Exception:
        msg = ("Failed to create topic %s." \
              %(topic)
              )
        fail_module(msg)


# delete topic, this func only needs the topicname
# param: topic = topicname, type: str
# return: no return
def delete_topic(topic):
    topic = [topic]
    fs = admin.delete_topics(topic)

    y = list(fs.values())

    try:
        deleted_topic = y[0].result()
    except Exception:
        msg = ("Failed to delete topic %s." \
              %(topic)
              )
        fail_module(msg)


# add different configs together in one dictionary for passing to compare-config and modify-config
# param: module = AnsibleModule, containing the possible configs
# return: new_config = dictionary containing all set configs, type: dict
def add_config_together(module):
    configs = {
        "cleanup.policy":module.params["cleanup_policy"],
        "retention.ms":module.params["retention"]
    }
    new_conf = {}
    for conf, value in configs.items():
        if configs[conf] is not None:
            new_conf[conf] = value
    return new_conf


##########################################
#                                        #
#           ANSIBLE-FUNCTIONS            #
#                                        #
##########################################

def fail_module(msg):
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

    module_args = dict(
        name = dict(type='str', required=True),
        state = dict(type='str', required=True, choices=['absent','present']),
        partitions = dict(type='int', required=True),
        replication_factor = dict(type='int', required=True),
        bootstrap_server = dict(type='list', required=True),
        cleanup_policy = dict(type='str', choices=['compact','delete']),
        retention = dict(type='str')
    )

    result = dict(
        changed = False,
        name = '',
        state = ''
    )

    module = AnsibleModule(
        argument_spec=module_args,
    )

    result['name'] = module.params['name']
    validate_name(module.params['name'])

    validate_factor(module.params['partitions'], "partitions")
    validate_factor(module.params['replication_factor'], "replication-factor")
    final_broker_definition = validate_broker(module.params['bootstrap_server'])

    if module.params['retention'] is not None:
        retention_ms = validate_retention_ms(module.params['retention'])
        module.params['retention'] = retention_ms

    admin_conf = {}
    admin_conf['bootstrap_server'] = final_broker_definition

    admin = AdminClient({'bootstrap.servers':final_broker_definition})

    topic_exists = check_topic(module.params['name'])


    if topic_exists and (module.params['state'] == "present"):
        result['state'] == "present"
        mod_part = compare_part_rep(module.params['name'], module.params['partitions'], module.params['replication_factor'])
        if mod_part:
            modify_part(module.params['name'], module.params['partitions'])
            result['changed'] = True
        new_conf = add_config_together(module)
        mod_conf = compare_config(module.params['name'], new_conf)
        if mod_conf:
            modify_config(module.params['name'], new_conf)
            result['changed'] = True


    if topic_exists and (module.params['state'] == "absent"):
        delete_topic(module.params['name'])
        result['changed'] = True
        result['state'] = "absent"


    if not topic_exists and (module.params['state'] == "present"):
        create_topic(module.params['name'], module.params['partitions'], module.params['replication_factor'])
        result['changed'] = True
        result['state'] = "present"
        new_conf = add_config_together(module)
        topic_exists = check_topic(module.params['name'])
        while not topic_exists:
            topic_exists = check_topic(module.params['name'])
        modify_config(module.params['name'], new_conf)


    if not topic_exists and (module.params['state'] == "absent"):
        result['state'] = "absent"

    module.exit_json(**result)

if __name__ == '__main__':
    main()

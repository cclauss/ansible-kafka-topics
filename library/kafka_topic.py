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
  retention_time:
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
    retention_time: 2d12h

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
import time

import pdb

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

    if not bool(new_config):
        default_configs = {
            "cleanup.policy":"delete",
            "retention.ms":"604800000",
            "compression_type":"producer"
        }
        for conf, defaultvalue in default_configs.items():
            if defaultvalue != old_conf[conf].value:
                return True

    else:
        for config, newvalue in new_config.items():       #iterate trough new-config-dict and compare with old-config-dict, using config as key
            if str(newvalue) != old_conf[config].value:
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

    try:
        des = admin.alter_configs(resource)             #alter topic with new config
        y = list(des.values())
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

    try:
        fs = admin.create_partitions(new_parts, validate_only=False)
        y = list(fs.values())
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
# param: new_conf = configuration-dict for topic, for example containing retention-time, type: dict
# return: no return
def create_topic(topic, partitions, replication_factor, new_conf):
    topic = [NewTopic(topic, num_partitions=partitions,  replication_factor=replication_factor, config=new_conf)]

    try:
        fs = admin.create_topics(topic)
        y = list(fs.values())
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

    try:
        fs = admin.delete_topics(topic)
        y = list(fs.values())
        deleted_topic = y[0].result()
    except Exception:
        msg = ("Failed to delete topic %s." \
              %(topic)
              )
        fail_module(msg)



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
def validate_factor(factor):
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

# add different configs together in one dictionary for passing to compare-config and modify-config
# param: module = AnsibleModule, containing the possible configs
# return: new_config = dictionary containing all set configs, type: dict
def add_config_together(module):
    configs = {
        "cleanup.policy":module.params["cleanup_policy"],
        "retention.ms":module.params["retention_time"],
        "compression.type":module.params["compression_type"]
    }
    new_conf = {}
    for conf, value in configs.items():
        if configs[conf] is not None:
            new_conf[conf] = value
    return new_conf

# validate retention time and convert to ms
# param: retention_time = retention-time, type: str, pattern: %d%h%m%s%ms
def validate_retention_ms(retention_time):
    if retention_time == "-1":     #sets retention-time to unlimited
        return retention_time
    convert_time_ms(retention_time,"retention_time")

# convert user-given time to ms
# param: time_ms = user-given time, config_type = for setting config and error-msg
def convert_time_ms(time_ms,config_type):
    #try to parse retention_time with regex into groups, split by timetype
    rema = re.match( r"(?P<days>\d+d)?(?P<hours>\d+h)?(?P<minutes>\d+m)?(?P<seconds>\d+s)?(?P<miliseconds>\d+m)?",time_ms)

    t = rema.span()
    if t[1] == 0:
        msg = ("Could not parse given %s: %s into ms." \
              " Please use the following pattern: %%d%%h%%m%%s%%ms." \
              %(config_type,time_ms)
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
        msg = ("Your chosen %s is way too long." \
              " Retention-time can not be over 2^63ms." \
              " You set %s as retention, which results in %s ms." \
              %(config_type,time_ms,ms_total)
              )
        fail_module(msg)

    module.params[config_type] =  ms_total



##########################################
#                                        #
#         ADMIN-CONFIG-VALIDATION        #
#                                        #
##########################################

# validate broker-definition
# param: broker_definition, type:list, pattern per broker: 'host:port'
# sets broker-list as a string for admin-conf: 'host:port,host:port'
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
    module.params['bootstrap_server'] = final_broker_definition

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
    if (port <= 1024) or (port > 65535):
        msg = ("Valid Port-Range is: 1-65535." \
              " But given Port is: %s" \
              %(port)
              )
        fail_module(msg)
    return port

# validate sasl-mechanism
# param: sasl_mechanism = user-defined param
def validate_sasl_mechanism(sasl_mechanism):
    if sasl_mechanism == "PLAIN":
        validate_sasl_PLAIN()
    if sasl_mechanism == "GSSAPI":
        fail_module("GSSAPI not supported so far")
    if sasl_mechanism == "SCRAM-SHA-256":
        fail_module("SCRAM-SHA-256 not supported so far")
    if sasl_mechanism == "SCRAM-SHA-512":
        fail_module("SCRAM-SHA-512 not supported so far")
    if sasl_mechanism == "OAUTHBEARER":
        fail_module("OAUTHBEARER not supported so far")
    else:
        msg = ("Supported SASL-Mechanisms are: PLAIN,"\
              " GSSAPI, SCRAM-SHA-256, SCRAM-SHA-512,"\
              " and OAUTHBEARER."\
              )
        fail_module(msg)

# validate sasl-mechanism PLAIN, check if username, password and protocol is set
# also check if ca-location is set
# set each value in admin_conf
def validate_sasl_PLAIN():
    if module.params['sasl_username'] == None \
    or module.params['sasl_password'] == None \
    or module.params['security_protocol'] == None:
        msg = ("If you choose PLAIN as sasl_mechanism," \
              " you also need to set: sasl_username," \
              " sasl_password and security_protocol." \
              )
        fail_module(msg)
    admin_conf['sasl.mechanism'] = "PLAIN"
    admin_conf['sasl.password'] = module.params['sasl_password']
    admin_conf['sasl.username'] = module.params['sasl_username']
    if module.params['security_protocol'] == "ssl":
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
        name = dict(type='str', required=True),
        state = dict(type='str', required=True, choices=['absent','present']),
        partitions = dict(type='int', required=True),
        replication_factor = dict(type='int', required=True),
        bootstrap_server = dict(type='list', required=True),
        cleanup_policy = dict(type='str', choices=['compact','delete']),
        compression_type = dict(type='str', choices=['uncompressed','zstd','lz4','snappy','gzip','producer']),
        retention_time = dict(type='str'),
        sasl_mechanism = dict(type='str', choices=['GSSAPI','PLAIN','SCRAM-SHA-256','SCRAM-SHA-512','OAUTHBEARER']),
        sasl_password = dict(type='str'),
        sasl_username = dict(type='str'),
        security_protocol = dict(type='str', choices=['plaintext','ssl']),
        ca_location = dict(type='str')
    )

    result = dict(
        changed = False,
        name = '',
        state = ''
    )

    module = AnsibleModule(
        argument_spec=module_args,
    )

    # admin-config dictionary for creating adminclient
    admin_conf = {}

    # set topicname as result as soon as possible, for meaningful error-messages
    result['name'] = module.params['name']

    # param-list for later iterating through it for validating.
    # Choice-Parameter are left out because Ansible validates them
    # Child-Parameter like sasl_username are left out aswell because
    # they get validated through their parent-param like sasl_mechanism
    params = ['name','partitions','replication_factor','bootstrap_server',\
              'retention_time',\
              'sasl_mechanism']


    #map validation-function to corresponding params
    params_valid_dict = dict(
        name = validate_name,
        partitions = validate_factor,
        replication_factor = validate_factor,
        bootstrap_server = validate_broker,
        retention_time = validate_retention_ms,
        sasl_mechanism = validate_sasl_mechanism
    )

    # validate all parameters
    for element in params:
        if module.params[element] is not None:
            params_valid_dict[element](module.params[element])


    #create admin_conf-dict for connection-params like authentication
    admin_conf['bootstrap.servers'] = module.params['bootstrap_server']

    #pdb.set_trace()

    # after validation, initialize object AdminClient for configuring topics on kafka-broker
    admin = AdminClient(admin_conf)

    # check if topic exists and act according to return-value
    topic_exists = check_topic(module.params['name'])

    # if topic exists and should stay so, compare configuration and modify them if needed
    if topic_exists and (module.params['state'] == "present"):
        result['state'] = "present"
        mod_part = compare_part_rep(module.params['name'], module.params['partitions'], module.params['replication_factor'])
        if mod_part:
            modify_part(module.params['name'], module.params['partitions'])
            result['changed'] = True
        new_conf = add_config_together(module)
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
        create_topic(module.params['name'], module.params['partitions'], module.params['replication_factor'], new_conf)
        result['changed'] = True
        result['state'] = "present"

    # if topic does not exist and should stay that way, do nothing
    if not topic_exists and (module.params['state'] == "absent"):
        result['state'] = "absent"

    # exit module and print result-dictionary
    module.exit_json(**result)

if __name__ == '__main__':
    main()

#! /usr/bin/env python

from confluent_kafka.admin import AdminClient, ConfigResource, ConfigSource
import sys
import subprocess
import json
import time
import pdb
import os

def get_part_info(topic, conf, admin):
    """Get actually set partitions and add to config-dict
    for comparing and stuff.

    Keyword Arguments:
    topic -- topicname
    conf -- dict for adding partitions-info
    admin -- adminclient for fetching partition-info

    Return:
    conf -- dict containing now partition-info
    """
    # GET PARTITION AND REPLICA INFORMATION
    m = admin.list_topics()
    parts = {}
    for part in m.topics[topic].partitions.values():
        parts[part.id]=part.replicas

    conf['partitions']=parts
    return conf

def get_config(topic, admin):
    """Get raw topic-config for other functions to use

    Keyword Arguments:
    topic -- topicname
    admin -- adminclient for fetching config

    Return:
    konf -- dict containing raw topic-config
    """
    resources = [ConfigResource("TOPIC", topic)]
    des = admin.describe_configs(resources)
    y = list(des.values())
    konf = y[0].result()
    return konf

def get_config_info(topic, conf, admin):
    """Fill Topic-Configuration in config-dict
    for comparing and stuff.

    Keyword Arguments:
    topic -- topicname
    conf -- dict for adding topic-configuration
    admin -- adminclient for fetching topic-config-info

    Return:
    conf -- dict now containing topic-configuration
    """
    # GET TOPIC CONFIG
    konf = get_config(topic, admin)
    c = {}
    for key in konf:
        c[konf[key].name]=konf[key].value

    conf['config']=c
    return conf

def check_topic(topic, admin):
    """Check if topic exists and return bool

    Keyword Arguments:
    topic -- topicname
    admin -- adminclient for fetching topic

    Return:
    bool -- if topic exists or not
    """
    topics = admin.list_topics(timeout=5).topics
    try:
        topics[topic]
    except KeyError:
        return False
    return True

def delete_topic(topic, admin):
    """Delete Topic.

    Keyword Arguments:
    topic -- topicname
    admin -- adminclient for deleting topic

    Return:
    bool -- if deleting worked or not
    """
    to_del = [topic]
    try:
        fs = admin.delete_topics(to_del)
        y = list(fs.values())
        y[0].result()
    except KafkaException as e:
        return False
    return True

def is_default(topic, admin, confignames):
    """Check if configs in list are on defaultvalue.
    If not, they will be filled in a separate list.

    Keyword Arguments:
    topic -- topicname
    admin -- adminclient for fetching topicconfig
    confignames -- list containing configs to compare

    Return:
    faulties -- list containing configs not on defaultvalue
    """
    faulties = []
    konf = get_config(topic, admin)

    for configname in confignames:
        if configname == "max.compaction.lag.ms":
            try:
                if not konf[configname].is_default:
                    # check if config is not on default because brokerconfig says so
                    if konf[configname].source != 4:
                        faulties.append(configname)
            except KeyError:
                pass
        elif not konf[configname].is_default:
            # check if config is not on default because brokerconfig says so
            if konf[configname].source != 4:
                faulties.append(configname)
    return faulties

def is_not_default(topic, admin, confignames):
    """Check if configs in list are NOT on defaultvalue.
    If they are, they will be filled in a separate list.

    Keyword Arguments:
    topic -- topicname
    admin -- adminclient for fetching topicconfig
    confignames -- list containing configs to compare

    Return:
    faulties -- list containing configs on defaultvalue
    """
    faulties = []
    konf = get_config(topic, admin)

    for configname in confignames:
        if configname == "max.compaction.lag.ms":
            try:
                if konf[configname].is_default:
                    faulties.append(configname)
            except KeyError:
                pass
        elif konf[configname].is_default:
            faulties.append(configname)
    return faulties

def get_econf(econf, jsonfile):
    """Load Json-File with specified, expected Topicconfig.

    Keyword Arguments:
    econf -- dict for loading json-file into
    jsonfile -- name of json-file to load

    Return:
    econf -- dict containing expected topicconfig
    """
    with open(jsonfile) as f:
        data = json.load(f)
    econf = data["ANSIBLE_MODULE_ARGS"]
    return econf

def run_module(args):
    """Run module kafka_topic with json-file arguments.
    Return Stdout as dict.

    Keyword Arguments:
    args -- jsonfile-name

    Return:
    js -- dict containing stdout
    """
    cmd = "python ../../library/kafka_topic.py " + args
    try:
        result = subprocess.check_output(cmd, shell=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))
    tmpstr = result.decode("utf-8")
    js = json.loads(tmpstr)
    return js

def compare_all(confignames, econf, aconf, result, echanged):
    """Compare expected Topic-Configuration with actual Configuration.

    Keyword arguments:
    confignames -- List containing all possible Topic-Configs
    econf -- expected configuration, loaded from Json-Argument-File
    aconf -- actual config, fetched with adminclient
    result -- result returned from module we test
    echanged -- expected result, if something changed or not (bool)
    """
    time.sleep(3)
    something_wrong = False

    if result["changed"] != echanged:
        print("EXPECTED RESULT changed = ",echanged,", but got: ")
        print(result["changed"])
        something_wrong = True

    ndf = []
    df=[]
    for config in confignames:
        df.append(config)

    for ekey, evalue in econf.items():
        for ckey, cvalue in confignames_map.items():
            if ekey == ckey:
                ndf.append(cvalue)
                df.remove(cvalue)

    while not check_topic(topic,a):
        time.sleep(1)

    aconf = get_part_info(topic, aconf, a)
    aconf = get_config_info(topic, aconf, a)

    faulties = is_default(topic, a, df)
    if faulties:
        something_wrong = True
        print("ERROR: THERE ARE CONFIGVALUES, WHICH ARE NOT AS EXPECTED. ")
        print("EXPECTED THEM TO BE ON DEFAULTVALUE.")
        for faultie in faulties:
            print(faultie)

    faulties = is_not_default(topic, a, ndf)
    if faulties:
        something_wrong = True
        print("ERROR: THERE ARE CONFIGVALUES, WHICH ARE NOT AS EXPECTED. ")
        print("EXPECTED THEM TO NOT BE ON DEFAULTVALUE.")
        for faultie in faulties:
            print(faultie)

    if econf["partitions"] != len(aconf["partitions"]):
        something_wrong = True
        print("ERROR: THE EXPECTED NUMBER OF PARTITIONS DOES NOT MATCH THE ACTUAL NUMBER: ")
        print("EXPECTED: ",econf["partitions"])
        print("ACTUAL: ",len(aconf["partitions"]))

    if econf["replication_factor"] != len(aconf["partitions"][0]):
        something_wrong = True
        print("ERROR: THE EXPECTED NUMBER OF REPLICAS DOES NOT MATCH THE ACTUAL NUMBER: ")
        print("EXPECTED: ",econf["replication_factor"])
        print("ACTUAL: ",len(aconf["partitions"][0]))

    if something_wrong:
        print("EXPECTED CONFIG:")
        print(econf)
        print("ACTUAL CONFIG:")
        print(aconf)
        sys.exit(0)

if __name__ == '__main__':
    topic = "testTheTopicMachine"

    # load KAFKA_BOOTSTRAP env-var for connection to kafka-cluster
    try:
        bootstrap_server = json.loads(os.environ['KAFKA_BOOTSTRAP'].replace("'","\""))
    except KeyError:
        print("There is no env-var definition for KAFKA_BOOTSTRAP. Please set one.")
        sys.exit(1)

    # convert loaded KAFKA_BOOTSTRAP(list) into string, because thats what we need
    # for adminclient-config
    bs_server =""
    for server in bootstrap_server:
        bs_server = bs_server + server + ","

    admin_conf={}
    admin_conf['bootstrap.servers'] = bs_server

    # if not localhost, load auth-env-var and add to adminclient-config
    env_param = dict(
        sasl_mechanism="KAFKA_SASL_MECHANISM",
        sasl_password="KAFKA_PASSWORD",
        sasl_username="KAFKA_USER",
        security_protocol="KAFKA_USE_TLS",
        ssl_ca_location="KAFKA_CA_LOCATION"
    )

    for key, value in env_param.items():
        tmp = os.environ.get(value)
        if tmp is not None:
            if key == "sasl_mechanism":
                admin_conf['sasl.mechanism'] = tmp
            elif key == "sasl_password":
                admin_conf['sasl.password'] = tmp
            elif key == "sasl_username":
                admin_conf['sasl.username'] = tmp
            elif key == "security_protocol":
                admin_conf['security.protocol'] = "sasl_ssl"
            elif key == "ssl_ca_location":
                admin_conf['ssl.ca.location'] = tmp

    a = AdminClient(admin_conf)
    aconf = {}  # actual topic-config
    econf={}    # expected topic-config
    confignames = [
        "cleanup.policy","compression.type","delete.retention.ms","file.delete.delay.ms",
        "flush.messages","flush.ms","follower.replication.throttled.replicas",
        "index.interval.bytes","leader.replication.throttled.replicas","max.compaction.lag.ms",
        "max.message.bytes","message.format.version","message.timestamp.difference.max.ms",
        "message.timestamp.type","min.cleanable.dirty.ratio","min.compaction.lag.ms",
        "min.insync.replicas","preallocate","retention.bytes","retention.ms",
        "segment.bytes","segment.index.bytes","segment.jitter.ms","segment.ms",
        "unclean.leader.election.enable","message.downconversion.enable"
    ]
    confignames_map = dict(
        cleanup_policy = "cleanup.policy",
        compression_type = "compression.type",
        delete_retention_ms = "delete.retention.ms",
        file_delete_delay_ms = "file.delete.delay.ms",
        flush_messages = "flush.messages",
        flush_ms = "flush.ms",
        follower_replication_throttled_replicas = "follower.replication.throttled.replicas",
        index_interval_bytes = "index.interval.bytes",
        leader_replication_throttled_replicas = "leader.replication.throttled.replicas",
        max_compaction_lag_ms = "max.compaction.lag.ms",
        max_message_bytes = "max.message.bytes",
        message_format_version = "message.format.version",
        message_timestamp_difference_max_ms = "message.timestamp.difference.max.ms",
        message_timestamp_type = "message.timestamp.type",
        min_cleanable_dirty_ratio = "min.cleanable.dirty.ratio",
        min_compaction_lag_ms = "min.compaction.lag.ms",
        min_insync_replicas = "min.insync.replicas",
        preallocate = "preallocate",
        retention_bytes = "retention.bytes",
        retention_ms = "retention.ms",
        segment_bytes = "segment.bytes",
        segment_index_bytes = "segment.index.bytes",
        segment_jitter_ms = "segment.jitter.ms",
        segment_ms = "segment.ms",
        unclean_leader_election_enable = "unclean.leader.election.enable",
        message_downconversion_enable = "message.downconversion.enable"
    )



    # Check if topic already exists.
    # Delete Topic if thats the case
    if check_topic(topic, a):
        print("WARNING: TOPIC "+topic+" ALREADY EXISTS.")
        print("ATTEMPT TO DELETE "+topic+" FOR PROCEEDING WITH INTEGRATIONTEST.")
        b = delete_topic(topic,a)
        if not b:
            print("ERROR: DELETING TOPIC "+topic+" FAILED")
            raise e
        while check_topic(topic,a):
            time.sleep(1)
        print("TOPIC "+topic+" SUCCESSFULLY DELETED.")
        print("PROCEED NOW WITH TESTING.")

    try:
        #######################################
        #                                     #
        #  Create Topic                       #
        #                                     #
        #######################################
        print("-------------------------------------")
        print("CREATE NEW TOPIC testTheTopicMachine")
        print("-------------------------------------")
        econf = get_econf(econf, "create_topic.json")
        result = run_module("create_topic.json")

        compare_all(confignames, econf, aconf, result, True)

        print("==> Everything as expected\n")

        #######################################
        #                                     #
        #  modify config                      #
        #                                     #
        #######################################
        print("--------------------------------------------------")
        print("MODIFY TOPIC testTheTopicMachine: ADD TOPICCONFIGS")
        print("--------------------------------------------------")
        econf = get_econf(econf, "modify_topic.json")
        result = run_module("modify_topic.json")

        compare_all(confignames, econf, aconf, result, True)

        print("==> Everything as expected\n")

        #######################################
        #                                     #
        #  modify config                      #
        #                                     #
        #######################################
        print("--------------------------------------------------------")
        print("MODIFY TOPIC testTheTopicMachine: ADD TOPICCONFIGS AGAIN")
        print("--------------------------------------------------------")
        econf = get_econf(econf, "modify_topic.json")
        result = run_module("modify_topic.json")

        compare_all(confignames, econf, aconf, result, False)

        print("==> Everything as expected\n")

        #######################################
        #                                     #
        #  remove some configs                #
        #                                     #
        #######################################
        print("----------------------------------------------------------")
        print("MODIFY TOPIC testTheTopicMachine: REMOVE SOME TOPICCONFIGS")
        print("----------------------------------------------------------")
        econf = get_econf(econf, "remove_configs.json")
        result = run_module("remove_configs.json")

        compare_all(confignames, econf, aconf, result, True)

        print("==> Everything as expected\n")

        #######################################
        #                                     #
        #  modify more configs                #
        #                                     #
        #######################################
        print("-----------------------------------------------------")
        print("MODIFY TOPIC testTheTopicMachine: MODIFY TOPICCONFIGS")
        print("-----------------------------------------------------")
        econf = get_econf(econf, "modify_more_configs.json")
        result = run_module("modify_more_configs.json")

        compare_all(confignames, econf, aconf, result, True)

        print("==> Everything as expected\n")

        #######################################
        #                                     #
        #  modify partitions
        #                                     #
        #######################################
        print("---------------------------------------------------------")
        print("MODIFY TOPIC testTheTopicMachine: MODIFY PARTITION-NUMBER")
        print("---------------------------------------------------------")
        econf = get_econf(econf, "modify_part.json")
        result = run_module("modify_part.json")

        compare_all(confignames, econf, aconf, result, True)

        print("==> Everything as expected\n")

        #######################################
        #                                     #
        #  increase replication-factor
        #                                     #
        #######################################
        print("-------------------------------------------------------------")
        print("MODIFY TOPIC testTheTopicMachine: INCREASE REPLICATION-FACTOR")
        print("-------------------------------------------------------------")
        econf = get_econf(econf, "increase_rep.json")
        result = run_module("increase_rep.json")

        compare_all(confignames, econf, aconf, result, True)

        print("==> Everything as expected\n")

        #######################################
        #                                     #
        #  reduce replication-factor
        #                                     #
        #######################################
        print("-----------------------------------------------------------")
        print("MODIFY TOPIC testTheTopicMachine: REDUCE REPLICATION-FACTOR")
        print("-----------------------------------------------------------")
        econf = get_econf(econf, "reduce_rep.json")
        result = run_module("reduce_rep.json")

        compare_all(confignames, econf, aconf, result, True)

        print("==> Everything as expected\n")

        #######################################
        #                                     #
        #  delete topic
        #                                     #
        #######################################
        print("--------------------------------")
        print("DELETE TOPIC testTheTopicMachine")
        print("--------------------------------")
        econf = get_econf(econf, "delete_topic.json")
        result = run_module("delete_topic.json")

        if result["changed"] != True:
            print("ERROR: EXPECTED RESULT changed = True, but got: ")
            print(result["changed"])


        time.sleep(3)

        if check_topic(topic, a):
            print("ERROR: EXPECTED THAT TOPIC IS ABSENT. BUT TOPIC IS STILL PRESENT.")

        print("==> Everything as expected\n")

        #######################################
        #                                     #
        #  Create Topic again with all the configs
        #                                     #
        #######################################
        print("------------------------------------------------------------------------------")
        print("CREATE TOPIC testTheTopicMachine AGAIN: with ALL the possible (or not) configs")
        print("------------------------------------------------------------------------------")
        econf = get_econf(econf, "create_topic2.json")
        result = run_module("create_topic2.json")

        compare_all(confignames, econf, aconf, result, True)

        print("==> Everything as expected\n")

        #######################################
        #                                     #
        #  delete topic
        #                                     #
        #######################################
        print("--------------------------------")
        print("DELETE TOPIC testTheTopicMachine")
        print("--------------------------------")
        econf = get_econf(econf, "delete_topic.json")
        result = run_module("delete_topic.json")

        if result["changed"] != True:
            print("ERROR: EXPECTED RESULT changed = True, but got: ")
            print(result["changed"])


        time.sleep(3)

        if check_topic(topic, a):
            print("ERROR: EXPECTED THAT TOPIC IS ABSENT. BUT TOPIC IS STILL PRESENT.")

        print("==> Everything as expected\n")

    finally:
        if check_topic(topic, a):
            print("ATTEMPT TO DELETE "+topic+" FOR A GRACEFUL FINISH.")
            b = delete_topic(topic,a)
            if not b:
                print("ERROR: DELETING TOPIC "+topic+" FAILED")
                raise e

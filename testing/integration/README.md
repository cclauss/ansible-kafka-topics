# What
This is for automated integration-testing.  
The script **integrationtest.py** executes the module: kafka_topic with jsonfiles as arguments.  
To check if everything is alright, the script loads the jsonfiles, which where used as arguements and compares the specified state of the topic in this files with the one actually achieved.  
If those states are not equal, then there is a bug in the module and we need to fix it.  

Topic-configurations are only checked if they are on default or not, because parsing the arguments in the json-file back to the actually used values would be too much work. (eg 4d to ms)

# requirements for local testing
This Project of course.
### Kafka-Cluster
You need 3 Kafka-Broker and at least one Zookeeper.  

### Python
You need following for using the kafka_topic-module:
- ansible
- confluent_kafka
- kazoo

Install best with pip in a virtualenv.

### Environment-Vars
For keeping the integration-test flexible, we need to set two environment-variables:
```bash
export ZOOKEEPER="['localhost:2181']"
export KAFKA_BOOTSTRAP="['localhost:9093']"
```
Maybe you need to modify those vars a little, if you mapped other ports and so on.

Unset those vars if you dont need them anymore:
```bash
unset KAFKA_BOOTSTRAP ZOOKEEPER
```



# Execute
```bash
python integrationtest.py
```

# Add new Tests
Copy as a reference the json-file: modify_topic.json.  
Modify the copy like an ansible-playbook.  
Copy following part:

```python
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

```
into integrationtest.py **before**:

```python
    #######################################
    #                                     #
    #  delete topic
    #                                     #
    #######################################
```
Replace Comment and Print-Statement with some text to describe your new test.  
Replace jsonfile-name with the name of your new jsonfile.

# Interpret Output
If it looks like this, everything is alright:
```bash
-----------------------------------------------------------
MODIFY TOPIC testTheTopicMachine: REDUCE REPLICATION-FACTOR
-----------------------------------------------------------
==> Everything as expected
```

If it looks like this, we have a problem:
```bash
---------------------------------------------------------
MODIFY TOPIC testTheTopicMachine: MODIFY PARTITION-NUMBER
---------------------------------------------------------
ERROR: THE EXPECTED NUMBER OF PARTITIONS DOES NOT MATCH THE ACTUAL NUMBER:
EXPECTED:  3
ACTUAL:  2
EXPECTED CONFIG:
{'name': 'testTheTopicMachine', 'state': 'present', 'partitions': 3, 'replication_factor': 2, 'bootstrap_server': ['[::1]:9092', 'localhost:9092', '127.0.0.1:9092'], 'zookeeper': ['localhost:2181'], 'cleanup_policy': 'compact', 'compression_type': 'gzip', 'delete_retention_ms': '567ms', 'file_delete_delay_ms': '8m', 'flush_messages': 25, 'flush_ms': '9h', 'index_interval_bytes': '5MiB', 'max_message_bytes': '15MB', 'message_format_version': '0.11.0-IV2', 'message_timestamp_difference_max_ms': '5d', 'message_timestamp_type': 'LogAppendTime', 'min_cleanable_dirty_ratio': 0.7, 'min_compaction_lag_ms': '3h', 'min_insync_replicas': 2, 'preallocate': 'True', 'retention_bytes': '2GiB', 'retention_ms': '12345ms', 'segment_bytes': '14MB', 'segment_index_bytes': '1GiB', 'segment_jitter_ms': '2ms', 'segment_ms': '4h', 'unclean_leader_election_enable': 'True', 'message_downconversion_enable': 'False'}
ACTUAL CONFIG:
{'partitions': {0: [2, 3], 1: [3, 1]}, 'config': {'compression.type': 'gzip', 'leader.replication.throttled.replicas': '', 'message.downconversion.enable': 'false', 'min.insync.replicas': '2', 'segment.jitter.ms': '2', 'cleanup.policy': 'compact', 'flush.ms': '32400000', 'follower.replication.throttled.replicas': '', 'segment.bytes': '14000000', 'retention.ms': '12345', 'flush.messages': '25', 'message.format.version': '0.11.0-IV2', 'file.delete.delay.ms': '480000', 'max.compaction.lag.ms': '9223372036854775807', 'max.message.bytes': '15000000', 'min.compaction.lag.ms': '10800000', 'message.timestamp.type': 'LogAppendTime', 'preallocate': 'true', 'min.cleanable.dirty.ratio': '0.7', 'index.interval.bytes': '5242880', 'unclean.leader.election.enable': 'true', 'retention.bytes': '2147483648', 'delete.retention.ms': '567', 'segment.ms': '14400000', 'message.timestamp.difference.max.ms': '432000000', 'segment.index.bytes': '1073741824'}}
```

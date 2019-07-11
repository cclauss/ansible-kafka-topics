# Ansible-Module Kafka-Topic
Create, delete and modify kafka-topics with the module **kafka_topic**.

## Get started
Copy the file library/kafka_topic.py in your Ansible-Module-Library. 
This file also contains a detailed documentation of the module.


The file alot_of_options.yml is an example-playbook using the module.
If you got a local Kafka-Cluster, maybe modify the bootstrap-server-param a little and get started with:
```bash
ansible-playbook alot_of_options.yml -vvv
```


## Prerequisites
1. Working Ansible and Python-environment with pip.
2. Install Python-module: confluent-kafka.
```bash
pip install confluent-kafka
```
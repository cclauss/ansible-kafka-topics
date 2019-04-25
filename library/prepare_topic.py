#! /usr/bin/env python

from confluent_kafka.admin import AdminClient, NewTopic
from time import sleep

#copied functions from kafka_topic.py
def check_topic(topic):
    topics = admin.list_topics(timeout=5).topics
    try:
        topics[topic]
    except KeyError:
        return False
    return True

def create_topic(topic, partitions, replication_factor):
    topic = [NewTopic(topic, num_partitions=partitions,  replication_factor=replication_factor)]
    fs = admin.create_topics(topic)
    y = list(fs.values())
    new_topic = y[0].result()


def delete_topic(topic):
    topic = [topic]
    fs = admin.delete_topics(topic)
    y = list(fs.values())
    deleted_topic = y[0].result()


if __name__ == '__main__':
    global admin
    admin = AdminClient({'bootstrap.servers':"localhost:9092"})

    #make sure topic foo is deleted
    foo_exists = check_topic("foo")

    if foo_exists:
        delete_topic("foo")

    #make sure topic foo.two exists with right configuration
    footwo_exists = check_topic("foo.two")

    if footwo_exists:
        delete_topic("foo.two")
        while footwo_exists:
            footwo_exists = check_topic("foo.two")
            #print("foo.two exists: ",footwo_exists)
            sleep(1)                                                        #sadly sleep is needed, because broker are not fast enough
    create_topic(topic = "foo.two", partitions = 2, replication_factor=1)

    #make sure topic bar does not exist
    bar_exists = check_topic("bar")

    if bar_exists:
        delete_topic("bar")

    #make sure topic bar.two exists with right configuration
    bartwo_exists = check_topic("bar.two")

    if bartwo_exists:
        delete_topic("bar.two")
        while bartwo_exists:
            bartwo_exists = check_topic("bar.two")
            #print("bar.two exists: ",bartwo_exists)
            sleep(1)                                                          #sleep is needed because broker are not fast enough
    create_topic(topic = "bar.two", partitions = 2, replication_factor = 1)

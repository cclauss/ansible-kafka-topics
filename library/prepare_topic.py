#! /usr/bin/env python

from confluent_kafka.admin import AdminClient, NewTopic

if __name__ == '__main__':
    admin = AdminClient({'bootstrap.servers':"localhost:9092"})

    topics = admin.list_topics().topics

    #topic foo
    try:
        topics["foo"]
        foo_exists = True
    except KeyError:
        foo_exists = False

    if foo_exists:
        res = admin.delete_topics(["foo"])
        y = list(res.values())
        final = y[0].result()

    #topic foo.two
    try:
        topics["foo.two"]
        footwo_exists = True
    except KeyError:
        footwo_exists = False

    if footwo_exists:
        res = admin.delete_topics(["foo.two"])
        y = list(res.values())
        final = y[0].result()

    topic = [NewTopic("foo.two", num_partitions=3, replication_factor=2)]
    res = admin.create_topics(topic)
    y = list(res.values())
    final = y[0].result()

    #topic bar
    try:
        topics["bar"]
        bar_exists = True
    except KeyError:
        bar_exists = False

    if bar_exists:
        res = admin.delete_topics(["bar"])
        y = list(res.values())
        final = y[0].result()

    #topic foo.two
    try:
        topics["bar.two"]
        bartwo_exists = True
    except KeyError:
        bartwo_exists = False

    if bartwo_exists:
        res = admin.delete_topics(["bar.two"])
        y = list(res.values())
        final = y[0].result()

    topic = [NewTopic("bar.two", num_partitions=2, replication_factor=1)]
    res = admin.create_topics(topic)
    y = list(res.values())
    final = y[0].result()

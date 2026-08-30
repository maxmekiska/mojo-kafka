"""Integration tests against a real Kafka broker.

    pixi run broker-up
    pixi run test-broker
    pixi run broker-down

Set MOJO_KAFKA_BOOTSTRAP to point somewhere else.

Most integration coverage lives in `test_mock.mojo`, which needs no broker.
This suite exists for what the mock cannot do:

- **The Topic Admin API.** librdkafka's mock broker does not implement
  CreateTopics, so `AdminClient.create_topic()` is only reachable here --
  and it was a segfault as recently as v0.1.0.
- **Real wire and real timing.** Topic creation is acked before metadata has
  propagated; the mock resolves that instantly and a real cluster does not.
"""

from std.os import getenv
from std.testing import TestSuite, assert_equal, assert_true
from std.time import perf_counter_ns, sleep

from kafka import (
    AdminClient,
    Consumer,
    ConsumerConfig,
    Producer,
    ProducerConfig,
)


def bootstrap() -> String:
    var override = getenv("MOJO_KAFKA_BOOTSTRAP")
    return override if override != "" else String("localhost:9092")


def unique_topic() -> String:
    return "mojo-kafka-it-" + String(perf_counter_ns())


def wait_for_topics(admin: AdminClient, want: List[String]) raises -> Int:
    """Poll metadata until every topic in `want` is visible.

    Topic creation is acked before the metadata has propagated, so tests
    that create then immediately list are racing the broker.
    """
    var seen_count = 0
    for _attempt in range(40):
        var names = admin.list_topics()
        seen_count = len(names)
        var missing = 0
        for wanted in want:
            var found = False
            for got in names:
                if got == wanted:
                    found = True
            if not found:
                missing += 1
        if missing == 0:
            return seen_count
        sleep(0.25)

    # One last pass so the failure message names the topic that is missing.
    var names = admin.list_topics()
    for wanted in want:
        var found = False
        for got in names:
            if got == wanted:
                found = True
        assert_true(found, "topic " + wanted + " never became visible")
    return seen_count


def test_admin_create_and_list() raises:
    """Create then list, the happy path."""
    var admin = AdminClient(bootstrap_servers=bootstrap())
    var topic = unique_topic()
    admin.create_topic(topic, num_partitions=1, replication_factor=1)
    var total = wait_for_topics(admin, [topic])
    print("    listed", total, "topic(s), including", topic)


def test_list_topics_walks_every_entry() raises:
    """Guards the metadata stride.

    A 24-byte stride returns garbage from the second topic on and faults
    around the fourth, so this only passes with the correct 32-byte one.
    """
    var admin = AdminClient(bootstrap_servers=bootstrap())
    var made = List[String]()
    var base = unique_topic()
    for i in range(6):
        var t = base + "-p" + String(i)
        admin.create_topic(t, num_partitions=1, replication_factor=1)
        made.append(t)

    var total = wait_for_topics(admin, made)
    print("    walked", total, "topics without drift")


def test_round_trip_preserves_key_and_value() raises:
    """The regression test for transposed key/value.

    Asserting only on the payload passes even when the vtype constants
    are swapped, so this checks both halves of every message.
    """
    var topic = unique_topic() + "-rt"
    var admin = AdminClient(bootstrap_servers=bootstrap())
    admin.create_topic(topic, num_partitions=1, replication_factor=1)
    _ = wait_for_topics(admin, [topic])

    var producer = Producer(
        ProducerConfig(bootstrap_servers=bootstrap(), linger_ms=5)
    )
    for i in range(5):
        _ = producer.produce(
            topic=topic,
            key="key-" + String(i),
            value="value-" + String(i),
        )
    producer.flush(10000)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap(),
            group_id=topic + "-group",
            auto_offset_reset="earliest",
        )
    )
    consumer.subscribe([topic])

    var seen = 0
    var attempts = 0
    while seen < 5 and attempts < 60:
        attempts += 1
        var maybe = consumer.poll(timeout_ms=1000)
        if not maybe:
            continue
        ref m = maybe.value()
        assert_equal(m.topic, topic)
        assert_equal(m.key_text(), "key-" + String(seen))
        assert_equal(m.value_text(), "value-" + String(seen))
        assert_equal(m.offset, Int64(seen))
        seen += 1

    assert_equal(seen, 5)
    consumer.close()
    print("    round-tripped 5 messages, key and value intact")


def test_create_topic_reports_rejection() raises:
    """The broker's per-topic verdict must not be swallowed.

    Both cases below come back with `RD_KAFKA_RESP_ERR_NO_ERROR` at the
    *request* level, with the real error attached to the topic inside the
    result. Reading only `rd_kafka_event_error()` therefore reported success
    for a topic that was never created.
    """
    var admin = AdminClient(bootstrap_servers=bootstrap())

    # More replicas than there are brokers.
    var bad = unique_topic() + "-rf"
    var raised = False
    try:
        admin.create_topic(bad, num_partitions=1, replication_factor=3)
    except e:
        raised = True
        print("    rejected as expected:", e)
    assert_true(
        raised, "create_topic accepted an impossible replication factor"
    )

    for got in admin.list_topics():
        assert_true(got != bad, "rejected topic " + bad + " exists after all")

    # And the same topic twice.
    var dup = unique_topic() + "-dup"
    admin.create_topic(dup, num_partitions=1, replication_factor=1)
    _ = wait_for_topics(admin, [dup])
    var dup_raised = False
    try:
        admin.create_topic(dup, num_partitions=1, replication_factor=1)
    except e:
        dup_raised = True
        print("    duplicate rejected as expected:", e)
    assert_true(dup_raised, "duplicate create_topic reported success")


def main() raises:
    print("broker:", bootstrap())
    TestSuite.discover_tests[__functions_in_module()]().run()

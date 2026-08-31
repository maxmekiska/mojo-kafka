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
- **ListOffsets by timestamp.** The mock answers every timestamp with
  `OFFSET_END`, without reporting an error, so `Consumer.offsets_for_times`
  has no meaningful assertion available there.
- **Real wire and real timing.** Topic creation is acked before metadata has
  propagated; the mock resolves that instantly and a real cluster does not.

The rest of the consumer control plane -- `assign`, `seek`, `position`,
`committed`, `pause`/`resume`, watermarks, end-of-partition and record
timestamps -- the mock does cover, so those live in `test_mock.mojo` where
CI runs them.
"""

from std.os import getenv
from std.testing import TestSuite, assert_equal, assert_true
from std.time import perf_counter_ns, sleep

from kafka import (
    OFFSET_BEGINNING,
    OFFSET_END,
    OFFSET_INVALID,
    TIMESTAMP_CREATE_TIME,
    Rebalance,
    AdminClient,
    Consumer,
    ConsumerConfig,
    Producer,
    ProducerConfig,
    TopicPartition,
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


def test_offsets_for_times_maps_a_clock_onto_offsets() raises:
    """The offset field carries a timestamp in and an offset out.

    That double duty is the whole shape of `offsets_for_times`, and getting
    it wrong -- reading the answer from the return value, or from the wrong
    field -- is the failure this pins.

    The records are written in **two batches with a real gap between them**,
    because a loop of produces lands them all in the same millisecond: with
    six identical timestamps, "the first offset at or after `stamps[3]`" is
    0, and the test would pass without the broker resolving anything. The
    gap is what makes offset 3 the only correct answer.

    Not in `test_mock.mojo` because the mock answers every timestamp with
    `OFFSET_END` and reports no error doing so.
    """
    var topic = unique_topic() + "-oft"
    var admin = AdminClient(bootstrap_servers=bootstrap())
    admin.create_topic(topic, num_partitions=1, replication_factor=1)
    _ = wait_for_topics(admin, [topic])

    var producer = Producer(
        ProducerConfig(bootstrap_servers=bootstrap(), linger_ms=5)
    )
    for i in range(3):
        _ = producer.produce(topic=topic, value="early-" + String(i))
    producer.flush(10000)
    sleep(1.5)
    for i in range(3):
        _ = producer.produce(topic=topic, value="late-" + String(i))
    producer.flush(10000)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap(),
            group_id=topic + "-group",
            auto_offset_reset="earliest",
        )
    )
    var only: List[TopicPartition] = [
        TopicPartition(topic, 0, OFFSET_BEGINNING)
    ]
    consumer.assign(only)

    var stamps = List[Int64]()
    var attempts = 0
    while len(stamps) < 6 and attempts < 60:
        attempts += 1
        var maybe = consumer.poll(timeout_ms=1000)
        if maybe:
            ref m = maybe.value()
            assert_true(m.has_timestamp(), "record carried no timestamp")
            stamps.append(m.timestamp)

    assert_equal(len(stamps), 6)
    assert_true(
        stamps[3] > stamps[2], "the two batches landed in the same instant"
    )

    # The first record at or after the second batch's timestamp is offset 3.
    var at_late = consumer.offsets_for_times(
        [TopicPartition(topic, 0, stamps[3])]
    )
    assert_equal(len(at_late), 1)
    assert_equal(at_late[0].topic, topic)
    assert_equal(at_late[0].offset, Int64(3))

    # And at or after the first batch's, offset 0.
    var at_early = consumer.offsets_for_times(
        [TopicPartition(topic, 0, stamps[0])]
    )
    assert_equal(at_early[0].offset, Int64(0))

    # Nothing is newer than a minute past the last record, so there is
    # nothing to replay: OFFSET_END, which is not an error.
    var beyond = consumer.offsets_for_times(
        [TopicPartition(topic, 0, stamps[5] + 60000)]
    )
    assert_equal(beyond[0].offset, OFFSET_END)
    assert_true(not beyond[0].has_error(), "a future timestamp errored")

    consumer.close()
    print("    a timestamp between two batches resolved to offset 3")


def test_produced_timestamp_survives_a_real_broker() raises:
    """An explicit CreateTime, through a real broker rather than the mock.

    The mock stores and returns whatever it is handed. A real broker applies
    its own topic policy -- the default is `CreateTime`, which preserves the
    producer's value, and `LogAppendTime` would overwrite it. This pins that
    the default path really does keep the producer's number.
    """
    var topic = unique_topic() + "-ts"
    var admin = AdminClient(bootstrap_servers=bootstrap())
    admin.create_topic(topic, num_partitions=1, replication_factor=1)
    _ = wait_for_topics(admin, [topic])

    var chosen = Int64(1609459200000)  # 2021-01-01
    var producer = Producer(
        ProducerConfig(bootstrap_servers=bootstrap(), linger_ms=5)
    )
    _ = producer.produce(topic=topic, value="stamped", timestamp=chosen)
    _ = producer.produce(topic=topic, value="now")
    producer.flush(10000)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap(),
            group_id=topic + "-group",
            auto_offset_reset="earliest",
        )
    )
    consumer.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])

    var got = List[Int64]()
    var types = List[Int32]()
    for _ in range(60):
        var maybe = consumer.poll(timeout_ms=1000)
        if maybe:
            ref m = maybe.value()
            assert_true(m.has_timestamp(), "record carried no timestamp")
            got.append(m.timestamp)
            types.append(m.timestamp_type)
        if len(got) == 2:
            break

    assert_equal(len(got), 2)
    assert_equal(types[0], TIMESTAMP_CREATE_TIME)
    assert_equal(got[0], chosen)
    assert_true(got[1] > chosen, "the default timestamp was not now")

    consumer.close()
    print("    broker preserved the producer's CreateTime")


def _broker_noop(event: Rebalance) raises:
    """An `on_assign` that does nothing -- the default must still apply."""
    pass


def _broker_commit(event: Rebalance) raises:
    """An `on_revoke` that commits before the partitions move."""
    event.commit()


def test_rebalance_handlers_run_against_a_real_group() raises:
    """The handlers, driven by a real group coordinator.

    The mock covers the same three behaviours, but it resolves the group
    protocol instantly and locally. Here a real coordinator runs the
    JoinGroup / SyncGroup exchange, so this is what proves the callback is
    wired into the real thing and not just into the mock's shortcut.
    """
    var topic = unique_topic() + "-rb"
    var admin = AdminClient(bootstrap_servers=bootstrap())
    admin.create_topic(topic, num_partitions=1, replication_factor=1)
    _ = wait_for_topics(admin, [topic])

    var producer = Producer(
        ProducerConfig(bootstrap_servers=bootstrap(), linger_ms=5)
    )
    for i in range(6):
        _ = producer.produce(topic=topic, value="g-" + String(i))
    producer.flush(10000)

    # A handler that only looks must still leave the consumer assigned.
    var looker = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap(),
            group_id=topic + "-noop",
            auto_offset_reset="earliest",
        )
    )
    looker.subscribe([topic], on_assign=_broker_noop)
    var seen = 0
    for _ in range(60):
        if looker.poll(timeout_ms=1000):
            seen += 1
        if seen == 6:
            break
    assert_equal(seen, 6)
    looker.close()

    # And an `on_revoke` that commits: auto-commit is off, so a committed
    # offset can only have come from the handler.
    var committer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap(),
            group_id=topic + "-commit",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
    )
    committer.subscribe([topic], on_revoke=_broker_commit)
    var read = 0
    for _ in range(60):
        if committer.poll(timeout_ms=1000):
            read += 1
        if read == 3:
            break
    assert_equal(read, 3)
    committer.close()

    var checker = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap(),
            group_id=topic + "-commit",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
    )
    var stored = checker.committed([TopicPartition(topic, 0)])
    assert_equal(stored[0].offset, Int64(3))
    checker.close()
    print("    real coordinator: default applied, on_revoke committed 3")


def test_a_joining_member_triggers_a_live_revoke() raises:
    """A second member joining forces the first to give partitions back.

    This is the case the mock cannot really produce: a rebalance mid-session,
    driven by group membership changing rather than by a consumer closing.
    The first member's `on_revoke` has to run *during* that, which is the
    whole reason the handler is worth having -- it is the last moment its
    offsets are still its own to commit.
    """
    var topic = unique_topic() + "-join"
    var admin = AdminClient(bootstrap_servers=bootstrap())
    admin.create_topic(topic, num_partitions=2, replication_factor=1)
    _ = wait_for_topics(admin, [topic])

    var producer = Producer(
        ProducerConfig(bootstrap_servers=bootstrap(), linger_ms=5)
    )
    for p in range(2):
        for i in range(2):
            _ = producer.produce(
                topic=topic, value="j-" + String(i), partition=Int32(p)
            )
    producer.flush(10000)

    var group = topic + "-group"
    var first = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap(),
            group_id=group,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
    )
    first.subscribe([topic], on_revoke=_broker_commit)

    # Sole member: it gets both partitions and every record.
    var read = 0
    for _ in range(60):
        if first.poll(timeout_ms=1000):
            read += 1
        if read == 4:
            break
    assert_equal(read, 4)
    assert_equal(
        first.committed([TopicPartition(topic, 0)])[0].offset,
        OFFSET_INVALID,
    )

    # A second member joins. The coordinator rebalances, which revokes the
    # first member's partitions -- and its handler commits on the way out.
    var second = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap(),
            group_id=group,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
    )
    second.subscribe([topic])

    var committed = False
    for _ in range(80):
        _ = first.poll(timeout_ms=500)
        _ = second.poll(timeout_ms=500)
        var stored = first.committed([TopicPartition(topic, 0)])
        if stored[0].offset != OFFSET_INVALID:
            committed = True
            break

    assert_true(committed, "on_revoke never ran when a second member joined")
    first.close()
    second.close()
    print("    a joining member drove a live revoke, and it committed")


def main() raises:
    print("broker:", bootstrap())
    TestSuite.discover_tests[__functions_in_module()]().run()

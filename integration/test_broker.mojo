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
    KIND_AUTHORIZATION,
    OFFSET_BEGINNING,
    OFFSET_END,
    OFFSET_INVALID,
    TIMESTAMP_CREATE_TIME,
    Rebalance,
    AdminClient,
    Consumer,
    ConsumerConfig,
    Message,
    Producer,
    ProducerConfig,
    TopicPartition,
)


def bootstrap() -> String:
    var override = getenv("MOJO_KAFKA_BOOTSTRAP")
    return override if override != "" else String("localhost:9092")


def sasl_bootstrap() -> String:
    """The SASL_PLAINTEXT listener compose opens on 9093."""
    var override = getenv("MOJO_KAFKA_SASL_BOOTSTRAP")
    return override if override != "" else String("localhost:9093")


def sasl_keys(password: String) -> List[Tuple[String, String]]:
    """The four keys a SASL/PLAIN client needs, all through `set()`.

    Not a test case. The mechanism and user match the listener in
    `docker-compose.yml`; the password is the caller's so a wrong one can
    be tried.
    """
    return [
        ("security.protocol", String("SASL_PLAINTEXT")),
        ("sasl.mechanism", String("PLAIN")),
        ("sasl.username", String("mojo")),
        ("sasl.password", password),
    ]


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


def test_transactional_offsets_are_visible_to_the_group() raises:
    """Exactly-once, and the half the mock cannot witness.

    `send_offsets_to_transaction` commits the consumer's offsets *inside* the
    producer's transaction. Whether that actually happened is only readable
    through `committed()`, and the mock answers `OFFSET_INVALID` there
    regardless -- it accepts `TxnOffsetCommit` and never serves it back
    through `OffsetFetch` (confirmed in plain C). So an assertion about the
    committed offset passes unconditionally on the mock and belongs here.

    Both directions, because one alone proves little:

    - a **committed** transaction leaves the group at last-processed + 1, so
      a restart resumes past the input; and
    - an **aborted** one leaves it untouched, so a restart replays the input
      rather than skipping records that were never written.

    The second is the real exactly-once claim. An implementation that
    committed offsets outside the transaction passes every other test in
    every suite and fails only this one.
    """
    var topic_in = unique_topic() + "-in"
    var topic_out = unique_topic() + "-out"
    var admin = AdminClient(bootstrap())
    admin.create_topic(topic_in, num_partitions=1, replication_factor=1)
    admin.create_topic(topic_out, num_partitions=1, replication_factor=1)
    _ = wait_for_topics(admin, [topic_in, topic_out])

    var upstream = Producer(ProducerConfig(bootstrap_servers=bootstrap()))
    for i in range(3):
        _ = upstream.produce(topic=topic_in, value=String(i))
    upstream.flush(15000)

    var group = "mojo-kafka-eos-" + String(perf_counter_ns())
    var cfg = ConsumerConfig(
        bootstrap_servers=bootstrap(),
        group_id=group,
        auto_offset_reset="earliest",
    )
    cfg.set("enable.auto.commit", "false")
    cfg.set("isolation.level", "read_committed")
    var consumer = Consumer(cfg)
    consumer.subscribe([topic_in])

    var seen = 0
    var attempts = 0
    while seen < 3 and attempts < 60:
        attempts += 1
        var maybe = consumer.poll(timeout_ms=1000)
        if maybe:
            seen += 1
    assert_equal(seen, 3, "the input was not readable")

    var assignment: List[TopicPartition] = [TopicPartition(topic_in, 0)]
    assert_equal(
        consumer.position(assignment)[0].offset,
        3,
        "position is not last-processed + 1",
    )

    var producer = Producer(_txn_config("mojo-kafka-eos-txn-" + group))
    assert_true(not producer.init_transactions(30000), "init failed")

    # --- the aborted transaction first: the group must be untouched --------
    assert_true(not producer.begin_transaction(), "begin failed")
    _ = producer.produce(topic=topic_out, value="discarded")
    assert_true(
        not producer.send_offsets_to_transaction(
            consumer.position(assignment),
            consumer.consumer_group_metadata(),
            30000,
        ),
        "send_offsets failed on the transaction that gets aborted",
    )
    assert_true(not producer.abort_transaction(), "abort failed")
    _ = producer.take_failures()

    assert_equal(
        consumer.committed(assignment)[0].offset,
        OFFSET_INVALID,
        "an aborted transaction committed the consumer's offsets anyway",
    )
    print("    aborted: group still uncommitted, so the input replays")

    # --- then the committed one: the group must advance --------------------
    assert_true(not producer.begin_transaction(), "second begin failed")
    _ = producer.produce(topic=topic_out, value="kept")
    assert_true(
        not producer.send_offsets_to_transaction(
            consumer.position(assignment),
            consumer.consumer_group_metadata(),
            30000,
        ),
        "send_offsets failed on the transaction that gets committed",
    )
    assert_true(not producer.commit_transaction(), "commit failed")

    assert_equal(
        consumer.committed(assignment)[0].offset,
        3,
        "a committed transaction did not commit the consumer's offsets",
    )
    consumer.close()
    print("    committed: group advanced to 3, so the input is not replayed")


def test_sasl_plain_round_trip() raises:
    """One record produced and consumed through the SASL/PLAIN listener.

    librdkafka does all of SASL; what this proves is that the conda build
    has it compiled in and that the four keys reach it through `set()`
    unchanged. The topic is created over the plaintext listener because
    the admin client is not what is under test. PLAIN is enough to prove
    the plumbing; SCRAM would need credentials created on the broker with
    `kafka-configs.sh` and buys nothing this does not.
    """
    var topic = unique_topic()
    var admin = AdminClient(bootstrap_servers=bootstrap())
    admin.create_topic(topic, num_partitions=1, replication_factor=1)
    _ = wait_for_topics(admin, [topic])

    var pcfg = ProducerConfig(bootstrap_servers=sasl_bootstrap())
    for pair in sasl_keys("mojo-secret"):
        pcfg.set(pair[0], pair[1])
    var producer = Producer(pcfg)
    _ = producer.produce(topic=topic, key="sasl", value="through 9093")
    producer.flush(15000)
    assert_equal(len(producer.failures()), 0, "SASL produce was rejected")
    assert_equal(len(producer.errors()), 0, "SASL produce reported an error")

    var ccfg = ConsumerConfig(
        bootstrap_servers=sasl_bootstrap(),
        group_id=topic + "-sasl",
        auto_offset_reset="earliest",
    )
    for pair in sasl_keys("mojo-secret"):
        ccfg.set(pair[0], pair[1])
    var consumer = Consumer(ccfg)
    consumer.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])
    var got = Optional[Message](None)
    for _attempt in range(30):
        got = consumer.poll(1000)
        if got:
            break
    assert_true(Bool(got), "no record came back through SASL/PLAIN")
    assert_equal(got.value().key_text(), "sasl")
    assert_equal(got.value().value_text(), "through 9093")
    assert_equal(len(consumer.errors()), 0, "SASL consume reported an error")
    consumer.close()
    print("    SASL/PLAIN: one record round-tripped through 9093")


def test_sasl_wrong_password_is_reported_as_authorization() raises:
    """A rejected credential lands in `errors()` as `KIND_AUTHORIZATION`.

    The half an operator actually meets. The broker refuses the handshake,
    librdkafka retries in the background, and nothing raises -- a produce
    merely times out later. `errors()` from item 1 of the v1.0 plan is
    where the reason surfaces, and it has to be branchable, which is what
    the kind is for.
    """
    var pcfg = ProducerConfig(bootstrap_servers=sasl_bootstrap())
    for pair in sasl_keys("not-the-secret"):
        pcfg.set(pair[0], pair[1])
    pcfg.set("message.timeout.ms", "3000")
    pcfg.set("log_level", "0")
    var producer = Producer(pcfg)
    _ = producer.produce(topic="mojo-kafka-it-sasl-refused", value="v")

    var deadline = perf_counter_ns() + 15_000_000_000
    var found = False
    var seen = List[String]()
    while perf_counter_ns() < deadline and not found:
        _ = producer.poll(200)
        for err in producer.errors():
            if err.kind() == KIND_AUTHORIZATION:
                found = True
                seen.append(String(err))
    if not found:
        for err in producer.errors():
            seen.append(String(err))
    var summary = String("")
    for line in seen:
        summary += line + "; "
    assert_true(found, "no KIND_AUTHORIZATION error was reported: " + summary)
    try:
        producer.flush(3000)
    except:
        pass
    _ = producer.take_failures()
    print("    wrong password:", seen[0])


def _txn_config(txn_id: String) raises -> ProducerConfig:
    """A transactional producer config. Not a test case -- see the module
    docstring on `TestSuite.discover_tests`."""
    var cfg = ProducerConfig(bootstrap_servers=bootstrap())
    cfg.set("transactional.id", txn_id)
    return cfg^


def main() raises:
    print("broker:", bootstrap())
    TestSuite.discover_tests[__functions_in_module()]().run()

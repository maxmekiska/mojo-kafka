"""Integration tests against librdkafka's in-process mock broker.

No broker process and no Docker: the mock speaks the real wire protocol
over a real socket, so the produce and consume paths under test are the
same ones used against a real cluster. That makes these runnable
everywhere, including macOS CI runners where Docker is not available.

    pixi run test-mock

Two things the mock does not implement, both covered in `test_broker.mojo`
instead:

- the Topic Admin API, so `AdminClient`'s topic *creation* is not reachable
  here;
- **ListOffsets by timestamp**, which it answers with `OFFSET_END` for every
  timestamp -- including ones that plainly precede every record on the
  partition. It reports no error doing so, so a `offsets_for_times` test
  written here would pass against an implementation that always returned
  `OFFSET_END` and against one that worked.
"""

from std.os import getenv, setenv
from std.testing import TestSuite, assert_equal, assert_true
from std.time import sleep

from kafka import (
    API_KEY_ADD_OFFSETS_TO_TXN,
    API_KEY_ADD_PARTITIONS_TO_TXN,
    API_KEY_INIT_PRODUCER_ID,
    OFFSET_BEGINNING,
    RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED,
    RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED,
    RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED,
    Rebalance,
    OFFSET_END,
    OFFSET_INVALID,
    TIMESTAMP_CREATE_TIME,
    TXN_ABORT,
    TXN_FATAL,
    AdminClient,
    Consumer,
    ConsumerConfig,
    Header,
    Message,
    Producer,
    ProducerConfig,
    TopicPartition,
)
from kafka.testing import MockCluster

# Every test ends with `_ = cluster^`. Mojo destroys values after their last
# use, so without it the mock broker is torn down mid-test -- see MockCluster.


def test_round_trip_preserves_key_and_value() raises:
    """Regression guard for transposed key/value.

    Asserting only on the payload passes even when the produce path swaps
    the two, so this checks both halves of every message.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("round-trip", partition_count=1)

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    for i in range(5):
        _ = producer.produce(
            topic="round-trip",
            key="key-" + String(i),
            value="value-" + String(i),
        )
    producer.flush(10000)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="round-trip-group",
            auto_offset_reset="earliest",
        )
    )
    consumer.subscribe(["round-trip"])

    var seen = 0
    var attempts = 0
    while seen < 5 and attempts < 60:
        attempts += 1
        var maybe = consumer.poll(timeout_ms=1000)
        if not maybe:
            continue
        ref m = maybe.value()
        assert_equal(m.topic, "round-trip")
        assert_equal(m.key_text(), "key-" + String(seen))
        assert_equal(m.value_text(), "value-" + String(seen))
        assert_equal(m.offset, Int64(seen))
        seen += 1

    assert_equal(seen, 5)
    consumer.close()
    print("    round-tripped 5 messages, key and value intact")
    _ = cluster^


def test_list_topics_walks_every_entry() raises:
    """Regression guard for the metadata stride.

    `sizeof(rd_kafka_metadata_topic_t)` is 32 bytes. A 24-byte stride
    returns a plausible name for the first topic, garbage for the next
    couple, and then faults -- so this needs enough topics to get there.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()

    var made = List[String]()
    for i in range(8):
        var name = "stride-" + String(i)
        cluster.create_topic(name, partition_count=1)
        made.append(name)

    var admin = AdminClient(bootstrap_servers=bootstrap)
    var names = admin.list_topics()
    for wanted in made:
        var found = False
        for got in names:
            if got == wanted:
                found = True
        assert_true(found, "topic " + wanted + " missing from metadata")

    print("    walked", len(names), "topics without drift")
    _ = cluster^


def test_message_topic_is_populated() raises:
    """`Message.topic` was always empty before v0.2.

    With a multi-topic subscription an empty topic makes the messages
    ambiguous, so this subscribes to two and checks both are attributed.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("alpha", partition_count=1)
    cluster.create_topic("beta", partition_count=1)

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    _ = producer.produce(topic="alpha", key="a", value="from-alpha")
    _ = producer.produce(topic="beta", key="b", value="from-beta")
    producer.flush(10000)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="multi-topic-group",
            auto_offset_reset="earliest",
        )
    )
    consumer.subscribe(["alpha", "beta"])

    var saw_alpha = False
    var saw_beta = False
    var attempts = 0
    while (not saw_alpha or not saw_beta) and attempts < 60:
        attempts += 1
        var maybe = consumer.poll(timeout_ms=1000)
        if not maybe:
            continue
        ref m = maybe.value()
        if m.topic == "alpha":
            assert_equal(m.value_text(), "from-alpha")
            saw_alpha = True
        elif m.topic == "beta":
            assert_equal(m.value_text(), "from-beta")
            saw_beta = True
        else:
            raise Error("unexpected topic: [" + m.topic + "]")

    assert_true(saw_alpha, "no message attributed to 'alpha'")
    assert_true(saw_beta, "no message attributed to 'beta'")
    consumer.close()
    print("    both topics correctly attributed")
    _ = cluster^


def test_poll_returns_none_when_idle() raises:
    """Timeout and end-of-partition are not messages."""
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("empty", partition_count=1)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="empty-group",
            auto_offset_reset="earliest",
        )
    )
    consumer.subscribe(["empty"])

    for _ in range(5):
        var maybe = consumer.poll(timeout_ms=500)
        assert_true(not maybe, "empty topic yielded a message")

    consumer.close()
    print("    idle polls returned None")
    _ = cluster^


def test_binary_payload_round_trips() raises:
    """`produce_bytes` carries bytes a `String` cannot hold.

    A NUL, a bare continuation byte and 0xFF are all illegal UTF-8, which is
    what an Avro or Protobuf frame looks like. They must come back byte for
    byte.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("binary", partition_count=1)

    var payload: List[UInt8] = [0x00, 0x80, 0xFF, 0x41, 0x00, 0xC3]
    var keyb: List[UInt8] = [0xFE, 0x01]

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    _ = producer.produce_bytes(
        topic="binary", value=payload.copy(), key=keyb.copy()
    )
    producer.flush(10000)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="binary-group",
            auto_offset_reset="earliest",
        )
    )
    consumer.subscribe(["binary"])

    var got = False
    for _ in range(60):
        var maybe = consumer.poll(timeout_ms=1000)
        if not maybe:
            continue
        ref m = maybe.value()
        assert_true(Bool(m.value), "binary value came back null")
        assert_true(Bool(m.key), "binary key came back null")
        ref value_bytes = m.value.value()
        ref key_bytes = m.key.value()
        assert_equal(len(value_bytes), len(payload))
        for i in range(len(payload)):
            assert_equal(Int(value_bytes[i]), Int(payload[i]))
        assert_equal(len(key_bytes), len(keyb))
        for i in range(len(keyb)):
            assert_equal(Int(key_bytes[i]), Int(keyb[i]))
        got = True
        break

    assert_true(got, "binary message never arrived")
    consumer.close()
    print("    binary payload survived byte for byte")
    _ = cluster^


def test_null_and_empty_fields_are_distinct() raises:
    """Null and empty are different messages, in both halves.

    Kafka carries presence separately from length, and librdkafka signals it
    with the pointer rather than the byte count. Before `Message` moved to
    optional bytes this was unrepresentable in either direction: the produce
    side turned an empty key into a null one, and the consume side reported
    a null field as `""`. A tombstone -- non-null key, null value -- could
    not be written at all.

    The four rows below are the whole truth table, and they are asserted on
    `key` / `value` rather than the `*_text()` helpers: the helpers collapse
    null onto their default, which is exactly the conflation under test.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("nullable", partition_count=1)

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    _ = producer.produce(topic="nullable", value=None, key="k-tomb")
    _ = producer.produce(topic="nullable", value="v-empty-key", key="")
    _ = producer.produce(topic="nullable", value="v-null-key", key=None)
    _ = producer.produce(topic="nullable", value="", key="k-empty-value")
    producer.flush(10000)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="nullable-group",
            auto_offset_reset="earliest",
        )
    )
    consumer.subscribe(["nullable"])

    var got = List[Message]()
    var attempts = 0
    while len(got) < 4 and attempts < 60:
        attempts += 1
        var maybe = consumer.poll(timeout_ms=1000)
        if not maybe:
            continue
        got.append(maybe.value().copy())

    assert_equal(len(got), 4)

    # A tombstone: the key survives, the value is absent -- not empty.
    assert_true(Bool(got[0].key), "tombstone lost its key")
    assert_equal(_text_of(got[0].key), "k-tomb")
    assert_true(not got[0].value, "tombstone value came back present")
    assert_true(got[0].is_tombstone(), "tombstone not reported as one")

    # An empty-but-present key: present, zero bytes, not null.
    assert_true(Bool(got[1].key), "empty key came back null")
    assert_equal(len(got[1].key.value()), 0)
    assert_equal(_text_of(got[1].value), "v-empty-key")

    # A genuinely null key stays null.
    assert_true(not got[2].key, "null key came back present")
    assert_equal(_text_of(got[2].value), "v-null-key")

    # And an empty-but-present value is not a tombstone.
    assert_true(Bool(got[3].value), "empty value came back null")
    assert_equal(len(got[3].value.value()), 0)
    assert_true(not got[3].is_tombstone(), "empty value read as a tombstone")

    consumer.close()
    print("    null and empty stayed distinct in all four rows")
    _ = cluster^


def test_headers_round_trip_in_order_with_duplicates() raises:
    """Headers are a list of pairs, and both halves of that matter.

    A `Dict` would satisfy a test that only checked one header of each name.
    This writes the same name twice with different values, so a map-backed
    implementation loses one of them, and it asserts on position, so one that
    keeps both but reorders them fails too.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("headers", partition_count=1)

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    _ = producer.produce(
        topic="headers",
        value="with-headers",
        key="k",
        headers=[
            Header("content-type", "application/json"),
            Header("trace", "first"),
            Header("trace", "second"),
        ],
    )
    # A record written with no headers must read back as an empty list, not
    # as a failure -- librdkafka reports that case as __NOENT.
    _ = producer.produce(topic="headers", value="no-headers", key="k2")
    producer.flush(10000)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="headers-group",
            auto_offset_reset="earliest",
        )
    )
    consumer.subscribe(["headers"])

    var got = List[Message]()
    var attempts = 0
    while len(got) < 2 and attempts < 60:
        attempts += 1
        var maybe = consumer.poll(timeout_ms=1000)
        if not maybe:
            continue
        got.append(maybe.value().copy())

    assert_equal(len(got), 2)

    ref carried = got[0].headers
    assert_equal(len(carried), 3)
    assert_equal(carried[0].name, "content-type")
    assert_equal(carried[0].value_text(), "application/json")
    # Both `trace` headers survive, in the order they were written.
    assert_equal(carried[1].name, "trace")
    assert_equal(carried[1].value_text(), "first")
    assert_equal(carried[2].name, "trace")
    assert_equal(carried[2].value_text(), "second")

    # The lookup helper takes the first of a repeated name.
    assert_equal(got[0].header_text("trace"), "first")
    assert_equal(got[0].header_text("absent", "fallback"), "fallback")

    assert_equal(len(got[1].headers), 0)
    assert_equal(got[1].value_text(), "no-headers")

    consumer.close()
    print("    3 headers round-tripped in order, duplicates intact")
    _ = cluster^


def test_header_values_keep_null_and_empty_apart() raises:
    """The null/empty rule again, one level down.

    A header value is carried by pointer exactly like a key or a payload, so
    a null value and a present-but-empty one are different headers. This is
    asserted on `value` rather than `value_text()` for the same reason the
    message-level test is: the text helper collapses null onto its default,
    which is the conflation under test.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("header-nulls", partition_count=1)

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    _ = producer.produce(
        topic="header-nulls",
        value="v",
        key="k",
        headers=[
            Header("null-value", None),
            Header("empty-value", ""),
            Header("has-value", "x"),
        ],
    )
    producer.flush(10000)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="header-nulls-group",
            auto_offset_reset="earliest",
        )
    )
    consumer.subscribe(["header-nulls"])

    var found = False
    for _ in range(60):
        var maybe = consumer.poll(timeout_ms=1000)
        if not maybe:
            continue
        ref carried = maybe.value().headers
        assert_equal(len(carried), 3)

        assert_equal(carried[0].name, "null-value")
        assert_true(not carried[0].value, "null header value came back present")

        assert_equal(carried[1].name, "empty-value")
        assert_true(Bool(carried[1].value), "empty header value came back null")
        assert_equal(len(carried[1].value.value()), 0)

        assert_equal(carried[2].name, "has-value")
        assert_equal(_text_of(carried[2].value), "x")
        found = True
        break

    assert_true(found, "header nullability message never arrived")
    consumer.close()
    print("    null, empty and present header values stayed distinct")
    _ = cluster^


def test_explicit_partition_is_honoured() raises:
    """`partition=` bypasses the partitioner.

    Without an explicit partition the key decides, so this writes the *same*
    key to three different partitions: under the default partitioner all
    three would land together, and only an honoured `partition=` spreads
    them.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("partitioned", partition_count=3)

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    for p in range(3):
        _ = producer.produce(
            topic="partitioned",
            value="to-" + String(p),
            key="same-key-every-time",
            partition=Int32(p),
        )
    producer.flush(10000)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="partitioned-group",
            auto_offset_reset="earliest",
        )
    )
    consumer.subscribe(["partitioned"])

    var landed = List[Int](length=3, fill=0)
    var seen = 0
    var attempts = 0
    while seen < 3 and attempts < 60:
        attempts += 1
        var maybe = consumer.poll(timeout_ms=1000)
        if not maybe:
            continue
        ref m = maybe.value()
        assert_equal(m.value_text(), "to-" + String(Int(m.partition)))
        landed[Int(m.partition)] += 1
        seen += 1

    assert_equal(seen, 3)
    for p in range(3):
        assert_equal(landed[p], 1)

    consumer.close()
    print("    one message on each of 3 partitions, same key throughout")
    _ = cluster^


def test_delivered_messages_report_no_failures() raises:
    """The success path stays clean once a per-message opaque is attached.

    `produce()` now sets `RD_KAFKA_VTYPE_OPAQUE` on every message, and the
    delivery report reads it back out of `_private`. A wrong offset there
    would not raise -- it would quietly return whatever word sits at 64 --
    so this pins the observable consequence: messages that really were
    delivered produce no reports at all, and every token is distinct.

    The failure side is covered in `tests/test_smoke.mojo`, where the
    sequences come back on real rejections.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("delivered", partition_count=1)

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    var tokens = List[Int]()
    for i in range(8):
        tokens.append(
            producer.produce(topic="delivered", value="msg-" + String(i))
        )
    producer.flush(10000)

    assert_equal(producer.delivery_failures(), 0)
    assert_equal(len(producer.failures()), 0)

    for i in range(len(tokens)):
        assert_true(tokens[i] != 0, "sequence 0 collides with 'no opaque'")
        for j in range(i + 1, len(tokens)):
            assert_true(
                tokens[i] != tokens[j],
                "token " + String(tokens[i]) + " handed out twice",
            )

    print("    8 delivered, 0 reports, 8 distinct tokens")
    _ = cluster^


def test_position_walks_every_partition() raises:
    """Regression guard for the topic-partition stride.

    `sizeof(rd_kafka_topic_partition_t)` is 64, not the 56 its members add
    up to -- there is a trailing `void *_private`. A 56-byte stride decodes
    the first element correctly and then drifts, exactly like the 24-byte
    metadata stride did, so this needs enough partitions to make the drift
    unmistakable rather than plausible.

    Asserting that entry `i` comes back as partition `i` is what catches it:
    a wrong stride returns *something* for every entry, just not the right
    thing.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("stride-tp", partition_count=12)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="stride-tp-group",
            auto_offset_reset="earliest",
        )
    )

    var wanted = List[TopicPartition]()
    for p in range(12):
        wanted.append(TopicPartition("stride-tp", Int32(p), OFFSET_BEGINNING))
    consumer.assign(wanted)

    var got = consumer.position(wanted)
    assert_equal(len(got), 12)
    for i in range(12):
        assert_equal(got[i].topic, "stride-tp")
        assert_equal(Int(got[i].partition), i)

    consumer.close()
    print("    decoded 12 partitions without stride drift")
    _ = cluster^


def test_assign_and_seek_replay_from_an_offset() raises:
    """Replay: the workload `assign` and `seek` exist for.

    Consumes a partition to the end, then seeks back into the middle of it
    and checks the *same* records come back from there -- which is the whole
    point, and which a seek that silently did nothing would fail. Both the
    absolute offset and `OFFSET_BEGINNING` are exercised, because they take
    different paths through librdkafka.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("replay", partition_count=1)

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    for i in range(10):
        _ = producer.produce(topic="replay", value="r-" + String(i))
    producer.flush(10000)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="replay-group",
            auto_offset_reset="earliest",
        )
    )
    # `assign`, not `subscribe`: this consumer names its own partition.
    var at_start: List[TopicPartition] = [
        TopicPartition("replay", 0, OFFSET_BEGINNING)
    ]
    consumer.assign(at_start)

    var first_pass = _drain(consumer, 10)
    assert_equal(len(first_pass), 10)
    assert_equal(first_pass[0].offset, Int64(0))
    assert_equal(first_pass[9].value_text(), "r-9")

    # Back to the middle. The partition is assigned and has been fetched
    # from, which is the state `seek_partitions` requires.
    consumer.seek([TopicPartition("replay", 0, 5)])
    var replayed = _drain(consumer, 5)
    assert_equal(len(replayed), 5)
    assert_equal(replayed[0].offset, Int64(5))
    assert_equal(replayed[0].value_text(), "r-5")
    assert_equal(replayed[4].value_text(), "r-9")

    # And all the way back to the start.
    consumer.seek([TopicPartition("replay", 0, OFFSET_BEGINNING)])
    var from_start = _drain(consumer, 1)
    assert_equal(len(from_start), 1)
    assert_equal(from_start[0].offset, Int64(0))

    consumer.close()
    print("    replayed from offset 5 and from the beginning")
    _ = cluster^


def test_position_and_watermarks_measure_lag() raises:
    """Lag is `high watermark - position`, and both halves must be real.

    The two are read through completely different paths -- `position` is
    local state, the watermarks come from the broker -- so a test that only
    checked one would pass with the other returning a plausible constant.
    This pins both against a partition whose contents it chose.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("lag", partition_count=1)

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    for i in range(20):
        _ = producer.produce(topic="lag", value="m-" + String(i))
    producer.flush(10000)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="lag-group",
            auto_offset_reset="earliest",
        )
    )
    var only: List[TopicPartition] = [
        TopicPartition("lag", 0, OFFSET_BEGINNING)
    ]
    consumer.assign(only)

    # Nothing read yet: position is INVALID, not 0. The difference matters --
    # 0 would claim 20 records of lag on a partition we have not started.
    var before = consumer.position(only)
    assert_equal(len(before), 1)
    assert_equal(before[0].offset, OFFSET_INVALID)

    var consumed = _drain(consumer, 8)
    assert_equal(len(consumed), 8)

    var after = consumer.position(only)
    assert_equal(after[0].offset, Int64(8))
    assert_true(not after[0].has_error(), "position reported a partition error")

    var marks = consumer.query_watermark_offsets("lag", 0)
    assert_equal(marks.low, Int64(0))
    assert_equal(marks.high, Int64(20))
    assert_true(not marks.is_empty(), "a 20-record partition read as empty")
    assert_equal(marks.high - after[0].offset, Int64(12))

    # The cached watermarks are a by-product of fetching, so they are only
    # available once this consumer has actually fetched -- which it has.
    var cached = consumer.get_watermark_offsets("lag", 0)
    assert_equal(cached.high, Int64(20))

    consumer.close()
    print("    position 8 of 20, lag 12, watermarks [0, 20)")
    _ = cluster^


def test_committed_is_not_position() raises:
    """The group's committed offset and this consumer's position differ.

    Everything consumed since the last commit sits between them, so a test
    that only checked they were equal after a commit would pass for an
    implementation that returned `position` from both. This reads them at a
    point where the right answers are different numbers.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("committed", partition_count=1)

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    for i in range(10):
        _ = producer.produce(topic="committed", value="c-" + String(i))
    producer.flush(10000)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="committed-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
    )
    var only: List[TopicPartition] = [
        TopicPartition("committed", 0, OFFSET_BEGINNING)
    ]
    consumer.assign(only)

    # Nothing committed by this group yet.
    var fresh = consumer.committed(only)
    assert_equal(len(fresh), 1)
    assert_equal(fresh[0].topic, "committed")
    assert_equal(fresh[0].offset, OFFSET_INVALID)

    assert_equal(len(_drain(consumer, 4)), 4)
    consumer.commit()

    var stored = consumer.committed(only)
    assert_equal(stored[0].offset, Int64(4))

    # Read four more without committing: position moves, the commit does not.
    assert_equal(len(_drain(consumer, 4)), 4)
    assert_equal(consumer.position(only)[0].offset, Int64(8))
    assert_equal(consumer.committed(only)[0].offset, Int64(4))

    consumer.close()
    print("    committed stayed at 4 while position reached 8")
    _ = cluster^


def test_pause_stops_the_flow_and_resume_restarts_it() raises:
    """Paused partitions stop delivering; resumed ones pick up where they
    stopped.

    The consumer is assigned at `OFFSET_END` and paused *before* anything it
    could read exists, then the records are produced. That ordering is what
    makes the assertion sound: with records already on the partition, a
    fetch could have buffered them locally before the pause landed, and
    `poll` would keep handing those out however correct the pause was.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("paused", partition_count=1)

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    for i in range(3):
        _ = producer.produce(topic="paused", value="before-" + String(i))
    producer.flush(10000)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="paused-group",
            auto_offset_reset="latest",
        )
    )
    var only: List[TopicPartition] = [TopicPartition("paused", 0, OFFSET_END)]
    consumer.assign(only)
    # Let the assignment settle. Nothing should arrive: the three records
    # above are behind OFFSET_END.
    for _ in range(3):
        assert_true(not consumer.poll(timeout_ms=300), "OFFSET_END delivered")

    consumer.pause(only)

    for i in range(5):
        _ = producer.produce(topic="paused", value="after-" + String(i))
    producer.flush(10000)

    for _ in range(6):
        assert_true(
            not consumer.poll(timeout_ms=300), "a paused partition delivered"
        )

    consumer.resume(only)
    var got = _drain(consumer, 5)
    assert_equal(len(got), 5)
    assert_equal(got[0].value_text(), "after-0")
    assert_equal(got[4].value_text(), "after-4")

    consumer.close()
    print("    5 records withheld while paused, all 5 after resume")
    _ = cluster^


def test_partition_eof_is_distinguishable_from_a_timeout() raises:
    """ "Caught up" and "nothing arrived" are different answers.

    Both come back from `poll()` as `None`, which is why a bounded drain was
    impossible before `poll_event`. This asserts both halves of the split:
    the same drained partition reports an EOF mark to a consumer configured
    for it, and a plain timeout to one that is not. Checking only the first
    would pass for an implementation that reported EOF unconditionally.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("eof", partition_count=1)

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    for i in range(4):
        _ = producer.produce(topic="eof", value="e-" + String(i))
    producer.flush(10000)

    var bounded = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="eof-group",
            auto_offset_reset="earliest",
            enable_partition_eof=True,
        )
    )
    bounded.assign([TopicPartition("eof", 0, OFFSET_BEGINNING)])

    var seen = 0
    var hit_eof = False
    for _ in range(60):
        var event = bounded.poll_event(timeout_ms=1000)
        if event.eof:
            ref at = event.eof.value()
            assert_equal(at.topic, "eof")
            assert_equal(Int(at.partition), 0)
            # The EOF mark carries where the partition ends, which is the
            # offset the next record written will get -- 4, not 3.
            assert_equal(at.offset, Int64(4))
            hit_eof = True
            break
        if event.message:
            assert_equal(
                event.message.value().value_text(), "e-" + String(seen)
            )
            seen += 1

    assert_equal(seen, 4)
    assert_true(hit_eof, "drained partition never reported end-of-partition")
    bounded.close()

    # The other half: without the flag the same drain only ever times out.
    var tailing = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="eof-tail-group",
            auto_offset_reset="earliest",
        )
    )
    tailing.assign([TopicPartition("eof", 0, OFFSET_BEGINNING)])
    assert_equal(len(_drain(tailing, 4)), 4)
    for _ in range(4):
        var event = tailing.poll_event(timeout_ms=500)
        assert_true(not event.eof, "EOF reported without enable_partition_eof")
        assert_true(event.is_timeout(), "idle poll was not a timeout")

    tailing.close()
    print("    EOF at offset 4 with the flag, plain timeouts without it")
    _ = cluster^


def test_message_timestamps_are_populated() raises:
    """Event-time processing needs a timestamp and needs to know its clock.

    `timestamp` is asserted through `has_timestamp()` rather than against
    -1: -1 is a legal `int64` millisecond value, so the type is the only
    field that actually answers whether there is a timestamp at all.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("stamped", partition_count=1)

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    for i in range(3):
        _ = producer.produce(topic="stamped", value="t-" + String(i))
    producer.flush(10000)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="stamped-group",
            auto_offset_reset="earliest",
        )
    )
    consumer.assign([TopicPartition("stamped", 0, OFFSET_BEGINNING)])

    var got = _drain(consumer, 3)
    assert_equal(len(got), 3)
    for m in got:
        assert_true(m.has_timestamp(), "record carried no timestamp")
        # The producer stamped these, so it is create time and not the
        # broker's log-append time.
        assert_equal(m.timestamp_type, TIMESTAMP_CREATE_TIME)
        # Bounded rather than compared to a clock: after 2020-09-13 and
        # before 2100-01-01, which no plausible garbage read satisfies.
        assert_true(m.timestamp > 1600000000000, "timestamp before 2020")
        assert_true(m.timestamp < 4102444800000, "timestamp after 2100")

    # Written in order, so stamped in order.
    assert_true(got[0].timestamp <= got[1].timestamp, "timestamps went back")
    assert_true(got[1].timestamp <= got[2].timestamp, "timestamps went back")

    consumer.close()
    print("    3 records carried create-time timestamps in order")
    _ = cluster^


def test_produced_timestamp_is_preserved() raises:
    """An explicit CreateTime survives the round trip.

    The reason `produce(timestamp=)` exists: replaying an archive, or
    forwarding records from another system, where the event time is not the
    time you happen to be publishing. Two fixed values in the past are used
    rather than a clock, so "preserved" means equal to a number this test
    chose -- a client that ignored the argument would stamp `now` and fail.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("stamps", partition_count=1)

    # 2021-01-01 and 2021-06-01, in milliseconds.
    var first = Int64(1609459200000)
    var second = Int64(1622505600000)

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    _ = producer.produce(topic="stamps", value="a", timestamp=first)
    _ = producer.produce(topic="stamps", value="b", timestamp=second)
    # And one without, which must be stamped now rather than 0.
    _ = producer.produce(topic="stamps", value="c")
    producer.flush(10000)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="stamps-group",
            auto_offset_reset="earliest",
        )
    )
    consumer.assign([TopicPartition("stamps", 0, OFFSET_BEGINNING)])

    var got = _drain(consumer, 3)
    assert_equal(len(got), 3)
    for m in got:
        assert_true(m.has_timestamp(), "record carried no timestamp")
        assert_equal(m.timestamp_type, TIMESTAMP_CREATE_TIME)

    assert_equal(got[0].timestamp, first)
    assert_equal(got[1].timestamp, second)
    # The default is "now", not 0 -- 0 would be 1970 and is what a missing
    # vu entry, or one written at the wrong offset, would look like.
    assert_true(got[2].timestamp > second, "default timestamp was not now")

    consumer.close()
    print("    two chosen timestamps preserved, the default stamped now")
    _ = cluster^


def _noop_handler(event: Rebalance) raises:
    """An `on_assign` that looks and does nothing.

    Which must still leave the consumer assigned -- see the test below.
    """
    pass


def _assign_from_offset_three(event: Rebalance) raises:
    """An `on_assign` that starts somewhere other than the group's commit.

    The shape a job with its own offset store uses: rewrite the offsets on
    the partitions being handed over, then assign those instead.
    """
    var start_at = List[TopicPartition]()
    for tp in event.partitions:
        start_at.append(TopicPartition(tp.topic, tp.partition, 3))
    event.assign(start_at)


def _commit_on_revoke(event: Rebalance) raises:
    """An `on_revoke` that commits before the partitions move elsewhere."""
    event.commit()


def test_rebalance_handler_that_does_nothing_still_gets_assigned() raises:
    """A handler is an opportunity, not an obligation.

    Registering a rebalance callback stops librdkafka assigning by itself,
    so a handler that only looks would strand the consumer with no
    partitions -- a silent stall, not an error. `_rebalance_trampoline`
    applies the default whenever the handler did not.

    Measured against `confluent-kafka` 2.15, whose `on_assign` likewise need
    not call `assign()` for records to flow.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("rb-default", partition_count=1)

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    for i in range(5):
        _ = producer.produce(topic="rb-default", value="d-" + String(i))
    producer.flush(10000)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="rb-default-group",
            auto_offset_reset="earliest",
        )
    )
    consumer.subscribe(["rb-default"], on_assign=_noop_handler)

    var got = _drain(consumer, 5)
    assert_equal(len(got), 5)
    assert_equal(got[0].offset, Int64(0))
    assert_equal(got[4].value_text(), "d-4")

    consumer.close()
    print("    a no-op on_assign still received all 5 records")
    _ = cluster^


def test_rebalance_handler_can_choose_the_starting_offset() raises:
    """`on_assign` can start the consumer somewhere else.

    This is what the handler is *for*, and it is asserted through the
    records that actually arrive rather than a flag saying the handler ran:
    starting at offset 3 means the first record is offset 3, which a handler
    whose `assign()` was ignored could not produce.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("rb-seek", partition_count=1)

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    for i in range(8):
        _ = producer.produce(topic="rb-seek", value="s-" + String(i))
    producer.flush(10000)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="rb-seek-group",
            auto_offset_reset="earliest",
        )
    )
    consumer.subscribe(["rb-seek"], on_assign=_assign_from_offset_three)

    var got = _drain(consumer, 5)
    assert_equal(len(got), 5)
    assert_equal(got[0].offset, Int64(3))
    assert_equal(got[0].value_text(), "s-3")
    assert_equal(got[4].value_text(), "s-7")

    consumer.close()
    print("    on_assign started the consumer at offset 3, not 0")
    _ = cluster^


def test_on_revoke_can_commit_before_the_partitions_move() raises:
    """The reason `on_revoke` exists.

    Once a rebalance completes the partitions belong to another member, and
    anything consumed but uncommitted is reprocessed there. The handler
    commits inside the callback, which is the last moment that is still
    possible.

    Asserted through Kafka itself rather than a counter: auto-commit is off,
    so if a *second* consumer in the same group sees a committed offset, the
    only thing that could have written it is the handler.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("rb-revoke", partition_count=1)

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    for i in range(6):
        _ = producer.produce(topic="rb-revoke", value="r-" + String(i))
    producer.flush(10000)

    var first = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="rb-revoke-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
    )
    first.subscribe(["rb-revoke"], on_revoke=_commit_on_revoke)
    assert_equal(len(_drain(first, 4)), 4)
    # Leaving the group revokes the partitions, which runs the handler.
    first.close()

    var second = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="rb-revoke-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
    )
    var only: List[TopicPartition] = [TopicPartition("rb-revoke", 0)]
    var stored = second.committed(only)
    assert_equal(len(stored), 1)
    assert_equal(stored[0].offset, Int64(4))

    second.close()
    print("    on_revoke committed offset 4 before leaving the group")
    _ = cluster^


def test_dropping_a_subscribed_consumer_does_not_fault() raises:
    """A consumer may be dropped without `close()`. It used to segfault.

    `Consumer.__deinit__` calls `rd_kafka_consumer_close`, which fires one
    last revoke through `_rebalance_trampoline` -- and the trampoline reaches
    the consumer's rebalance box by the raw address given to
    `rd_kafka_conf_set_opaque`. Mojo releases each field at its last use
    *inside the destructor body*, not after it, so a `_rebalance` that the
    body never mentions was freed **before** `consumer_close` was called and
    the callback read a dangling box.

    Every other case in this file closes by hand, which is exactly why the
    suite was green while `close()`-less teardown faulted every time. This
    one deliberately does not close, so the destructor path is covered.

    Reaching the assertion at all is the assertion: the failure mode is a
    SIGSEGV, not a wrong value.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("drop-me", partition_count=1)

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    _ = producer.produce(topic="drop-me", value="only")
    producer.flush(10000)

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="drop-me-group",
            auto_offset_reset="earliest",
        )
    )
    consumer.subscribe(["drop-me"])
    # Poll to the point of a real assignment: the final revoke only has
    # something to hand back if the consumer actually owns a partition.
    var seen = len(_drain(consumer, 1))
    assert_equal(seen, 1)

    # No `close()`. `^` runs the destructor here rather than at the end of
    # the function, so the fault -- if it comes back -- lands on this line.
    _ = consumer^

    assert_equal(seen, 1)
    print("    a consumer dropped without close() tore down cleanly")
    _ = cluster^


# `on_lost` cannot be observed the way the other handlers are. The rebalance
# handlers below it commit or assign, and the test then reads that back out
# of Kafka -- but a *lost* assignment is precisely the case where those side
# effects fail: another member may already own the partition, and librdkafka
# logs `COMMITFAIL` for exactly that reason.
#
# A handler is also **thin**: it captures nothing, so it cannot tick a
# counter the test owns. What it can do is name a compile-time constant, so
# the marker goes in the environment of this process -- no filesystem, and
# readable straight after the poll that triggered the rebalance.
comptime LOST_MARKER = "MOJO_KAFKA_TEST_ON_LOST"
comptime REVOKE_MARKER = "MOJO_KAFKA_TEST_ON_REVOKE"


def _record_lost(event: Rebalance) raises:
    """An `on_lost` that records that it ran, and what it was told."""
    _ = setenv(
        LOST_MARKER,
        String(len(event.partitions)) + ":" + String(event.lost),
        True,
    )


def _record_revoke(event: Rebalance) raises:
    """An `on_revoke` that records the same, to prove routing went elsewhere."""
    _ = setenv(
        REVOKE_MARKER,
        String(len(event.partitions)) + ":" + String(event.lost),
        True,
    )


def test_on_lost_takes_over_from_on_revoke_for_a_lost_assignment() raises:
    """Partitions lost involuntarily route to `on_lost`, not `on_revoke`.

    The assignment is lost for real rather than simulated: exceeding
    `max.poll.interval.ms` is librdkafka's own liveness check, and a
    consumer that trips it is thrown out of the group with its partitions
    marked lost. That is a client-side timer, so the mock broker serves it
    like any other -- no Docker, and no coordinator failover to arrange.

    The two timeouts are pinned together because librdkafka refuses a
    `max.poll.interval.ms` below `session.timeout.ms`, and 3s is the floor
    at which the mock still completes a JoinGroup: at 1s the request itself
    times out and the group never forms, so nothing is ever assigned to
    lose.

    This is the branch of `_rebalance_trampoline` that nothing else runs.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("rb-lost", partition_count=1)

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    for i in range(3):
        _ = producer.produce(topic="rb-lost", value="l-" + String(i))
    producer.flush(10000)

    _ = setenv(LOST_MARKER, "", True)
    _ = setenv(REVOKE_MARKER, "", True)

    var cfg = ConsumerConfig(
        bootstrap_servers=bootstrap,
        group_id="rb-lost-group",
        auto_offset_reset="earliest",
    )
    cfg.set("max.poll.interval.ms", "3000")
    cfg.set("session.timeout.ms", "3000")
    cfg.set("heartbeat.interval.ms", "1000")
    var consumer = Consumer(cfg)
    consumer.subscribe(
        ["rb-lost"], on_revoke=_record_revoke, on_lost=_record_lost
    )

    var got = _drain(consumer, 3)
    assert_equal(len(got), 3)
    assert_equal(getenv(LOST_MARKER), "", "nothing should be lost yet")

    # Stop polling for longer than max.poll.interval.ms. librdkafka checks
    # twice a second, so this overshoots rather than racing the timer.
    sleep(4.0)

    # The eviction surfaces on the next poll: it raises
    # `__MAX_POLL_EXCEEDED` once, and the rebalance runs shortly after.
    for _ in range(12):
        try:
            _ = consumer.poll(timeout_ms=500)
        except:
            # The MAXPOLL error itself is expected -- it is the mechanism.
            pass
        if getenv(LOST_MARKER) != "":
            break

    assert_equal(
        getenv(LOST_MARKER),
        "1:True",
        "on_lost did not run with the lost partition",
    )
    assert_equal(
        getenv(REVOKE_MARKER),
        "",
        "on_revoke ran; a lost assignment must not fall through to it",
    )

    consumer.close()
    print("    a lost assignment routed to on_lost, not on_revoke")
    _ = cluster^


def test_a_lost_assignment_falls_back_to_on_revoke_when_on_lost_is_unset() raises:
    """With no `on_lost`, a lost assignment goes to `on_revoke` instead.

    The documented fallback, and `confluent-kafka`'s. It is the other half
    of the routing rule above: without this, an `on_lost` that was never
    reached and a lost event that was dropped on the floor look the same.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("rb-fallback", partition_count=1)

    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    for i in range(3):
        _ = producer.produce(topic="rb-fallback", value="f-" + String(i))
    producer.flush(10000)

    _ = setenv(LOST_MARKER, "", True)
    _ = setenv(REVOKE_MARKER, "", True)

    var cfg = ConsumerConfig(
        bootstrap_servers=bootstrap,
        group_id="rb-fallback-group",
        auto_offset_reset="earliest",
    )
    cfg.set("max.poll.interval.ms", "3000")
    cfg.set("session.timeout.ms", "3000")
    cfg.set("heartbeat.interval.ms", "1000")
    var consumer = Consumer(cfg)
    # No `on_lost` this time -- that is the whole point.
    consumer.subscribe(["rb-fallback"], on_revoke=_record_revoke)

    var got = _drain(consumer, 3)
    assert_equal(len(got), 3)

    sleep(4.0)

    for _ in range(12):
        try:
            _ = consumer.poll(timeout_ms=500)
        except:
            pass
        if getenv(REVOKE_MARKER) != "":
            break

    # `lost` is still true on the context -- the fallback changes which
    # handler runs, not what happened.
    assert_equal(
        getenv(REVOKE_MARKER),
        "1:True",
        "on_revoke did not receive the lost assignment",
    )
    assert_equal(getenv(LOST_MARKER), "", "on_lost is unset and must not run")

    consumer.close()
    print("    with no on_lost, the lost assignment fell back to on_revoke")
    _ = cluster^


# --- transactions -----------------------------------------------------------
#
# The mock serves InitProducerId, AddPartitionsToTxn and EndTxn, so the whole
# producer side of exactly-once is reachable here with no Docker -- including
# the two error branches, which `MockCluster.push_request_errors` drives by
# making the coordinator answer with a chosen code. That matters more than it
# looks: the fatal / abortable / retriable decision is made by the *client*
# from the broker's code, so injecting at the mock exercises the real
# classification rather than a stand-in for it.


def _txn_producer(bootstrap: String, txn_id: String) raises -> Producer:
    """A producer configured for transactions. Not a test case -- see `_drain`.
    """
    var cfg = ProducerConfig(bootstrap_servers=bootstrap)
    cfg.set("transactional.id", txn_id)
    return Producer(cfg)


def _committed_reader(bootstrap: String, group: String) raises -> Consumer:
    """A `read_committed` consumer, which is the only kind that can tell a
    committed transaction from an aborted one."""
    var cfg = ConsumerConfig(
        bootstrap_servers=bootstrap,
        group_id=group,
        auto_offset_reset="earliest",
    )
    cfg.set("isolation.level", "read_committed")
    return Consumer(cfg)


def test_a_committed_transaction_is_visible_to_a_read_committed_consumer() raises:
    """The happy path, end to end and observed through Kafka.

    `read_committed` is the point: a consumer left on the default
    `read_uncommitted` sees the records either way, so it cannot tell a
    working commit from a missing one. This one and its aborted twin below
    are a pair -- neither means much alone.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("txn-commit", partition_count=1)

    var producer = _txn_producer(bootstrap, "txn-commit-id")
    assert_true(
        not producer.init_transactions(10000), "init_transactions failed"
    )
    assert_true(not producer.begin_transaction(), "begin_transaction failed")
    for i in range(3):
        _ = producer.produce(
            topic="txn-commit", key="k" + String(i), value="v" + String(i)
        )
    # No flush() -- commit_transaction() flushes, and a caller who adds one
    # is guessing at librdkafka's job.
    assert_true(not producer.commit_transaction(), "commit_transaction failed")
    assert_equal(
        len(producer.failures()), 0, "a committed transaction lost a message"
    )

    var consumer = _committed_reader(bootstrap, "txn-commit-group")
    consumer.subscribe(["txn-commit"])
    var seen = _drain(consumer, 3)
    assert_equal(len(seen), 3, "committed records were not readable")
    for i in range(3):
        assert_equal(_text_of(seen[i].value), "v" + String(i))
    consumer.close()

    print("    committed transaction: 3 records visible as read_committed")
    _ = cluster^


def test_an_aborted_transaction_is_invisible_to_a_read_committed_consumer() raises:
    """The other half, and the one that proves the transaction is real.

    An abort also **purges** everything still queued, and each purged
    message surfaces as an ordinary delivery failure. That is documented
    behaviour rather than a defect, but it is retained like any other
    failure, so this asserts on it: a caller who does not `take_failures()`
    after an abort meets them at the next `flush()`.

    **Absence is proved with a barrier, not a timeout.** After the abort the
    producer commits one marker record and the consumer reads until the
    marker arrives; anything aborted sits *earlier* in the log, so it would
    be delivered first. Polling for a fixed period instead proves nothing --
    it passes whenever the broker was merely slower than the wait, and the
    first version of this test spent 60 seconds doing that.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("txn-abort", partition_count=1)

    var producer = _txn_producer(bootstrap, "txn-abort-id")
    assert_true(
        not producer.init_transactions(10000), "init_transactions failed"
    )
    assert_true(not producer.begin_transaction(), "begin_transaction failed")
    for i in range(3):
        _ = producer.produce(
            topic="txn-abort", key="k" + String(i), value="v" + String(i)
        )
    assert_true(not producer.abort_transaction(), "abort_transaction failed")

    # The purge is the abort working. Acknowledge it the way the docstring
    # says to, and check the reports are addressable rather than a tally.
    var purged = producer.take_failures()
    assert_equal(len(purged), 3, "an abort must purge every queued message")
    for report in purged:
        assert_equal(report.topic, "txn-abort")
        assert_true(report.error != "", "a purge report with no error text")
    print("    abort purged 3 messages:", purged[0])

    # The barrier: one committed record, behind the three aborted ones.
    assert_true(
        not producer.begin_transaction(), "could not begin after an abort"
    )
    _ = producer.produce(topic="txn-abort", key="marker", value="marker")
    assert_true(not producer.commit_transaction(), "the marker did not commit")

    var consumer = _committed_reader(bootstrap, "txn-abort-group")
    consumer.subscribe(["txn-abort"])
    var seen = _drain(consumer, 1)
    assert_equal(len(seen), 1, "the marker never arrived")
    assert_equal(
        _text_of(seen[0].key),
        "marker",
        "an aborted record was readable as committed",
    )
    # And nothing behind it either.
    var extra = consumer.poll(timeout_ms=1000)
    assert_true(
        not extra, "a second record followed the marker: " + String(len(seen))
    )
    consumer.close()

    print("    aborted transaction: only the post-abort marker is readable")
    _ = cluster^


def test_a_fatal_transaction_error_is_flagged_fatal() raises:
    """`is_fatal` off a real error, not a hand-built `KafkaError`.

    `CLUSTER_AUTHORIZATION_FAILED` from the coordinator on InitProducerId is
    one librdkafka classifies fatal: the producer can never acquire a
    producer id, so there is nothing to retry and no transaction to abort.
    The client is what decides that, from the code the mock returns -- which
    is why injecting at the broker tests the real thing.

    Guards the `TXN_FATAL` arm of `txn_action()` and, with the abortable
    case below, the two flags that no other suite can set.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("txn-fatal", partition_count=1)
    cluster.push_request_errors(
        API_KEY_INIT_PRODUCER_ID,
        [RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED],
    )

    var producer = _txn_producer(bootstrap, "txn-fatal-id")
    var failure = producer.init_transactions(10000)
    assert_true(Bool(failure), "init_transactions ignored an injected error")
    ref err = failure.value()
    assert_true(err.is_fatal, "the fatal flag was lost: " + String(err))
    assert_true(not err.is_retriable, "a fatal error must not be retriable")
    assert_true(
        err.txn_action() == TXN_FATAL,
        "expected TXN_FATAL, got " + String(err.txn_action()),
    )
    print("    fatal:", err, "->", err.txn_action())
    _ = cluster^


def test_an_abortable_transaction_error_asks_for_an_abort() raises:
    """`txn_requires_abort` off a real error, and the recovery that follows.

    `TOPIC_AUTHORIZATION_FAILED` on AddPartitionsToTxn puts the transaction
    into the abortable state: this producer is still usable, but *this*
    transaction is finished and must be aborted before another can begin.
    That is the case `txn_action()`'s branch order exists for -- librdkafka
    can flag an error both fatal and abortable, and answering `TXN_FATAL`
    there tears down a producer that only needed an abort.

    The test does not stop at the flag. It follows the verdict, because a
    branch that names the right action and then cannot carry it out is not
    worth much: it aborts, begins a second transaction and commits it.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("txn-abortable", partition_count=1)

    var producer = _txn_producer(bootstrap, "txn-abortable-id")
    assert_true(
        not producer.init_transactions(10000), "init_transactions failed"
    )

    cluster.push_request_errors(
        API_KEY_ADD_PARTITIONS_TO_TXN,
        [RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED],
    )
    assert_true(not producer.begin_transaction(), "begin_transaction failed")
    _ = producer.produce(topic="txn-abortable", key="k", value="v")

    var failure = producer.commit_transaction()
    assert_true(Bool(failure), "commit ignored an injected error")
    ref err = failure.value()
    assert_true(
        err.txn_requires_abort,
        "the abortable flag was lost: " + String(err),
    )
    assert_true(
        err.txn_action() == TXN_ABORT,
        "expected TXN_ABORT, got " + String(err.txn_action()),
    )
    print("    abortable:", err, "->", err.txn_action())

    # Follow the verdict. This is what TXN_ABORT instructs, and it must work.
    assert_true(not producer.abort_transaction(), "the prescribed abort failed")
    _ = producer.take_failures()

    assert_true(
        not producer.begin_transaction(),
        "the producer was unusable after an abortable error",
    )
    _ = producer.produce(topic="txn-abortable", key="k2", value="v2")
    assert_true(
        not producer.commit_transaction(),
        "the transaction after the abort did not commit",
    )

    var consumer = _committed_reader(bootstrap, "txn-abortable-group")
    consumer.subscribe(["txn-abortable"])
    var seen = _drain(consumer, 1)
    assert_equal(len(seen), 1, "the recovered transaction is not readable")
    assert_equal(_text_of(seen[0].value), "v2")
    assert_equal(
        _text_of(seen[0].key), "k2", "the aborted record leaked through"
    )
    consumer.close()

    print("    aborted, began again, committed; only the second record is up")
    _ = cluster^


# **A third thing the mock does not implement, and it is silent about it.**
# It accepts `TxnOffsetCommit` and answers success, but never serves those
# offsets back through `OffsetFetch`: `committed()` reports `OFFSET_INVALID`
# afterwards whether the transaction committed or aborted. Confirmed in plain
# C against librdkafka 2.15, with no Mojo involved, so it is the mock and not
# this package. The consequence for tests here is sharp -- any assertion
# about a committed offset passes unconditionally, which is worse than no
# assertion. Those live in `test_broker.mojo`.


def _eos_reader(bootstrap: String, group: String) raises -> Consumer:
    """The input side of a read-process-write loop.

    `enable.auto.commit=false` is not optional: librdkafka requires it, and
    with it on the consumer would commit on its own schedule and the
    transaction would no longer be what decides the input was consumed.
    """
    var cfg = ConsumerConfig(
        bootstrap_servers=bootstrap,
        group_id=group,
        auto_offset_reset="earliest",
    )
    cfg.set("enable.auto.commit", "false")
    cfg.set("isolation.level", "read_committed")
    return Consumer(cfg)


def test_a_read_process_write_loop_completes() raises:
    """Read-process-write end to end, as far as the mock can witness it.

    **What this cannot check is the committed offset**, and that is a mock
    limitation rather than a choice -- see the note above `_eos_reader`. It
    checks the shape of the loop instead: every call succeeds, the offsets
    handed over are the ones `position()` reports, and the transformed output
    is readable as `read_committed`. `test_transactional_offsets_are_visible_
    to_the_group` in `test_broker.mojo` is the other half, and it needs a
    real broker.

    3 is the expected position, not 2: librdkafka wants the *next* message to
    consume, which is what `position()` reports and what makes it the natural
    argument.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("eos-in", partition_count=1)
    cluster.create_topic("eos-out", partition_count=1)

    var upstream = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    for i in range(3):
        _ = upstream.produce(
            topic="eos-in", key="k" + String(i), value=String(i)
        )
    upstream.flush(10000)

    var consumer = _eos_reader(bootstrap, "eos-group")
    consumer.subscribe(["eos-in"])
    var work = _drain(consumer, 3)
    assert_equal(len(work), 3, "the input was not readable")

    var producer = _txn_producer(bootstrap, "eos-txn-id")
    assert_true(not producer.init_transactions(10000), "init failed")
    assert_true(not producer.begin_transaction(), "begin failed")
    for record in work:
        _ = producer.produce(
            topic="eos-out",
            key=record.key_text(),
            value=record.value_text() + "-done",
        )

    var assignment: List[TopicPartition] = [TopicPartition("eos-in", 0)]
    var positions = consumer.position(assignment)
    assert_equal(positions[0].offset, 3, "position is not last-consumed + 1")
    assert_true(
        not producer.send_offsets_to_transaction(
            positions, consumer.consumer_group_metadata(), 10000
        ),
        "send_offsets_to_transaction failed",
    )
    assert_true(not producer.commit_transaction(), "commit failed")

    # The output.
    var reader = _committed_reader(bootstrap, "eos-out-group")
    reader.subscribe(["eos-out"])
    var out = _drain(reader, 3)
    assert_equal(len(out), 3, "the transformed output is not readable")
    for i in range(3):
        assert_equal(_text_of(out[i].value), String(i) + "-done")
    reader.close()

    consumer.close()

    print("    read-process-write: 3 in, 3 transformed out, offsets sent")
    _ = cluster^


def test_a_failed_offset_commit_asks_for_an_abort() raises:
    """The failing side of read-process-write.

    `GROUP_AUTHORIZATION_FAILED` on AddOffsetsToTxn is abortable, so the
    verdict is `TXN_ABORT`, and the producer recovers by taking it.

    An earlier version of this also asserted the group was left at
    `OFFSET_INVALID` afterwards -- which is the property that actually
    matters, and which **cannot be tested here**: the mock answers
    `OFFSET_INVALID` whether or not anything was committed, so the assertion
    passed unconditionally. It lives in `test_broker.mojo` now. Do not add it
    back.
    """
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    cluster.create_topic("eos-fail-in", partition_count=1)
    cluster.create_topic("eos-fail-out", partition_count=1)

    var upstream = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    for i in range(3):
        _ = upstream.produce(topic="eos-fail-in", value=String(i))
    upstream.flush(10000)

    var consumer = _eos_reader(bootstrap, "eos-fail-group")
    consumer.subscribe(["eos-fail-in"])
    var work = _drain(consumer, 3)
    assert_equal(len(work), 3, "the input was not readable")

    var producer = _txn_producer(bootstrap, "eos-fail-txn-id")
    assert_true(not producer.init_transactions(10000), "init failed")
    assert_true(not producer.begin_transaction(), "begin failed")
    _ = producer.produce(topic="eos-fail-out", value="processed")

    cluster.push_request_errors(
        API_KEY_ADD_OFFSETS_TO_TXN,
        [RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED],
    )
    var assignment: List[TopicPartition] = [TopicPartition("eos-fail-in", 0)]
    var failure = producer.send_offsets_to_transaction(
        consumer.position(assignment),
        consumer.consumer_group_metadata(),
        10000,
    )
    assert_true(Bool(failure), "send_offsets ignored an injected error")
    ref err = failure.value()
    assert_true(
        err.txn_requires_abort, "the abortable flag was lost: " + String(err)
    )
    assert_true(
        err.txn_action() == TXN_ABORT,
        "expected TXN_ABORT, got " + String(err.txn_action()),
    )
    print("    offset-commit failure:", err, "->", err.txn_action())

    assert_true(not producer.abort_transaction(), "the prescribed abort failed")
    _ = producer.take_failures()
    consumer.close()

    print("    a failed offset commit asked for an abort, and took it")
    _ = cluster^


def _drain(mut consumer: Consumer, want: Int) raises -> List[Message]:
    """Poll until `want` messages have arrived, or give up.

    A helper and **not** a test case: `TestSuite.discover_tests` runs every
    `test_*` function it finds, so helpers here must not be named like one.
    """
    var got = List[Message]()
    var attempts = 0
    while len(got) < want and attempts < 60:
        attempts += 1
        var maybe = consumer.poll(timeout_ms=1000)
        if maybe:
            got.append(maybe.value().copy())
    return got^


def _text_of(field: Optional[List[UInt8]]) raises -> String:
    """Decode a present field; raises rather than papering over a null one."""
    if not field:
        raise Error("expected a present field, got null")
    return String(unsafe_from_utf8=Span(field.value()))


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

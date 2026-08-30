"""Integration tests against librdkafka's in-process mock broker.

No broker process and no Docker: the mock speaks the real wire protocol
over a real socket, so the produce and consume paths under test are the
same ones used against a real cluster. That makes these runnable
everywhere, including macOS CI runners where Docker is not available.

    pixi run test-mock

The mock does not implement the Topic Admin API, so `AdminClient`'s
topic *creation* is covered in `test_broker.mojo` instead.
"""

from std.testing import TestSuite, assert_equal, assert_true

from kafka import (
    AdminClient,
    Consumer,
    ConsumerConfig,
    Header,
    Message,
    Producer,
    ProducerConfig,
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


def _text_of(field: Optional[List[UInt8]]) raises -> String:
    """Decode a present field; raises rather than papering over a null one."""
    if not field:
        raise Error("expected a present field, got null")
    return String(unsafe_from_utf8=Span(field.value()))


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

"""Integration tests against librdkafka's in-process mock broker.

No broker process and no Docker: the mock speaks the real wire protocol
over a real socket, so the produce and consume paths under test are the
same ones used against a real cluster. That makes these runnable
everywhere, including macOS CI runners where Docker is not available.

    pixi run test-mock

The mock does not implement the Topic Admin API, so `AdminClient`'s
topic *creation* is covered in `test_broker.mojo` instead.
"""

from std.testing import assert_equal, assert_true

from kafka import (
    AdminClient,
    Consumer,
    ConsumerConfig,
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
        producer.produce(
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
        var m = maybe.value()
        assert_equal(m.topic, "round-trip")
        assert_equal(m.key, "key-" + String(seen))
        assert_equal(m.value, "value-" + String(seen))
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
    producer.produce(topic="alpha", key="a", value="from-alpha")
    producer.produce(topic="beta", key="b", value="from-beta")
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
        var m = maybe.value()
        if m.topic == "alpha":
            assert_equal(m.value, "from-alpha")
            saw_alpha = True
        elif m.topic == "beta":
            assert_equal(m.value, "from-beta")
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
    producer.produce_bytes(topic="binary", value=payload, key=keyb)
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
        var m = maybe.value()
        var value_bytes = m.value.as_bytes()
        var key_bytes = m.key.as_bytes()
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


def main() raises:
    print("mock broker (in-process, no Docker)")
    print("test_round_trip_preserves_key_and_value")
    test_round_trip_preserves_key_and_value()
    print("test_list_topics_walks_every_entry")
    test_list_topics_walks_every_entry()
    print("test_message_topic_is_populated")
    test_message_topic_is_populated()
    print("test_poll_returns_none_when_idle")
    test_poll_returns_none_when_idle()
    print("test_binary_payload_round_trips")
    test_binary_payload_round_trips()
    print("mock integration tests passed")

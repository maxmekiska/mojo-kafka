"""Smoke tests -- no broker required.

Run with `pixi run test`. These catch FFI breakage: if librdkafka cannot
be loaded, or a symbol has moved, the first test fails loudly rather than
at the first produce in someone's pipeline.
"""

from std.testing import TestSuite, assert_equal, assert_true

from kafka import (
    KIND_AUTHORIZATION,
    KIND_FATAL,
    KIND_MESSAGE_TOO_LARGE,
    KIND_OTHER,
    KIND_QUEUE_FULL,
    KIND_TIMED_OUT,
    KIND_TRANSPORT,
    KIND_UNKNOWN_TOPIC_OR_PARTITION,
    ConsumerConfig,
    Producer,
    ProducerConfig,
    kind_of,
    librdkafka_version,
)


def test_librdkafka_loadable() raises:
    """Loads the shared library and calls into it."""
    var v = librdkafka_version()
    assert_true(len(v.codepoints()) > 0, "empty librdkafka version string")
    print("    librdkafka", v)


def test_producer_config_defaults() raises:
    var cfg = ProducerConfig(bootstrap_servers="localhost:9092")
    assert_equal(cfg.client_id, "mojo-kafka")
    assert_equal(cfg.acks, "all")
    assert_equal(cfg.compression_type, "none")


def test_consumer_config_defaults() raises:
    var cfg = ConsumerConfig(bootstrap_servers="localhost:9092", group_id="g")
    assert_equal(cfg.group_id, "g")
    assert_equal(cfg.auto_offset_reset, "latest")
    assert_true(cfg.enable_auto_commit, "auto commit should default on")
    # Off by default, matching librdkafka: a tail-following consumer would
    # otherwise be handed an end-of-partition mark every time it caught up.
    assert_true(
        not cfg.enable_partition_eof, "partition EOF should default off"
    )


def test_produce_accepts_an_explicit_timestamp() raises:
    """Guards the `_VuArray` entry count, without needing a broker.

    Adding the timestamp took the array from seven `rd_kafka_vu_t` entries
    to eight. `_VuArray` fixes its capacity at construction and `_entry`
    raises rather than overrunning, so a forgotten `_VuArray(7)` fails on
    the first produce -- here, loudly, rather than by corrupting the array
    in somebody's pipeline.

    Nothing listens on port 9, so this asserts only that the message was
    *enqueued*; whether the broker preserved the timestamp is covered on the
    mock and against a real broker.
    """
    var cfg = ProducerConfig(bootstrap_servers="127.0.0.1:9")
    cfg.set("message.timeout.ms", "300")
    cfg.set("log_level", "0")
    var p = Producer(cfg)

    # A named time, and the 0 default that means "stamp it now".
    var stamped = p.produce(topic="t", value="v", timestamp=1600000000000)
    var now = p.produce(topic="t", value="v")
    var payload: List[UInt8] = [1]
    var raw = p.produce_bytes(topic="t", value=payload^, timestamp=Int64(42))
    assert_true(stamped != now, "sequences collided")
    assert_true(raw != now, "sequences collided")

    try:
        p.flush(3000)
    except:
        pass
    _ = p.take_failures()
    print("    8-entry vu array accepted a timestamp")


def test_extra_keys_recorded() raises:
    var cfg = ProducerConfig(bootstrap_servers="localhost:9092")
    cfg.set("message.max.bytes", "1000000")
    assert_equal(cfg.extra["message.max.bytes"], "1000000")


def test_set_passes_keys_verbatim() raises:
    """`log_level` is a real librdkafka property, underscore and all.

    `set()` used to rewrite `_` to `.`, which turned this into an invalid
    `log.level` and put the property permanently out of reach.
    """
    var cfg = ProducerConfig(bootstrap_servers="127.0.0.1:9")
    cfg.set("log_level", "0")
    var p = Producer(cfg)  # raises if librdkafka rejected the key
    _ = p.poll(0)


def test_flush_reports_undelivered_messages() raises:
    """An undelivered message must never look like a successful one.

    Nothing listens on port 9, so every message is dropped once
    `message.timeout.ms` expires. That empties the local queue, and before
    delivery reports were wired up `flush()` saw an empty queue and returned
    cleanly -- losing three messages without a word.
    """
    var cfg = ProducerConfig(bootstrap_servers="127.0.0.1:9")
    cfg.set("message.timeout.ms", "1500")
    cfg.set("log_level", "0")
    var p = Producer(cfg)
    var sent = List[Int]()
    for i in range(3):
        sent.append(
            p.produce(
                topic="nowhere", key="k" + String(i), value="v" + String(i)
            )
        )

    var raised = False
    try:
        p.flush(10000)
    except e:
        raised = True
        var text = String(e)
        assert_true(
            text.find("failed delivery") >= 0, "unexpected error: " + text
        )
        print("    flush reported:", text)
    assert_true(raised, "flush() reported success for 3 lost messages")

    # The reports must survive the raise -- discarding them there would put
    # the caller back to a count and a string.
    assert_equal(len(p.failures()), 3)

    var reports = p.take_failures()
    assert_equal(len(reports), 3)
    for i in range(3):
        # Each verdict is addressable: it carries the token produce() handed
        # back, in the order the messages were enqueued.
        assert_equal(reports[i].sequence, sent[i])
        assert_equal(reports[i].topic, "nowhere")
        assert_true(
            reports[i].error != "", "a failure report with no error text"
        )
    print("    per-message reports:", reports[0])

    # Taking them is what acknowledges them.
    assert_equal(len(p.failures()), 0)


def test_produce_sequences_are_distinct() raises:
    """Every message gets its own token, and none of them is 0.

    The opaque travels as a `void *` and returns as `_private`, where 0 is
    indistinguishable from a message produced without one -- so sequences
    start at 1.
    """
    var cfg = ProducerConfig(bootstrap_servers="127.0.0.1:9")
    cfg.set("message.timeout.ms", "300")
    cfg.set("log_level", "0")
    var p = Producer(cfg)

    var seen = List[Int]()
    for i in range(5):
        var seq = p.produce(topic="nowhere", value="v" + String(i))
        assert_true(seq != 0, "sequence 0 is indistinguishable from no opaque")
        for prior in seen:
            assert_true(prior != seq, "sequence " + String(seq) + " reused")
        seen.append(seq)

    try:
        p.flush(5000)
    except:
        pass
    _ = p.take_failures()
    print("    5 distinct tokens, first was", seen[0])


def test_error_kinds_classify_the_codes_callers_branch_on() raises:
    """`kind_of` groups librdkafka codes into branchable categories.

    Several codes collapse onto one kind deliberately, so this asserts the
    grouping rather than a one-to-one mapping -- and asserts that an
    unclassified code lands on OTHER instead of being mistaken for a
    neighbour.
    """
    assert_true(kind_of(-184) == KIND_QUEUE_FULL, "__QUEUE_FULL")

    # Both timeout codes are one kind: a caller retrying does not care which.
    assert_true(kind_of(-185) == KIND_TIMED_OUT, "__TIMED_OUT")
    assert_true(kind_of(-192) == KIND_TIMED_OUT, "__MSG_TIMED_OUT")

    # Connectivity, likewise.
    assert_true(kind_of(-195) == KIND_TRANSPORT, "__TRANSPORT")
    assert_true(kind_of(-187) == KIND_TRANSPORT, "__ALL_BROKERS_DOWN")

    # Local and broker-side "no such topic/partition" are the same problem.
    assert_true(
        kind_of(-188) == KIND_UNKNOWN_TOPIC_OR_PARTITION, "__UNKNOWN_TOPIC"
    )
    assert_true(
        kind_of(-190) == KIND_UNKNOWN_TOPIC_OR_PARTITION, "__UNKNOWN_PART"
    )
    assert_true(
        kind_of(3) == KIND_UNKNOWN_TOPIC_OR_PARTITION, "UNKNOWN_TOPIC_OR_PART"
    )

    assert_true(kind_of(10) == KIND_MESSAGE_TOO_LARGE, "MSG_SIZE_TOO_LARGE")
    assert_true(kind_of(-169) == KIND_AUTHORIZATION, "__AUTHENTICATION")
    assert_true(kind_of(29) == KIND_AUTHORIZATION, "TOPIC_AUTHORIZATION_FAILED")
    assert_true(kind_of(-150) == KIND_FATAL, "__FATAL")
    assert_true(kind_of(-144) == KIND_FATAL, "__FENCED")

    # Anything not called out is OTHER, including success.
    assert_true(kind_of(0) == KIND_OTHER, "NO_ERROR")
    assert_true(kind_of(22) == KIND_OTHER, "an uncategorised broker code")

    # Distinct kinds must not compare equal -- the whole point of the type.
    assert_true(KIND_QUEUE_FULL != KIND_TIMED_OUT, "kinds collapsed")
    print("    kinds classify; QUEUE_FULL prints as", String(KIND_QUEUE_FULL))


def test_delivery_reports_carry_a_branchable_kind() raises:
    """A rejection's kind is readable without matching on error text.

    Nothing listens on port 9, so every message times out. The report must
    say TIMED_OUT as a value, which is what makes a retry policy expressible.
    """
    var cfg = ProducerConfig(bootstrap_servers="127.0.0.1:9")
    cfg.set("message.timeout.ms", "800")
    cfg.set("log_level", "0")
    var p = Producer(cfg)
    for i in range(2):
        _ = p.produce(topic="nowhere", value="v" + String(i))
    try:
        p.flush(10000)
    except:
        pass

    var reports = p.take_failures()
    assert_equal(len(reports), 2)
    for report in reports:
        assert_true(
            report.kind() == KIND_TIMED_OUT,
            "expected TIMED_OUT, got " + String(report.kind()),
        )
        # The exact code is still there when it matters.
        assert_equal(report.error_code, Int32(-192))
    print("    both rejections classified as", String(reports[0].kind()))


def test_queue_full_is_branchable_backpressure() raises:
    """The case this whole item exists for.

    `confluent-kafka-python` raises `BufferError` for `__QUEUE_FULL` and
    nothing else, so a caller can drain and retry rather than parsing error
    text. Mojo cannot type an exception, so the kind is read back from the
    producer instead.

    A one-message local queue pointed at a dead broker fills on the second
    produce, since nothing is ever acknowledged to free a slot.
    """
    var cfg = ProducerConfig(bootstrap_servers="127.0.0.1:9")
    cfg.set("queue.buffering.max.messages", "1")
    cfg.set("message.timeout.ms", "30000")
    cfg.set("log_level", "0")
    var p = Producer(cfg)

    _ = p.produce(topic="nowhere", value="fills-the-queue")

    var refused = False
    for _ in range(10):
        try:
            _ = p.produce(topic="nowhere", value="one-too-many")
        except:
            refused = True
            break
    assert_true(refused, "a 1-message queue accepted more than one message")

    assert_true(
        p.last_error_kind() == KIND_QUEUE_FULL,
        "expected QUEUE_FULL, got " + String(p.last_error_kind()),
    )
    print("    backpressure surfaced as", String(p.last_error_kind()))
    _ = p.take_failures()


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

"""Smoke tests -- no broker required.

Run with `pixi run test`. These catch FFI breakage: if librdkafka cannot
be loaded, or a symbol has moved, the first test fails loudly rather than
at the first produce in someone's pipeline.
"""

from std.testing import assert_equal, assert_true

from kafka import ConsumerConfig, Producer, ProducerConfig, librdkafka_version


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
    for i in range(3):
        p.produce(topic="nowhere", key="k" + String(i), value="v" + String(i))

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


def main() raises:
    print("test_librdkafka_loadable")
    test_librdkafka_loadable()
    print("test_producer_config_defaults")
    test_producer_config_defaults()
    print("test_consumer_config_defaults")
    test_consumer_config_defaults()
    print("test_extra_keys_recorded")
    test_extra_keys_recorded()
    print("test_set_passes_keys_verbatim")
    test_set_passes_keys_verbatim()
    print("test_flush_reports_undelivered_messages")
    test_flush_reports_undelivered_messages()
    print("smoke tests passed")

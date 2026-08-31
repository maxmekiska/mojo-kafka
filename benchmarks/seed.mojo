"""Fill a topic for the consume benchmark.

    mojo run -I src benchmarks/seed.mojo <bootstrap> <topic> <count> <value_bytes>

Produces `count` records with an 8-byte key and a `value_bytes` payload, then
flushes. Idempotence is not attempted: the runner creates a fresh topic per
benchmark run, because a partially-seeded topic would silently change the
message count every timed pass reads.
"""

from std.sys import argv

from kafka import KIND_QUEUE_FULL, Producer, ProducerConfig


def main() raises:
    var args = argv()
    if len(args) != 5:
        raise Error("usage: seed <bootstrap> <topic> <count> <value_bytes>")
    var bootstrap = String(args[1])
    var topic = String(args[2])
    var count = Int(String(args[3]))
    var width = Int(String(args[4]))

    var payload = String("")
    for i in range(width):
        payload += String(i % 10)

    # linger a little and batch hard: seeding speed is not what is being
    # measured, and a slow seed makes the whole run tedious.
    var cfg = ProducerConfig(bootstrap_servers=bootstrap, linger_ms=50)
    cfg.set("batch.num.messages", "10000")
    cfg.set("log_level", "3")
    var producer = Producer(cfg)

    # Backpressure, handled the way this package documents it rather than by
    # polling on a guessed interval: `queue.buffering.max.messages` defaults
    # to 100k, so any seed larger than that fills the local queue and the
    # verdict comes back as `KIND_QUEUE_FULL`. Drain, then retry **the same
    # record** -- a `produce()` that raised did not enqueue it, so moving on
    # would seed the topic short and every timed pass would then read a
    # different count than the runner asserts on.
    for i in range(count):
        var placed = False
        while not placed:
            try:
                _ = producer.produce(
                    topic=topic, key="k" + String(i), value=payload
                )
                placed = True
            except e:
                if producer.last_error_kind() != KIND_QUEUE_FULL:
                    raise e
                _ = producer.poll(100)
    producer.flush(120000)
    print("SEEDED", count)

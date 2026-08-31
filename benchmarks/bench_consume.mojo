"""Time this package's two consume paths over one topic.

    mojo run -I src benchmarks/bench_consume.mojo \
        <bootstrap> <topic> <group> <poll|batch|borrowed> <batch_size> <prefetch_ms>

Prints one machine-readable line:

    RESULT mojo <mode> <messages> <nanoseconds>

**The run terminates on end-of-partition, not on a count.** Both this and the
`confluent-kafka` side set `enable.partition.eof`, so both stop at exactly the
same place for exactly the same reason -- a benchmark that stopped after *n*
messages would let a client that returned fewer look faster.

Only the drain is timed, and **`prefetch_ms` is what makes that meaningful**.
librdkafka fetches on its own background thread into a local queue, so a pause
before the clock starts lets the whole topic land in memory; the timed loop
then dequeues and decodes with no broker in it. Without that pause every
configuration measures the same fetch latency and comes out identical, which
is what the first version of this benchmark did -- four numbers within 15% of
each other and batching apparently *slower* than polling.

The queue sizes below are raised to match: they have to hold the whole topic
or the drain wanders back onto the network partway and the number becomes a
blend of two things again.
"""

from std.sys import argv
from std.time import perf_counter_ns, sleep

from kafka import Consumer, ConsumerConfig


def _config(bootstrap: String, group: String) raises -> ConsumerConfig:
    """The same knobs the Python side sets, spelled the same way.

    `enable_partition_eof` is what ends the run. The fetch sizes are raised
    together on both sides so neither client is measured against a different
    prefetch policy -- the difference under test is per-message work in the
    binding layer, not how much librdkafka was allowed to buffer.
    """
    var cfg = ConsumerConfig(
        bootstrap_servers=bootstrap,
        group_id=group,
        auto_offset_reset="earliest",
        enable_partition_eof=True,
    )
    cfg.set("enable.auto.commit", "false")
    cfg.set("fetch.max.bytes", "52428800")
    cfg.set("queued.max.messages.kbytes", "2097151")
    cfg.set("queued.min.messages", "10000000")
    cfg.set("fetch.wait.max.ms", "10")
    cfg.set("log_level", "3")
    return cfg^


def main() raises:
    var args = argv()
    if len(args) != 7:
        raise Error(
            "usage: bench_consume <bootstrap> <topic> <group> <mode> <batch>"
            " <prefetch_ms>"
        )
    var bootstrap = String(args[1])
    var topic = String(args[2])
    var group = String(args[3])
    var mode = String(args[4])
    var batch = Int(String(args[5]))
    var prefetch_ms = Int(String(args[6]))

    var consumer = Consumer(_config(bootstrap, group))
    consumer.subscribe([topic])

    # Warm up outside the clock: join the group and land the first fetch, so
    # the rebalance round trip is not charged to the per-message path.
    var seen = 0
    var warm = 0
    while warm < 200:
        warm += 1
        var event = consumer.poll_event(timeout_ms=1000)
        if event.message:
            seen += 1
            break
        if event.eof:
            break

    # Let the background fetcher pull the topic into the local queue, so the
    # timed loop below is dequeue-and-decode and nothing else.
    sleep(Float64(prefetch_ms) / 1000.0)

    var checksum = 0
    var started = perf_counter_ns()
    var done = False
    if mode == "poll":
        while not done:
            var event = consumer.poll_event(timeout_ms=10000)
            if event.message:
                ref payload = event.message.value().value
                if payload:
                    checksum += Int(payload.value()[0])
                seen += 1
            elif event.eof:
                done = True
            elif event.is_timeout():
                done = True
    elif mode == "borrowed":
        # The zero-copy path. Every record is summed *through the span*, in
        # librdkafka's own buffer -- no owned `Message`, no copy. Touching
        # the bytes is deliberate: a loop that only counted would let a
        # decoder that never read the payload look fastest.
        while not done:
            var records = consumer.consume_borrowed(batch, timeout_ms=10000)
            if records.reached_end() or len(records) == 0:
                done = True
            for i in range(len(records)):
                var record = records[i]
                var payload = record.value()
                if payload:
                    checksum += Int(payload.value()[0])
                seen += 1
            _ = records^
    elif mode == "batch":
        while not done:
            var events = consumer.consume_events(batch, timeout_ms=10000)
            if len(events) == 0:
                done = True
            for ref event in events:
                if event.message:
                    ref payload = event.message.value().value
                    if payload:
                        checksum += Int(payload.value()[0])
                    seen += 1
                elif event.eof:
                    done = True
    else:
        raise Error("mode must be 'poll', 'batch' or 'borrowed', got " + mode)
    var elapsed = perf_counter_ns() - started

    consumer.close()
    # Printed so the checksum cannot be optimised away, and so a run that
    # read no payloads is visible rather than merely fast.
    print("CHECKSUM", checksum)
    print("RESULT", "mojo", mode, seen, elapsed)

"""Time this package's consume paths over one topic.

    bench_consume <bootstrap> <topic> <group> <mode> <batch> <prefetch_ms>
                  <repeats>

Prints one line per repeat, the shape every peer prints:

    RESULT mojo <mode> <repeat> <messages> <nanoseconds> <stalls> <checksum>

Six modes, and each one exists to answer a different question:

    poll          poll_event(), one record per call
    poll-nohdr    poll_event(headers=False), the same without the crossing
    batch         consume_events(), the lossless batch API
    consume       consume(), the batch API most callers use
    consume-nohdr consume(headers=False), the same without the headers crossing
    borrowed      consume_borrowed(), zero copy

**Repeats happen inside one process, by seeking back to the start.** Every
repeat re-reads the same partition, so the JIT, the allocator, the page cache
and the group membership are all warm and identical across them -- and the
spread that is left is this machine's, not the harness's. An earlier version
ran one drain per process and the spread swamped every effect worth
measuring: a 2.8x swing between runs of one configuration, which is what
`CLAUDE.md` records as the reason not to trust the owned-path numbers it had.

**The drain polls with a zero timeout and counts the times it comes back
empty.** With the whole topic already in the local queue a zero-timeout call
never comes back empty before EOF, so a non-zero `stalls` means that repeat
went to the network mid-drain and is not measuring the decode path at all.
That is a *detector*, and it replaces the guesswork the prefetch pause used
to be: `run.py` discards a repeat that stalled instead of averaging it in.
Reporting a median over contaminated runs hides exactly the runs that need
throwing away.
"""

from std.sys import argv
from std.time import perf_counter_ns, sleep

from kafka import (
    Consumer,
    ConsumerConfig,
    OFFSET_BEGINNING,
    TopicPartition,
)


def _config(bootstrap: String, group: String) raises -> ConsumerConfig:
    """The knobs every peer sets, spelled the same way in all four.

    The fetch sizes are raised together across the peers so none of them is
    measured against a different prefetch policy -- the difference under
    test is per-record work in the binding, not how much librdkafka was
    allowed to buffer.
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


def _drain(
    mut c: Consumer, mode: String, batch: Int
) raises -> Tuple[Int, Int, Int]:
    """One full pass over the partition. Returns (seen, checksum, stalls).

    Every mode reads the first payload byte of every record into the
    checksum, so no mode can look fast by skipping the payload -- and the
    checksum is printed, so a run that read nothing is visible rather than
    merely quick.
    """
    var seen = 0
    var checksum = 0
    var stalls = 0
    var done = False

    while not done:
        if mode == "poll" or mode == "poll-nohdr":
            var event = c.poll_event(timeout_ms=0, headers=(mode == "poll"))
            if event.message:
                ref payload = event.message.value().value
                if payload:
                    checksum += Int(payload.value()[0])
                seen += 1
            elif event.eof:
                done = True
            else:
                stalls += 1
        elif mode == "batch":
            var events = c.consume_events(batch, timeout_ms=0)
            if len(events) == 0:
                stalls += 1
            for ref event in events:
                if event.message:
                    ref payload = event.message.value().value
                    if payload:
                        checksum += Int(payload.value()[0])
                    seen += 1
                elif event.eof:
                    done = True
        elif mode == "consume" or mode == "consume-nohdr":
            var msgs = c.consume(
                batch, timeout_ms=0, headers=(mode == "consume")
            )
            if len(msgs) == 0 and not c.reached_end():
                stalls += 1
            for ref m in msgs:
                if m.value:
                    checksum += Int(m.value.value()[0])
                seen += 1
            if c.reached_end():
                done = True
        elif mode == "borrowed":
            # Every record is summed *through the span*, in librdkafka's own
            # buffer -- no owned `Message`, no copy.
            var records = c.consume_borrowed(batch, timeout_ms=0)
            if len(records) == 0 and not records.reached_end():
                stalls += 1
            if records.reached_end():
                done = True
            for i in range(len(records)):
                var record = records[i]
                var payload = record.value()
                if payload:
                    checksum += Int(payload.value()[0])
                seen += 1
            _ = records^
        else:
            raise Error(
                "mode must be poll, poll-nohdr, batch, consume,"
                " consume-nohdr or borrowed, got "
                + mode
            )
        if stalls > 5000000:
            done = True
    return (seen, checksum, stalls)


def main() raises:
    var args = argv()
    if len(args) != 8:
        raise Error(
            "usage: bench_consume <bootstrap> <topic> <group> <mode> <batch>"
            " <prefetch_ms> <repeats>"
        )
    var bootstrap = String(args[1])
    var topic = String(args[2])
    var group = String(args[3])
    var mode = String(args[4])
    var batch = Int(String(args[5]))
    var prefetch_ms = Int(String(args[6]))
    var repeats = Int(String(args[7]))

    var consumer = Consumer(_config(bootstrap, group))
    # `assign`, not `subscribe`: the repeats below seek back to the start,
    # and an explicit assignment means no rebalance round trip between them.
    consumer.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])

    for r in range(repeats):
        consumer.seek([TopicPartition(topic, 0, 0)])
        # Let the background fetcher pull the topic into the local queue, so
        # the timed loop below is dequeue-and-decode and nothing else.
        sleep(Float64(prefetch_ms) / 1000.0)
        var started = perf_counter_ns()
        var got = _drain(consumer, mode, batch)
        var elapsed = perf_counter_ns() - started
        print(
            "RESULT",
            "mojo",
            mode,
            r,
            got[0],
            elapsed,
            got[2],
            got[1],
        )
    consumer.close()

"""The `confluent-kafka` half of the consume benchmark.

    python benchmarks/bench_consume.py <bootstrap> <topic> <group> \
        <poll|batch> <batch> <prefetch_ms> <repeats>

Prints the same line shape as every other peer:

    RESULT confluent <mode> <repeat> <messages> <nanoseconds> <stalls> <checksum>

Mirrors the Mojo peer call for call: same config keys, same assign-and-seek
replay, same zero-timeout drain with a stall counter, same first payload byte
summed into a checksum.

**One caveat this file is the right place to record.** `confluent-kafka` ships
a manylinux wheel with its *own* librdkafka bundled -- `ldd` on `cimpl...so`
names `librdkafka-c87086af.so.1` out of `confluent_kafka.libs`, not the
conda-forge library the Mojo, C and Rust peers all load. The version string
matches (2.15.0) but the build does not, so this peer alone is not strictly
comparing binding against binding; the other three are.
"""

import sys
import time

from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_BEGINNING


def config(bootstrap: str, group: str) -> dict:
    return {
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.partition.eof": True,
        "enable.auto.commit": False,
        "fetch.max.bytes": 52428800,
        "queued.max.messages.kbytes": 2097151,
        "queued.min.messages": 10000000,
        "fetch.wait.max.ms": 10,
        "log_level": 3,
    }


def is_eof(msg) -> bool:
    err = msg.error()
    return err is not None and err.code() == KafkaError._PARTITION_EOF


def drain(consumer, mode: str, batch: int):
    """One full pass. Returns (seen, checksum, stalls)."""
    seen = 0
    checksum = 0
    stalls = 0
    done = False
    while not done:
        if mode == "poll":
            msg = consumer.poll(0)
            if msg is None:
                stalls += 1
            elif is_eof(msg):
                done = True
            elif msg.error() is not None:
                raise RuntimeError(msg.error())
            else:
                payload = msg.value()
                if payload:
                    checksum += payload[0]
                seen += 1
        elif mode == "batch":
            msgs = consumer.consume(batch, timeout=0)
            if not msgs:
                stalls += 1
            for msg in msgs:
                if is_eof(msg):
                    done = True
                elif msg.error() is not None:
                    raise RuntimeError(msg.error())
                else:
                    payload = msg.value()
                    if payload:
                        checksum += payload[0]
                    seen += 1
        else:
            raise SystemExit(f"mode must be 'poll' or 'batch', got {mode!r}")
        if stalls > 5_000_000:
            done = True
    return seen, checksum, stalls


def main() -> None:
    if len(sys.argv) != 8:
        raise SystemExit(
            "usage: bench_consume.py <bootstrap> <topic> <group> <mode> "
            "<batch> <prefetch_ms> <repeats>"
        )
    bootstrap, topic, group, mode, batch, prefetch_ms, repeats = sys.argv[1:]
    batch, prefetch_ms, repeats = int(batch), int(prefetch_ms), int(repeats)

    consumer = Consumer(config(bootstrap, group))
    # assign, not subscribe -- the repeats seek back to the start and an
    # explicit assignment keeps a rebalance out of the loop.
    consumer.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])

    for r in range(repeats):
        consumer.seek(TopicPartition(topic, 0, 0))
        time.sleep(prefetch_ms / 1000.0)
        started = time.perf_counter_ns()
        seen, checksum, stalls = drain(consumer, mode, batch)
        elapsed = time.perf_counter_ns() - started
        print(
            f"RESULT confluent {mode} {r} {seen} {elapsed} {stalls} {checksum}",
            flush=True,
        )
    consumer.close()


if __name__ == "__main__":
    main()

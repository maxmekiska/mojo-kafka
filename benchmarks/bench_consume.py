"""The `confluent-kafka` half of the consume benchmark.

    python benchmarks/bench_consume.py \
        <bootstrap> <topic> <group> <poll|batch> <batch_size> <prefetch_ms>

Prints the same line shape as `bench_consume.mojo`:

    RESULT confluent <mode> <messages> <nanoseconds>

Deliberately mirrors the Mojo side call for call: same config keys, same
end-on-EOF rule, same warm-up outside the clock, same `prefetch_ms` pause so
the timed loop is dequeue-and-decode with no broker in it. Both clients wrap the *same*
librdkafka build, so anything this measures is the binding layer -- which is
the layer `mojo-kafka` actually is.
"""

import sys
import time

from confluent_kafka import Consumer, KafkaError


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


def main() -> None:
    if len(sys.argv) != 7:
        raise SystemExit(
            "usage: bench_consume.py <bootstrap> <topic> <group> <mode> "
            "<batch> <prefetch_ms>"
        )
    bootstrap, topic, group, mode, batch, prefetch_ms = sys.argv[1:]
    batch = int(batch)
    prefetch_ms = int(prefetch_ms)

    consumer = Consumer(config(bootstrap, group))
    consumer.subscribe([topic])

    # Warm up outside the clock, same as the Mojo side.
    seen = 0
    for _ in range(200):
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if is_eof(msg):
            break
        seen += 1
        break

    # Same pause as the Mojo side: fill the local queue before the clock.
    time.sleep(prefetch_ms / 1000.0)

    checksum = 0
    started = time.perf_counter_ns()
    done = False
    if mode == "poll":
        while not done:
            msg = consumer.poll(10.0)
            if msg is None:
                done = True
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
        while not done:
            msgs = consumer.consume(batch, timeout=10.0)
            if not msgs:
                done = True
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
    elapsed = time.perf_counter_ns() - started

    consumer.close()
    print("CHECKSUM", checksum)
    print("RESULT", "confluent", mode, seen, elapsed)


if __name__ == "__main__":
    main()

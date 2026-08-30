"""Interop peer backed by `confluent-kafka`, the reference Python client.

    python3 py_peer.py --client confluent --role produce \
        --bootstrap localhost:9092 --topic t --fixture f.tsv
    python3 py_peer.py --client confluent --role consume \
        --bootstrap localhost:9092 --topic t --group g --count 8

Speaks the same wire contract as the Mojo peers (see `../fixtures.py`), so any
peer can be swapped for any other on either side of a test.

`confluent-kafka` is the client this package's API is measured against, and it
is the independent end of every crossing cell here. It drives the same
librdkafka `_ffi.mojo` binds, through a binding layer that shares no code with
ours -- which is the layer every bug this suite has caught has lived in. What
it cannot check is librdkafka's own encoder, and that is deliberate: this
package reimplements none of it.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fixtures import (  # noqa: E402
    NULL_FIELD,
    from_field,
    from_headers,
    to_field,
    to_headers,
)

CLIENTS = ("confluent",)


def read_fixture(path):
    """Parse the fixture file into (key, value, headers) triples."""
    out = []
    for lineno, line in enumerate(Path(path).read_text("ascii").splitlines(), 1):
        if not line:
            continue
        fields = line.split("\t")
        if len(fields) != 3:
            raise SystemExit(f"malformed fixture line {lineno}: {line!r}")
        out.append(
            (from_field(fields[0]), from_field(fields[1]), from_headers(fields[2]))
        )
    return out


def emit(partition, offset, key, value, headers):
    """Write one message in the contract's `MSG` form.

    `headers` is normalised to a tuple of pairs: confluent-kafka hands back a
    list of tuples, or None when the record carried none.
    """
    print(
        f"MSG\t{partition}\t{offset}\t{to_field(key)}\t{to_field(value)}"
        f"\t{to_headers(tuple(headers or ()))}"
    )


# --------------------------------------------------------------------------
# confluent-kafka
# --------------------------------------------------------------------------


def confluent_produce(bootstrap, topic, messages):
    from confluent_kafka import Producer

    failures = []
    producer = Producer(
        {"bootstrap.servers": bootstrap, "linger.ms": 20, "acks": "all"}
    )

    def on_delivery(err, _msg):
        if err is not None:
            failures.append(str(err))

    for key, value, headers in messages:
        producer.produce(
            topic=topic,
            key=key,
            value=value,
            headers=list(headers),
            callback=on_delivery,
        )
    producer.flush(20)
    if failures:
        raise SystemExit(f"delivery failed: {failures[0]}")
    return len(messages)


def confluent_consume(bootstrap, topic, group, count):
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    seen = 0
    quiet = 0
    try:
        while seen < count and quiet < 25:
            msg = consumer.poll(1.0)
            if msg is None:
                quiet += 1
                continue
            if msg.error():
                raise SystemExit(f"consume error: {msg.error()}")
            quiet = 0
            emit(
                msg.partition(),
                msg.offset(),
                msg.key(),
                msg.value(),
                msg.headers(),
            )
            seen += 1
    finally:
        consumer.close()
    return seen


PRODUCERS = {"confluent": confluent_produce}
CONSUMERS = {"confluent": confluent_consume}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True, choices=CLIENTS)
    parser.add_argument("--role", required=True, choices=("produce", "consume"))
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--fixture", help="produce: the fixture file to send")
    parser.add_argument("--group", help="consume: consumer group id")
    parser.add_argument("--count", type=int, help="consume: messages to expect")
    args = parser.parse_args()

    if args.role == "produce":
        if not args.fixture:
            parser.error("--fixture is required for --role produce")
        sent = PRODUCERS[args.client](
            args.bootstrap, args.topic, read_fixture(args.fixture)
        )
        print(f"PRODUCED {sent}")
    else:
        if not args.group or args.count is None:
            parser.error("--group and --count are required for --role consume")
        seen = CONSUMERS[args.client](
            args.bootstrap, args.topic, args.group, args.count
        )
        print(f"CONSUMED {seen}")


if __name__ == "__main__":
    main()

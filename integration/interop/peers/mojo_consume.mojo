"""Interop peer: consume with this package's `Consumer` and report what it saw.

    mojo run -I src -I integration/interop/peers \
        integration/interop/peers/mojo_consume.mojo \
        <bootstrap> <topic> <group> <expected_count>

Emits one `MSG` line per message (see `../fixtures.py` for the contract) and a
final `CONSUMED <n>` line. Stops at `expected_count` messages or when polling
goes quiet, whichever comes first; the runner decides whether a short read is a
failure, so a timeout here is reported rather than raised.
"""

from std.sys import argv

from _hexwire import NULL_FIELD, headers_encode, hex_encode
from kafka import Consumer, ConsumerConfig


def _field(raw: Optional[List[UInt8]]) -> String:
    """One key or value on the wire: hex if present, `-` if it was null.

    The distinction is the point of the case set -- a tombstone's value and
    an empty-but-present key are told apart here and nowhere else.
    """
    if not raw:
        return String(NULL_FIELD)
    return hex_encode(Span(raw.value()))


def main() raises:
    var args = argv()
    if len(args) != 5:
        print("USAGE mojo_consume <bootstrap> <topic> <group> <count>")
        raise Error("expected 4 arguments, got " + String(len(args) - 1))

    var bootstrap = String(args[1])
    var topic = String(args[2])
    var group = String(args[3])
    var want = Int(String(args[4]))

    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id=group,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
    )
    consumer.subscribe([topic])

    var seen = 0
    # `poll` returns None both on timeout and at end-of-partition, so progress
    # is measured in consecutive empty polls rather than elapsed time.
    var quiet = 0
    while seen < want and quiet < 25:
        var maybe = consumer.poll(timeout_ms=1000)
        if not maybe:
            quiet += 1
            continue
        quiet = 0
        ref m = maybe.value()
        print(
            "MSG\t",
            String(m.partition),
            "\t",
            String(m.offset),
            "\t",
            _field(m.key),
            "\t",
            _field(m.value),
            "\t",
            headers_encode(m.headers),
            sep="",
        )
        seen += 1

    consumer.close()
    print("CONSUMED", seen)

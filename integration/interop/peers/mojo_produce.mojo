"""Interop peer: produce the fixture set with this package's `Producer`.

    mojo run -I src -I integration/interop/peers \
        integration/interop/peers/mojo_produce.mojo <bootstrap> <topic> <fixture>

Reads the fixture file described in `../fixtures.py` and writes every line to
`topic` in order. A `-` field is null and is produced as such: `produce_bytes`
takes `Optional[List[UInt8]]` in both halves, so a tombstone (null value) and
an empty-but-present key are both expressible. The third column carries the
record's headers, which have the same null-versus-empty rule one level down.
"""

from std.sys import argv

from _hexwire import NULL_FIELD, headers_decode, hex_decode
from kafka import Producer, ProducerConfig


def main() raises:
    var args = argv()
    if len(args) != 4:
        print("USAGE mojo_produce <bootstrap> <topic> <fixture>")
        raise Error("expected 3 arguments, got " + String(len(args) - 1))

    var bootstrap = String(args[1])
    var topic = String(args[2])
    var fixture_path = String(args[3])

    var body: String
    with open(fixture_path, "r") as fh:
        body = fh.read()

    # linger so the whole set batches, matching how a real producer behaves.
    var producer = Producer(
        ProducerConfig(bootstrap_servers=bootstrap, linger_ms=20)
    )

    var sent = 0
    for line in body.split("\n"):
        if line.byte_length() == 0:
            continue
        var fields = line.split("\t")
        if len(fields) != 3:
            raise Error("malformed fixture line: " + line)

        var key_field = String(fields[0])
        var value_field = String(fields[1])
        var headers = headers_decode(String(fields[2]))

        # `-` is null, and null is not the same as empty -- an empty hex
        # field decodes to a present, zero-length buffer.
        var value = Optional[List[UInt8]](None)
        if value_field != NULL_FIELD:
            value = hex_decode(value_field)
        var key = Optional[List[UInt8]](None)
        if key_field != NULL_FIELD:
            key = hex_decode(key_field)

        _ = producer.produce_bytes(
            topic=topic, value=value^, key=key^, headers=headers^
        )
        sent += 1

    producer.flush(20000)
    print("PRODUCED", sent)

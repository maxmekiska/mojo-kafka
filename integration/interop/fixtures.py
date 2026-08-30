"""The canonical message set, defined once and shared by every peer.

This module is the **single source of truth** for what gets produced and what
each consumer must see. Both the Mojo peers and the Python peers read the same
generated fixture file, so there is no mirrored constant to drift out of sync.

## Wire contract

A fixture file has one message per line:

    <key_hex>\t<value_hex>\t<headers>

Consumers emit one line per message on stdout, each prefixed so that librdkafka
log noise on the same stream can be filtered out:

    MSG\t<partition>\t<offset>\t<key_hex>\t<value_hex>\t<headers>

Hex is lowercase with no separators. A single `-` means the field is **null**,
which Kafka treats as distinct from an empty byte array -- the distinction that
tombstones depend on.

`<headers>` is `-` for a record with no headers, or a comma-separated list of
`<name_hex>:<value_hex>` pairs. It is a **list, not a map**: the order is part
of the contract and a name may repeat, so a client that stores headers in a
dict fails here rather than silently dropping one. A header value of `-` is
null, which Kafka distinguishes from an empty one exactly as it does for keys
and values. Names are hex-encoded like everything else, so a `,` or `:` in a
name cannot break the framing.

Everything is compared as bytes. Comparing decoded text would hide precisely the
transposition and encoding bugs this suite exists to catch.
"""

from dataclasses import dataclass

NULL_FIELD = "-"


# A record's headers: an ordered list of (name, value) pairs, where the value
# may be null. Not a mapping -- Kafka permits a name to repeat and keeps the
# order, and a dict would silently discard both.
Headers = tuple[tuple[str, bytes | None], ...]


@dataclass(frozen=True)
class Case:
    """One message, plus why it is in the set."""

    id: str
    key: bytes | None
    value: bytes | None
    why: str
    headers: Headers = ()


# Cases every client pair must handle today. A failure here is a real bug.
CORE: list[Case] = [
    Case("ascii", b"k-ascii", b"hello world", "the baseline"),
    Case(
        "unicode",
        "kéy-ünïcode".encode(),
        "héllo wörld ✓ 日本語".encode(),
        "multi-byte UTF-8 in both halves",
    ),
    Case(
        "emoji",
        "🔑".encode(),
        "🎉 payload with astral plane 𝕏".encode(),
        "4-byte UTF-8 sequences",
    ),
    Case(
        "control-chars",
        b"k-ctrl",
        b"line1\nline2\ttabbed\r\ntrailing",
        "newlines and tabs must survive the peer transport, not just Kafka",
    ),
    Case(
        "json",
        b"k-json",
        b'{"nested": {"a": 1}, "list": [2, 3], "quote": "\\""}',
        "quoting, the shape most real payloads take",
    ),
    Case("single-byte", b"k", b"z", "shortest non-empty key and value"),
    Case(
        "large",
        b"k-large",
        b"x" * 8192,
        "crosses the default batch and copy paths",
    ),
    Case(
        "binary-utf8-safe",
        bytes([0x01, 0x02, 0x03, 0x7F]),
        bytes(range(1, 128)),
        "high-bit-free binary -- the half of the range a String could hold",
    ),
    Case(
        "binary-non-utf8",
        b"k-bin",
        bytes([0x00, 0xFF, 0xFE, 0x80, 0x01, 0xC0, 0xC1]),
        "a NUL byte and invalid UTF-8; byte-exact through Mojo even when "
        "`Message` was a String, which is why that gap was one of type",
    ),
]

# Null and empty are different things in Kafka, and telling them apart is
# only meaningful one message at a time: a batch assertion lines up either
# way if a client conflates them consistently. So these run per case, per
# cell, which is also what makes `expected_failure` addressable per cell.
#
# Every one of them failed somewhere before `Message` carried optional bytes
# (CLAUDE.md, "Already built"); they are ordinary requirements now.
NULLABILITY: list[Case] = [
    Case(
        "tombstone",
        b"k-tombstone",
        None,
        "compaction tombstone: non-null key, null value",
    ),
    Case(
        "empty-key",
        b"",
        b"value-with-empty-but-present-key",
        "empty-but-present key, which Kafka distinguishes from a null key",
    ),
    Case(
        "null-key",
        None,
        b"value-with-null-key",
        "the other half of the pair: a genuinely absent key",
    ),
    Case(
        "empty-value",
        b"k-empty-value",
        b"",
        "empty-but-present value -- zero bytes, and not a tombstone",
    ),
]

# Headers, which are a list of pairs rather than a map. Every case here rides
# on an ordinary key and value, so a failure is attributable to the header
# path alone -- if the key or value also came back wrong, the bug is upstream
# of headers and CORE will have caught it too.
HEADERS: list[Case] = [
    Case(
        "one-header",
        b"k-hdr",
        b"v-hdr",
        "the baseline: a single text header",
        (("content-type", b"application/json"),),
    ),
    Case(
        "many-headers",
        b"k-many",
        b"v-many",
        "several headers keep their order",
        (("first", b"1"), ("second", b"2"), ("third", b"3")),
    ),
    Case(
        "duplicate-names",
        b"k-dup",
        b"v-dup",
        "Kafka permits a repeated header name; a dict-backed client drops one",
        (("trace", b"outer"), ("trace", b"inner"), ("span", b"s1")),
    ),
    Case(
        "binary-header-value",
        b"k-binhdr",
        b"v-binhdr",
        "header values are opaque bytes, not text -- NUL and invalid UTF-8",
        (("frame", bytes([0x00, 0xFF, 0xC0, 0xC1, 0x80])),),
    ),
    Case(
        "unicode-header",
        b"k-uni-hdr",
        b"v-uni-hdr",
        "multi-byte UTF-8 in a header name and value",
        (("ünïcode-名前", "héllo ✓ 日本語".encode()),),
    ),
    Case(
        "empty-header-value",
        b"k-empty-hdr",
        b"v-empty-hdr",
        "empty-but-present header value -- zero bytes, and not null",
        (("empty", b""),),
    ),
    Case(
        "tombstone-with-headers",
        b"k-hdr-tomb",
        None,
        "headers survive on a tombstone, whose value is null",
        (("reason", b"deleted"),),
    ),
    Case(
        "null-header-value",
        b"k-null-hdr",
        b"v-null-hdr",
        "a null header value, which Kafka distinguishes from an empty one",
        (("null-valued", None),),
    ),
]

ALL: list[Case] = CORE + NULLABILITY + HEADERS

BY_ID: dict[str, Case] = {c.id: c for c in ALL}


def to_headers(headers: Headers) -> str:
    """Encode a header list for the wire."""
    if not headers:
        return NULL_FIELD
    return ",".join(
        f"{name.encode().hex()}:{to_field(value)}" for name, value in headers
    )


def from_headers(field: str) -> Headers:
    """Decode a header list from the wire."""
    if field == NULL_FIELD:
        return ()
    out = []
    for pair in field.split(","):
        name_hex, _, value_hex = pair.partition(":")
        out.append((bytes.fromhex(name_hex).decode(), from_field(value_hex)))
    return tuple(out)


def unsupported_by_producer(case: Case, producer: str) -> str | None:
    """Why `producer` cannot even express `case`, or None if it can.

    This is **not** `expected_failure`, and the two must not be merged. That
    one names a cell where we produce or consume something wrongly and the run
    should report it. This one names a case a peer's own API cannot construct,
    so there is nothing about us to measure: the cell is skipped rather than
    failed or xfailed, because failing it would indict this package for a
    limitation that is not ours.

    No peer has such a limitation today -- `confluent-kafka` can express every
    case in `ALL`, null header values included. The hook stays for the same
    reason `expected_failure` does: the distinction is what is worth keeping,
    and it is the branch a future peer would need. It held one entry while
    `kafka-python-ng` was a peer, which asserts that a header value is bytes
    and so could not produce a null one.
    """
    return None


def to_field(raw: bytes | None) -> str:
    """Encode one key or value for the wire."""
    return NULL_FIELD if raw is None else raw.hex()


def from_field(field: str) -> bytes | None:
    """Decode one key or value from the wire."""
    return None if field == NULL_FIELD else bytes.fromhex(field)


def write_fixture(cases: list[Case], path) -> None:
    """Render `cases` to the fixture file the producer peers read."""
    with open(path, "w", encoding="ascii") as fh:
        for case in cases:
            fh.write(
                f"{to_field(case.key)}\t{to_field(case.value)}"
                f"\t{to_headers(case.headers)}\n"
            )


def expected(
    cases: list[Case],
) -> list[tuple[bytes | None, bytes | None, Headers]]:
    """The (key, value, headers) a consumer must report, in produce order."""
    return [(c.key, c.value, c.headers) for c in cases]


def expected_failure(case: Case, producer: str, consumer: str) -> str | None:
    """Why `case` must fail for this producer/consumer pair, or None if it passes.

    Nothing is expected to fail today: every case in `ALL` must round-trip in
    all four cells. This hook stays because the *shape* of the answer matters
    when the next gap turns up -- an xfail belongs to a cell, not to a case.

    It held two entries until `Message` moved to optional bytes, and how they
    differed is the reason for the per-cell signature:

        tombstone   failed wherever Mojo was on either side. `produce_bytes`
                    took `value: List[UInt8]` with no null path, so Mojo
                    could not write one; and `Message.value` was a `String`,
                    so a null read back from any producer arrived as `b""`.

        empty-key   failed only when Mojo *produced* and something else
                    consumed. `_enqueue` set the key pointer only when
                    `byte_length() > 0`, so an empty key went on the wire as
                    a null one. When Mojo also consumed, its own null/empty
                    conflation cancelled the bug out and the cell passed --
                    which is exactly why a same-client round trip could not
                    find this and a cross-client one could.

    Blanket-xfailing either would have buried the difference. Measure the
    cells before adding an entry here, and return the reason only for the
    ones that actually fail.
    """
    return None

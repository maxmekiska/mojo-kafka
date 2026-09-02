"""Cross-client interop: this package against `confluent-kafka`.

Every cell of the matrix produces the canonical fixture set with one client and
consumes it with another, then asserts the messages come back byte-identical and
in order.

    producer \\ consumer   mojo    confluent
    mojo                    .          X
    confluent               X          .

The cells marked `X` are the ones that can catch a bug in this package. The `.`
cells are controls: `mojo -> mojo` restates what `test_broker.mojo` covers, and
`confluent -> confluent` proves the harness, the fixture and the broker are
sound, so a failure in an `X` cell can be attributed to us rather than to the
scaffolding.

What an `X` cell can and cannot prove is worth stating plainly, because
`confluent-kafka` wraps librdkafka too (its own bundled build, not the one
`src/kafka/_ffi.mojo` opens -- irrelevant here, where the subject is wire
behaviour). It is
not an independent *protocol* implementation and cannot catch a bug in
librdkafka's encoder. It is an independent *binding* layer, and that is the
layer this package actually is -- every bug this suite has caught lived there,
in Mojo code above the C library, and a peer driving that library through
different bindings catches exactly those. The encoder underneath is out of
scope on purpose: this package reimplements none of the Kafka protocol, so a
bug in it would be a bug in librdkafka, reportable upstream rather than here.

Three groups run over the matrix: `fixtures.CORE` as one batch per cell,
and `fixtures.NULLABILITY` and `fixtures.HEADERS` one case at a time --
null-versus-empty is only visible per message, and a header case names in its
id exactly which property broke.

Run with:

    pixi run broker-up
    pixi run -e interop test-interop
"""

import pytest

import fixtures

CLIENTS = ("mojo", "confluent")

# Cells where a client talks to itself. Kept as controls, but a failure here
# indicts the harness or the broker rather than interop.
CONTROLS = {("confluent", "confluent"), ("mojo", "mojo")}


def skip_if_inexpressible(cases, producer):
    """Skip when the producing peer's own API cannot build one of `cases`.

    Distinct from an xfail: nothing about our client is being measured in that
    cell, so failing it would report a peer's limitation as our bug. No case
    is inexpressible today -- see `fixtures.unsupported_by_producer` for why
    the hook is still here.
    """
    for case in cases:
        reason = fixtures.unsupported_by_producer(case, producer)
        if reason:
            pytest.skip(f"{producer} cannot produce [{case.id}]: {reason}")


def run_round_trip(peers, topic, tmp_path, producer, consumer, cases):
    """Produce `cases` with one client, consume with another; return what came back."""
    skip_if_inexpressible(cases, producer)
    fixture_path = tmp_path / "fixture.tsv"
    fixtures.write_fixture(cases, fixture_path)

    sent = peers[producer].produce(topic, fixture_path)
    assert sent == len(cases), f"{producer} queued {sent} of {len(cases)}"

    return peers[consumer].consume(topic, f"{topic}-grp", len(cases))


def assert_round_trip(got, cases, producer, consumer):
    """Assert the consumer saw exactly `cases`, in order, byte for byte."""
    want = fixtures.expected(cases)
    assert len(got) == len(want), (
        f"{producer} -> {consumer}: expected {len(want)} messages, got {len(got)}"
    )

    for i, (case, (_partition, offset, key, value, headers)) in enumerate(
        zip(cases, got)
    ):
        # Single-partition topic, produced in order, consumed from earliest.
        assert offset == i, (
            f"{producer} -> {consumer} [{case.id}]: expected offset {i}, got {offset}"
        )
        # Key and value are asserted separately and both are checked -- a
        # combined assertion passes when the two are transposed, which is the
        # bug that shipped in v0.1.0.
        assert key == case.key, (
            f"{producer} -> {consumer} [{case.id}]: key mismatch\n"
            f"  expected {case.key!r}\n  got      {key!r}"
        )
        assert value == case.value, (
            f"{producer} -> {consumer} [{case.id}]: value mismatch ({case.why})\n"
            f"  expected {case.value!r}\n  got      {value!r}"
        )
        # Compared as an ordered sequence, not a set or a dict: order and
        # repeated names are both part of what Kafka guarantees, and a client
        # that keeps headers in a map passes any weaker comparison.
        assert headers == case.headers, (
            f"{producer} -> {consumer} [{case.id}]: header mismatch ({case.why})\n"
            f"  expected {case.headers!r}\n  got      {headers!r}"
        )


@pytest.mark.parametrize("consumer", CLIENTS)
@pytest.mark.parametrize("producer", CLIENTS)
def test_round_trip(peers, topic, tmp_path, producer, consumer):
    """The core set survives every producer/consumer pairing intact."""
    got = run_round_trip(
        peers, topic, tmp_path, producer, consumer, fixtures.CORE
    )
    assert_round_trip(got, fixtures.CORE, producer, consumer)


@pytest.mark.parametrize(
    "case", fixtures.NULLABILITY, ids=[c.id for c in fixtures.NULLABILITY]
)
@pytest.mark.parametrize("consumer", CLIENTS)
@pytest.mark.parametrize("producer", CLIENTS)
def test_nullability(peers, topic, tmp_path, producer, consumer, case, request):
    """Null and empty, one case per cell.

    Run singly rather than as a batch: a client that conflates null with empty
    conflates it consistently, so a batch still lines up and only a per-message
    comparison catches it. Both halves of every case are asserted, and `-` on
    the wire means null, so `b""` cannot satisfy an expectation of `None`.

    Any cell `fixtures.expected_failure` names is a **strict** xfail: if one
    starts passing, pytest reports XPASS and the run goes red, so a fix is
    reported rather than quietly absorbed. It names none today -- every cell
    here must pass. Until `Message` carried optional bytes there were seven,
    five for `tombstone` and two for `empty-key`.

    `confluent -> confluent` is never xfailed, which is what establishes that
    each case is legitimate Kafka rather than a bad fixture.
    """
    reason = fixtures.expected_failure(case, producer, consumer)
    if reason:
        request.applymarker(pytest.mark.xfail(strict=True, reason=reason))

    got = run_round_trip(peers, topic, tmp_path, producer, consumer, [case])
    assert_round_trip(got, [case], producer, consumer)


@pytest.mark.parametrize(
    "case", fixtures.HEADERS, ids=[c.id for c in fixtures.HEADERS]
)
@pytest.mark.parametrize("consumer", CLIENTS)
@pytest.mark.parametrize("producer", CLIENTS)
def test_headers(peers, topic, tmp_path, producer, consumer, case, request):
    """Record headers, one case per cell.

    Headers are the newest thing here and the easiest to get *symmetrically*
    wrong -- a produce side and a consume side that agree with each other and
    with nobody else round-trip perfectly through `mojo -> mojo`. That is the
    same shape as the `empty-key` bug, so the cells that matter are the two
    with `confluent` on exactly one side: it drives librdkafka's header API
    through bindings that share no code with ours.

    Run per case rather than batched so the failing id names the property --
    order, duplicate names, a null value -- instead of just "headers".
    """
    reason = fixtures.expected_failure(case, producer, consumer)
    if reason:
        request.applymarker(pytest.mark.xfail(strict=True, reason=reason))

    got = run_round_trip(peers, topic, tmp_path, producer, consumer, [case])
    assert_round_trip(got, [case], producer, consumer)


def test_mojo_consumer_distinguishes_null_and_empty_key(peers, topic, tmp_path):
    """Isolates the consume half of the null/empty distinction.

    `mojo -> mojo` cannot see this on its own. It passed the `empty-key` case
    even when both halves were broken, because the produce-side bug (an empty
    key written as null) and the consume-side one (a null key read as `b""`)
    cancelled out. So the producer here is deliberately an independent client:
    it writes one null key and one empty key, and Mojo has to tell them apart
    on the way back.

    This test used to assert the conflation -- `keys == [b"", b""]` -- as a pin
    on the documented gap. `Message.key` is `Optional[List[UInt8]]` now, so a
    null key comes back as `None` and an empty one as a present, zero-length
    field.
    """
    both = [
        fixtures.Case("null-key", None, b"v-null-key", "null key on the wire"),
        fixtures.Case("empty-key", b"", b"v-empty-key", "empty but present key"),
    ]
    got = run_round_trip(peers, topic, tmp_path, "confluent", "mojo", both)

    assert len(got) == 2, f"expected 2 messages, got {len(got)}"
    keys = [key for _partition, _offset, key, _value, _headers in got]
    assert keys == [None, b""], (
        "Mojo must report a null key as null and an empty key as empty; "
        f"got {keys!r}"
    )
    # The values separate the two rows, so a mismatch above is key-side.
    values = [value for _partition, _offset, _key, value, _headers in got]
    assert values == [b"v-null-key", b"v-empty-key"]


def test_mojo_consumer_distinguishes_null_and_empty_header_value(
    peers, topic, tmp_path
):
    """The same isolation, one level down, for header values.

    `mojo -> mojo` cannot establish this on its own for exactly the reason the
    key version cannot: a produce side that writes a null header value as empty
    and a consume side that reads null back as empty agree with each other. So
    the producer is `confluent`, which writes both variants independently, and
    Mojo has to tell them apart on the way back.
    """
    case = fixtures.Case(
        "header-null-vs-empty",
        b"k",
        b"v",
        "a null and an empty header value on one record",
        (("null-valued", None), ("empty-valued", b"")),
    )
    got = run_round_trip(peers, topic, tmp_path, "confluent", "mojo", [case])

    assert len(got) == 1, f"expected 1 message, got {len(got)}"
    headers = got[0][4]
    assert headers == (("null-valued", None), ("empty-valued", b"")), (
        "Mojo must report a null header value as null and an empty one as "
        f"empty; got {headers!r}"
    )


def test_matrix_is_fully_covered():
    """Guards the matrix itself.

    A parametrize list that silently loses an entry still reports success, so
    pin the expected shape: every ordered pair, with at least one cross-language
    cell in each direction.
    """
    pairs = {(p, c) for p in CLIENTS for c in CLIENTS}
    assert len(pairs) == 4
    crossing = {(p, c) for p, c in pairs if ("mojo" in (p, c)) and p != c}
    assert len(crossing) == 2, "expected 2 cells with mojo on exactly one side"
    assert CONTROLS < pairs


def test_header_cases_are_reachable_from_every_side():
    """Guards the header case set the way the matrix above is guarded.

    No producer may find a header case inexpressible today. If that stops being
    true silently, header coverage quietly stops crossing the language boundary
    in one direction and every remaining cell still reports green -- which is
    the failure mode this pin exists for, not the specific peer that once had
    such a limitation.
    """
    skipped = {
        (case.id, producer)
        for case in fixtures.HEADERS
        for producer in CLIENTS
        if fixtures.unsupported_by_producer(case, producer)
    }
    assert skipped == set(), (
        f"unexpected inexpressible header cases: {skipped!r}"
    )

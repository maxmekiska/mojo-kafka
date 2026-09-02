# Cross-client interop

Produces the same message set with one Kafka client and consumes it with
another, then asserts the messages come back byte-identical and in order.

```bash
pixi run broker-up
pixi run -e interop test-interop
pixi run broker-down
```

Needs a real broker. The mock is in-process and has no independent client on
the other end, so it cannot answer the question this suite asks.

## The peer

| Peer | Implementation |
|---|---|
| `mojo` | this package |
| `confluent` | `confluent-kafka`, the reference Python client |

`confluent-kafka` is the client this package's API is measured against, so it
is the right peer on grounds of relevance alone: if the two disagree about what
a message is, the one that is wrong is almost certainly us.

What it can and cannot prove is worth stating plainly, because it wraps the
librdkafka too. (Not the same *build*: its manylinux wheel bundles
`librdkafka-c87086af.so.1` rather than loading the conda-forge library
`_ffi.mojo` opens. That does not matter here, where the subject is wire
behaviour, but it does in `benchmarks/`.)

- It is **not** an independent protocol implementation, so it cannot catch a
  bug in librdkafka's encoder. Both sides would be wrong in the same way.
- It **is** an independent binding layer, and that is the layer this package
  actually is. Every bug this suite has caught lived in Mojo code above the C
  library, and a peer that drives that library through completely different
  bindings catches exactly those.

The encoder underneath is out of scope on purpose. This package reimplements
no part of the Kafka protocol — that is the whole design — so a wire-format
bug would be a bug in librdkafka, reportable upstream rather than fixable here.

## The matrix

```
producer \ consumer    mojo    confluent
mojo                     .          X
confluent                X          .
```

`X` marks the two cells that can catch a bug here. The `.` cells are controls,
and they earn their place: when an `X` cell fails, the controls are what tell
you the broker, the fixture and the harness are fine and the fault is ours.
`mojo -> mojo` additionally restates in this harness what `test_broker.mojo`
covers natively.

## The contract

`fixtures.py` is the single source of truth for the message set. It renders a
fixture file that every producer peer reads, so there is no message list
mirrored between Mojo and Python that could drift apart.

Message bytes cross every process boundary as lowercase hex:

```
fixture file    <key_hex>\t<value_hex>\t<headers>
consumer stdout MSG\t<partition>\t<offset>\t<key_hex>\t<value_hex>\t<headers>
```

A single `-` means **null**, which Kafka distinguishes from an empty byte
array.

`<headers>` is `-` for a record with no headers, or `<name_hex>:<value_hex>`
pairs joined by `,`. Names are hex-encoded like everything else, so a `,` or a
`:` inside a header name cannot break the framing, and a header value of `-`
is null. It is compared as an **ordered sequence**: order and repeated names
are both things Kafka guarantees, and any weaker comparison passes for a
client that keeps headers in a map. Hex rather than raw text for two reasons: the comparison is then over
bytes, which is what Kafka actually stores and what a transposition bug shows
up in; and payloads in the set contain tabs, newlines and NUL bytes that would
otherwise collide with the line-oriented transport.

Topics are created with `confluent-kafka`'s admin client rather than our own,
so the scaffolding is never the code under test.

## Null and empty

`fixtures.NULLABILITY` runs one case at a time rather than as a batch, because
that is the only way the distinction is visible: a client that conflates null
with empty conflates it consistently, so a batch assertion lines up either way.

| Case | What it pins |
|---|---|
| `tombstone` | non-null key, null value — the shape log compaction deletes on |
| `empty-key` | a key that is present and zero bytes, which is not a null key |
| `null-key` | the other half of that pair |
| `empty-value` | a value that is present and zero bytes, which is not a tombstone |

All four pass in all four cells. Until `Message` moved to optional bytes
(CLAUDE.md, "Already built") the first two did not: they carried
**strict xfails** between them, for `tombstone` and `empty-key`.

`empty-key` is the case this whole suite exists for. **`mojo -> mojo` passed
it even when both halves were broken** — the produce-side bug (an empty key
written as null) and the consume-side bug (a null key read as `b""`) cancelled
out exactly. No same-client round trip could see it; only an independent
client on one end could.
`test_mojo_consumer_distinguishes_null_and_empty_key` isolates the consume
half of that with an independent producer. It used to assert the conflation,
as a pin on the gap; it now asserts the two are told apart.

`fixtures.expected_failure` names no cell today. It stays because the *shape*
of the answer is the point: an xfail belongs to a cell, not to a case.
Blanket-xfailing `empty-key` on both sides would have buried the fact that
only the produce side was broken.

## Headers

`fixtures.HEADERS` runs one case per cell, like `NULLABILITY` and for a
related reason — the failing id then names the property that broke rather than
just "headers".

| Case | What it pins |
|---|---|
| `one-header` | the baseline: a single text header survives at all |
| `many-headers` | several headers keep their order |
| `duplicate-names` | a repeated name is legal Kafka; a dict-backed client drops one |
| `binary-header-value` | header values are opaque bytes — NUL and invalid UTF-8 |
| `unicode-header` | multi-byte UTF-8 in both the name and the value |
| `empty-header-value` | present and zero bytes, which is not a null value |
| `tombstone-with-headers` | headers survive on a record whose value is null |
| `null-header-value` | a null header value, which is not an empty one |

Headers are the newest thing on the wire here and the easiest to get
*symmetrically* wrong, which is exactly what `mojo -> mojo` cannot see. That
was measured, not assumed, and re-measured after `kafka-python-ng` was dropped
as a peer, since the argument had rested on it. Break the produce side (a null
header value written as empty) and the consume side (an empty one read back as
null) together and:

```
mojo -> mojo        null-header-value    PASS   <- the two bugs cancel
mojo -> confluent   null-header-value    FAIL   <- the produce half, caught
confluent -> mojo   empty-header-value   FAIL   <- the consume half, caught
```

Same shape as `empty-key`: the same-client cell is green and the crossing cells
are red. One independent peer on one end is what the argument needs, and it
does not have to be a second protocol implementation to supply it.
`test_mojo_consumer_distinguishes_null_and_empty_header_value` isolates the
consume half with an independent producer, mirroring the key-side test above.

### A peer limitation is not an xfail

`fixtures.unsupported_by_producer` is a separate hook from `expected_failure`,
and merging the two would lose something. It names a case a peer's own API
cannot **construct** — nothing about this client is being measured there, so
the cell is skipped rather than failed or xfailed, which would file a peer's
limitation as our bug.

It names nothing today: `confluent-kafka` can express every case in the set,
null header values included, and the suite runs with **no skips**. The hook
held one entry while `kafka-python-ng` was a peer, which asserts that a header
value is bytes and so could not produce a null one.

## One thing that was never a gap

Non-UTF-8 payloads — including embedded NUL bytes and invalid sequences like
`0xC0 0xC1` — round-tripped **byte-exact** through this package even while
`Message` was a `String`, in every cell. That was measured, not assumed, and
it is why `binary-non-utf8` sat in `CORE` throughout. The change to optional
bytes was a fix to the *type* of `Message`, not a repair of corrupted data.

## Adding a case

Add a `Case` to `CORE` in `fixtures.py`. Nothing else needs touching — both
producer peers read the generated fixture and both consumer peers report in
the same format, so a new case immediately runs in all four cells.

If it turns on the difference between null and empty, put it in `NULLABILITY`
instead: those run one case per cell, which is where that difference shows.
If it is about headers, put it in `HEADERS`, which runs the same way.

If a peer's own API cannot express it, add a branch to
`unsupported_by_producer` rather than to `expected_failure` — and keep
`test_header_cases_are_reachable_from_every_side` honest, which pins the set
of skipped cells so header coverage cannot quietly stop crossing the language
boundary.

If it exercises a gap this client has not closed yet, add a branch to
`expected_failure` naming the cells it fails in and why. Measure that; do not
guess it — and mark only the cells that actually fail.

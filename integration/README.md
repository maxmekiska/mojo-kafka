# Integration tests

Three suites. The first two run this client against a broker; the third runs it
against another client.

| | Broker | Docker | Where it runs |
|---|---|---|---|
| `test_mock.mojo` | librdkafka's in-process mock | no | CI and local, incl. macOS |
| `test_broker.mojo` | real `apache/kafka:3.7.0` | yes | local only |
| `interop/` | real `apache/kafka:3.7.0` | yes | local only |

**Docker is a local tool in this project.** CI runs only the mock suite, which
needs no daemon and therefore also runs on macOS. The two Docker-backed suites
are yours to run before pushing anything they cover — see the note under each
for what that is. Nothing gates them but you, so the mock suite is deliberately
carrying as much of the regression coverage as it can.

## Mock suite — the default

```bash
pixi run test-mock
```

librdkafka ships a mock broker that speaks the real wire protocol over a real
socket, so the produce and consume paths under test are the same ones used
against a real cluster. No broker process, no Docker, no port conflicts, and
about 30 seconds faster per run than waiting for a container to come up.

This is where the regression guards live:

- `test_round_trip_preserves_key_and_value` asserts on **both** halves of every
  message. A payload-only assertion passes even when key and value are
  transposed — the bug that shipped in `v0.1.0`.
- `test_list_topics_walks_every_entry` creates enough topics that a wrong
  metadata stride crashes instead of quietly returning junk.
- `test_null_and_empty_fields_are_distinct` walks the whole null/empty truth
  table and asserts on `key` / `value`, not the `*_text()` helpers — those
  collapse null onto their default, which is the conflation being guarded.
- `test_headers_round_trip_in_order_with_duplicates` writes one header name
  twice and asserts on position, so a `Dict`-backed implementation fails
  instead of quietly keeping one of them.
- `test_header_values_keep_null_and_empty_apart` applies the same null/empty
  rule one level down, to header values.

### Keeping the cluster alive

Mojo destroys a value after its **last use**, not at the end of the scope. A
`MockCluster` that is only touched during setup gets torn down before the first
`produce()`, and the symptom is indirect — clients log `1/1 brokers are down`
and `flush()` raises a timeout. Every test therefore ends with:

```mojo
_ = cluster^
```

## Broker suite — real wire, real admin

```bash
pixi run broker-up        # docker compose up -d --wait
pixi run test-broker
pixi run broker-down
```

Point it elsewhere with `MOJO_KAFKA_BOOTSTRAP=host:9092`.

This suite is not redundant with the mock. The mock **does not implement the
Topic Admin API**, so `AdminClient.create_topic()` can only be exercised
against a real broker — and that call was a segfault as recently as `v0.1.0`.
The broker suite also covers metadata propagation timing, which the mock
resolves instantly and a real cluster does not.

## Interop suite — against `confluent-kafka`

```bash
pixi run broker-up
pixi run -e interop test-interop
```

Produces with one client and consumes with the other, across this package and
`confluent-kafka`, asserting byte-identical round trips in every direction.

This answers a question neither suite above can. Both of those check us against
a broker using only our own code on both ends, so a bug that is symmetric —
one where produce and consume are wrong in matching ways — round-trips cleanly
and looks correct. The `empty-key` case is exactly that shape: it passes
`mojo -> mojo` and fails against any independent client. Record headers were
measured to behave the same way — break the produce and consume halves
together and `mojo -> mojo` stays green on `null-header-value` while the two
crossing cells fail.

`confluent-kafka` is the client this package's API is measured against. It
wraps the same librdkafka we bind, so it cannot catch a bug in librdkafka's
encoder — but it is an independent *binding* layer, which is the layer this
package is and where every bug this suite has caught has lived. See
`interop/README.md`.

Python and `confluent-kafka` live in a separate pixi environment, so the
default one stays just the compiler plus librdkafka.

# Integration tests

Two suites, covering the same client against two different brokers.

| | Broker | Docker | Runs on |
|---|---|---|---|
| `test_mock.mojo` | librdkafka's in-process mock | no | everywhere, incl. macOS |
| `test_broker.mojo` | real `apache/kafka:3.7.0` | yes | Linux CI, local |

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

<div align="center">

# mojo-kafka

**Apache Kafka client for [Mojo🔥](https://www.modular.com/mojo) — backed by [`librdkafka`](https://github.com/confluentinc/librdkafka).**

Stream Kafka straight into your Mojo / MAX inference loop. No Python hop, no GIL on the hot path.

[![CI](https://github.com/dvirarad/mojo-kafka/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/dvirarad/mojo-kafka/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/dvirarad/mojo-kafka?include_prereleases&sort=semver&label=release)](https://github.com/dvirarad/mojo-kafka/releases)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Mojo](https://img.shields.io/badge/Mojo-1.0-orange)](https://docs.modular.com/mojo/)
[![librdkafka](https://img.shields.io/badge/librdkafka-%E2%89%A52.3-red)](https://github.com/confluentinc/librdkafka)
[![Status](https://img.shields.io/badge/status-alpha-yellow)](#status)

</div>

---

## Why this exists

Mojo's pitch is *"Python ergonomics, systems performance, AI-native."* The place that pitch meets the real world is the **data pipeline** — and most ML pipelines today drink from Kafka.

If you want a Kafka topic feeding a Mojo model today, your options are:

1. **Hop through Python** with `confluent-kafka-python` — every message pays a Python ↔ Mojo FFI tax, plus you're back in GIL territory.
2. **Hand-roll `librdkafka` bindings** yourself — possible, but it's a lot of opaque pointers and struct offsets.

`mojo-kafka` is **option 3**: a Mojo-idiomatic, Pythonic API over the same `librdkafka` C foundation that every non-JVM Kafka client (Go, Rust, Python, Node, .NET) is already built on. Familiar shape, native perf, and no Python hop per message — a produce or a poll is a Mojo call straight into C. Every `librdkafka` symbol is resolved once when the client is built, so no message pays a `dlsym` for the privilege.

```mojo
from kafka import Consumer, ConsumerConfig

def main() raises:
    var c = Consumer(ConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="mojo-ml-trainer",
        auto_offset_reset="earliest",
    ))
    c.subscribe(["embeddings"])
    while True:
        var maybe = c.poll(timeout_ms=1000)
        if maybe:
            ref msg = maybe.value()
            if msg.value:                        # None is a tombstone
                run_inference(msg.value.value()) # bytes, straight into MAX
```

## Install

`mojo-kafka` depends on `librdkafka` at runtime. Recommended: let `pixi` handle everything.

```toml
# pixi.toml
[workspace]
channels = ["https://conda.modular.com/max", "conda-forge"]
platforms = ["linux-64", "osx-arm64"]

[dependencies]
mojo = "==1.0.0"
librdkafka = ">=2.3.0"
```

`mojo-kafka` is pure Mojo over a C library — it does not depend on MAX, so the
environment is just the compiler plus `librdkafka`. The library is loaded at
runtime by soname; set `MOJO_KAFKA_LIBRDKAFKA` to an explicit path if you need
to override the search.

Then add `mojo-kafka` as a Mojo dependency (vendor the package, or pull `src/kafka/` into your tree — it's small and dependency-free on the Mojo side):

```bash
git clone https://github.com/dvirarad/mojo-kafka.git
cp -r mojo-kafka/src/kafka your_project/src/
```

Prefer system packages? `librdkafka` is widely available:

```bash
brew install librdkafka                 # macOS
sudo apt install librdkafka-dev         # Debian / Ubuntu
sudo dnf install librdkafka-devel       # Fedora
```

## Quickstart

### Produce

```mojo
from kafka import Header, Producer, ProducerConfig

def main() raises:
    var p = Producer(ProducerConfig(bootstrap_servers="localhost:9092"))
    p.produce(topic="events", key="user-42", value="login")
    p.produce(topic="events", key="user-42", value=None)   # tombstone
    p.produce(
        topic="events",
        key="user-42",
        value="login",
        headers=[
            Header("content-type", "application/json"),
            Header("trace-id", "9f2c"),
        ],
        partition=0,          # optional; omit to let the partitioner choose
    )
    p.flush(timeout_ms=5000)
```

`key` and `value` are both `Optional`, exactly as they are in Kafka: `None`
writes a **null** field, `""` writes one that is present and empty, and the
broker treats those as different messages. `produce_bytes()` is the same call
over `Optional[List[UInt8]]` for payloads that are not text.

`headers` is a **list of pairs, not a map** — Kafka permits a repeated header
name and preserves the order, both of which a `Dict` would quietly drop. A
header value is `Optional` too, and follows the same null-versus-empty rule as
the key and the value.

### Consume

```mojo
from kafka import Consumer, ConsumerConfig

def main() raises:
    var c = Consumer(ConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="my-app",
        auto_offset_reset="earliest",
    ))
    c.subscribe(["events"])
    for _ in range(100):
        var maybe = c.poll(1000)
        if maybe:
            ref msg = maybe.value()
            print(msg.partition, msg.offset, msg.key_text(), msg.value_text())
    c.close()
```

`msg.key` and `msg.value` are `Optional[List[UInt8]]` — opaque bytes, and
`None` when the field was null on the wire. `key_text()` / `value_text()`
decode UTF-8 for the common case and take a `default` for the null one;
`msg.is_tombstone()` asks the question directly.

### Admin

```mojo
from kafka import AdminClient

def main() raises:
    var admin = AdminClient(bootstrap_servers="localhost:9092")
    admin.create_topic("events", num_partitions=3, replication_factor=1)
    for t in admin.list_topics():
        print(t)
```

See [`examples/`](examples/) for runnable scripts, including [`examples/ml_pipeline.mojo`](examples/ml_pipeline.mojo) — a streaming feature pipeline that reads events off Kafka and feeds them into a tensor.

## API surface

| Symbol | What it does |
|---|---|
| `Producer` / `ProducerConfig` | Produce messages, with optional `headers` and explicit `partition`; `produce_bytes()` for binary; `flush()` / `poll()` drain delivery reports and raise on rejection; `failures()` / `take_failures()` name which messages were rejected |
| `DeliveryReport` | One rejection: the `sequence` `produce()` returned, plus topic, partition, offset and error |
| `PARTITION_UNASSIGNED` | The `partition=` default — leaves the choice to the topic's partitioner |
| `Consumer` / `ConsumerConfig` | Subscribe, poll for messages, commit offsets, close |
| `AdminClient` | Create / list topics |
| `Message` | `topic`, `partition`, `offset`, `key`, `value` as `Optional[List[UInt8]]`, `headers` as `List[Header]`; `key_text()` / `value_text()` / `is_tombstone()` / `header()` / `header_text()` |
| `Header` | One record header: `name`, plus an `Optional` byte `value` and `value_text()` |
| `KafkaError` | Raised with `librdkafka` error code + human description; `kind()` for the branchable category |
| `KafkaErrorKind` | Eight tags — `KIND_QUEUE_FULL`, `KIND_TIMED_OUT`, … — for handling rather than reporting |
| `kafka.testing.MockCluster` | In-process broker for tests — no Docker |

The named fields are Mojo-idiomatic (`bootstrap_servers` → `bootstrap.servers`, `auto_offset_reset` → `auto.offset.reset`). Anything else the C client supports is reachable through `set()`, which takes the **librdkafka property name verbatim**:

```mojo
var cfg = ProducerConfig(bootstrap_servers="localhost:9092")
cfg.set("message.max.bytes", "1000000")
cfg.set("log_level", "3")          # librdkafka spells this one with an underscore
```

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  your Mojo / MAX code                               │
│                                                     │
│  from kafka import Producer, Consumer, AdminClient  │
└────────────────────┬────────────────────────────────┘
                     │  Pythonic Mojo API
┌────────────────────▼────────────────────────────────┐
│  src/kafka/{producer,consumer,admin,config}.mojo    │
│  typed structs, lifetime management, error mapping  │
└────────────────────┬────────────────────────────────┘
                     │  OwnedDLHandle.get_function[...]
┌────────────────────▼────────────────────────────────┐
│  src/kafka/_ffi.mojo                                │
│  raw librdkafka symbol declarations                 │
└────────────────────┬────────────────────────────────┘
                     │  C ABI
┌────────────────────▼────────────────────────────────┐
│  librdkafka.so / .dylib    (BSD-2-Clause, dynamic)  │
└─────────────────────────────────────────────────────┘
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the longer write-up on FFI lifetimes and the C handle story.

## Status

**Alpha, and honest about it.** `v0.2.0` targets **Mojo 1.0**. The FFI layer
loads real `librdkafka` symbols, and CI builds the package, builds every
example, and runs smoke plus full integration tests on Linux **and** macOS —
the integration suite uses librdkafka's in-process mock broker, so it needs no
Docker.

Two further suites run against a real `apache/kafka:3.7.0` in Docker: one for
the Topic Admin API, which the mock does not implement, and one that produces
with this client and consumes with `confluent-kafka` in both directions. Both
are **local** — Docker is a local tool in this project, not a CI dependency.
See [`integration/README.md`](integration/README.md).

Testing your own Kafka code is a supported use case:

```mojo
from kafka.testing import MockCluster

var cluster = MockCluster()
cluster.create_topic("events")
# point ProducerConfig / ConsumerConfig at cluster.bootstrap_servers()
_ = cluster^
```

What changed since `v0.1.0` is worth reading before you upgrade — `v0.1.0`
transposed the key and value of every message it produced, and both
`AdminClient` methods crashed. See the [CHANGELOG](CHANGELOG.md).

Known limitations today:

- No transactional producer, no manual partition assignment or `seek()`, and
  `Message` carries no `timestamp` yet.
- `poll()` returns `None` both on timeout and at end-of-partition, so a job
  that drains to the end of a partition cannot tell the two apart.
- `AdminClient` does create and list only — no delete, alter, configs,
  partitions, consumer groups or ACLs.
- Retiring the topic-handle cache removed the Mojo-side state that made
  `produce()` unsafe to share across threads, but the delivery-failure
  bookkeeping `poll()` and `flush()` maintain is still unsynchronised, so a
  `Producer` is not yet a thread-safe object end to end.
- Dropping a `Producer` that still holds undeliverable messages blocks for up
  to 5s while they time out, and swallows their failures. Call `flush()`
  first to see the verdict and to choose the wait.

Use it in spikes and prototypes today. Wait for `v1.0` before betting a
production pipeline.

## Roadmap

Ordered by **leverage, not parity**. `confluent-kafka` is the API shape we
honour and the peer the interop suite runs against, but matching it
feature-for-feature is not the goal — much of its surface is administrative work
people do from a CLI or Terraform.

- **Landed, unreleased** — `Message.key` / `.value` as optional bytes rather
  than `String`. This was previously filed at v0.5 as a zero-copy performance
  item; it is a **correctness** fix — without it tombstones could not be
  written and an empty-but-present key was unreachable — so it came first, and
  the performance win is incidental. Also: each `librdkafka` symbol is
  resolved once at load rather than per call. Also: producing goes through
  `rd_kafka_produceva`, which retired the per-topic handle cache — and with it
  the one thing keeping `Producer` from being thread-safe — and unblocked
  record **headers**, now carried on both sides, plus an explicit `partition`
  on `produce()`. Also: `produce()` returns a sequence token and
  `Producer.failures()` reports every rejection against it, so a message's
  verdict is addressable rather than a count plus the first failure string.
  Also: a typed `KafkaErrorKind`, so queue-full backpressure can be handled
  programmatically rather than by matching on error text.
- **v0.3 — consumer control plane.** `assign()`, `seek()`, `position()`,
  `committed()`, `pause()` / `resume()`, watermark offsets,
  `Message.timestamp`, and end-of-partition told apart from a poll timeout.
  Unblocks replay, lag measurement, event-time processing and bounded drains,
  none of which are reachable today.
- **v0.4 — batch `consume(n)`.** One FFI crossing for a whole batch instead of
  three per message. This is the item where Mojo beats a Python client, so it
  ranks higher here than its place in `confluent-kafka` would suggest; it ships
  with a benchmark.
- **v0.5 — transactions, for exactly-once.** `init_transactions` / `begin` /
  `send_offsets_to_transaction` / `commit` / `abort`. Gated on binding
  librdkafka's error predicates (`is_fatal` / `is_retriable` /
  `txn_requires_abort`), which a transactional caller must branch on.
- **v1.0** — API stable and production-ready. Not feature parity with
  `confluent-kafka-python`: deliberately no ACL / consumer-group / alter-config
  admin surface, and Schema Registry belongs in a second package.

Feature requests go in the [issue tracker](https://github.com/dvirarad/mojo-kafka/issues). Comment with a 👍 to vote.

## Contributing

We protect `main` — contributions land via PR with passing CI and a review from a maintainer. See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the full guide, but the short version is:

1. Fork & branch.
2. `pixi install`
3. Make your change; add a test in `tests/` if behavior changes.
4. `pixi run lint && pixi run test && pixi run test-mock` — no Docker needed
5. Open a PR. CI must be green.

Interesting layers if you're new:

- [`src/kafka/_ffi.mojo`](src/kafka/_ffi.mojo) — raw `librdkafka` symbol declarations, and the conventions that keep FFI safe.
- [`src/kafka/config.mojo`](src/kafka/config.mojo) — typed config builder over `rd_kafka_conf_t`.
- [`src/kafka/{producer,consumer,admin,header}.mojo`](src/kafka/) — public API.
- [`integration/interop/`](integration/interop/) — the suite that runs this client against `confluent-kafka` in both directions.

Security issues? See [`SECURITY.md`](SECURITY.md) — please **don't** open a public issue for a CVE-shaped thing.

## License

Apache 2.0 — see [`LICENSE`](LICENSE). `librdkafka` itself is BSD-2-Clause and is dynamically linked, not bundled or redistributed.

## Acknowledgments

- Confluent's [`librdkafka`](https://github.com/confluentinc/librdkafka) — the C client this whole project stands on.
- [`confluent-kafka-python`](https://github.com/confluentinc/confluent-kafka-python) — API shape we tried to honor.
- The Modular team, for [Mojo🔥](https://www.modular.com/mojo) and a C FFI that makes wrappers like this possible.

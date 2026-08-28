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

`mojo-kafka` is **option 3**: a Mojo-idiomatic, Pythonic API over the same `librdkafka` C foundation that every non-JVM Kafka client (Go, Rust, Python, Node, .NET) is already built on. Familiar shape, native perf, no FFI tax per message.

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
            var msg = maybe.value()
            run_inference(msg.value)    # straight into your Mojo / MAX model
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
from kafka import Producer, ProducerConfig

def main() raises:
    var p = Producer(ProducerConfig(bootstrap_servers="localhost:9092"))
    p.produce(topic="events", key="user-42", value="login")
    p.flush(timeout_ms=5000)
```

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
            var msg = maybe.value()
            print(msg.partition, msg.offset, msg.key, msg.value)
    c.close()
```

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

## API surface (v0.2)

| Symbol | What it does |
|---|---|
| `Producer` / `ProducerConfig` | Produce messages; `produce_bytes()` for binary; `flush()` / `poll()` drain delivery reports and raise on rejection |
| `Consumer` / `ConsumerConfig` | Subscribe, poll for messages, commit offsets, close |
| `AdminClient` | Create / list topics |
| `Message` | `topic`, `partition`, `offset`, `key`, `value` (headers still to come) |
| `KafkaError` | Raised with `librdkafka` error code + human description |
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
Docker. A separate Linux job covers the Topic Admin API against a real
`apache/kafka:3.7.0`.

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

- Producing binary works (`produce_bytes()`), but **consuming** it still hands
  back a `String`. Moving `Message` to a byte span is the biggest remaining API
  decision and is the next breaking change.
- `Message.key` / `.value` are `String` on the **consume** side. Bytes survive
  intact — read them with `.as_bytes()` — but the `String` is not valid UTF-8
  for a binary payload, so `codepoints()` and friends return silent nonsense.
  Produce binary with `produce_bytes()`; a byte-span `Message` is the next
  breaking change.
- No headers, no typed `KafkaErrorKind`, no transactional producer, no manual
  partition assignment or `seek()`.
- `Producer` is not safe to share across threads: `librdkafka` is, but the
  Mojo-side topic-handle cache is not.

Use it in spikes and prototypes today. Wait for `v1.0` before betting a
production pipeline.

## Roadmap

- **v0.3** — headers, typed `KafkaErrorKind`, async `consume()` generator.
- **v0.4** — Transactional producer, exactly-once semantics, Schema Registry helpers (Avro / Protobuf).
- **v0.5** — Tensor-zero-copy (`Message.value` as a byte span) so MAX tensors can wrap incoming bytes without a copy. This is also the fix for binary payloads, so it may land sooner.
- **v1.0** — API stable; production-ready feature parity with `confluent-kafka-python`.

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
- [`src/kafka/{producer,consumer,admin}.mojo`](src/kafka/) — public API.

Security issues? See [`SECURITY.md`](SECURITY.md) — please **don't** open a public issue for a CVE-shaped thing.

## License

Apache 2.0 — see [`LICENSE`](LICENSE). `librdkafka` itself is BSD-2-Clause and is dynamically linked, not bundled or redistributed.

## Acknowledgments

- Confluent's [`librdkafka`](https://github.com/confluentinc/librdkafka) — the C client this whole project stands on.
- [`confluent-kafka-python`](https://github.com/confluentinc/confluent-kafka-python) — API shape we tried to honor.
- The Modular team, for [Mojo🔥](https://www.modular.com/mojo) and a C FFI that makes wrappers like this possible.

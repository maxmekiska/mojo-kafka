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

`produce()` also takes `timestamp=` — the record's CreateTime in milliseconds
since the epoch, where **0 (the default) means now**, exactly as
`confluent-kafka` documents it. Set it when the event time is not the time
you are publishing.

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

### Replay, lag, and bounded drains

`subscribe()` hands partition assignment to the group. `assign()` takes it
back, which is what replay and lag measurement need:

```mojo
from kafka import (
    OFFSET_BEGINNING, Consumer, ConsumerConfig, TopicPartition,
)

def main() raises:
    var c = Consumer(ConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="my-app",
        enable_partition_eof=True,      # so EOF is not just a timeout
    ))
    var only: List[TopicPartition] = [
        TopicPartition("events", 0, OFFSET_BEGINNING)
    ]
    c.assign(only)

    while True:
        var event = c.poll_event(1000)
        if event.eof:
            break                       # caught up — this is a bounded drain
        if event.message:
            ref msg = event.message.value()
            print(msg.offset, msg.timestamp, msg.value_text())

    # Lag: how far behind the end of the partition this consumer is.
    var marks = c.query_watermark_offsets("events", 0)
    print("lag:", marks.high - c.position(only)[0].offset)

    c.seek([TopicPartition("events", 0, 4096)])   # replay from an offset
    c.close()
```

`enable_partition_eof` is off by default, matching librdkafka — without it
`poll_event()` reports a timeout where it would otherwise report EOF, and the
loop above never terminates. A tail-following job wants it off.

### Rebalance handlers

When the group moves partitions between members, `on_revoke` is the last
moment a member's offsets are still its own to commit, and `on_assign` is
the chance to start somewhere other than the group's commit:

```mojo
from kafka import Consumer, ConsumerConfig, Rebalance, TopicPartition

def commit_before_losing_them(event: Rebalance) raises:
    event.commit()

def start_from_my_store(event: Rebalance) raises:
    var start_at = List[TopicPartition]()
    for tp in event.partitions:
        start_at.append(TopicPartition(tp.topic, tp.partition, lookup(tp)))
    event.assign(start_at)

c.subscribe(["events"],
            on_assign=start_from_my_store,
            on_revoke=commit_before_losing_them)
```

Same signature as `confluent-kafka`, and the same rule: **a handler need not
assign anything.** Doing nothing gets the default assignment, so a handler
is an opportunity to intervene rather than an obligation to reimplement.

A handler must be a top-level `def`, not a closure — it is called from a C
callback, which carries no captured state, so everything it needs arrives on
the `Rebalance` it is given. `on_lost` takes over from `on_revoke` when
partitions were lost involuntarily; without one, lost assignments fall
through to `on_revoke`.

`position()` is local and immediate; `committed()` asks the broker what the
*group* has stored, which is a different number whenever anything has been
consumed since the last commit. `offsets_for_times()` maps a wall-clock
millisecond onto the first offset at or after it, for replaying from a point
in time rather than an offset.

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
| `Producer` / `ProducerConfig` | Produce messages, with optional `headers` and explicit `partition`; `produce_bytes()` for binary; `flush()` / `poll()` drain delivery reports and raise on rejection; `failures()` / `take_failures()` name which messages were rejected; `init_transactions()` / `begin_transaction()` / `commit_transaction()` / `abort_transaction()` / `send_offsets_to_transaction()` for exactly-once |
| `DeliveryReport` | One rejection: the `sequence` `produce()` returned, plus topic, partition, offset and error |
| `PARTITION_UNASSIGNED` | The `partition=` default — leaves the choice to the topic's partitioner |
| `Consumer` / `ConsumerConfig` | Subscribe, poll for messages, commit offsets, close; manual `assign()` / `unassign()`, `seek()`, `position()`, `committed()`, `pause()` / `resume()`, `query_watermark_offsets()` / `get_watermark_offsets()`, `offsets_for_times()`, `poll_event()`, and `consumer_group_metadata()` for exactly-once; `consume(n)` / `consume_events(n)` for batch reads; `consume_borrowed(n)` for zero-copy reads |
| `TopicPartition` | One partition at an offset — what the control plane speaks in; `has_error()` / `kind()` for the per-partition verdict |
| `OFFSET_BEGINNING` / `OFFSET_END` / `OFFSET_STORED` / `OFFSET_INVALID` | Offset sentinels, for `assign()` and `seek()` |
| `Watermarks` | A partition's `low` and `high` offsets; lag is `high - position` |
| `PollEvent` | What one `poll_event()` turned up: a `message`, an `eof` mark, or `is_timeout()` |
| `Rebalance` | Context handed to an `on_assign` / `on_revoke` / `on_lost` handler: `partitions`, `lost`, plus `assign()` / `unassign()` / `commit()` / `protocol()` |
| `AdminClient` | Create / list topics |
| `Message` | `topic`, `partition`, `offset`, `key`, `value` as `Optional[List[UInt8]]`, `headers` as `List[Header]`, `timestamp` + `timestamp_type`; `key_text()` / `value_text()` / `is_tombstone()` / `has_timestamp()` / `header()` / `header_text()` |
| `Header` | One record header: `name`, plus an `Optional` byte `value` and `value_text()` |
| `KafkaError` | An `librdkafka` error code + human description; `kind()` for the branchable category, `is_fatal` / `is_retriable` / `txn_requires_abort` for a transactional one |
| `KafkaErrorKind` | Eight tags — `KIND_QUEUE_FULL`, `KIND_TIMED_OUT`, … — for handling rather than reporting |
| `ConsumerGroupMetadata` | A consumer's group identity, from `Consumer.consumer_group_metadata()` — the bridge that lets a transaction commit that consumer's offsets |
| `TxnAction` | What a failed transactional call needs: `TXN_ABORT`, `TXN_RETRY` or `TXN_FATAL`, from `KafkaError.txn_action()` |
| `MessageBatch` / `BorrowedMessage` | A batch still owned by librdkafka, lending `Span`s into its buffer — zero copy, with the lifetime compiler-enforced |
| `kafka.testing.MockCluster` | In-process broker for tests — no Docker; `push_request_errors()` makes it answer chosen requests with chosen errors |

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

- No transactional producer, and no batch `consume(n)` — `poll()` is still one
  message per call.
- `AdminClient` does create and list only — no delete, alter, configs,
  partitions, consumer groups or ACLs.
- `Producer.last_error_kind()` is a single slot on the producer, so with more
  than one thread producing it cannot be attributed to a particular call.
  Branch on `DeliveryReport.kind()` instead, which names its message. The
  rest of `Producer` — and `Consumer` — is safe to share across threads.
- A shared `Consumer` is not a work-sharing primitive: two threads polling one
  consumer split a single assignment's records between them. Use one consumer
  per thread, or more members in the group.
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
  `rd_kafka_produceva`, which retired the per-topic handle cache and unblocked
  record **headers**, now carried on both sides, plus an explicit `partition`
  on `produce()`. Also: both clients are now **thread-safe** — the producer's
  sequence counter is atomic and its delivery-failure list is locked, and the
  consumer's `close()` is compare-exchanged, which closed a reachable
  deadlock. Both are verified by tests that drive eight real threads. Also: `produce()` returns a sequence token and
  `Producer.failures()` reports every rejection against it, so a message's
  verdict is addressable rather than a count plus the first failure string.
  Also: a typed `KafkaErrorKind`, so queue-full backpressure can be handled
  programmatically rather than by matching on error text. Also: the
  **consumer control plane** — `assign()`, `seek()`, `position()`,
  `committed()`, `pause()` / `resume()`, watermark offsets,
  `Message.timestamp` and `offsets_for_times()`, plus `poll_event()`, which
  tells end-of-partition apart from a poll timeout. That unblocks replay, lag
  measurement, event-time processing and bounded drains, none of which were
  reachable before. Also: **rebalance callbacks** —
  `subscribe(topics, on_assign=, on_revoke=, on_lost=)`, matching
  `confluent-kafka`'s signature — built on a real C function pointer via
  Mojo 1.0's `abi("C")` effect. Also: `produce(timestamp=)`, completing the
  event-time pair with `Message.timestamp`. Also: delivery reports moved from
  librdkafka's event queue to a `dr_msg_cb`, so both callback paths in the
  package now work the same way. Also: **transactions on the producer side** —
  `init_transactions()` / `begin_transaction()` / `commit_transaction()` /
  `abort_transaction()`, which return `Optional[KafkaError]` rather than
  raising, because Mojo 1.0's `Error` is text and would discard the fatal /
  retriable / abortable flags a transactional caller has to branch on.
  `KafkaError.txn_action()` reduces those to the three-way decision, in
  librdkafka's order — abort before fatal. Also: **batch and zero-copy
  consume** — `consume(n)` returns a run of records from one FFI crossing, and
  `consume_borrowed(n)` lends `Span`s straight into librdkafka's buffer with
  the lifetime compiler-enforced, measuring ~2x `confluent-kafka`'s
  `consume()` on identical records (see `benchmarks/`). Also:
  `send_offsets_to_transaction()` with `Consumer.consumer_group_metadata()`,
  which completes **read-process-write** exactly-once: the consumer's offsets
  commit inside the producer's transaction, so a failed transaction replays
  the input rather than skipping it.
- **v0.4** — open. Everything previously planned here has landed: batch
  `consume(n)`, transactions end to end, and a zero-copy `consume_borrowed(n)`
  that measures ~2x `confluent-kafka`'s `consume()` on the same records.
  Suggestions welcome in the issue tracker.
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

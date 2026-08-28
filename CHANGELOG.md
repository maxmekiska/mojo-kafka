# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Nothing yet.

## [0.2.0] — 2026-08-28

Targets **Mojo 1.0**. Fixes every defect that made `v0.1.0` unsafe to use —
transposed keys, two crashing `AdminClient` methods, unloaded symbols,
non-terminated C strings — and closes the two silent-success paths that
remained: a `flush()` that could not see undelivered messages, and a
`create_topic()` that ignored the broker's per-topic verdict. It also replaces
a CI pipeline that never compiled the code.

### Fixed
- **`flush()` reported success for messages that were never delivered.** No
  delivery-report handler was registered, so a message dropped at
  `message.timeout.ms` left the local queue empty and `rd_kafka_flush`
  returned `NO_ERROR` over the top of it — a total broker outage was
  indistinguishable from a clean write. The producer now enables
  `RD_KAFKA_EVENT_DR`, drains the reports in `poll()` / `flush()`, and
  `flush()` raises with the count and the first broker error. Mojo cannot
  hand C a function pointer, so this uses librdkafka's event sourcing rather
  than a `dr_msg_cb`.
- **`AdminClient.create_topic()` reported success for topics the broker
  refused.** `rd_kafka_event_error()` is the *request*-level error; a rejected
  topic returns `NO_ERROR` there and carries its real error per topic inside
  the result. Creating a topic with more replicas than brokers, or creating
  one that already exists, both returned cleanly while nothing was created.
  It now reads `rd_kafka_CreateTopics_result_topics()` and raises what the
  broker actually said.
- `Consumer.poll()` leaked the `rd_kafka_message_t` if decoding it raised.
- Buffers passed to C from `_c_string()` are now held across the call. Mojo
  frees a value after its last *use*, and the last use was the
  `unsafe_ptr()` inside the argument list.
- **Key and value were transposed on every produced message.** The
  `rd_kafka_vtype_t` constants had `KEY = 4` / `VALUE = 5`; librdkafka defines
  `VALUE = 4` / `KEY = 5`. Nothing errored — messages were accepted by the
  broker with the two halves swapped, and partitioning keyed on the payload.
- **`AdminClient.list_topics()` crashed.** It walked the metadata array with a
  24-byte stride; `sizeof(rd_kafka_metadata_topic_t)` is 32 on 64-bit. One
  topic worked by luck, two or three returned garbage, four or more
  segfaulted.
- **`AdminClient.create_topic()` crashed.** It passed NULL as the
  `rd_kafka_CreateTopics` result queue, which librdkafka dereferences. It now
  creates a real queue, waits on it, and raises whatever the broker reports
  instead of assuming success.
- **Nothing loaded `librdkafka`.** Every call went through bare
  `external_call`, whose symbols are resolved when the JIT materialises the
  program — under `mojo run` the library is not loaded yet and every symbol is
  missing. The package now loads it with `OwnedDLHandle`, which works under
  both `mojo run` and `mojo build`.
- **Strings passed to C were not NUL-terminated.** `String.unsafe_ptr()` does
  not guarantee a terminator in Mojo 1.0, and whether the next byte is zero
  depends on allocator reuse — roughly a quarter of short strings in a test
  run overran. Topic names, broker lists and config values could all pick up
  trailing garbage intermittently. Everything crossing to C now goes through
  an explicit NUL-terminated copy.
- `rd_kafka_conf_t` is no longer leaked when client construction fails.
- `produce()` no longer calls the variadic `rd_kafka_producev` through a fixed
  prototype (undefined on SysV and AAPCS). It binds the non-variadic
  `rd_kafka_produce` and caches `rd_kafka_topic_t` handles per topic name.
- `Message.topic` is now populated instead of always being empty.
- `poll()` no longer surfaces `PARTITION_EOF` as an ordinary empty message.
- Payload lengths use byte counts rather than character counts, so non-ASCII
  values are no longer truncated.

### Changed
- **`ProducerConfig.set()` / `ConsumerConfig.set()` take librdkafka property
  names verbatim.** They used to rewrite `_` to `.`, which made librdkafka's
  real `log_level` property unreachable as an invalid `log.level`. Callers
  passing `set("message_max_bytes", …)` must now pass
  `set("message.max.bytes", …)`. The named config fields are unaffected.
- `Producer.poll()` and `Producer.flush()` now take `mut self`.
- `librdkafka` is pinned to `>=2.3.0,<3`: this package hardcodes struct
  offsets and a metadata stride, so a major version is an ABI risk.
- Ported to Mojo 1.0: `fn` → `def`, `alias` → `comptime`, `@value` →
  `@fieldwise_init`, `__del__` → `__deinit__`, `sys.ffi` → `std.ffi`, and the
  rest of the 1.0 migration.
- Dropped the `max` dependency — the package is pure Mojo over a C library.
  The environment is now just `mojo` + `librdkafka`.
- `mojo package` → `mojo precompile`; artifacts are `.mojoc`, not `.mojopkg`.
- `MOJO_KAFKA_LIBRDKAFKA` overrides the library search path.

### Added
- `Producer.produce_bytes()` — produce an arbitrary byte payload. `produce()`
  takes a `String`, which cannot hold non-UTF-8 bytes, so Avro / Protobuf /
  compressed frames were previously impossible to write.
- `Producer.delivery_failures()` — rejections tallied since the last `flush()`.
- Regression tests for all of the above, each of which fails against the
  previous behaviour: `test_flush_reports_undelivered_messages` and
  `test_set_passes_keys_verbatim` (smoke),
  `test_binary_payload_round_trips` (mock),
  `test_create_topic_reports_rejection` (real broker).
- `Consumer.commit()`.
- `kafka.testing.MockCluster` — an in-process Kafka backed by librdkafka's
  mock broker, for testing Kafka code without Docker.
- `integration/` — both integration suites plus a compose file for the real
  broker. `test_mock.mojo` runs anywhere; `test_broker.mojo` covers the Topic
  Admin API, which the mock does not implement.
- CI now builds the package, builds every example, and runs smoke plus full
  integration tests on Linux **and** macOS via the mock broker, with a
  separate Linux job against a real `apache/kafka:3.7.0`.

### Known limitations
- **Consuming** binary is lossy in type, not in bytes. `Message.key` /
  `.value` are `String`: the bytes survive and `.as_bytes()` returns them
  intact, but the `String` is not valid UTF-8 for a binary payload, so
  `codepoints()` yields silent nonsense. Producing binary is covered by
  `produce_bytes()`. Moving `Message` to a byte span is the next breaking
  change.
- No headers, no typed `KafkaErrorKind`, no transactional producer, no manual
  partition assignment or `seek()`.
- `Producer` is not safe to share across threads: `librdkafka` is, but the
  Mojo-side topic-handle cache is not.

## [0.1.0] — 2026-05-12

Initial public alpha.

### Added
- `Producer` / `ProducerConfig` — typed producer over `rd_kafka_t` with `flush()` and `poll()`.
- `Consumer` / `ConsumerConfig` — `subscribe()` / `poll()` / `close()`.
- `AdminClient` — `create_topic()` / `list_topics()`.
- `Message` carrying `partition`, `offset`, `key`, `value`.
- `KafkaError` wrapping `rd_kafka_resp_err_t` with the human description from `rd_kafka_err2str`.
- Examples: `producer_basic.mojo`, `consumer_basic.mojo`, `ml_pipeline.mojo`.
- CI: format check, smoke tests on Linux + macOS, integration test against `apache/kafka:3.7.0`.
- Release workflow: builds `.mojopkg` on tag push and attaches it to the GitHub Release.
- Project hygiene: `LICENSE` (Apache-2.0), `SECURITY.md`, `CONTRIBUTING.md`, `CODEOWNERS`, issue / PR templates, Dependabot for GitHub Actions, CodeQL scanning.

### Known limitations
- `Message.topic` not yet exposed (#1).
- Headers not yet supported (#2).
- No typed `KafkaErrorKind` enum — error codes are raw `Int32` (#3).
- Transactional producer not implemented (#4).

> **Do not use `v0.1.0`.** It transposes the key and value of every message it
> produces, and both `AdminClient` methods crash. See `0.2.0` above.

[Unreleased]: https://github.com/dvirarad/mojo-kafka/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/dvirarad/mojo-kafka/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/dvirarad/mojo-kafka/releases/tag/v0.1.0

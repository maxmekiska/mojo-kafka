# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **Delivery reports now go through a `dr_msg_cb`** rather than librdkafka's
  event queue. No API change — `failures()`, `take_failures()` and
  `flush()`'s raise-on-rejection behave exactly as before — but the producer
  and the consumer's rebalance handling now use the same mechanism, and
  `flush()` is `rd_kafka_flush()` instead of a hand-written drain loop.

  The event queue was never the preferred design; it was a workaround for
  pre-1.0 Mojo being unable to hand C a function pointer. Measured A/B over
  200k messages, throughput is indistinguishable — the run-to-run spread is
  far wider than the difference between the two paths.

### Added
- **Rebalance callbacks.** `subscribe()` now takes `on_assign`, `on_revoke`
  and `on_lost`, matching `confluent-kafka`'s signature and its semantics: a
  handler is an *opportunity* to intervene, not an obligation — one that does
  nothing still gets the default assignment.

  `on_assign` can start the consumer at offsets from your own store rather
  than the group's commit; `on_revoke` can commit synchronously before the
  partitions belong to another member, which is the last moment that is
  possible. `on_lost` takes over from `on_revoke` when partitions were lost
  involuntarily, falling back to `on_revoke` when it is not set.

  Handlers receive a `Rebalance` context carrying `partitions` plus
  `assign()`, `unassign()`, `commit()` and `protocol()`. A handler must be a
  top-level `def` rather than a closure: it is called from a C callback,
  which carries no captured state.

  This is built on a genuine C function pointer — Mojo 1.0's `abi("C")`
  effect — not an event-queue workaround. Earlier notes in this repo claimed
  Mojo could not supply one; that was true before 1.0 and is not now.

- **`produce(timestamp=)` / `produce_bytes(timestamp=)`.** The record's
  CreateTime in milliseconds since the Unix epoch, completing the pair with
  `Message.timestamp`. **0 means now**, which is librdkafka's own rule and
  the default `confluent-kafka` documents for the same argument. Set it when
  the event time is not the time you are publishing — replaying an archive,
  or forwarding records from another system.

- **Consumer control plane.** `Consumer` can now name its own partitions and
  move around inside them: `assign()` / `unassign()`, `seek()`, `position()`,
  `committed()`, `pause()` / `resume()`, `query_watermark_offsets()` /
  `get_watermark_offsets()` and `offsets_for_times()`. Four workloads that
  were unreachable become reachable: replay from an offset, lag measurement
  (`watermarks.high - position`), event-time processing, and bounded drains.

  The calls speak in `TopicPartition`, a new type mapping onto librdkafka's
  `rd_kafka_topic_partition_t`. Its `offset` field carries a question in and
  an answer out — most obviously in `offsets_for_times()`, where a millisecond
  timestamp goes in and an offset comes back. `OFFSET_BEGINNING`,
  `OFFSET_END`, `OFFSET_STORED` and `OFFSET_INVALID` are exported for it.

  Several of these calls report **per partition** rather than through their
  return code — the same shape that made `AdminClient.create_topic()` report
  success for a topic the broker refused. The ones that return a list
  (`position`, `committed`, `offsets_for_times`) leave the per-partition
  verdict on each entry, readable with `TopicPartition.has_error()` and
  `.kind()`; the ones that return nothing (`seek`, `pause`, `resume`) raise on
  the first failed partition, because there would be nowhere else to put it.

- **End-of-partition told apart from a poll timeout.** `poll()` returns `None`
  for both, so a job draining a partition to its end could not tell "caught
  up" from "nothing arrived yet". `poll_event()` returns a `PollEvent` that
  keeps the three cases apart — `message`, `eof`, or `is_timeout()` — and
  `poll()` is now written in terms of it.

  End-of-partition is opt-in, matching librdkafka: build the consumer with
  `ConsumerConfig(..., enable_partition_eof=True)`. Without it `poll_event()`
  reports a timeout where it would otherwise report EOF. A tail-following job
  wants it off; a bounded drain needs it on.

- **`Message.timestamp` and `Message.timestamp_type`.** Milliseconds since the
  epoch, plus which clock stamped it — `TIMESTAMP_CREATE_TIME` (the producer)
  or `TIMESTAMP_LOG_APPEND_TIME` (the broker). Read `has_timestamp()` rather
  than testing `timestamp != -1`: -1 is a legal `int64` millisecond value, so
  the type is the only field that answers whether there is a timestamp at all.

  This is the **consume** half only. `produce()` still cannot set a timestamp;
  librdkafka stamps each record with the current time.

- **Typed `KafkaErrorKind`.** `kind_of(code)` classifies a librdkafka error
  into one of eight branchable tags, read from `KafkaError.kind()`,
  `DeliveryReport.kind()` and `Producer.last_error_kind()`. Backpressure is
  the case it exists for: a `produce()` refused because the local queue is
  full now reports `KIND_QUEUE_FULL`, so a caller can drain and retry instead
  of matching on error text.

  The kind is exposed on **values** rather than on raised exceptions because
  Mojo 1.0's `Error` carries only text — `except` has no type to match on.
  That is why the producer records the kind of the rejection it just raised.

  The set is deliberately small, and several codes collapse onto one tag: a
  caller retrying a transient failure does not care whether librdkafka said
  `__TRANSPORT` or `__ALL_BROKERS_DOWN`. The exact value stays on
  `KafkaError.code` and `DeliveryReport.error_code`.

- **Per-message delivery reports.** `produce()` and `produce_bytes()` return a
  sequence token, carried to librdkafka as `RD_KAFKA_VTYPE_OPAQUE` and read
  back from the delivery report, so a rejection is addressable instead of
  being one entry in a tally. `Producer.failures()` returns a `DeliveryReport`
  per rejection — sequence, topic, partition, offset, error code and text —
  and `take_failures()` acknowledges them.

  This needed no C function pointer, despite the design note that said
  otherwise: librdkafka stores the opaque without ever dereferencing it, so a
  plain integer token is a legitimate `void *`.

  Only failures are retained. A report per delivered message would grow
  without bound in a long-running producer that never reads them, and after a
  `flush()` that does not raise, everything produced before it was delivered.

- **Record headers, on both sides.** `produce()` / `produce_bytes()` take
  `headers=[Header(...), ...]` and `Message.headers` carries what came back,
  with `Message.header(name)` / `header_text(name)` for the common lookup.

  Headers are a **list of pairs, not a `Dict`**. Kafka permits the same header
  name more than once and preserves the order they were written in, and a map
  drops both properties silently — which is how conventions like tracing
  baggage are carried.

  A header value is `Optional[List[UInt8]]`, for the same reason
  `Message.value` is: librdkafka reads its presence from the **pointer**, so a
  null header value and a present-but-empty one are different records. The
  produce side reuses the same `_Field` encoder as keys and payloads and the
  consume side the same `copy_bytes`, so that rule is expressed once.

- **`produce(..., partition=)`**, defaulting to `PARTITION_UNASSIGNED`, which
  leaves the choice to the topic's partitioner as before. Message timestamps
  were deliberately left out: `Message` has no `timestamp` field yet, so a
  produce-side timestamp could not be verified end to end.

- Mock suite: `test_headers_round_trip_in_order_with_duplicates` writes the
  same header name twice and asserts on position, so a map-backed
  implementation fails rather than silently keeping one;
  `test_header_values_keep_null_and_empty_apart` walks the null / empty /
  present truth table for header values, asserting on `value` rather than
  `value_text()`; and `test_explicit_partition_is_honoured` writes one key to
  three partitions, which only lands spread if `partition=` is respected.
- Interop: `fixtures.HEADERS` adds eight cases — order, duplicate names,
  binary and unicode values, empty and null values, and headers on a
  tombstone — run one per cell across every pairing. The wire contract
  grew a third field for them; see `integration/interop/README.md`.

  These are not ceremony. Breaking the produce side (a null header value
  written as empty) and the consume side (an empty one read back as null)
  *together* leaves `mojo -> mojo` green on `null-header-value` while
  `mojo -> confluent` fails on it and `confluent -> mojo` fails on
  `empty-header-value` — the same shape as the `empty-key` bug, measured
  rather than assumed.
- Interop: `fixtures.unsupported_by_producer`, which is **not**
  `expected_failure`. It names a case a peer's own API cannot construct, and
  those cells are skipped rather than failed, so a peer's limitation is never
  filed as our bug. It names nothing today — `confluent-kafka` can express
  every case in the set — and the hook is kept for the distinction it draws.
- `Message.key_text()` / `Message.value_text()` — decode a field as UTF-8,
  with a `default` for the null case (`""` unless given). `Message.is_tombstone()`.
- `_ffi.copy_bytes()` — copies a length-delimited C buffer into
  `Optional[List[UInt8]]`, returning `None` for a NULL pointer.
- `test_null_and_empty_fields_are_distinct` (mock) — the full truth table:
  tombstone, empty key, null key, empty value. It asserts on the raw optional
  fields, not the text helpers, because the helpers collapse null onto their
  default.
- Interop: `fixtures.NULLABILITY` replaces `fixtures.GAPS`, adding `null-key`
  and `empty-value` alongside `tombstone` and `empty-key`, and every one of
  them now passes in every client pairing. The suite carried strict xfails
  for this item and carries none.
  `test_mojo_consumer_conflates_null_and_empty_key`, which pinned the
  conflation, is now `test_mojo_consumer_distinguishes_null_and_empty_key`
  and asserts the two are told apart.

### Changed
- **`KafkaError` and `DeliveryReport` are `Writable`.** `String(err)` and
  `print(report)` work directly, the way they do for any other Mojo value.
  Their bespoke `describe()` methods are gone — that was the pre-1.0
  `Stringable` idiom under a different name.

- **`flush()` no longer discards rejection reports when it raises.** It keeps
  raising while any rejection is unacknowledged, and `take_failures()` is how
  a caller acknowledges them. Previously the reports were cleared as the error
  was raised, leaving nothing to inspect.

- **Producing goes through `rd_kafka_produceva` rather than
  `rd_kafka_produce`.** Not `rd_kafka_producev` — that one is variadic, and
  calling a C variadic through a fixed prototype is undefined on the SysV and
  AAPCS ABIs. `produceva` takes an **array** of `rd_kafka_vu_t` instead and is
  an ordinary fixed-arity call, which is what makes headers reachable at all:
  `rd_kafka_produce` has nowhere to attach them.

  It also names the topic **by string**, which retired the per-topic
  `rd_kafka_topic_t` cache on `Producer`. That unsynchronised `Dict` was the
  headline reason a `Producer` could not be shared across threads when
  librdkafka's own handle can be; the delivery-failure counters are still
  unsynchronised, so this narrows the gap rather than closing it.
  `rd_kafka_topic_new` / `rd_kafka_topic_destroy` are no longer bound.

  `produceva` reports failure as a `rd_kafka_error_t*` that is **NULL on
  success** — the opposite polarity to the handle-returning calls elsewhere —
  and the object is caller-owned, so `Lib.take_error` reads and destroys it.
  Error messages are now `rd_kafka_error_string`'s, which says what actually
  went wrong, rather than `err2str`'s category name.

  No behaviour changed for existing calls: the whole prior suite, including
  the null/empty truth table, passes unmodified.

- **BREAKING: `Message.key` and `Message.value` are `Optional[List[UInt8]]`,
  and `Producer.produce()` / `produce_bytes()` take `Optional` in both
  halves.** Kafka keys and values are opaque byte arrays and either half may
  be *absent*, which the broker treats as distinct from present-and-empty.
  Modelling them as `String` was a category error, and it left two things
  unreachable rather than merely untested:

  - **Tombstones could not be written.** A compaction tombstone is a non-null
    key with a null value; `produce()` took `value: String` and
    `produce_bytes()` took `value: List[UInt8]`, neither with a null path.
  - **An empty-but-present key was unreachable.** `_enqueue` set the key
    pointer only when the length was non-zero, so `key=""` went on the wire as
    a *null* key. `confluent-kafka-python` distinguishes `None` from `b""`;
    this did not.

  A third symptom was type-level only: a non-UTF-8 payload came back in a
  `String` whose bytes were intact but whose `codepoints()` yielded silent
  nonsense. That one was never a data problem — the interop suite measured
  embedded NULs and invalid sequences such as `0xC0 0xC1` round-tripping
  byte-exact in every client pairing — which is why it is listed last.

  Presence now travels as the pointer, which is how librdkafka signals it:
  NULL is null, and non-NULL with length 0 is present and empty.

  **Migration.** On the consume side, `key` / `value` are optional bytes:

  ```mojo
  # before
  ref m = maybe.value()
  print(m.key, m.value)
  var raw = m.value.as_bytes()

  # after
  ref m = maybe.value()
  print(m.key_text(), m.value_text())        # "" for a null field
  print(m.key_text(default="<null>"))        # or say so explicitly
  if m.value:
      ref raw = m.value.value()              # List[UInt8], present only
  if m.is_tombstone():
      ...
  ```

  `Message` no longer conforms to `ImplicitlyCopyable` (a `List` field cannot),
  so bind it with `ref m = maybe.value()` rather than `var m = ...`, or take an
  explicit `.copy()`.

  On the produce side, the previous default `key=""` meant *no key*, and
  `key=None` means that now — so existing calls that pass a non-empty key, or
  no key at all, are unchanged and produce identical bytes. Calls that passed
  an explicit `key=""` expecting it to be dropped now write a present, empty
  key:

  ```mojo
  p.produce("t", "hello")                 # unchanged: null key
  p.produce("t", "hello", key="k")        # unchanged
  p.produce("t", "hello", key="")         # now an empty *present* key
  p.produce("t", None, key="k")           # new: a tombstone
  p.produce("t", "", key="k")             # new: an empty present value

  var payload = load()                    # List[UInt8]
  p.produce_bytes("t", value=payload^)    # Mojo will not copy a List
  p.produce_bytes("t", value=payload.copy(), key=k.copy())
  p.produce_bytes("t", value=None, key=k^) # binary tombstone
  ```

- Each `librdkafka` symbol is resolved **once**, when a client is built,
  instead of on every call. Every wrapper in `Lib` used to call
  `get_function[...]("name")(...)`, so each FFI crossing paid a `dlsym` by
  string, and that lookup cost more than the C call it wrapped. Measured
  against librdkafka 2.15: 45–55 ns/call resolving per call against
  1.2–1.5 ns/call resolving once, and through the wrappers an idle
  `Producer.poll(0)` — one crossing and nothing else — went from 97–110 ns to
  54–56 ns. `Consumer.poll()` crosses three times per message.

  The 44 hot symbols are bound in `Lib.__init__`. The four `rd_kafka_mock_*`
  ones stay lazy on purpose: they are cold, and binding them eagerly would
  make every client fail to construct against a `librdkafka` built without
  the mock broker rather than only `MockCluster`.

  No API change. `Lib` now keeps its `OwnedDLHandle` in a one-element `List`
  so the handle's address survives a move of the `Lib`.

### Removed
- **`kafka-python-ng` as an interop peer.** The cross-client suite now runs
  against `confluent-kafka` alone — the client this API is measured against —
  which takes the matrix from nine cells to four and the interop environment
  from two client libraries to one.

  The argument for keeping a pure-Python peer was that it shares no code with
  us and so is the only one that can prove our *bytes on the wire*. That is
  true and it is not the property the suite needs. This package reimplements
  no part of the Kafka protocol: the encoder is librdkafka's, so a wire-format
  bug would be a bug in librdkafka, reportable upstream rather than fixable
  here. What the suite has to catch is a bug in the **binding** layer, which is
  what this package is — and `confluent-kafka` is an independent binding layer
  over the same C library. Every bug this suite has caught lived there.

  Re-measured before removing it, not assumed: with the produce side (a null
  header value written as empty) and the consume side (an empty one read back
  as null) both broken, `mojo -> mojo` still passes `null-header-value` while
  `mojo -> confluent` fails it and `confluent -> mojo` fails
  `empty-header-value`. The symmetric-bug argument survives the peer.

  One thing genuinely improved: the suite now runs with **no skips**.
  `kafka-python-ng` asserts a header value is bytes and so could not produce a
  null one, which skipped three cells; `confluent-kafka` expresses every case
  in the set.

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

# Architecture

This is the longer write-up on how `mojo-kafka` is layered. Read [`README.md`](../README.md) first for the usage story; this document is for contributors and people considering the FFI design.

## Layers

```
┌────────────────────────────────────────────────────────┐
│  user code (Mojo / MAX)                                │
│     from kafka import Producer, Consumer, AdminClient  │
├────────────────────────────────────────────────────────┤  ← public Mojo surface
│  Pythonic Mojo API                                     │
│     src/kafka/__init__.mojo                            │
│     src/kafka/{producer,consumer,admin,config}.mojo    │
├────────────────────────────────────────────────────────┤  ← `OwnedDLHandle`
│  raw FFI surface                                       │
│     src/kafka/_ffi.mojo                                │
├────────────────────────────────────────────────────────┤  ← C ABI
│  librdkafka.so / .dylib   (BSD-2-Clause, dynamic)      │
└────────────────────────────────────────────────────────┘
```

Each arrow is a contract. The Mojo layer hides everything underneath — users see typed `struct`s and Mojo exceptions, not raw addresses or `rd_kafka_resp_err_t` ints.

## Why `librdkafka`

It's the C foundation under almost every non-JVM Kafka client (Confluent Python / Go / .NET, node-rdkafka, Rust `rdkafka`, etc.). That means:

- The wire protocol, broker compatibility, transactions, and exactly-once semantics are battle-tested.
- Behavior under network failure, rebalance, and broker upgrade matches what production Kafka users expect.
- We inherit the documentation, the bug fixes, and the `librdkafka` mailing-list culture.

Writing the Kafka wire protocol from scratch in Mojo would be more "native" but would either take years or be subtly broken on real clusters. We chose battle-tested over native.

## Why a Pythonic Mojo API

Mojo's superpower is being Python-like. Two consequences:

1. The audience most likely to pick this up is Python data / ML engineers — `confluent-kafka-python` is the API they know.
2. Mojo has direct syntactic support for Python-style `class`-like usage via `struct`. Mirroring the Python API costs little and lowers the learning curve to ~zero.

We diverge from `confluent-kafka-python` only where:
- Mojo's lack of `**kwargs` would make a Python-style call awkward (we use explicit fields on `ProducerConfig` / `ConsumerConfig`).
- Mojo's resource management (`__deinit__`) lets us drop the explicit `del consumer` dance.

## FFI design: `_ffi.mojo`

This file declares every `librdkafka` symbol we call, and it is the only
file that touches C.

### Loading, not linking

We load `librdkafka` with `OwnedDLHandle` rather than calling it through bare
`external_call`. That is not a style preference. `external_call` resolves its
symbols when the JIT materialises the program, which happens before anything
has had a chance to load the library, so under `mojo run` every call fails
with:

```
JIT session error: Symbols not found: [ rd_kafka_version ]
```

`LD_PRELOAD` does not help — the JIT does not consult preloaded objects. An
AOT build with `mojo build -Xlinker -lrdkafka` does work, but that pushes
link configuration onto every downstream user and gives up the REPL and
`mojo run` entirely. `OwnedDLHandle` works under both.

The library is found by soname (`librdkafka.so.1`, `librdkafka.so`, and the
macOS equivalents), and `MOJO_KAFKA_LIBRDKAFKA` overrides the search with an
explicit path.

### Four conventions

1. **C pointers cross the boundary as `Int` addresses, never as `Pointer`.**
   Mojo 1.0's `Pointer` is non-nullable by design, but nearly every
   `librdkafka` handle-returning call uses NULL to signal failure. The null
   check has to happen while the value is still an integer.

2. **Foreign memory is read through `ImmutAnyOrigin`.** `ImmStaticOrigin`
   looks right and faults at runtime: it lets the optimiser assume the data
   lives in this module's own static storage, and loads from another shared
   object's rodata then miscompile.

3. **No variadic C calls.** Calling a C variadic through a fixed prototype is
   undefined on both SysV and AAPCS — the `%al` vector-register count is
   never set. `rd_kafka_producev` is variadic, so producing goes through
   `rd_kafka_produceva`, which takes an **array** of `rd_kafka_vu_t` and is an
   ordinary fixed-arity function. `_VuArray` lays that array out; its stride
   is 72 and not the 24 the live union members would suggest, because
   `rd_kafka_vu_t` ends in a `char _pad[64]`.

   Two consequences are easy to undo by accident. `produceva` returns a
   `rd_kafka_error_t*` that is **NULL on success** — the opposite polarity to
   every handle-returning call here — and it is caller-owned, so it goes
   through `Lib.take_error`, which reads it and destroys it. And a header list
   passed as `RD_KAFKA_VTYPE_HEADERS` is adopted by the message **only if the
   call succeeds**; on every other path it is still ours to destroy.

   Taking the topic by name is also what retired the per-topic
   `rd_kafka_topic_t` cache on `Producer`, and with it the unsynchronised
   `Dict` that was the headline reason a `Producer` could not be shared
   across threads.

4. **Every symbol is resolved once, in `Lib.__init__`.**
   `OwnedDLHandle.get_function` does a `dlsym` by string on each call, and
   that lookup costs more than the C call it wraps — 45–55 ns/call against
   1.2–1.5 ns/call once resolved, measured against librdkafka 2.15. The
   resolved callables are held as fields on `Lib`.

   Two mechanics make that work. `get_function` returns a callable whose
   origin is borrowed from the handle, and Mojo will not let a struct field
   name the origin of one of its own fields, so the symbol is re-wrapped with
   `ImmUntrackedOrigin` — the lifetime argument moves out of the type system
   and into `_bind`'s docstring: a callable is only ever a field of the `Lib`
   that resolved it. And `Lib` is movable, so the handle lives in a
   one-element `List` whose heap address survives the move.

   The four `rd_kafka_mock_*` symbols stay lazily resolved. They are cold, and
   binding them eagerly would make every client fail to construct against a
   `librdkafka` built without the mock broker rather than only `MockCluster`.

## Lifetime story

Every `librdkafka` resource has a paired `_new` / `_destroy` (or `_free`). The Mojo wrappers tie that to Mojo's RAII:

- `Producer.__init__` calls `rd_kafka_new(RD_KAFKA_PRODUCER, …)` and stores the resulting `rd_kafka_t*` as an `Int` address.
- `Producer.__deinit__` drains outstanding delivery reports, destroys its
  main-queue reference, then calls `rd_kafka_destroy`. There are no topic
  handles left to release — `rd_kafka_produceva` takes the topic by name.
- Same pattern for `Consumer` (with an explicit `close()` for graceful rebalance), and for `AdminClient`.

`ProducerConfig._build()` returns the `rd_kafka_conf_t*` and **transfers
ownership** to `rd_kafka_new` — but only on success. `librdkafka` does not
adopt the conf when it fails, so `Lib.new_client` destroys it on the error
path, and `_build` destroys it if any `conf_set` is rejected part-way
through.

The library handle itself is the outermost lifetime: each client owns an
`OwnedDLHandle`, and because `dlopen` refcounts, `librdkafka` stays mapped
as long as any client is alive. Destructors call `rd_kafka_destroy` in the
body, which runs before fields are released, so the library is never
unloaded out from under a live broker thread.

## Error mapping

`librdkafka` returns `rd_kafka_resp_err_t` (a 32-bit enum). We:

1. Translate it to a human string via `rd_kafka_err2str`.
2. Wrap both into our `KafkaError(code: Int32, message: String)`.
3. `raise` it from any wrapper that detected a non-`NO_ERROR` code.

The roadmap is to expose a typed `KafkaErrorKind` enum so users can `match` on specific errors instead of comparing magic numbers — see [#3](https://github.com/dvirarad/mojo-kafka/issues/3).

## Reading `rd_kafka_message_t`

`rd_kafka_message_t` is a C struct with a public field layout. We decode it at known byte offsets:

| Field    | Offset | Type             |
|----------|-------:|------------------|
| `err`    |      0 | `rd_kafka_resp_err_t` (Int32) |
| `rkt`    |      8 | `rd_kafka_topic_t*` (opaque) |
| `partition` | 16 | Int32            |
| `payload` | 24    | `void*`          |
| `len`    |     32 | size_t (Int)     |
| `key`    |     40 | `void*`          |
| `key_len` |    48 | size_t (Int)     |
| `offset` |     56 | int64            |

These are verified against the installed headers with `offsetof`, and the
integration suite round-trips real messages, so a layout change shows up as a
test failure rather than as garbage in someone's pipeline.

`payload` and `key` are read for **presence** as well as content: a NULL
pointer is a null field and a non-NULL one with length 0 is a field that is
present and empty, and Kafka treats those as different. `_ffi.copy_bytes`
therefore returns `Optional[List[UInt8]]` and never collapses NULL into an
empty list — that pointer is the whole mechanism behind compaction
tombstones. The produce path reads the same rule in reverse: `_enqueue` hands
`rd_kafka_produceva` address 0 for an absent field and a real address,
possibly to a zero-length buffer, for a present one.

The same technique applies to metadata, where the stride matters as much as
the offsets: `sizeof(rd_kafka_metadata_topic_t)` is **32** bytes on 64-bit
(`char *topic; int partition_cnt; <pad>; partitions*; err; <pad>`). A 24-byte
stride reads a plausible-looking name for the first topic and then walks into
unmapped memory. `integration/test_mock.mojo` creates enough topics to catch
that.

## What we do *not* do

- We don't ship `librdkafka` itself. We dynamic-link against whatever the user provides (system package, conda-forge, or vendored).
- We don't implement transactions / exactly-once / Schema Registry yet — those are explicit v0.2 / v0.3 work in [Roadmap](../README.md#roadmap).
- We don't expose `librdkafka`'s callback-driven API. The Mojo idiom is a
  polling loop, and Mojo cannot hand C a function pointer anyway. Delivery
  reports therefore use **event sourcing** rather than a `dr_msg_cb`:
  `rd_kafka_conf_set_events(conf, RD_KAFKA_EVENT_DR)` routes each report to
  the client's main queue, and `Producer.poll()` / `flush()` drain it with
  `rd_kafka_queue_poll` and tally rejections.

  This is also why `Producer` does not call `rd_kafka_flush`. With
  `RD_KAFKA_EVENT_DR` enabled that function expects a second thread to be
  serving the queue, and `rd_kafka_outq_len` counts undrained events as
  outstanding — so it would block until its timeout. `flush()` runs the
  drain loop itself and then raises if any report carried an error.

## Testing strategy

- **Smoke tests** (`tests/test_smoke.mojo`) — load `librdkafka`, call into it,
  and build every config type. No broker required. `pixi run test`.
- **Integration tests on the mock broker** (`integration/test_mock.mojo`) —
  `librdkafka` ships an in-process broker that speaks the real wire protocol
  over a real socket, so clients under test are ordinary clients. No Docker,
  which means this suite also runs on macOS CI. `pixi run test-mock`.
- **Integration tests on a real broker** (`integration/test_broker.mojo`) —
  `pixi run broker-up && pixi run test-broker`. Not redundant with the mock:
  the mock does **not** implement the Topic Admin API, so
  `AdminClient.create_topic()` is only reachable here, and real metadata
  propagation timing only shows up against a real cluster. **Local only** —
  it needs Docker, and Docker is a local tool in this project.
- **Interop against `confluent-kafka`** (`integration/interop/`) —
  `pixi run broker-up && pixi run -e interop test-interop`. The only suite
  with an independent client on one end, so it is the only one that can catch
  a bug that is symmetric across produce and consume. Local only, same
  reason.
- **Lint** — `mojo format` over `src/`, `examples/`, `tests/`, with CI failing
  on drift.

Three of the mock tests exist specifically as regression guards:

- `test_round_trip_preserves_key_and_value` asserts on **both** halves of every
  message. A test that only checks the payload passes even when the
  `rd_kafka_vtype_t` constants are transposed, which is exactly the bug that
  shipped in `v0.1.0`.
- `test_list_topics_walks_every_entry` creates enough topics that a wrong
  metadata stride crashes instead of quietly returning junk.
- `test_null_and_empty_fields_are_distinct` walks the whole null/empty truth
  table — tombstone, empty key, null key, empty value — asserting on `key` and
  `value` rather than the `*_text()` helpers, which collapse null onto their
  default and would hide exactly the conflation being guarded.

**CI runs only what needs no Docker daemon** — lint, and the build plus the
smoke and mock suites on Linux and macOS. Both jobs gate every PR. The
Docker-backed suites above are run locally, by whoever is touching the code
they cover. See `.github/workflows/ci.yml` and `integration/README.md`.

`kafka.testing.MockCluster` is public API, not test-only scaffolding — users
testing their own Kafka code get the same Docker-free broker.

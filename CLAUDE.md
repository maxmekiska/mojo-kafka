# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Mojo client for Apache Kafka, implemented as FFI bindings over `librdkafka`.
There is no Mojo-side reimplementation of the Kafka protocol — every behaviour
comes from the C library, and the Mojo layer's job is lifetime management, type
safety, and error mapping.

## Commands

Everything runs through `pixi`. `pixi install` first.

```bash
pixi run lint          # mojo format over src/ examples/ tests/ integration/
pixi run package       # mojo precompile -> dist/kafka.mojoc
pixi run test          # smoke tests, no broker
pixi run test-mock     # full integration suite on the in-process mock broker
pixi run example-producer   # also: example-consumer, example-ml
```

**`test-mock` is the default integration suite** — it uses librdkafka's
in-process mock broker, so it needs no Docker and runs everywhere including
macOS. Reach for the real broker only when changing something the mock cannot
cover:

```bash
pixi run broker-up      # docker compose up -d --wait
pixi run test-broker
pixi run broker-down
```

The mock does **not** implement the Topic Admin API, so
`AdminClient.create_topic()` is only reachable against a real broker. See
`integration/README.md`.

Override the broker with `MOJO_KAFKA_BOOTSTRAP`, the Kafka image tag with
`KAFKA_VERSION`, and the library search with `MOJO_KAFKA_LIBRDKAFKA`.

### Running things directly

`-I src` is required whenever invoking `mojo` by hand — without it the `kafka`
package is not on the import path:

```bash
pixi run -- mojo run -I src tests/test_smoke.mojo
pixi run -- mojo build -I src examples/producer_basic.mojo -o /tmp/prod
```

There is no `mojo test` subcommand. **Tests are ordinary programs with a
`main()`** that call each `test_*` function in turn. To run a single test,
comment out the others in `main()`, or copy the case into a scratch file
outside the repo and run that with `-I src`. A test file without a `main()`
silently does nothing, and it will still exit 0 — a suite that stops calling
its cases reports success.

## Mojo 1.0

This project targets **Mojo 1.0** (`mojo = "==1.0.0"`). 1.0 removed a lot of
what older Mojo code and most training data assume. If you write pre-1.0 Mojo
here it will not compile:

| Pre-1.0 | Mojo 1.0 |
|---|---|
| `fn` | removed — use `def` |
| `alias` | `comptime` |
| `@value` | `@fieldwise_init` + explicit `Copyable, Movable` |
| `__del__` | `__deinit__(deinit self)` |
| `owned` argument | `var` |
| `sys.ffi`, `memory` | `std.ffi`, `std.memory` |
| `collections` | in the prelude — no import |
| `String.unsafe_cstr_ptr()` | `String.unsafe_ptr()` |
| `len(some_string)` | `.byte_length()` — `len()` on a string is an error |
| `UnsafePointer` | `Pointer` (origin-parameterised) |
| `mojo package`, `.mojopkg` | `mojo precompile`, `.mojoc` |
| `InlineArray`, `StringSlice` | `Array`, `StringSpan` |

Two consequences that come up constantly:

- **`def` is non-raising by default.** `OwnedDLHandle.get_function` raises, so
  every wrapper that touches FFI needs an explicit `raises`, and that
  propagates all the way up through callers and test functions.
- **Destructors cannot raise.** `__deinit__` bodies wrap their FFI calls in
  `try/except: pass`.

Two runtime traps that cost real debugging time here:

- **`String.unsafe_ptr()` is not NUL-terminated.** Whether the next byte is
  zero depends on allocator reuse, so passing it to a C function expecting a
  C string works most of the time and then intermittently appends garbage.
  Use `_c_string()` in `_ffi.mojo` for anything crossing to C; only
  length-delimited arguments (message keys and payloads) may skip it.
- **Values die after their last use, not at end of scope.** This bites
  `MockCluster` in particular: a cluster touched only during setup is torn
  down before the first `produce()`, and the symptom is a misleading
  `1/1 brokers are down`. End such scopes with `_ = cluster^`.

## Architecture

```
examples/, tests/, integration/        user-facing Mojo
  └── src/kafka/{producer,consumer,admin,config}.mojo   typed API, RAII, errors
      src/kafka/testing.mojo           MockCluster — in-process broker
        └── src/kafka/_ffi.mojo        the only file that touches C
              └── librdkafka.so        loaded at runtime, not linked
```

`_ffi.mojo` is the load-bearing file. Read it before changing anything below
the public API. It owns four conventions that are not obvious and are unsafe
to violate:

**1. librdkafka is loaded, not linked.** Bare `external_call` resolves symbols
when the JIT materialises the program — before anything has loaded the
library — so under `mojo run` every call dies with
`JIT session error: Symbols not found`. `LD_PRELOAD` does not help. The
package uses `OwnedDLHandle` + `get_function`, which works under both
`mojo run` and `mojo build`. Do not "simplify" this back to `external_call`.

**2. C pointers cross the FFI boundary as `Int` addresses, never as
`Pointer`.** Mojo 1.0's `Pointer` is non-nullable by design, but nearly every
librdkafka handle-returning call uses NULL to signal failure, so the null
check has to happen while the value is still an integer. `Pointer` is
materialised only at the moment of dereference.

**3. Foreign memory is read through `ImmutAnyOrigin`.** `ImmStaticOrigin`
looks correct and **miscompiles** — loads from another shared object's rodata
fault at runtime. This is hard to diagnose because the address is fine;
`strlen`/`puts` on the same pointer work.

**4. No variadic C calls.** Calling a C variadic through a fixed prototype is
undefined on SysV and AAPCS (the `%al` vector-register count is never set).
`rd_kafka_producev` is variadic, so the package binds the older non-variadic
`rd_kafka_produce` and caches `rd_kafka_topic_t` handles per topic name on the
`Producer` instead.

### Lifetimes

`Lib` (in `_ffi.mojo`) wraps the `OwnedDLHandle` and exposes one typed method
per C symbol. Each client owns a `Lib`; `dlopen` refcounts, so librdkafka is
mapped once regardless of client count.

Destructor ordering matters: `__deinit__` bodies run **before** fields are
released, so `rd_kafka_destroy` completes while the library is still mapped.
Moving that teardown out of the destructor body reintroduces a segfault at
process exit.

Config ownership follows librdkafka's rule: `rd_kafka_new` adopts the
`rd_kafka_conf_t` **only on success**. `Lib.new_client` destroys it on the
error path, and `Config._build` destroys it if a `conf_set` is rejected
partway through.

### Struct offsets

`consumer.mojo` and `admin.mojo` decode C structs by hand-computed byte
offsets, declared as `comptime` constants in `_ffi.mojo`. These were verified
against the installed headers with `offsetof`/`sizeof`. If you touch them,
verify the same way rather than reasoning about padding:

```bash
gcc probe.c -o probe -I"$(pixi run -- bash -c 'echo $CONDA_PREFIX')/include" \
    -L"$(pixi run -- bash -c 'echo $CONDA_PREFIX')/lib" -lrdkafka
```

The stride matters as much as the offsets: `sizeof(rd_kafka_metadata_topic_t)`
is **32** on 64-bit, not 24. A short stride reads a plausible name for the
first topic and then walks into unmapped memory.

### Delivery reports are event-sourced, not callbacks

Mojo cannot hand librdkafka a C function pointer, so there is no `dr_msg_cb`.
`Producer.__init__` calls `rd_kafka_conf_set_events(conf, RD_KAFKA_EVENT_DR)`,
which routes every delivery report to the client's main queue, and
`Producer._drain()` walks each batch with `rd_kafka_event_message_next`.

Two consequences worth knowing before touching `producer.mojo`:

- **Do not reintroduce `rd_kafka_flush`.** With `RD_KAFKA_EVENT_DR` enabled it
  expects another thread to be serving the queue, and `rd_kafka_outq_len`
  counts undrained events as outstanding — so it blocks until its timeout.
  `flush()` runs its own drain loop against `outq_len`.
- **The queue from `rd_kafka_queue_get_main` is a new reference** and must be
  destroyed before `rd_kafka_destroy`.

`AdminClient.create_topic()` has the matching trap: `rd_kafka_event_error()`
is only the *request*-level verdict. A topic the broker refused comes back
`NO_ERROR` there, with the real error per topic inside
`rd_kafka_CreateTopics_result_topics()`. Reading only the outer one reports
success for a topic that was never created.

## Testing

`tests/` holds broker-free unit tests; `integration/` holds both integration
suites and the compose file. `integration/test_mock.mojo` contains two cases
that exist as regression guards, not as feature coverage. Keep their shape if
you touch them:

- `test_round_trip_preserves_key_and_value` asserts on **both** halves of every
  message. A payload-only assertion passes even when key and value are
  transposed — which is exactly the bug that shipped in v0.1.0.
- `test_list_topics_walks_every_entry` creates enough topics that a wrong
  metadata stride crashes instead of quietly returning junk.

Against a **real** broker, topic creation is acked before metadata propagates,
so tests that create then immediately list must poll — use the
`wait_for_topics` helper in `test_broker.mojo`. The mock resolves this
instantly, which is one reason to keep the broker suite. Avoid giving test
topics names where one is a prefix of another.

## Known limitation

`Message.key` and `Message.value` are `String`, so **consuming** binary is
lossy in type if not in bytes: the bytes come back intact via `.as_bytes()`,
but the `String` is not valid UTF-8 and `codepoints()` yields silent garbage.
Producing binary is covered by `Producer.produce_bytes()`. Moving `Message` to
a byte span is the next intended breaking change and is tracked in README's
Status section — it is a correctness fix, not just the zero-copy performance
item the roadmap files it under.

## What we build next

The order below is deliberate — each item unblocks the ones under it, and the
first three are worth more than the rest combined. `confluent-kafka-python` is
the client this API is measured against; most of what follows is a gap against
it. README's roadmap still lists these in a different order (byte-span
`Message` sits at v0.5 there), so realign it once this order is confirmed.

### 1. `Message` as a byte span, not `String` — breaking

See Known limitation above. Kafka keys and values are opaque byte arrays and
`confluent-kafka-python` returns `bytes`; modelling them as `String` is a
category error rather than a missing feature. Two costs beyond the UTF-8 one:

- **Tombstones are inexpressible.** `produce()` takes `value: String` with no
  null path, so a compaction tombstone — non-null key, null value — cannot be
  written at all.
- `key=""` produces a *null* key, because `_enqueue` sets the key pointer only
  when `byte_length() > 0`. An intentionally empty-but-present key is
  unreachable; `confluent-kafka-python` distinguishes `None` from `b""`.

README files this under zero-copy performance. It is a correctness fix; the
performance win is incidental. Do it first — it changes `Message`, so
everything below is cheaper to build on top of the new shape.

### 2. Resolve each C symbol once, not on every call

Every wrapper in `Lib` calls `self._h.get_function[...]("name")(...)`, so each
FFI call pays a `dlsym` by string. Measured against librdkafka 2.15 here:
86 ns/call resolving per call against 2 ns/call resolving once — 84 ns of pure
lookup overhead. `Consumer.poll()` pays it three times per message
(`consumer_poll`, `topic_name`, `message_destroy`); produce plus drain pays it
about twice. `confluent-kafka-python` resolves once at extension load.

Resolve the symbols in `Lib.__init__` and hold the function values as fields.
Until that lands, README's "no FFI tax per message" is not accurate.

### 3. Produce through `rd_kafka_produceva`

`rd_kafka_produce` cannot attach headers, so item 4 is unreachable until this
moves. The replacement is *not* `rd_kafka_producev` — that one is variadic and
convention 4 above rules it out. It is:

    rd_kafka_produceva(rd_kafka_t *rk, const rd_kafka_vu_t *vus, size_t cnt)

which takes an **array** of `rd_kafka_vu_t`, is not variadic, and supports
`RD_KAFKA_VTYPE_HEADERS`, timestamps and explicit partition. It also accepts
the topic by name, which retires the per-topic `rd_kafka_topic_t` cache on
`Producer` — the one thing making `Producer` unsafe to share across threads
when librdkafka itself is not. So this buys headers *and* thread safety.

### 4. Headers

Cheap once 3 lands. Produce side is `RD_KAFKA_VTYPE_HEADERS` in the `vu`
array; consume side is `rd_kafka_message_headers()` plus
`rd_kafka_header_get_all()`, neither of which needs the produce rework.
Exposed on `Message` as a `Dict[String, String]` — or a list of pairs, since
Kafka permits duplicate header keys and a `Dict` silently drops them.

### 5. Per-message delivery reports

Not blocked by Mojo's lack of C function pointers, despite the current design
note. `rd_kafka_produce`'s 8th argument is `msg_opaque`, hardcoded `0` today,
and it comes back as `_private` at offset **64** of the DR message. Pass a
sequence number and read it in `_drain`, and each message's verdict becomes
addressable instead of a count plus the first failure string.

### 6. Typed `KafkaErrorKind`

Queue-full is the case that matters: `confluent-kafka-python` raises
`BufferError` there specifically, so callers know to poll and retry. Here
every failure is an `Error` carrying its reason in text, so backpressure
cannot be handled programmatically.

### 7. Consumer surface

`assign()`, `seek()`, `position()`, `committed()`, `pause()` / `resume()`,
`get_watermark_offsets()`, batch `consume(n)`, and `Message.timestamp`.
Separately, `PARTITION_EOF` is currently conflated with timeout — both return
`None` from `poll()`. Jobs that drain to end-of-partition need to tell them
apart; `confluent-kafka-python` surfaces EOF as a message with `.error()` set.
Rebalance callbacks (`on_assign` / `on_revoke`) need a design that works
without C function pointers.

### 8. `AdminClient` beyond create + list

Delete and alter topics, configs, partitions, consumer groups, ACLs. Each
carries the same two-level verdict trap as `create_topic()` — request-level
error, then per-item error inside the result.

### 9. Transactions

`init_transactions` / `begin` / `send_offsets_to_transaction` / `commit` /
`abort`, for exactly-once. Largest single feature here and the one that most
needs the real-broker suite; the mock does not cover it.

### Bugs to fix along the way

- `Producer.__deinit__` calls `_drain_until_empty(5000)`, so dropping a
  producer with undeliverable messages blocks five seconds and swallows the
  failures. Decide whether to shorten it or document it; today it does neither.
- `AdminClient.list_topics` skips `metadata_destroy` if `cstr()` raises inside
  the walk, and `create_topic` leaks `new_topic` if `queue_new` raises.
  `Consumer.poll` guards exactly this shape; neither of these does.

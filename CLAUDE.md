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
`AdminClient.create_topic()` is only reachable against a real broker.

A third suite produces with this client and consumes with `confluent-kafka`,
and the reverse. It needs the separate `interop` pixi environment, which keeps
Python out of the default one:

```bash
pixi run broker-up
pixi run -e interop test-interop
```

See `integration/README.md` and `integration/interop/README.md`.

Override the broker with `MOJO_KAFKA_BOOTSTRAP`, the Kafka image tag with
`KAFKA_VERSION`, and the library search with `MOJO_KAFKA_LIBRDKAFKA`.

### Docker is local-only

**CI runs no Docker.** It is lint plus the build, smoke and mock suites, on
Linux and macOS — everything that needs no daemon. `docker compose`, the real
broker suite, the interop suite and any benchmarking are local tools, run by
whoever is touching the code they cover. Do not add a Docker-backed job to
`.github/workflows/ci.yml`.

Two consequences to work with rather than around:

- **The mock suite carries the regression coverage**, because it is the only
  integration suite that gates a PR. A guard that can be written against the
  mock belongs there, not in `test_broker.mojo`.
- **Nothing but you runs the Docker suites.** Run `test-broker` when touching
  `AdminClient`, and `test-interop` when touching what goes on the wire --
  keys, values, headers, null versus empty. A symmetric produce/consume bug
  passes every suite CI runs.

### Running things directly

`-I src` is required whenever invoking `mojo` by hand — without it the `kafka`
package is not on the import path:

```bash
pixi run -- mojo run -I src tests/test_smoke.mojo
pixi run -- mojo build -I src examples/producer_basic.mojo -o /tmp/prod
```

There is no `mojo test` subcommand, but there **is** a discovery runner. Every
suite here ends with:

```mojo
def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
```

`discover_tests` finds every `test_*` function, reports each with a PASS/FAIL
line and a timing, and exits non-zero if any fail. Do not go back to a
hand-written `main()` calling each case: that had a silent failure mode — a
case that stopped being called was never run and the suite still passed.

Two consequences. Helpers must **not** be named `test_*` or they run as cases
(`bootstrap`, `unique_topic`, `wait_for_topics`, `_text_of` are named
accordingly). And `main()` may do setup first — `test_broker.mojo` prints the
bootstrap address — but the run itself is the one line above.

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
| `Stringable` / `__str__` | `Writable` / `write_to(self, mut writer: Some[Writer])` |
| hand-written test `main()` | `TestSuite.discover_tests[__functions_in_module()]().run()` |

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
      src/kafka/header.mojo            Header, shared by both sides
      src/kafka/testing.mojo           MockCluster — in-process broker
        └── src/kafka/_ffi.mojo        the only file that touches C
              └── librdkafka.so        loaded at runtime, not linked
```

`_ffi.mojo` is the load-bearing file. Read it before changing anything below
the public API. It owns five conventions that are not obvious and are unsafe
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

**4. Every symbol is resolved once, in `Lib.__init__`, and held as a field.**
`get_function` does a `dlsym` by string per call — 45-55 ns against 1.2-1.5 ns
once resolved, measured against librdkafka 2.15. Two mechanics make holding the
result possible and both are easy to undo: `_bind` re-wraps the callable with
`ImmUntrackedOrigin`, because a struct field cannot name the origin of one of
its own fields, and `Lib` keeps its handle in a one-element `List` so the
address survives a move. The four `rd_kafka_mock_*` symbols stay lazy on
purpose — they are cold, and binding them eagerly would make *every* client
fail to construct against a librdkafka built without the mock broker.

**5. No variadic C calls.** Calling a C variadic through a fixed prototype is
undefined on SysV and AAPCS (the `%al` vector-register count is never set), so
`rd_kafka_producev` is off limits. Producing goes through `rd_kafka_produceva`,
which takes an **array** of `rd_kafka_vu_t` and is ordinary fixed-arity;
`_VuArray` lays it out (stride 72 — see "Struct offsets").

Two `produceva` consequences are easy to undo. It returns a
`rd_kafka_error_t*` that is **NULL on success** — the opposite polarity to
every handle-returning call here — and it is caller-owned, so it goes through
`Lib.take_error`, which reads and destroys it. And a header list passed as
`RD_KAFKA_VTYPE_HEADERS` is adopted by the message **only if the call
succeeds**; on every other path it is still ours to destroy.

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
error path, and `config.mojo`'s shared `_build_conf` destroys it if a
`conf_set` is rejected partway through -- written once there rather than
per config type.

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

`_VuArray` has the same trap with a wider margin. `sizeof(rd_kafka_vu_t)` is
**72**, because its union ends in a `char _pad[64]` librdkafka keeps for
future vtypes — the largest member actually in use would suggest 24. Two
things about the buffer are deliberate: it is a `List[Int]` and not a
`List[UInt8]`, so the 8-byte alignment the union needs comes from the element
type rather than from luck; and its capacity is fixed at construction, because
`_entry` hands out raw addresses into it that a reallocating append would
leave dangling.

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
suites and the compose file. `integration/test_mock.mojo` contains three cases
that exist as regression guards, not as feature coverage. Keep their shape if
you touch them:

- `test_round_trip_preserves_key_and_value` asserts on **both** halves of every
  message. A payload-only assertion passes even when key and value are
  transposed — which is exactly the bug that shipped in v0.1.0.
- `test_list_topics_walks_every_entry` creates enough topics that a wrong
  metadata stride crashes instead of quietly returning junk.
- `test_null_and_empty_fields_are_distinct` walks the whole null/empty truth
  table and asserts on `key` / `value` rather than `key_text()` /
  `value_text()`. The text helpers collapse null onto their default, so an
  assertion through them passes under exactly the conflation being guarded.

Against a **real** broker, topic creation is acked before metadata propagates,
so tests that create then immediately list must poll — use the
`wait_for_topics` helper in `test_broker.mojo`. The mock resolves this
instantly, which is one reason to keep the broker suite. Avoid giving test
topics names where one is a prefix of another.

`integration/interop/` holds the cross-client suite, and it exists because
both suites above run us against a broker using **only our own code on both
ends** — so a bug that is symmetric, produce and consume wrong in matching
ways, round-trips cleanly and reports success. The peer is `confluent-kafka`.

That is worth one paragraph of precision, because `confluent-kafka` wraps the
same librdkafka `_ffi.mojo` binds. It is **not** an independent protocol
implementation and cannot catch a bug in librdkafka's encoder. It **is** an
independent binding layer — which is the layer this package actually is, and
where every bug this suite has caught has lived. The encoder is out of scope by
design: we reimplement no part of the protocol, so a wire-format bug is
librdkafka's, reportable upstream rather than fixable here.

Measured, not assumed — break the produce side (null header value written as
empty) and the consume side (empty read back as null) together:

```
mojo → mojo        null-header-value    PASS   <- the two bugs cancel
mojo → confluent   null-header-value    FAIL   <- the produce half, caught
confluent → mojo   empty-header-value   FAIL   <- the consume half, caught
```

One independent peer on one end is all the argument needs. Three rules follow,
and `integration/interop/README.md` has the reasoning for each:

- **`NULLABILITY` and `HEADERS` run one case per cell**, not batched. A client
  that conflates null with empty conflates it consistently, so a batch lines up
  either way.
- **Headers are compared as an ordered sequence.** Any weaker comparison passes
  for a client that keeps them in a map, and Kafka guarantees both order and
  repeated names.
- **`unsupported_by_producer` is not `expected_failure`.** The first names a
  case a peer's API cannot construct (skipped — a peer's limitation is not our
  bug); the second names a cell we get wrong (**strict** xfail, so a fix turns
  the run red rather than passing quietly). Both name nothing today. Keep them,
  and measure which cells fail before adding an entry.

## Null is not empty

`Message.key` / `.value` and both halves of `produce()` / `produce_bytes()` are
`Optional`. That is not decoration: librdkafka reads a field's presence from
the **pointer** and not the length, so NULL is a null field and a non-NULL
pointer with length 0 is one that is present and empty. Kafka treats those as
different messages, and tombstones are built on the difference.

Two rules follow, and both have already been broken once here:

- `_enqueue` must hand C address 0 for an absent field and a real address for
  a present one, even when that buffer is empty. It pins a present field to a
  local placeholder if its own address is 0.
- `copy_bytes` must not collapse a NULL pointer into an empty list.

`Message.key_text()` / `.value_text()` deliberately collapse null onto their
default, so never assert on them in a test about nullability -- that is
exactly the conflation such a test exists to catch. Assert on `key` / `value`.

## Already built — and what not to undo

Six items landed together on `feat-mojo-1-0`. The full reasoning for each lives
in the relevant docstring; what follows is only the part that is easy to undo by
accident, and the `_ffi.mojo` conventions above cover the rest.

**`CONTRIBUTING.md` is the upstream author's file — leave it alone.** Its
"help wanted" list predates all of this and is stale: consumer-side headers,
binary payloads and error mapping are done, and only the transactional producer
is still open (schema registry was always scoped out to a second package). Two
of those three shipped in a different shape than it asks for, so read the
bullets below rather than that list, and do not "fix" the code to match it:

- Headers are a `List[Header]`, **not** the `Dict[String, String]` it requests
  — a map drops repeated names and ordering, both of which Kafka guarantees.
- `KafkaErrorKind` is read off **values**, not an `enum` matched in `except` —
  Mojo 1.0's `Error` carries only text, so a typed exception cannot exist.

- **`Message` carries optional bytes, not `String`.** `key` / `value` are
  `Optional[List[UInt8]]` on both sides; "Null is not empty" above has the
  rules. Settled, so do not re-litigate: non-UTF-8 payloads always
  round-tripped byte-exact, even as a `String` — the bug was the *type*, not
  the data.

- **Every symbol is resolved once**, in `Lib.__init__` via `_bind`. A
  `get_function` call anywhere but `_bind` and the four `rd_kafka_mock_*`
  wrappers is a bug.

- **Producing goes through `rd_kafka_produceva`.** Taking the topic by name is
  what retired the per-topic handle cache. Two things follow:
  - `Producer` is still **not** thread-safe — `_failures` and `_next_sequence`
    are unsynchronised. Do not document it as safe until they are dealt with.
  - **Timestamps were left out deliberately.** The `vu` entry is trivial, but
    `Message` has no `timestamp` (see "Consumer surface" below), so a
    produce-side one could not be verified by any test here. Add both halves
    together or neither.

- **Headers are a list of pairs, not a `Dict`.** Kafka permits a repeated name
  and preserves order; a map drops both silently. Names cross to C as
  pointer + length, which keeps them off the `_c_string` NUL trap.

- **Per-message delivery reports.** `produce()` returns a sequence token, sent
  as `RD_KAFKA_VTYPE_OPAQUE` and read back from `_private`.
  - **Sequences start at 1** — the opaque is a `void *`, so 0 is
    indistinguishable from a message produced without one.
  - **`flush()` does not clear reports as it raises.** Discarding them there
    hands the caller back a count and a string, which is the thing this
    replaced. `take_failures()` acknowledges; `flush()` raises until it does.
  - **Only failures are retained**, or a long-running producer that never reads
    them grows without bound. After a clean `flush()`, everything before it was
    delivered.

- **`KafkaErrorKind` lives on values, not exceptions** — `KafkaError.kind()`,
  `DeliveryReport.kind()`, `Producer.last_error_kind()`. That is forced: Mojo
  1.0's `Error` carries only text, so `except` has no type to match on. The set
  is deliberately small and lossy (several codes share a tag); the exact value
  stays on `.code` / `.error_code`. Resist growing it into a mirror of
  librdkafka's table.

## What we build next

Ordered by **leverage, not parity**. `confluent-kafka` is the reference for API
*shape*, but matching it feature-for-feature is explicitly not the goal: much of
its surface is administrative work people do from a CLI or Terraform. Build what
unblocks a workload that is impossible today, and prefer the things that are
worth more in Mojo than they are in Python.

### 1. Consumer control plane

`assign()`, `seek()`, `position()`, `committed()`, `pause()` / `resume()`,
`get_watermark_offsets()`, and `Message.timestamp`. Also split `PARTITION_EOF`
from timeout — both return `None` from `poll()` today, so a job that drains to
end-of-partition cannot tell "caught up" from "nothing arrived".

Highest leverage of anything left, because four workloads are outright
unreachable without it: **replay** from an offset, **lag measurement** (watermark
minus position), **event-time** processing (`timestamp`), and any **bounded
drain** (EOF). It is also the prerequisite for exactly-once, below.

Rebalance callbacks (`on_assign` / `on_revoke`) are the hard part and can come
later — they need a design that works without C function pointers.

**Start by decoding `rd_kafka_topic_partition_list_t`.** Today the TPL is
write-only — `subscribe()` builds one and destroys it, and nothing ever reads
one back — but `assign`, `position`, `committed` and the watermark calls all
return or fill one, so the decode is the first task and every later call
depends on it. Probed with `offsetof`/`sizeof` against librdkafka 2.15, so add
these to `_ffi.mojo` as `comptime` rather than re-deriving them:

```
rd_kafka_topic_partition_list_t:  cnt @0 (i32), size @4 (i32), elems @8 (ptr)
rd_kafka_topic_partition_t:       topic @0, partition @8 (i32), offset @16 (i64),
                                  metadata @24, metadata_size @32, opaque @40,
                                  err @48   -- STRIDE 64
```

**Stride is 64, not 56** — the same trap as the 32-byte metadata stride, with
the same failure mode: the first element decodes plausibly, then it walks off.

Offset sentinels: `BEGINNING -2`, `END -1`, `STORED -1000`, `INVALID -1001`.
Timestamp types: `NOT_AVAILABLE 0`, `CREATE_TIME 1`, `LOG_APPEND_TIME 2`.

Symbols, all verified exported: `rd_kafka_assign`, `_seek_partitions` (prefer
it over the deprecated per-topic `_seek`), `_position`, `_committed`,
`_pause_partitions`, `_resume_partitions` (note: **not** `rd_kafka_pause` /
`_resume`, which do not exist), `_query_watermark_offsets`,
`_get_watermark_offsets`, `_offsets_for_times`, `_message_timestamp`.

### 2. Batch `consume(n)`

One call returning up to `n` messages, over `rd_kafka_consume_batch_queue`.

**This is the item where Mojo beats a Python client, so it is worth more here
than its position in `confluent-kafka` suggests.** `Consumer.poll()` crosses the
FFI three times per message; a batch crosses once for the whole set and hands
back a run of records that Mojo can then process without a per-message
interpreter round trip. For the ML and data pipelines this package is aimed at,
that is the difference between "a Kafka client in Mojo" and "a reason to use
Mojo for Kafka". Ship it with a benchmark against `confluent-kafka`.

### 3. Transactions, for exactly-once

`init_transactions` / `begin` / `send_offsets_to_transaction` / `commit` /
`abort`. The headline correctness feature, and the one most worth having that
`confluent-kafka` users reach for.

**The mock does cover it** — an earlier note here claimed otherwise and was
wrong. Probed against librdkafka 2.15: a transactional producer pointed at
`MockCluster` completes `init_transactions`, `begin_transaction`, a produce and
`commit_transaction`, so the mock serves `InitProducerId`, `AddPartitionsToTxn`
and `EndTxn`. Docker-free, and therefore CI-gatable.

Build it in two steps:

- **Step 0, and a hard gate: bind the `rd_kafka_error_t` predicates** —
  `rd_kafka_error_is_fatal`, `_is_retriable`, `_txn_requires_abort`. Every
  transactional call returns an error object the caller must branch on three
  ways (retry the call / abort the transaction / destroy the producer), and
  `Lib.take_error` currently reads only code and string before destroying it,
  throwing those bits away. Without them a transactional producer cannot be
  used correctly. Small, and really the unfinished half of `KafkaErrorKind`.
- **Then producer-only transactions** (atomic multi-topic write), which need
  nothing else. `send_offsets_to_transaction` comes after item 1: it takes a
  `rd_kafka_consumer_group_metadata_t*` (not bound) plus the offsets to commit,
  so read-process-write depends on the consumer control plane.

### Deliberately not chasing

Parity for its own sake costs more than it returns. Currently declined:

- **`AdminClient` beyond create + list** — delete/alter topics, configs,
  partitions, consumer groups, ACLs. Real, but operators do this from the CLI,
  Terraform or the JVM tooling, not from inside a stream job. Add a piece when
  something concrete needs it, not to fill the table.
- **Schema Registry / Avro / Protobuf** — genuinely valuable and genuinely
  large: an HTTP client, a schema cache and a codec. It belongs in a second
  package, which is where the upstream author put it too.
- **Full `KafkaErrorKind` coverage** — the tag set stays small and lossy on
  purpose. Only step 0 above extends it.

### Known bugs

None outstanding.

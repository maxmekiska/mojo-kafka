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
`AdminClient.create_topic()` is only reachable against a real broker — nor
ListOffsets by timestamp. See "Testing" for both, and for what that costs.

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
  `AdminClient`, consumer-group behaviour (rebalance handlers, `committed`) or
  anything time-based -- the mock fakes the group protocol and does not
  implement ListOffsets-by-timestamp at all. Run `test-interop` when touching
  what goes on the wire -- keys, values, headers, timestamps, null versus
  empty. A symmetric produce/consume bug passes every suite CI runs.

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
(`bootstrap`, `unique_topic`, `wait_for_topics`, `_text_of`, `_drain` are named
accordingly, as are the rebalance handlers the suites register --
`_noop_handler`, `_commit_on_revoke` and friends). And `main()` may do setup
first — `test_broker.mojo` prints the bootstrap address — but the run itself is
the one line above.

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
| no C callbacks (a common training-data belief) | `abi("C")` + `thin` — see below |

Two consequences that come up constantly:

- **`def` is non-raising by default.** `OwnedDLHandle.get_function` raises, so
  every wrapper that touches FFI needs an explicit `raises`, and that
  propagates all the way up through callers and test functions.
- **Destructors cannot raise.** `__deinit__` bodies wrap their FFI calls in
  `try/except: pass`.

### C callbacks are possible in 1.0 — `abi("C")`

Pre-1.0 Mojo could not hand C a function pointer, and several comments in
this repo were written under that assumption. **1.0 can.** The `abi("C")`
function effect declares the platform C calling convention, and such a
function can be passed straight to a C API expecting a callback. Verified
here against libc's `qsort` and against
`rd_kafka_conf_set_rebalance_cb` — the latter fired with
`__ASSIGN_PARTITIONS` and a decodable partition list on the mock broker.

The effect goes after the closing paren, before the `->`:

```mojo
def compare(a: Pointer[c_int, ImmutAnyOrigin],
            b: Pointer[c_int, ImmutAnyOrigin]) abi("C") -> c_int:
```

`thin` is the companion effect: a function type that captures nothing, i.e.
a plain function pointer rather than a closure. C has no closures, so
`abi("C")` always implies it. A pointer *type* is spelled
`def(Int32, Int32) thin abi("C") -> Int32`.

Three constraints, all confirmed by compiling rather than assumed:

- **`abi("C")` may not be `raises`.** The compiler rejects it outright:
  `'abi("C")' function may not be marked 'raises'`. Nearly every `Lib` method
  here raises, so a callback body needs the `try/except: pass` discipline
  `__deinit__` already uses.
- **A callback captures nothing.** The only route back to Mojo state is
  whatever `void *` the C API carries for you — for librdkafka,
  `rd_kafka_conf_set_opaque`. An object addressed that way must not move, so
  it needs the one-element `List` heap box `Lib` already uses for its handle.
- **Borrowed arguments die when the callback returns.** librdkafka destroys
  the partition list on return from `rebalance_cb`; a `char *` saved past
  that reads as garbage. Copy inside the callback.

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
- **`where` compiles but does not format.** It is a keyword in 1.0 (the
  `where conforms_to(...)` constraint clause), and `mojo format` refuses the
  file with `Cannot parse` — but the *compiler* still accepts it as a variable
  name, so the code builds and the tests pass and only `pixi run lint` fails.
  It reads naturally in exactly the place it bites (`var where = ...` for a
  list of offsets to seek to), so it has already been written twice here. Use
  `start_at` or similar.

## Architecture

```
examples/, tests/, integration/        user-facing Mojo
  └── src/kafka/__init__.mojo          the public surface — re-exports only
      src/kafka/{producer,consumer,admin,config}.mojo   typed API, RAII, errors
      src/kafka/header.mojo            Header, shared by both sides
      src/kafka/partition.mojo         TopicPartition, Watermarks, offsets
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

`consumer.mojo`, `admin.mojo` and `producer.mojo`'s delivery-report
trampoline decode C structs by hand-computed byte offsets, declared as
`comptime` constants in `_ffi.mojo`. These were verified
against the installed headers with `offsetof`/`sizeof`. If you touch them,
verify the same way rather than reasoning about padding:

```bash
gcc probe.c -o probe -I"$(pixi run -- bash -c 'echo $CONDA_PREFIX')/include" \
    -L"$(pixi run -- bash -c 'echo $CONDA_PREFIX')/lib" -lrdkafka
```

The stride matters as much as the offsets: `sizeof(rd_kafka_metadata_topic_t)`
is **32** on 64-bit, not 24. A short stride reads a plausible name for the
first topic and then walks into unmapped memory.

`rd_kafka_topic_partition_t` is the third one, and the consumer control
plane walks arrays of it constantly. `sizeof` is **64**, not the 56 its seven
members add up to: `err` at 48 is a 4-byte enum, and the struct ends with a
`void *_private` at 56 that librdkafka reserves. `test_position_walks_every_partition`
in the mock suite is the guard, and it asks for 12 partitions for the same
reason the metadata one asks for 8 — a wrong stride returns *something* for
every entry, so the test has to assert that entry `i` really is partition `i`.

`_VuArray` has the same trap with a wider margin. `sizeof(rd_kafka_vu_t)` is
**72**, because its union ends in a `char _pad[64]` librdkafka keeps for
future vtypes — the largest member actually in use would suggest 24. Two
things about the buffer are deliberate: it is a `List[Int]` and not a
`List[UInt8]`, so the 8-byte alignment the union needs comes from the element
type rather than from luck; and its capacity is fixed at construction, because
`_entry` hands out raw addresses into it that a reallocating append would
leave dangling.

### Delivery reports go through a `dr_msg_cb`

`Producer.__init__` registers `_delivery_trampoline` with
`rd_kafka_conf_set_dr_msg_cb`, and librdkafka calls it once per produced
message from inside `poll()` or `flush()`.

This was event-sourced until Mojo 1.0 — `rd_kafka_conf_set_events(conf,
RD_KAFKA_EVENT_DR)` plus a hand-written drain over
`rd_kafka_event_message_next` — because pre-1.0 Mojo could not hand C a
function pointer. `abi("C")` removed that constraint, and converting brought
the producer in line with the rebalance callback. Measured A/B over 200k
messages against the mock, the two are indistinguishable: the run-to-run
spread (~280 ns/msg) is far wider than the difference between them.

Four things to know before touching `producer.mojo`:

- **`rd_kafka_flush` is correct again, and is used.** An older note here said
  never to reintroduce it. That was true *with* `RD_KAFKA_EVENT_DR`, where
  `rd_kafka_outq_len` counted undrained events as outstanding so flush sat
  until its timeout. With a `dr_msg_cb` it serves the callback on the calling
  thread and returns when the queue is genuinely empty, which is what
  `_drain_until_empty` now is.
- **Callbacks run on the calling thread**, never on a librdkafka background
  thread. That is what makes touching Mojo state from the trampoline safe --
  and why it does not make `Producer` any more thread-safe than it was.
- **Only failures are retained.** The trampoline's success path is a single
  load and a return; a report per delivered message would grow without bound
  in a long-running producer that never reads them.
- **The state is in a one-element `List`.** The callback reaches the failure
  list by address through `rd_kafka_conf_set_opaque`, so it lives in a heap
  box (`Producer._dr`, holding a `_DrState`) like `Lib`'s handle and
  `Consumer`'s rebalance state, not in a bare field.

`AdminClient.create_topic()` has the matching trap: `rd_kafka_event_error()`
is only the *request*-level verdict. A topic the broker refused comes back
`NO_ERROR` there, with the real error per topic inside
`rd_kafka_CreateTopics_result_topics()`. Reading only the outer one reports
success for a topic that was never created.

## Testing

`tests/` holds broker-free unit tests; `integration/` holds both integration
suites and the compose file. Several cases in `integration/test_mock.mojo`
exist as regression guards rather than feature coverage. Keep their shape if
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
- `test_position_walks_every_partition` does for the 64-byte
  `rd_kafka_topic_partition_t` stride what the metadata one does for 32, and
  asserts entry `i` really is partition `i` — a wrong stride returns
  *something* for every entry.
- `test_rebalance_handler_that_does_nothing_still_gets_assigned` guards the
  silent-stall case: registering a rebalance callback stops librdkafka
  assigning by itself, so a handler that only looks must still end up
  assigned.
- `test_produce_accepts_an_explicit_timestamp` (in the **smoke** suite, not
  the mock one) guards the `_VuArray` entry count without needing a broker.

Two things the mock does not implement, and both are silent about it:

- **CreateTopics** -- so `AdminClient.create_topic()` is only reachable
  against a real broker.
- **ListOffsets by timestamp** -- it answers *every* timestamp with
  `OFFSET_END`, including ones that plainly precede every record on the
  partition, and reports no error doing so. An `offsets_for_times` test
  written against the mock would therefore pass for an implementation that
  always returned `OFFSET_END`, which is why that one case lives in
  `test_broker.mojo`. Everything else in the consumer control plane the mock
  does cover.

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

Everything below landed on `feat-mojo-1-0`: six client features first, then
the consumer control plane, then rebalance callbacks and the produce-side
timestamp, and finally the delivery-report conversion. The full reasoning for
each lives in the relevant docstring; what follows is only the part that is
easy to undo by accident, and the `_ffi.mojo` conventions above cover the rest.

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
  - `Producer` is still **not** thread-safe — `_dr[0].failures` and
    `_next_sequence` are unsynchronised. The `dr_msg_cb` did not change that:
    librdkafka runs it on whichever thread called `poll` / `flush`, so two
    threads produce two unsynchronised writers. Do not document it as safe
    until they are dealt with.
  - **Both timestamp halves have landed.** `produce(timestamp=)` is the
    record's CreateTime in milliseconds, and **0 means now** -- librdkafka's
    rule and `confluent-kafka`'s documented default -- so the `vu` entry is
    emitted unconditionally rather than only when a caller names a time.
    `RD_KAFKA_VTYPE_TIMESTAMP` is **8**, read off the enum in the installed
    header and not guessed: the enum starts at `END=0` and includes `RKT=2`,
    which this package never emits, so the live vtypes are not densely
    numbered. 2 would pass an `int64` where librdkafka expects a
    `rd_kafka_topic_t *`.

    The array is now **eight** entries. `_VuArray`'s capacity is fixed at
    construction and `_entry` raises rather than overrunning, so a stale
    `_VuArray(7)` fails on the first produce --
    `test_produce_accepts_an_explicit_timestamp` in the smoke suite pins
    that without needing a broker.

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

### The consumer control plane

Landed on `feat-mojo-1-0` after the six items above: `assign` / `unassign`,
`seek`, `position`, `committed`, `pause` / `resume`, both watermark calls,
`offsets_for_times`, `Message.timestamp`, and `poll_event`. What is easy to
undo by accident:

- **Per-partition errors are the real verdict, and the two policies are
  deliberate.** Most of these calls answer *into* the list they were given —
  the return code is only the request-level result, exactly as
  `rd_kafka_event_error` is for CreateTopics. The calls that hand a list back
  (`position`, `committed`, `offsets_for_times`) leave the verdict on each
  entry for the caller to read with `has_error()`, because one bad partition
  should not hide two good answers. The calls that return nothing (`seek`,
  `pause`, `resume`) raise on the first failure through
  `_raise_on_partition_error`, because otherwise it is lost. Do not unify
  these onto one policy.

- **The list ownership dance is written once**, in `Consumer._control`, which
  builds the TPL, makes one call, decodes, and destroys it on every path
  including the raising ones. The C call it makes is chosen by a `comptime if`
  on a `StaticString` parameter. That indirection exists so there is one
  destroy site rather than seven; inlining it back into each method
  reintroduces six chances to leak the list.

- **`seek_partitions` has `produceva`'s reversed polarity** — a
  `rd_kafka_error_t*` that is NULL on success and caller-owned. It goes
  through `Lib.take_error`. Every other call here returns an ordinary
  `rd_kafka_resp_err_t`.

- **`topic_partition_list_add` returns an address that dies at the next add.**
  The list grows by reallocating `elems`, so an element address kept across a
  second add dangles. `_tpl_build` sizes the list up front *and* writes each
  offset immediately; keep both halves.

- **`OFFSET_INVALID` is not an error.** It is what `position()` reports for a
  partition nothing has been read from, and what `committed()` reports for a
  group that has committed nothing. Mapping it to 0 would claim a full
  partition of lag on a consumer that has not started.

- **`enable_partition_eof` defaults to `False`**, matching librdkafka. It is
  what makes `poll_event()` able to report EOF at all, and a tail-following
  job wants it off. `poll()` is written in terms of `poll_event()` and takes
  the message out with `Optional.take()` rather than copying — it is the hot
  path.

- **`Message.has_timestamp()` reads the *type*, not the value.** -1 is a legal
  `int64` millisecond value, so `timestamp != -1` is not the question;
  `timestamp_type != TIMESTAMP_NOT_AVAILABLE` is.

### Rebalance callbacks

`subscribe(topics, on_assign=, on_revoke=, on_lost=)`, matching
`confluent-kafka`'s signature. Built on a real `abi("C")` callback handed to
`rd_kafka_conf_set_rebalance_cb` — see "C callbacks are possible in 1.0".
Four things not to undo:

- **The trampoline is installed on *every* consumer, at construction.** The
  callback has to go on the `rd_kafka_conf_t` before the client exists, but
  handlers only arrive at `subscribe()`, so the slots start empty. This is
  load-bearing: registering a callback **stops librdkafka assigning by
  itself**, so `_settle` must run whenever a handler did not take over, or
  the consumer silently consumes nothing. `handled` on `_RebalanceState`
  records which happened.

- **A handler that does nothing must still get the default assignment.**
  Measured against `confluent-kafka` 2.15: its `on_assign` need not call
  `assign()` for records to flow, and neither does ours.
  `test_rebalance_handler_that_does_nothing_still_gets_assigned` guards it.

- **Handlers are thin — they capture nothing.** A C callback carries no
  captured state, so a handler is a top-level `def`, never a closure, and
  everything it needs arrives on the `Rebalance` context. That is also why
  the tests observe handlers through *Kafka* — a committed offset, a
  starting offset — rather than through a counter they cannot write to.

- **`_settle` dispatches on `rebalance_protocol()`.** Eager assignors replace
  the whole assignment; cooperative ones add and remove incrementally. The
  wrong call stalls the group rather than raising.

`Rebalance` is only valid for the duration of its callback: librdkafka
destroys the partition list on return, which is why `_decode_tpl` copies
every name out immediately.

## What we build next

Ordered by **leverage, not parity**. `confluent-kafka` is the reference for API
*shape*, but matching it feature-for-feature is explicitly not the goal: much of
its surface is administrative work people do from a CLI or Terraform. Build what
unblocks a workload that is impossible today, and prefer the things that are
worth more in Mojo than they are in Python.

### 1. Batch `consume(n)`

One call returning up to `n` messages, over `rd_kafka_consume_batch_queue`.

**This is the item where Mojo beats a Python client, so it is worth more here
than its position in `confluent-kafka` suggests.** `Consumer.poll()` crosses the
FFI four times per message — `consumer_poll`, `topic_name`,
`message_timestamp`, `message_destroy` — where a batch crosses once for the
whole set and hands back a run of records that Mojo can then process without a
per-message interpreter round trip. For the ML and data pipelines this package is aimed at,
that is the difference between "a Kafka client in Mojo" and "a reason to use
Mojo for Kafka". Ship it with a benchmark against `confluent-kafka`.

### 2. Transactions, for exactly-once

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
  nothing else. `send_offsets_to_transaction` needs one more binding on top:
  it takes a `rd_kafka_consumer_group_metadata_t*`, and
  `rd_kafka_consumer_group_metadata` is the one piece of the consumer side
  still unbound. Everything else read-process-write wants -- the TPL decode,
  `committed`, the rebalance hooks -- has landed, so this does **not** wait on
  item 1.

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

### Known coverage gaps

- **`on_lost` has no test.** The path exists and is documented -- a lost
  assignment routes there instead of `on_revoke`, falling back to `on_revoke`
  when it is unset -- but exercising it needs partitions genuinely lost to a
  session timeout or a coordinator failure, which neither suite forces today.
  It is the one branch of the rebalance trampoline nothing runs. Whoever
  makes a broker fail over should add it.

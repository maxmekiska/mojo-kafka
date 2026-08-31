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
  `AdminClient`, consumer-group behaviour (rebalance handlers, `committed`),
  `send_offsets_to_transaction`, or anything time-based -- the mock fakes the group protocol and does not
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

  It applies **inside `__deinit__` too**, per field -- see "Lifetimes". A
  destructor that never mentions a field runs with that field already freed,
  which is how every `close()`-less `Consumer` came to segfault.

  And it applies **across a single call**, which is subtler: a stack buffer
  handed to C, read back after some *other* call, can be read after the
  compiler released it. `Lib.new_client` returned a half-overwritten
  `rd_kafka_new` error for exactly this reason -- the errbuf was decoded
  after the intervening `conf_destroy`. Decode straight after the call that
  filled the buffer, then `_ = buf^`.
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
      src/kafka/_sync.mojo             _Latch — the only lock in the package
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

Destructor ordering matters, and the rule is narrower than it looks. A field
is released at its **last use inside the destructor body** -- not after the
body returns. `rd_kafka_destroy` completes while the library is still mapped
because `self._lib.destroy(...)` *is* a use of `_lib`; move that teardown out
of the body and the segfault at process exit comes back.

The corollary bit hard, and is the reason both clients end their destructors
with a `_ = self._box^` line: **a field the body never mentions is already
gone by the first statement.** `Consumer.__deinit__` calls
`rd_kafka_consumer_close`, which fires a final revoke into
`_rebalance_trampoline`, which reaches the consumer's state through the raw
address given to `rd_kafka_conf_set_opaque` -- so a `_rebalance` box with no
use in the destructor was freed *before* the close that needs it, and every
consumer dropped without an explicit `close()` faulted. The whole suite
missed it because all 21 cases closed by hand;
`test_dropping_a_subscribed_consumer_does_not_fault` is the guard now.

So: **any state a C callback reaches by address must be pinned with an
explicit `_ = ...^` at the end of the destructor that can trigger that
callback.** `Producer` survives its drain only because
`_drain_until_empty` takes `mut self` and therefore borrows the whole
struct; that is an accident of one signature, so it is pinned too.

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
- `test_dropping_a_subscribed_consumer_does_not_fault` is the **only** case
  that lets a consumer go without calling `close()`. Every other one closes
  by hand, which is precisely why a destructor that faulted every single
  time went unnoticed. Do not "tidy" it by adding a `close()`; the missing
  close *is* the test.
- `test_concurrent_produce_keeps_every_sequence_and_report` (**smoke**, no
  broker) drives eight real threads at a dead port. Two things about it are
  deliberate: the messages must **fail**, because the delivery-report
  trampoline returns on the success path before it ever touches the shared
  list, so a working broker would exercise nothing; and the 8x400 counts are
  measured, not chosen -- at 4x50 it caught a reverted fix only 3 runs in 6.
- `test_txn_action_orders_abort_before_fatal` (**smoke**) builds its errors
  from values, because the both-flags cell it exists for cannot be produced
  on demand even with injection. It fails with the branch order swapped.
  `test_take_error_reads_the_flags_off_a_real_error` is the opposite: only a
  real `rd_kafka_error_t` shows that the flags survive `take_error`, so it
  spends 500ms on `init_transactions` against a dead port. Neither replaces
  the other; keep both.
- The six transaction cases in the **mock** suite are three pairs, and each
  pair is worth less by half. `..._committed_transaction_is_visible...` and
  `..._aborted_transaction_is_invisible...` are only meaningful because the
  consumer is `read_committed` -- the default `read_uncommitted` reads the
  records either way and cannot tell a commit from an abort.
  `..._fatal_transaction_error_is_flagged_fatal` and
  `..._abortable_transaction_error_asks_for_an_abort` inject at the mock so
  the client does the classifying; the abortable one then *follows* its own
  verdict -- abort, begin again, commit -- because a branch that names the
  right action and cannot carry it out is not worth much.
- The **read-process-write** pair on the mock (`..._read_process_write_loop_
  completes`, `..._failed_offset_commit_asks_for_an_abort`) deliberately
  asserts nothing about the committed offset, because the mock cannot answer
  -- see the third bullet under "Testing". They cover the shape of the loop
  and the abortable branch. The offset itself is
  `test_transactional_offsets_are_visible_to_the_group` in
  `test_broker.mojo`, which asserts **both** directions: committed leaves the
  group at last-processed + 1, aborted leaves it `OFFSET_INVALID`. The second
  is the exactly-once claim -- an implementation that committed offsets
  outside the transaction passes every other test in every suite and fails
  only that one.
- **The aborted-transaction case proves absence with a barrier, not a
  timeout.** It commits a marker record after the abort and reads until the
  marker arrives; anything aborted sits earlier in the log and would be
  delivered first. The first version polled for a fixed period instead: it
  passed, took 60 seconds, and would have passed just as well against a
  broker that was merely slow.

Three things the mock does not implement, and all three are silent about it:

- **CreateTopics** -- so `AdminClient.create_topic()` is only reachable
  against a real broker.
- **Transactional offset commits, on the read-back side.** It accepts
  `TxnOffsetCommit` and answers success, but never serves those offsets
  through `OffsetFetch`: `committed()` reports `OFFSET_INVALID` afterwards
  whether the transaction committed or aborted. Confirmed in plain C against
  librdkafka 2.15 with no Mojo in the picture, so it is the mock. This one is
  the most dangerous of the three, because the natural assertion --
  "an aborted transaction left the group uncommitted" -- **passes
  unconditionally**. It was written that way once here and had to be moved;
  `test_transactional_offsets_are_visible_to_the_group` in
  `test_broker.mojo` is where it lives, and it asserts both directions.
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
  - `Producer` **is** thread-safe now, and the two pieces that were not are
    dealt with: `_next_sequence` is an `Atomic` (`fetch_add`, so no two
    messages can claim one token) and `_dr[0].failures` is guarded by
    `_Latch`, a spinlock over `Atomic` because Mojo 1.0 has no mutex and no
    `std.sync`. The `dr_msg_cb` is why the latch is needed at all:
    librdkafka runs it on whichever thread called `poll` / `flush`, so
    several threads draining are several writers.

    Two rules keep it correct. **No FFI call inside a critical section** --
    that is what stops the callback waiting on a lock its own caller holds,
    so the trampoline decodes the report *before* acquiring. And **never
    raise under the lock**: `_raise_if_undelivered` builds its message
    inside and raises outside.

    `last_error_kind()` is the one thing that stays unsafe, and no
    arrangement fixes it: one slot on the producer cannot be attributed to a
    caller once two threads produce, because Mojo 1.0's `Error` carries only
    text and the kind cannot ride the exception. Say so rather than locking
    it harder. `test_concurrent_produce_keeps_every_sequence_and_report`
    drives eight real threads through `pthread_create` and fails 6/6 with
    the synchronisation reverted.

    **`Consumer` is thread-safe too, and `close()` is why it was not.** It
    checked a `Bool` then set it, so every thread calling `close()` passed
    the check before any set it -- and concurrent `rd_kafka_consumer_close`
    on one handle **deadlocks**: measured at 8 threads, one returns and
    seven never do. Sequentially the second call merely returns -197, which
    is what made it look cosmetic. It is a compare-exchange now, and the
    loser returns rather than waiting: leaving a group is a network round
    trip and there is no blocking primitive in 1.0 to wait on properly.

    Two honest notes on the rest of `Consumer`. The reading calls hold no
    mutable Mojo state and never needed anything. And **the three
    rebalance-handler slots hold raw code addresses in `Atomic` words, not
    `Optional[RebalanceHandler]` behind a latch** -- see `_RebalanceState`.
    An earlier note here said the latch was "reasoned, not measured" and
    justified it by the slots being "aligned and pointer-sized". The second
    half was **measured and is false**: `Optional[RebalanceHandler]` is 16
    bytes against 8 for the bare handler, so an unguarded read could pair
    `has_value = True` with the other value's payload and jump through it.
    That is a worse bug than the mispaired *set* the note described, and no
    test could provoke it -- the trampoline reads a slot only during a
    rebalance, which happens twice in a test against thousands of writes, so
    the window is never hit. One word per slot makes it impossible instead of
    unlikely, and `test_a_handler_slot_is_one_word` is a guard that can
    actually fail. Do not put a multi-word type back in those slots.
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

### Batch `consume(n)`

`Consumer.consume(n, timeout_ms)` over `rd_kafka_consume_batch_queue`, on the
queue from `rd_kafka_queue_get_consumer` -- the same construction
`confluent-kafka`'s `Consumer.consume()` uses. `consume_events(n)` is its
`poll_event` counterpart. Six things not to undo:

- **One `consume()` at a time per consumer, and a second caller is
  *refused*.** librdkafka calls concurrent `rd_kafka_consume_batch_queue` on
  one queue undefined behaviour and says the case "will not be supported in
  future as well" -- and it is in `INTRODUCTION.md`, **not** `rdkafka.h`, so
  nothing a reader of the header would see. It is also silent: no error, no
  crash, just UB. `_batch` is a `_Latch` used through `try_acquire`, which
  raises on contention rather than queueing. Serialising instead would hide
  the caller's bug and silently change their execution.
  `test_concurrent_consume_is_refused_not_serialised` asserts exactly one of
  eight threads gets in, and fails both ways -- serialise it or drop it and
  all eight return.

- **`_consume_locked` makes FFI calls inside a critical section**, which
  `_sync`'s first rule forbids. The exemption is reasoned and written there:
  that rule exists because the *producer's* latch is contended by a callback
  librdkafka invokes from inside the very call the holder is making. This one
  is contended only by other `consume()` callers, and the rebalance
  trampoline takes no latch at all any more. There is no cycle to close.

- **The consumer-queue reference is destroyed before `consumer_close`**, in
  both `close()` and `__deinit__`, via `_release_queue`. librdkafka says
  MUST. `_queue` is a bare `Int` for that reason: a field with a destructor
  of its own would be released at its last use in the destructor body, which
  is the wrong order relative to the close -- exactly the bug `_rebalance`
  had.

- **It is taken at construction, not lazily.** A reference that may or may
  not exist makes the teardown rule conditional, and a conditional rule is
  one that gets missed.

- **`n` is bounded at both ends, and the upper bound is the load-bearing
  one.** `MAX_BATCH` is 1,000,000, matching `confluent-kafka`'s cap on
  `num_messages` -- checked against it, not assumed. The pointer array is
  allocated before the call, so an unbounded `n` is an unbounded allocation
  from one caller's slip. **0 is a deliberate divergence**: `confluent-kafka`
  returns an empty list, this raises, because a drain loop built on a call
  that can silently return nothing for a reason unrelated to the topic spins
  forever. `test_consume_rejects_a_batch_size_it_cannot_honour` also asserts
  `MAX_BATCH` itself is *accepted* -- an off-by-one that rejected the largest
  legal batch would otherwise go unnoticed.

- **`consume()` on a closed consumer raises, and must.** Releasing the queue
  before the close -- which librdkafka requires -- leaves `_queue` at 0, and
  `rd_kafka_consume_batch_queue` **faults** on NULL inside
  `rd_kafka_consume_batch0` rather than returning an error. Measured: a
  segfault, 1 run in 1. `poll()` after `close()` does not crash, so this is
  the batch path's alone. `test_consume_after_close_raises_instead_of_faulting`
  is the guard, and like the racing-close case it **crashes rather than
  fails** if the check is removed.

- **The topic name is cached across the batch.** `rd_kafka_topic_name` is a
  crossing per message otherwise, and a batch is nearly always one topic.
  That is what takes the per-message crossings from 3 to 2; the fetch itself
  is 1 for the whole batch. `_decode` is shared with `poll_event`, which is
  why it takes the topic name instead of reading it.

- **The benchmark lives in `benchmarks/`, is local-only, and its cross-client
  number is not settled.** `pixi run -e interop bench`, needs a real broker.
  Two methodology points were learned the hard way and are written up in
  `benchmarks/README.md`: the timed loop must drain a **warm local queue**
  (without a prefetch pause every configuration measures fetch latency and
  lands within 15% of the others, with batching apparently slower), and the
  **median** must be reported, not the best (the spread is not one-sided, and
  best-of-N rewards whichever client got luckiest). What reproduces is that
  batching beats polling in both clients -- 1.44x here for us, 2.08x for
  `confluent-kafka`.

  **`consume_borrowed()` is where the win is, and it is measured.** All
  three mojo modes and `confluent-kafka` return the same checksum over the
  same topic, so the comparison is like for like and the speed is not
  skipped work. Back to back in one session on one 50k topic: borrowed
  17.4ms, our own `consume()` 35.7ms, `confluent-kafka`'s `consume()`
  32.2ms. So **borrowed is ~2x our owned path and ~1.8-3.3x
  `confluent-kafka`** -- the two runs disagree on magnitude, so quote
  "roughly 2x". The owned path against `confluent-kafka` is **parity**,
  which is what the code predicts: both copy every key and value, and only
  the borrowed path does not.

  **The owned-path cross-client ratio does not reproduce and must not be
  quoted.** A
  decomposition that removed our per-message decode work one piece at a time
  produced a variant doing strictly less work that measured *slower* than the
  one above it, and a single variant swinging 2.8x between runs -- so the
  noise on this hardware exceeds the signal outright. The unmodified decode
  reached 1,557,155 msg/s in that run, above the `confluent-kafka` median
  from the published table. **Do not optimise our decode on the strength of
  the 0.58x figure**; it is an artefact.

  Parity is what the structure predicts anyway. `confluent-kafka`'s
  `Message_new0` does, per message: `rd_kafka_topic_name` plus a Python str,
  `PyBytes` for key and value, and `rd_kafka_message_timestamp`. We do the
  same four with the topic name cached across the batch, plus **one** extra
  crossing -- `rd_kafka_message_headers`, which it defers to `msg.headers()`.
  That one call was measured and rejected as an explanation for any gap.

- **`consume()` raises on a hard error and loses the batch with it;
  `consume_events()` does not.** A Mojo `Error` is text and cannot carry the
  records that arrived alongside. So the pair exists for the same reason
  `poll`/`poll_event` does, and `consume_events` follows the control plane's
  policy -- one bad entry must not hide the good ones beside it, so it raises
  only when the error is the *only* thing in the batch.

### Transactions, and exactly-once

`init_transactions` / `begin_transaction` / `send_offsets_to_transaction` /
`commit_transaction` / `abort_transaction` on `Producer`, plus the three
`rd_kafka_error_t` predicates every one of them is branched on. Complete:
read-process-write included. Nine things not to undo:

- **They return their error; they do not raise it.** `confluent-kafka`
  raises, but its `KafkaException` wraps a `KafkaError` carrying
  `.retriable()`, `.txn_requires_abort()` and `.fatal()`. Mojo 1.0's `Error`
  is text, so a raise here would discard exactly the three bits the caller is
  required to branch on. `Optional[KafkaError]` -- `None` on success -- is how
  the same information arrives. `last_error_kind()` is the side-channel this
  package uses to work around the same limitation for `produce()`, and it is
  explicitly not attributable once two threads produce; a transactional call
  cannot take that compromise, because a wrong branch corrupts a transaction
  rather than mis-reporting one rejection.

- **`take_error` is the only place the flags can be read**, because it is what
  destroys the object holding them. They are not on the error code, so once
  the `rd_kafka_error_t` is gone they are unrecoverable -- which is what the
  pre-step-0 version did, reading code and string and dropping the rest.

- **`txn_action()`'s branch order is librdkafka's, and it is not the obvious
  one.** Abort, then retriable, then everything else. Testing `is_fatal`
  first reads correctly and is wrong: the transactional producer treats most
  of the idempotent producer's fatal errors as abortable, since a transaction
  can be aborted and replayed whole, so an error can carry both flags and the
  abort is what to do. `rdkafka.h`'s own worked example branches in exactly
  this order. An error with **none** of the three flags is `TXN_FATAL`,
  following librdkafka's explicit "treat all other errors as fatal" -- not a
  fourth tag no caller could handle.

- **A `KafkaError` built from a bare code carries no flags**, and False there
  means "no opinion", not "survivable": a `rd_kafka_resp_err_t` has nowhere to
  keep one. So `txn_action()` is meaningful only on an error a transactional
  call returned, exactly as librdkafka documents for `txn_requires_abort`.

- **`commit_transaction` and `abort_transaction` default to `timeout_ms=-1`
  and should stay that way.** librdkafka calls -1 "strongly recommended": it
  means the transaction's remaining time, and any other value "risk[s]
  internal state desynchronization" if a protocol request fails mid-commit.

- **`commit_transaction` flushes on its own.** librdkafka's warning about
  having to serve delivery reports from another thread during that flush
  applies to `RD_KAFKA_EVENT_DR`, which this package no longer uses -- a
  `dr_msg_cb` is served on the flushing thread. Do not add a `flush()` before
  the commit; it is guessing at librdkafka's job.

- **`abort_transaction` purges, and the purged messages surface as delivery
  failures** (`__PURGE_QUEUE` / `__PURGE_INFLIGHT`). That is the abort
  working, but failures are retained until acknowledged, so a later `flush()`
  raises about messages discarded on purpose. The docstring tells callers to
  `take_failures()` after an abort, and the mock test asserts on all three
  reports rather than pretending they are not there.

- **`send_offsets_to_transaction` is what makes it exactly-**once**, not
  just atomic-write.** It commits the *consumer's* offsets inside the
  producer's transaction, so input and output land together or not at all.
  Three of its requirements fail **silently** and are in the docstring for
  that reason: offsets are the *next* message to consume (`position()`
  reports exactly that, which is why it is the natural argument); the
  consumer needs `enable.auto.commit=false`; and invalid offsets are skipped,
  so a call with nothing valid returns success having done nothing. It is
  also the one call here that is retriable but **not resumable** -- a retry
  must carry the same offsets and the same metadata.

- **`ConsumerGroupMetadata` (`group.mojo`) is not copyable and is a
  snapshot.** Not copyable because the handle is freed once in `__deinit__`
  and a copy double-frees; a snapshot because it captures the group
  generation, which a rebalance supersedes -- so it is taken inside the
  transaction that sends the offsets, not once at startup. It lives in its
  own module for the reason `header.mojo` does: it belongs to neither client
  and both need it. `_build_tpl` moved to `partition.mojo` for the same
  reason -- the producer needs it now, and importing it from `consumer.mojo`
  would be the only edge between the two clients.

Testing them needed one more binding, `rd_kafka_mock_push_request_errors_array`
-- the **array** form, because the `..._errors` form is variadic and
convention 5 rules it out. It is lazy like the other `rd_kafka_mock_*`
symbols. It is how librdkafka tests its own transactional error handling
(`tests/0105-transactions_mock.c`), and it is what closed the coverage gap
step 0 left: the fatal and abortable flags are set by the **client**, from
the code the coordinator returned, so injecting a code at the mock drives the
real classification path rather than a simulation of it. Measured against
librdkafka 2.15, and these are the two the mock suite uses:

| inject | into | flags |
|---|---|---|
| `CLUSTER_AUTHORIZATION_FAILED` | `InitProducerId` (22) | `is_fatal` |
| `TOPIC_AUTHORIZATION_FAILED` | `AddPartitionsToTxn` (24) | `txn_requires_abort` |
| `GROUP_AUTHORIZATION_FAILED` | `AddOffsetsToTxn` (25) | `txn_requires_abort` |
| `INVALID_TXN_STATE` | `AddOffsetsToTxn` (25) | `is_fatal` |

One probe result worth keeping: a `send_offsets_to_transaction` naming a
topic the mock does not have comes back **abortable**, not "unknown topic".
Create the input topic in these tests even though nothing produces to it
through the mock.

### Borrowed message views

`Consumer.consume_borrowed(n)` returns a `MessageBatch` that still owns
librdkafka's messages and lends `BorrowedMessage` views into them --
`key()` and `value()` are `Span`s pointing at the fetched bytes, copied
nowhere. The same shape as `rust-rdkafka`'s `BorrowedMessage`, alongside the
owned `Message` rather than instead of it. Five things not to undo:

- **The origin is a struct parameter, never `origin_of(self)` on the
  accessor.** This is the whole safety argument and it is easy to "simplify"
  into a use-after-free. Probed both ways: an origin taken from a `ref self`
  borrow ends when the method returns, and the message is then freed
  **before** the read -- it only looks correct because `free()` does not
  scrub. Threading the batch's origin through `BorrowedMessage[origin]` and
  returning `Span[UInt8, Self.origin]` instead makes the compiler extend the
  batch's life past the last use of any span taken from it. Verified with an
  instrumented destructor, and again through a span extracted and used after
  the batch's last mention.

- **The batch owns the messages and destroys them.** That is what bounds
  every borrowed view. It is not `Copyable` -- two batches destroying one
  message is a double free.

- **Error and EOF entries are destroyed inside the fetch**, not handed to the
  batch: they have no payload to lend, and a batch must not hold a message it
  cannot describe.

- **`rd_kafka_topic_name` is resolved once per batch**, via
  `Lib.topic_name_ptr` which does *not* copy into a `String` -- the pointer
  belongs to the topic handle and outlives every message in the batch. The
  owned path's `topic_name` still copies; both exist deliberately.

- **No headers on a borrowed view.** Each one is a separate name and value
  that would have to be copied out individually, which is the cost this type
  exists to avoid -- `confluent-kafka` defers them for the same reason. Use
  `consume()` when you need them.

`test_borrowed_and_owned_consume_agree` is the correctness guard: it reads
the same topic both ways and asserts record for record on both halves,
including a tombstone and an empty-but-present value, because presence is
read from the pointer on this path too.
`test_a_borrowed_batch_outlives_the_spans_taken_from_it` is the lifetime
guard, and if it regresses it is a **use-after-free** -- it may crash or
return plausible garbage, which is why it asserts on the payload's contents
and not merely its length.

### The origin research behind it

`rust-rdkafka`'s `BorrowedMessage` hands back `Option<&[u8]>` pointing into
librdkafka's own buffer -- no copy. We copy key and value into owned
`List[UInt8]` on every message, as `confluent-kafka` does into `PyBytes`.
That is the one structural difference between us and a zero-copy binding, and
it is worth knowing exactly what Mojo can do about it. Probed, not assumed:

- **Reading straight out of librdkafka's allocation works.** A
  `Span[UInt8, origin_of(...)]` built over a `malloc`'d C buffer reads
  correctly with no copy. Nothing stops us offering this.

- **Mojo's origins *are* enforced for tracked memory.** A struct owning a
  `List[UInt8]` that lends `Span[UInt8, origin_of(self._buf)]` keeps the
  owner alive: with an instrumented `__deinit__`, transferring the owner
  away (`_ = h^`) mid-scope ran the destructor **after** the last use of the
  view, not at the transfer. That is Rust's guarantee, and the compiler
  provides it.

- **It is *not* enforced across `unsafe_from_address`.** The same shape over
  foreign memory freed the buffer **before** the read -- a real
  use-after-free that only looked fine because `free()` does not scrub.
  Tying the origin to a real tracked field instead of to `self` did not help.
  The reason is provenance: the pointer is fabricated from an `Int`, so the
  compiler sees no live borrow of the owner and the declared origin is
  decoration.

So a zero-copy `BorrowedMessage` here would match Rust's *performance* but
not its *safety*: the lifetime would rest on API shape and the `_ = owner^`
discipline this file already documents under "Lifetimes", not on the checker.
If it is built, the batch must own the raw messages and lend views that
cannot outlive the loop -- and it belongs **beside** the owned `Message`, not
instead of it, because the owned one is what makes a record safe to keep.

## What we build next

Ordered by **leverage, not parity**. `confluent-kafka` is the reference for API
*shape*, but matching it feature-for-feature is explicitly not the goal: much of
its surface is administrative work people do from a CLI or Terraform. Build what
unblocks a workload that is impossible today, and prefer the things that are
worth more in Mojo than they are in Python.

**Everything previously listed here is built.** Batch `consume(n)`,
transactions end to end, and the borrowed zero-copy view all landed on
`feat-mojo-1-0`; see "Already built" for each, and read those notes before
touching any of it. There is no queued feature work, which means the next
thing is a judgement call rather than a plan. Three candidates, none started:

- **A borrowed producer path.** `produce()` copies its key and value through
  `RD_KAFKA_MSG_F_COPY`. The consume side just showed what removing a copy
  per record is worth (~2x); the produce side has never been measured. Do
  the measurement before the work -- `F_COPY` exists so the caller's buffer
  can go out of scope immediately, and taking that away is a real API cost
  that a number should justify.

- **Settle the cross-client benchmark somewhere quiet.** The numbers in
  `benchmarks/README.md` are from a WSL2 laptop sharing a kernel with the
  broker, and a single configuration swung 2.8x between runs there. The
  borrowed result is large enough to survive that; the owned-path parity
  claim is not. A quiet machine with `--repeat 9` would turn two ranges into
  two numbers.

- **A `rust-rdkafka` peer for the benchmark.** `cargo` is on this machine.
  It would answer the question the borrowed view was built to answer --
  whether we are actually level with a zero-copy Rust binding, rather than
  merely faster than a copying Python one.

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
  purpose. Step 0 was the one sanctioned extension and it is done; it added
  no tags, because the flags are a separate axis from the kind.

### Known bugs

None outstanding.

### Known coverage gaps

**None outstanding.** Both entries that once stood here are closed, and the
way they closed is the part worth carrying forward:

- When a guard cannot be tested, the fix is usually to **remove the need for
  it**, not to write a test that passes either way. That is what happened to
  the `subscribe()` latch below.
- When a test keeps needing its bounds loosened, it is measuring the
  environment rather than the code. `test_a_borrowed_batch_reports_end_of_
  partition` asserts the contract for that reason, and its docstring records
  the three attempts that did not.

**All four `txn_action()` arms are covered by
real errors** -- `is_fatal` and `txn_requires_abort` by mock error injection,
retriable and unflagged by `init_transactions` against a dead port in the
smoke suite -- so the gap recorded here earlier is closed. Do not reopen it
by "simplifying" the mock tests down to hand-built `KafkaError` values; the
whole point is that the *client* classifies the broker's code.

**The `subscribe()` handler slots were the other gap, and they are closed --
by deleting the thing that could not be tested rather than by testing it.**
They were three `Optional[RebalanceHandler]` under a `_Latch`, and no test
that failed without the latch could be written. The reason turned out not to
be the one recorded: measuring showed the slot was 16 bytes, not one word, so
the hazard was a jump through a half-written pointer -- real, and *still*
unprovokable, because the trampoline reads a slot only during a rebalance.
So the slots became single `Atomic` words holding the bare code address,
which is unable to tear, and the latch went. `test_a_handler_slot_is_one_word`
and `test_a_handler_survives_the_round_trip_through_its_slot` guard the shape
and the pun the safety now rests on, and both fail when broken.

`on_lost` was the other one, and is now covered **on the mock**, which an earlier note here assumed impossible -- it claimed the path
needed a coordinator failure no suite could force. It does not: exceeding
`max.poll.interval.ms` is librdkafka's own *client-side* liveness timer, so
a consumer that stops polling is thrown out of the group with its partitions
genuinely marked lost, and the mock serves that like any other rebalance. No
Docker, and it gates a PR.

Three things about those two tests are load-bearing:

- **`max.poll.interval.ms` may not be below `session.timeout.ms`** --
  librdkafka rejects the client outright -- and **3000 is the floor** at
  which the mock still completes a JoinGroup. At 1000 the JoinGroup request
  itself times out, the group never forms, and nothing is ever assigned to
  lose. The tests pin both to 3000 and sleep 4s.

- **A thin handler cannot tick a counter**, and every Kafka-side side effect
  the other rebalance tests observe through -- a commit, an assignment --
  *fails by design* during a lost rebalance, because another member may
  already own the partition (librdkafka logs `COMMITFAIL`). So the handlers
  record through `setenv` / `getenv`: process-local, no filesystem, and the
  key is a `comptime` constant, which is the one kind of state a thin `def`
  can name.

- **Both directions are tested.** One case asserts a lost assignment reaches
  `on_lost` and *not* `on_revoke`; the other unsets `on_lost` and asserts the
  documented fallback to `on_revoke`. Without the second, an `on_lost` that
  was never reached and a lost event dropped on the floor look identical.

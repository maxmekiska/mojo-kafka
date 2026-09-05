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
pixi run example-produce    # also: example-pipeline, example-consume
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

The consume benchmark is a fourth local-only thing, in the same environment
because it uses `confluent-kafka` as one of its peers:

```bash
pixi run broker-up
pixi run -e interop bench --pin 5,6,7
```

It builds a **C** peer (needs `cc` and the conda `librdkafka` headers) and a
**Rust** peer (needs `cargo`; first build fetches crates) and **skips** either
if the toolchain is missing, so the table degrades rather than failing. See
`benchmarks/README.md`.

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
pixi run -- mojo build -I src examples/produce.mojo -o /tmp/prod
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

Those 45-55 ns are a **warm** figure — one symbol resolved over and over. The
cold cost of resolving 76 *distinct* symbols, which is what `Lib.__init__`
actually does, is ~260-400 ns each: a `Lib` costs **~20 µs** to construct,
against ~850 ns for the `dlopen` alone. Both numbers are real and neither
supersedes the other. The cold one is why `_Freer` exists — see the
`MessageBatch` note under "Batch `consume(n)`" — and it is worth checking
before putting a `Lib()` on any path that runs more than once.

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

That is worth one paragraph of precision, because `confluent-kafka` wraps
librdkafka too. It is **not** an independent protocol implementation and
cannot catch a bug in librdkafka's encoder. (It does not wrap the *same
build*: its manylinux wheel bundles `librdkafka-c87086af.so.1` rather than
loading the conda-forge library `_ffi.mojo` opens -- irrelevant to this
suite, which tests wire behaviour, but see the benchmark notes, where it
matters.) It **is** an
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
  fails** if the check is removed. The check runs **under `_batch`**, and
  `close()` takes the same latch around releasing the queue: a check made
  outside it could pass and then fetch through a queue a `close()` on
  another thread had just destroyed. So `close()` waits for a fetch in
  flight, bounded by that fetch's timeout;
  `test_close_waits_for_a_batch_fetch_in_flight` (smoke) is the guard.

- **The topic name is cached across the batch.** `rd_kafka_topic_name` is a
  crossing per message otherwise, and a batch is nearly always one topic.
  That is what takes the per-message crossings from 3 to 2; the fetch itself
  is 1 for the whole batch. `_decode` is shared with `poll_event`, which is
  why it takes the topic name instead of reading it.

- **The benchmark lives in `benchmarks/`, is local-only, and it now has four
  peers.** `pixi run -e interop bench`, needs a real broker. C, `rust-rdkafka`,
  this package and `confluent-kafka`, all reading one topic and all asserting
  on the same payload checksum. `benchmarks/README.md` has the numbers; what
  belongs here is what is easy to undo.

  **The harness was rebuilt because the old one could not measure.** Three
  things are load-bearing and all three replaced a mistake:

  - **Repeats happen inside one process**, by seeking back to offset 0. One
    drain per process left a spread that swamped every effect worth
    measuring -- the 2.8x swing this file used to record as a reason to
    distrust the owned-path numbers was the harness, not the hardware.
  - **A stalled repeat is discarded, not averaged.** Every peer drains with
    a **zero** timeout and counts empty returns; with the topic already in
    the local queue there cannot be one before EOF, so a non-zero count
    means that repeat went to the network. That is a detector, and it is
    what makes the numbers trustworthy rather than the prefetch pause having
    been guessed correctly.
  - **The plan is interleaved across rounds**, so drift penalises every
    configuration equally.

  **Cross-binary comparisons are not trustworthy. Interleaved in-binary ones
  are.** Two builds running the *same* work measured 143 ns/record apart.
  Every A/B behind the changes below was done with both variants compiled
  into one binary and run interleaved; a decomposition split across builds
  produced a variant doing strictly less work that measured slower, which is
  the artefact this file previously blamed on the hardware. Do not reopen
  any of these questions with a before-and-after across two builds.

  What reproduces, over two full runs:

  - **`consume_borrowed()` is 85-87% of C** -- +23.6 ns a record over
    librdkafka driven directly. The tightest number in the table.
  - **`consume_borrowed()` is ~2x `rust-rdkafka`'s `BorrowedMessage`** (1.99x,
    2.07x), **but most of that is the API, not the language**:
    `rust-rdkafka` exposes no batch consume, so it polls per record, and the
    C peer prices that at ~111 ns a record (`c batch` 129.2 against `c poll`
    240.0). Measured against C doing *the same* access pattern the honest
    figure is +23.6 ns for us against +76.3 ns for Rust. Quote that one.
  - **`consume()` beats `confluent-kafka`** 1.26x and 1.21x, and
    `rust-rdkafka`'s `.detach()` 1.07x and 1.15x. Modest, consistent in
    direction.
  - **`consume(headers=False)` against `confluent-kafka`** measured 2.06x and
    1.50x -- direction solid, magnitude not. Say 1.5x.

  **`confluent-kafka` does not load the same librdkafka, and this file used
  to claim it did.** Its manylinux wheel bundles its own build
  (`librdkafka-c87086af.so.1` in `confluent_kafka.libs`); the version string
  matches and the build need not. The Rust peer is pinned to the conda
  library on purpose so that at least one cross-client row really is
  binding-against-binding.

  **`confluent-kafka` does *not* defer `rd_kafka_message_headers`, and this
  file used to claim it did.** Counted with an `LD_PRELOAD` interposer:
  **200,001 calls for 200,000 records**. It is eager, exactly as we are, so
  that crossing is not a handicap we carry and it does not explain any gap.
  What it *is* is expensive -- see the next bullet.

- **`copy_bytes` is one `memcpy`, not `List(Span)`.** The stdlib
  constructor appends byte by byte, and every owned key, value and header
  value goes through `copy_bytes`, so it was most of the copy cost of the
  owned paths. Measured paired in one process, alternating old and new per
  repeat, over two independent runs: `consume()` 0.89x then 0.93x of the
  old cost (7/10 and 6/10 pairs faster), `consume(headers=False)` 0.87x
  then 0.91x (6/10, 7/10). `poll_event(headers=False)` measured 0.85x then
  1.03x -- no signal on that path, where the per-record topic `String`
  allocation and `rd_kafka_consumer_poll` dominate. On a throttled laptop;
  ratios only. Two things measured alongside and **not** shipped:
  dropping `raises` from the `BorrowedMessage` accessors and the `_load_*`
  helpers (`_DLCallable.__call__` does not raise, so it is legal) measured
  1.17x, faster in 2/10 pairs -- noise, not a win; and `String.copy()`
  **shares its buffer** while `List.copy()` does not, so `Message.topic`
  copies are cheap and the poll path's extra cost is *building* a `String`
  per record, which is an allocation and therefore not something to judge
  on a throttled machine (see "Settle the numbers").

- **The headers crossing costs ~117 ns a record even when the record has
  none, and that is librdkafka's cost, not ours.** Measured in plain C with
  no binding in the picture: `c batch` 129.2 ns a record against
  `c batchhdr` 267.8. Our own wrapper around it adds only ~18 ns. No binding
  can optimise this away; the only way not to pay it is not to call it,
  which is what `consume(headers=False)` is for. It is a third of what the
  owned decode does.

- **`consume()` decodes straight into the `List[Message]` it returns.** It
  used to call `consume_events()` and deep-copy every `Message` out of the
  `PollEvent` wrapper, and that was worth **1.7x**. Do not route it back
  through `consume_events()` for the sake of sharing the loop.

  The cost is specifically **materialising a `PollEvent` into a `List`**, not
  the `PollEvent` itself. Both variants in one binary: `poll()` decoding
  straight into its `Optional` measured 733.7 ns against 720.7 through the
  `PollEvent` -- **no gain**, because one that never reaches a container is
  elided. So `poll()` is still written on `poll_event()` deliberately, and
  the comment there says so; the same change that bought `consume()` 1.7x
  buys `poll()` nothing.

  The consequence is uncomfortable and is written up in
  `benchmarks/README.md`: `consume_events()` (777.4 ns) is now **slower than
  `consume()`** (431.6 ns) while doing strictly less, and `poll_event()`
  (830.9) is the slowest thing here against a C poll of 240.0. Making
  `PollEvent` cheap to store is the open piece of work.

- **The fetch buffer lives on the `Consumer`, not in each call.**
  `_fetch_slots` hands out one `List[Int]` that only grows, shared by all
  three batch entry points -- safe precisely *because* they all hold
  `_batch` for the whole fetch, so the latch that refuses a concurrent
  `consume()` is also what makes one buffer enough.

- **`MessageBatch` holds a `_Freer`, not a `Lib`, and the difference is
  ~19.4 us per `consume_borrowed()` call.** A `Lib` resolves 76 symbols and
  `dlsym` measures ~350 ns each here, so constructing one costs ~20 us
  against ~850 ns for the `dlopen` alone -- and it was being built **per
  fetch**. That is 19 ns a record at a batch of 1000 and 1.9 us a record at
  a batch of 10, i.e. it punished exactly the small low-latency batches the
  zero-copy path is best at. Measured after the fix: 194 ns/record at
  batch=10, against roughly 2.1 us before.

  `_Freer` keeps its own `OwnedDLHandle` and that is the point of it
  existing rather than a bare function pointer -- a `MessageBatch` may
  outlive the `Consumer` that made it, and the handle is what keeps
  librdkafka mapped until the last message is freed. A raw address would be
  a use-after-unload the compiler cannot see.

- **`Consumer.reached_end()` is how `consume()` reports end-of-partition.**
  `consume()` drops the EOF mark the way `poll()` does, so without it a
  bounded drain cannot tell "finished" from "nothing right now" and has to
  burn a whole `timeout_ms` guessing. It reflects the most recent batch call
  only. `test_consume_reports_reaching_the_end_of_a_partition` asserts
  **both** directions, and needs `n=1` to do it: with a batch big enough to
  hold the topic the records and the EOF mark arrive in the same call, and
  the "false while records are still coming" half can never fail.

- **`consume()` and `consume_events()` have the *same* error policy, and the
  note here used to say they did not.** Both raise only when the batch held
  nothing usable, and both drop a hard error that arrived alongside good
  records -- the control plane's policy, that one bad entry must not hide the
  good ones beside it. Neither produces an entry for a hard error, so
  `consume_events()` gives no per-entry verdict for one either; its own
  docstring was right about this ("nothing at all where a hard error was
  decoded") while `consume()`'s was not. What actually separates the two is
  **end-of-partition marks** -- `consume_events()` returns them as entries,
  `consume()` drops them and reports them through `reached_end()` -- and
  cost, which is ~1.7x.

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

- **`rd_kafka_topic_name` is resolved once per run of records sharing a
  handle, and stored per record** (`_Lent`), via `Lib.topic_name_ptr` which
  does *not* copy into a `String` -- the pointer belongs to the topic handle
  and outlives every message in the batch. It used to be resolved once per
  *batch*, from the first record, which labelled every record of a
  multi-topic subscription with the first topic;
  `test_a_borrowed_batch_names_each_records_own_topic` is the guard, and
  the three older borrowed tests read one topic each, which is why none of
  them saw it. The owned path's `topic_name` still copies; both exist
  deliberately.

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

**Everything previously listed here is built**, and the two benchmark items
that stood here are done: the `rust-rdkafka` peer exists, and the harness was
rebuilt to the point where the numbers reproduce. See "Already built" and
`benchmarks/README.md` before touching any of it. The queued `PollEvent`
item is now done too -- see the section below for what worked and, more
usefully, for the four things that were measured and rejected. What is left
is three candidates.

### `PollEvent` was made cheaper to store -- and most of the plan was wrong

**Done.** The queued item is closed, but almost none of it closed the way
the write-up here predicted, so what follows is the measurements rather than
the plan they replaced. Every number is from variants compiled into **one
binary** and interleaved, which is the only comparison this project trusts.

**What shipped, and it is three lines.** `_consume_locked` appends an empty
`PollEvent` and then *constructs the real one into that slot* through
`unsafe_write`, instead of building a 208-byte `PollEvent` on the stack and
moving it in:

    out.append(PollEvent())
    out.unsafe_ptr().unsafe_offset(len(out) - 1).unsafe_write(
        PollEvent(self._decode(raw, last_name))
    )

Measured three times: **-66, -112 and -74 ns a record**. Direction solid,
magnitude 66-112 -- so `consume_events()` is now ~1.6x `consume()` where it
was ~1.8x. Three things make it sound and all three are in the comment at
the site: the list cannot reallocate (`capacity=count`, at most `count`
appends), the overwritten placeholder owns nothing so skipping its
destructor leaks nothing, and a decode that raises **pops the placeholder**
-- without that it survives as a phantom timeout entry, which is a
correctness bug the first draft had.

**Four things were measured and rejected. Do not retry them without reading
this.**

- **Assigning into the slot through a `ref`** (`slot.message = ...`) is
  worth **nothing** -- 786.9 against 781.0, i.e. noise. A move-assign has to
  destroy the old value first, so the win is specifically
  *construction-in-place*, not avoiding the `append`.
- **A lazy container was *worse* than the fix that shipped**: 713.1 against
  673.3 in the same run. Storing `List[Message]` (144 bytes) plus a one-byte
  tag per entry and building the `PollEvent` on demand -- reverse-stored so
  `pop()` yields forward order, which is safe and needs no unsafe code -- is
  a real 64-bytes-an-entry saving that the move-out at access time more than
  gives back. **So the footprint was never the problem**, which is what the
  earlier note here assumed. It also means no API change is needed: the fix
  that won is source-compatible and keeps `List[PollEvent]`.
- **The same trick applied to `consume()` makes it slower** -- 467.9 against
  426.9. `Message` has no cheap default, so the placeholder costs an empty
  `String` and an empty `List`, and `append(self._decode(...))` was already
  being built in place. The trick is worth something only where the element
  is big *and* has a free default.
- **`poll_event()`'s slowness is not `rd_kafka_topic_name`.** That was the
  recorded first suspect and it is **false**: caching the name and its
  `String` across calls on the consumer measured **1119.3 against 904.3 --
  a 215 ns regression**. The hypothesis is now tested; do not re-run it.

**What did help `poll_event()` was giving it the escape hatch `consume()`
already had.** `poll(headers=False)` / `poll_event(headers=False)` skip the
`rd_kafka_message_headers` crossing, measured at **-75, -157 and -212 ns a
record** across three runs -- direction solid, magnitude not. That crossing costs ~117 ns even for a record with
no headers and the cost is librdkafka's, not ours, so not calling it is the
only way not to pay it. `test_poll_without_headers_keeps_every_other_field`
in the mock suite is the guard, and it compares both decodes field by field
because a flag that quietly dropped a key would be trading correctness for
the time.

**What is left, and why it was not taken.** `consume_events()` still costs
~1.6x `consume()`, and the remainder really is the 208-byte
`Optional[Message]` + `Optional[TopicPartition]` pair -- that is the floor
for a *stored* `PollEvent`, measured: `Message` is 144, `Optional[Message]`
152, `TopicPartition` 48, `Optional[TopicPartition]` 56, `PollEvent` 208.
Going below it means changing what the two public fields hold, which is a
breaking change to a public type and a decision about the API rather than a
performance question. The lazy-container result above says it would buy less
than it looks like anyway.


### The last quarter: the road to v1.0

Written 2026-09-05. The stated goals of the upstream scaffold are met and
passed; what separates this package from "production-credible" is
operational hardening, not features. This section is the whole plan, in
execution order, and it is written so a session with no other context can
run it. **Do the items in order, one commit per item, and run the gates
named on each before committing.** The four gates are `pixi run lint`,
`pixi run test`, `pixi run test-mock`, and where an item says so
`pixi run broker-up && pixi run test-broker` and
`pixi run -e interop test-interop`.

Two things about the machine before starting. Read "Settle the numbers"
above: check `docker inspect --format '{{.State.Health.Status}}'
mojo-kafka-broker` says healthy, and do not quote an absolute benchmark
number from this laptop while it is throttled. Nothing in this plan needs
a benchmark; the paired `a:b` bench mode exists if one is wanted.

Two items need Max's decision and are marked. Everything else is
pre-approved: build it.

**1. Observability: errors, fatal errors, logs, stats.** *The biggest
gap.* Today a failure on a librdkafka background thread -- all brokers
down, an auth rejection, a fenced producer -- reaches the application
only if a `poll()` or `flush()` happens to report it, and there is no
metrics hook at all. A production job cannot be run blind.

- *librdkafka surface*, all set on the conf before `rd_kafka_new`, all
  **served on the thread that polls**, which is what makes touching Mojo
  state from them safe, exactly as the `dr_msg_cb` is:
  `rd_kafka_conf_set_error_cb(conf, void(*)(rd_kafka_t*, int err, const
  char* reason, void* opaque))`; `rd_kafka_conf_set_stats_cb(conf,
  int(*)(rd_kafka_t*, char* json, size_t len, void* opaque))`, which must
  **return 0** so librdkafka frees `json` -- copy it first; and
  `rd_kafka_conf_set_log_cb(conf, void(*)(const rd_kafka_t*, int level,
  const char* fac, const char* buf))`, which carries **no opaque** -- reach
  it with `rd_kafka_opaque(rk)` -- and which is called from **arbitrary
  threads unless `log.queue=true`**. Set `log.queue=true` whenever a log
  hook is installed, so logs are served through the poll path like the
  other two; for a consumer, `poll_set_consumer` already forwards the main
  queue to the consumer queue, so `consumer_poll` and
  `consume_batch_queue` serve all three. Bind
  `rd_kafka_fatal_error(rk, errstr, size)` too: an `error_cb` carrying
  `RD_KAFKA_RESP_ERR__FATAL` (-150) is a *notification*, and this call is
  what returns the underlying error. Stats need `statistics.interval.ms`
  on the conf; add `statistics_interval_ms: Int = 0` to both configs.
- *API shape*: **retain, do not callback**, matching `failures()` /
  `take_failures()`. A thin `abi("C")` handler cannot capture anything, so
  a callback API would force every user into the `setenv` tricks the lost-
  rebalance tests use. Add to both clients: `errors() -> List[KafkaError]`
  and `take_errors()`, bounded to the most recent 256 with a
  `dropped_errors() -> Int` counter; `fatal_error() -> Optional[KafkaError]`
  reading `rd_kafka_fatal_error`; `latest_stats() -> Optional[String]`,
  the last JSON document; and `logs()` / `take_logs()` returning a
  `List[LogLine]` (`level: Int32`, `facility: String`, `message: String`),
  bounded the same way, installed only when `ProducerConfig` /
  `ConsumerConfig` has `capture_logs=True` because it forces
  `log.queue=true`. The trampolines follow `_delivery_trampoline`
  exactly: decode everything **before** acquiring the latch, never raise
  under it, and the state lives in the existing heap boxes -- `_DrState`
  for the producer and `_RebalanceState` for the consumer, since each
  client has one conf opaque and it already points there. Do not add a
  second opaque.
- *Tests* (smoke, no broker, dead port `127.0.0.1:9`): a producer that
  produces once and polls for 1500 ms has `errors()` containing a
  `KIND_TRANSPORT` entry; a consumer built with
  `statistics_interval_ms=100` and polled for 500 ms has `latest_stats()`
  containing `"name"`; with `capture_logs=True` and `log_level=7`, `logs()`
  is non-empty and every entry has a non-empty facility. Extend
  `test_a_fatal_transaction_error_is_flagged_fatal` (mock): after the
  injected `CLUSTER_AUTHORIZATION_FAILED`, `producer.fatal_error()` is
  present with code 31. Assert on `errors()` never being longer than 256
  after a 1000-error burst, and that `dropped_errors()` counts the rest.
- *Done when* the four calls exist on both clients with docstrings, the
  README gains a "Running in production" section naming them, and the
  gates pass. Run `test-broker` too: the callback conf changes touch every
  client construction.

**2. The small API holes.** All cheap, all things an operator asks for
in week one.

- `Consumer.commit(offsets: List[TopicPartition], asynchronous=False)`,
  explicit offsets through `_build_tpl` and the already-bound `Lib.commit`.
  Offsets are the **next** offset to read, as `position()` reports; say so
  in the docstring, it is the same trap `send_offsets_to_transaction` has.
- `Consumer.store_offsets(offsets)` over `rd_kafka_offsets_store` (new
  binding), which requires `enable.auto.offset.store=false`; add that as
  `enable_auto_offset_store: Bool = True` on `ConsumerConfig`. Per-
  partition verdicts come back in the list: use `_control`'s shape, and
  raise on the first error the way `seek` does, since this call returns
  nothing.
- `Consumer.assignment()` and `Consumer.subscription()` over
  `rd_kafka_assignment` / `rd_kafka_subscription`, both of which hand back
  a caller-owned list: decode with `_decode_tpl`, destroy on every path.
- `ProducerConfig.drain_timeout_ms: Int = 5000`, the wait `__deinit__`
  makes today with a hard-coded 5000. Keep the default.
- *Tests* (mock): commit explicit offsets on one partition, then
  `committed()` reports them and `position()` is unchanged; `store_offsets`
  then `commit()` commits the stored value and not the position; after
  `subscribe(["a","b"])` and one poll, `subscription()` has both names and
  `assignment()` has both partitions. Every assertion on a list checks
  entry `i` is partition `i`, as `test_position_walks_every_partition`
  does.

**3. Soak and leak.** *The claim "no leak per message" has never been
tested.* A leak in the headers walk or the `PollEvent` slot trick would
show only after hours.

- Add `integration/soak.mojo` and a **local-only** task `pixi run soak`.
  It runs each path for a duration given on the command line (default
  120 s each; 600 s is the number to quote) against `MockCluster`, with a
  producer refilling the topic: `poll`, `poll_event`, `consume`,
  `consume_events`, `consume_borrowed`, produce with three headers, and a
  transaction loop of `begin / produce / send_offsets / commit`. Every
  record read is checked against its key, so a decode that goes wrong
  under load is a failure and not a leak.
- Measure with `getrusage(RUSAGE_SELF).ru_maxrss` through libc, loaded the
  way `_open_libc` in the smoke suite loads it for `pthread_create` -- it
  is portable to macOS where `/proc` is not, and peak RSS is enough
  because a leak is monotonic. Units differ: kilobytes on Linux, bytes on
  macOS; normalise. Sample every 10 s after a 20 s warm-up; **fail** if
  the last sample exceeds the first post-warm-up sample by more than 10%
  and more than 16 MB. Print the table either way.
- Also bind `rd_kafka_outq_len`-style counters that already exist and
  print `rd_kafka_outq_len(rk)` at each sample for the producer: a queue
  that grows without bound is the other way a long run dies.
- Run it once under `valgrind --leak-check=full --error-exitcode=1` with
  a 20 s duration (`apt install valgrind` if absent; skip on macOS), and
  record the result -- pass or the exact leak -- in this file under
  "Known bugs". librdkafka itself reports a few "still reachable" blocks
  at exit; only "definitely lost" counts.
- *Done when* a 600 s run of every path passes the RSS rule, the
  valgrind result is recorded here, and the README's status section says
  so with the date.

**4. SASL and SSL.** librdkafka does all of it; what is missing is proof
that the conda build has it and that the configuration path through
`set()` works.

- Bind `rd_kafka_conf_get` and expose `kafka.builtin_features() -> String`
  (the `builtin.features` property: on a full build it contains `ssl`,
  `sasl_gssapi`, `sasl_plain`, `sasl_scram`, `sasl_oauthbearer`). A
  smoke test asserts `ssl` and `sasl_scram` are present, so a librdkafka
  built without them fails the suite rather than the first production
  connect.
- Smoke test for SSL without a broker: `security.protocol=SSL` plus
  `ssl.ca.location=/nonexistent` must fail at construction with an error
  naming the CA file -- that proves the OpenSSL path is compiled in and
  reached.
- Real-broker test for SASL: add a second listener to
  `integration/docker-compose.yml` on **9093** with
  `SASL_PLAINTEXT` / `PLAIN`, user `mojo` password `mojo-secret`, using
  the `apache/kafka` image's `KAFKA_*` environment (listener name map,
  `KAFKA_SASL_ENABLED_MECHANISMS=PLAIN`, and the
  `KAFKA_LISTENER_NAME_SASL_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG` line). Keep
  9092 plaintext so every existing test is untouched. Add
  `test_sasl_plain_round_trip` to `test_broker.mojo`: produce and consume
  one record through 9093 with `security.protocol`, `sasl.mechanism`,
  `sasl.username`, `sasl.password` set through `set()`, and a second case
  asserting a wrong password surfaces as `KIND_AUTHORIZATION` in
  `errors()` from item 1. SCRAM needs the broker to hold credentials
  created with `kafka-configs.sh`; PLAIN proves the plumbing and is
  enough for v1.0. SSL against a real broker needs certificate generation
  in compose and is **not** in this plan; say so in the README.
- README: a "Security" section listing the four keys and the
  `builtin_features()` check.

**5. A nightly integration workflow. Needs Max's decision.** This file
says do not add Docker to `.github/workflows/ci.yml`, and that stands:
the PR gate stays Docker-free. The proposal is a **separate**
`.github/workflows/integration.yml` on `ubuntu-latest` only, on
`schedule` (nightly) and `workflow_dispatch`, not required for merging,
that runs `broker-up`, `test-broker`, `test-interop` and item 4's SASL
case, and `soak` with a 60 s duration. GitHub-hosted Ubuntu runners have
Docker. This turns "nothing but you runs the Docker suites" into "they
run every night and someone reads the badge". Ask before adding it, and
add the badge to the README if the answer is yes.

**6. Docs and the release. The version number needs Max's decision.**

- README: the "Running in production" section from item 1, covering
  shutdown order (`flush()` then drop the producer; `close()` then drop
  the consumer, and why), what to do on `fatal_error()` (destroy and
  rebuild the client -- librdkafka will not recover it), the thread model
  in three sentences, `errors()` / `latest_stats()` / `logs()`, and the
  security keys. Rewrite the status section: what has been soaked, for
  how long, on what date, and what is still local-only.
- `CHANGELOG.md`: move "Unreleased" under a version heading and
  `pixi.toml`'s `version` to match. Propose `v0.3.0` as the release
  candidate and `v1.0.0` after a soak on a real workload of Max's; ask
  which. `.github/workflows/release.yml` already exists: read it before
  tagging, and tag only when told to.
- Update the "Status" bullets at the top of this section of `CLAUDE.md`
  as each item lands, so the next session sees what is left.

**Definition of done for v1.0**: items 1-4 landed with their tests; the
soak result recorded; the README production and security sections
written; a decision recorded on items 5 and 6; all four gates green plus
`test-broker` and `test-interop` on the day of the tag.

**Status**: nothing started.

### Candidates, none started

- **A borrowed producer path.** `produce()` copies its key and value through
  `RD_KAFKA_MSG_F_COPY`. The consume side is now measured properly and the
  borrowed view gives up only ~24 ns a record against C, so the produce side
  is the remaining copy. Do the measurement before the work -- `F_COPY`
  exists so the caller's buffer can go out of scope immediately, and taking
  that away is a real API cost that a number should justify.

- **Settle the numbers somewhere quiet.** The table is from a WSL2 laptop
  sharing a kernel with the broker. The harness now detects a contaminated
  run rather than averaging it, and the tight rows (`c batch`,
  `mojo borrowed`) reproduce to within a few per cent -- but the
  `consume(headers=False)` cross-client ratio still swings 1.50-2.06x. A
  quiet machine with `--rounds 4 --repeat 9` would turn that range into a
  number.

  Two things found on 2026-09-05 that make "quiet" a precondition rather
  than a nicety. **The compose healthcheck is a port probe now**:
  `kafka-topics.sh --list` took 32 s here, Docker killed it at the 5 s
  timeout and started another every 10 s, the container never reported
  healthy, and the JVM storm made every peer -- `c batch` included --
  10x its table figure. And **a throttled CPU is not a scaled-down one**:
  at ~0.8 GHz effective (Balanced plan) a plain C `malloc`+`free` measured
  312 ns and a vDSO `clock_gettime` 415 ns, ~10x normal, so anything that
  removes an allocation looks far better than it is. Check
  `taskset -c 6` on a dependent multiply-add loop before trusting an
  absolute number, and on such a machine compare only paired, in one
  process, alternating variants per repeat. The stall detector cannot see
  this: a slow machine never empties the queue.

- **A borrowed path for the owned decode's two copies.** `consume()` spends
  ~146 ns a record on the key and value `malloc`+`memcpy` pair. An arena per
  batch would cut it, but `Message` owns its `List[UInt8]`s by contract, so
  this is a type change and not a local one. Measure before designing.

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

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Nothing yet.

## [0.3.0] — 2026-09-05

### Added
- **Observability on both clients: `errors()` / `take_errors()` /
  `dropped_errors()`, `fatal_error()`, `latest_stats()`, and `logs()` /
  `take_logs()` / `dropped_logs()`.** Errors from librdkafka's background
  threads — brokers down, authentication rejected, a fenced producer — used
  to reach the application only if a `poll()` or `flush()` happened to
  report one, and there was no metrics hook at all. The three librdkafka
  callbacks (`error_cb`, `stats_cb`, `log_cb`) are now installed on every
  client and **retain** what arrives rather than calling back, matching
  `failures()` / `take_failures()`: a Mojo handler reached from C captures
  nothing, so a callback API would have pushed every user into `setenv`
  tricks. Bounded to the most recent 256 with a counter for what was
  dropped. `fatal_error()` reads `rd_kafka_fatal_error`, the one call that
  returns the *underlying* error after the callback's generic `KIND_FATAL`
  notification. New config fields: `statistics_interval_ms` (default 0,
  never) and `capture_logs` (default off, because it forces
  `log.queue=true`). New public type `LogLine`.

  One trap found on the way and written down: `log.queue=true` on its own
  delivers nothing — it parks lines on a queue no poll serves until
  `rd_kafka_set_log_queue(rk, NULL)` joins it to the main one, which is the
  call `confluent-kafka` makes too.

- **The small API holes an operator hits in week one.**
  `Consumer.commit(offsets, asynchronous=)` commits explicit offsets — the
  *next* offset to read, as `position()` reports, the same convention
  `send_offsets_to_transaction()` uses. `Consumer.store_offsets(offsets)`
  over `rd_kafka_offsets_store`, with `enable_auto_offset_store` on
  `ConsumerConfig` (default `True`, matching librdkafka), so a job can commit
  what it has finished rather than what it has fetched; per-partition errors
  raise on the first, as `seek()` does, since the call returns nothing.
  `Consumer.assignment()` and `Consumer.subscription()` over the two
  control-plane calls that hand back a caller-owned list, decoded and
  destroyed on every path. `ProducerConfig.drain_timeout_ms` (default 5000)
  names the wait the destructor was making with a hard-coded 5000.

- **SASL and SSL, proven rather than assumed.** `kafka.builtin_features()`
  over `rd_kafka_conf_get` reports what the loaded librdkafka was built
  with, and the smoke suite asserts `ssl`, `sasl_plain` and `sasl_scram`
  are in it, so a build without them fails the suite rather than the first
  production connect. A second smoke case proves the OpenSSL path is
  reached at construction: `security.protocol=SSL` with a CA file that does
  not exist raises from `Producer(cfg)` naming the file. The compose broker
  gained a `SASL_PLAINTEXT` / `PLAIN` listener on 9093 (user `mojo`), and
  the broker suite round-trips a record through it and asserts a wrong
  password surfaces in `errors()` as `KIND_AUTHORIZATION`. README gains a
  "Security" section. SSL against a real broker needs certificate
  generation in compose and is deliberately not set up.

- **A soak harness**, `integration/soak.mojo` and `pixi run soak`: every
  consume path, produce with three headers and a read-process-write
  transaction loop, each for a given number of seconds, in rounds of a fixed
  record count against a fresh mock cluster per round, every record checked
  against its key, peak and current RSS sampled every 10 s. It caps glibc's
  malloc arenas itself, because the thread churn of starting clients per
  round otherwise grows RSS for minutes with no leak anywhere — measured,
  and written up in CLAUDE.md. `Producer.queue_length()` over
  `rd_kafka_outq_len` is printed beside RSS. Every path passes at 30 s per
  path; the 600 s run and a valgrind pass are still to do before `v1.0`.

- **`Consumer.poll(headers=False)` / `Consumer.poll_event(headers=False)`**
  skip the `rd_kafka_message_headers` crossing, the same escape hatch
  `consume()` already had. Measured interleaved in one binary three times at
  **-75, -157 and -212 ns a record**: the crossing costs that even for a
  record with no headers, because the cost is librdkafka's rather than this
  binding's, so not calling it is the only way not to pay it. Default stays
  `True`. `Message.headers` comes back empty for every record when it is
  off, exactly as `consume(headers=False)` leaves it.

- **A four-peer consume benchmark**: C, `rust-rdkafka`, this package and
  `confluent-kafka`, all reading one topic and all asserting on the same
  payload checksum. `run.py` builds the C and Rust peers itself and skips
  either if the toolchain is missing. The C peer is the ceiling — nothing
  calling librdkafka can beat it — and the Rust peer is what makes the
  zero-copy claim falsifiable, since beating a copying Python client proves
  nothing about a span.

- **`Consumer.consume(n, headers=False)`** skips reading record headers.
  `rd_kafka_message_headers` costs ~117ns per record *even when the record
  has none* — measured in plain C, so it is librdkafka's cost and no binding
  can optimise it away. It is roughly a third of the owned decode. Default
  stays `True`, so nothing changes for a caller who does not ask.

- **`Consumer.reached_end()`** reports whether the last batch call ran off
  the end of a partition. `consume()` drops the EOF mark the way `poll()`
  does, so a bounded drain previously could not tell "finished" from
  "nothing right now" and had to burn a whole `timeout_ms` guessing.
  `MessageBatch.reached_end()` already answered this for the borrowed path.

### Changed
- **`consume_events()` is ~10-15% faster**, and costs ~1.6x `consume()`
  where it was ~1.8x. Its entries are now **constructed into the list's
  slot** rather than built on the stack and moved in — a `PollEvent` is 208
  bytes, measured, against a `Message`'s 144. Interleaved in one binary
  three times: **-66, -112 and -74 ns a record**. No API change: the same
  `List[PollEvent]` comes back with the same entries in the same order.

  Four alternatives were measured and rejected, and they are written up in
  `benchmarks/README.md` so nobody retries them: a lazy container that
  stores `Message`s and builds the `PollEvent` on demand is *worse* (so the
  208-byte footprint was never the problem); assigning into the slot through
  a `ref` is worth nothing; the same trick on `consume()` is worse; and
  caching `rd_kafka_topic_name` across `poll_event()` calls — the long-
  recorded suspect for that path's cost — is a **215 ns regression**.

- **`consume()` is ~1.7x faster.** It decoded through `consume_events()` and
  then deep-copied every `Message` out of the `PollEvent` wrapper it did not
  want; it now decodes straight into the `List[Message]` it returns. The cost
  was specifically *materialising a `PollEvent` into a `List`* — a
  `PollEvent` that never reaches a container is elided by the compiler, which
  is why the same change applied to `poll()` measured 733.7ns against
  720.7ns and was reverted rather than kept.

- **`consume_borrowed()` no longer costs ~19.4µs per call.** `MessageBatch`
  held a `Lib`, and constructing one resolves 76 symbols at ~350ns each. It
  now holds a `_Freer`, which resolves one. That was 19ns a record at a batch
  of 1000 and ~1.9µs a record at a batch of 10 — it punished exactly the
  small low-latency batches the zero-copy path is best at. At batch=10 the
  path went from roughly 2.1µs a record to 194ns.

- **The batch fetch buffer lives on the `Consumer`** rather than being
  allocated and zero-filled on every call. Safe to share between all three
  batch entry points because they all hold the `_batch` latch for the whole
  fetch.

- **The benchmark harness was rebuilt**, because the old one could not
  measure what it claimed to. Repeats now happen inside one process by
  seeking back to offset 0; each peer drains with a **zero** timeout and
  counts empty returns, so a repeat that went to the network mid-drain is
  *detected and discarded* rather than averaged in; and the plan is
  interleaved across rounds so machine drift penalises every configuration
  equally. The 2.8x swing the notes previously blamed on the hardware was the
  harness.

### Fixed
- **Two claims in the project notes were wrong, and are corrected.**
  `confluent-kafka` does **not** defer `rd_kafka_message_headers` to
  `msg.headers()` — an `LD_PRELOAD` interposer counted 200,001 calls for
  200,000 records, so it is eager exactly as we are. And it does **not** load
  the same librdkafka build: its manylinux wheel bundles
  `librdkafka-c87086af.so.1` rather than the conda-forge library. The version
  string matches; the build need not. The Rust peer is pinned to the conda
  library so that at least one cross-client row is genuinely
  binding-against-binding.


### Fixed
- **Dropping a `Consumer` without calling `close()` segfaulted.** Every
  consumer, subscribed or not, whether or not it had rebalance handlers.

  `Consumer.__deinit__` calls `rd_kafka_consumer_close`, which fires one last
  revoke through the rebalance trampoline — and the trampoline reaches the
  consumer's state by the raw address handed to `rd_kafka_conf_set_opaque`.
  Mojo releases each field at its last use **inside the destructor body**,
  not after the body returns, so a `_rebalance` box that the body never
  mentions was already freed by the time `consumer_close` ran, and the
  callback read dangling memory.

  The whole test suite missed this because all 21 cases closed by hand.
  `test_dropping_a_subscribed_consumer_does_not_fault` now covers the
  destructor path; it faults 3 runs out of 3 with the fix reverted.

  `Producer` had the same shape and survived only by accident — its
  `_drain_until_empty` takes `mut self`, which borrows the whole struct and
  so happens to cover its `_dr` box. Both are now pinned explicitly.

- **Concurrent `Consumer.close()` calls deadlocked the process.** `close()`
  read a `Bool` and then set it, so every thread calling it could pass the
  check before any of them set it. Sequentially a second
  `rd_kafka_consumer_close` merely returns -197, which made this look
  cosmetic; concurrently it is not. Measured with 8 threads: all 8 reach the
  C call, exactly one returns, and the other 7 never do.

  The flag is now claimed with a compare-exchange, so exactly one caller
  reaches librdkafka and the losers return immediately — documented as such,
  since leaving a group is a network round trip and there is no blocking
  primitive in Mojo 1.0 to wait on properly.
  `test_racing_close_calls_close_the_consumer_exactly_once` guards it and
  needs no broker. Note its failure mode: a regression **hangs** the run
  rather than failing it, 3 times out of 3.

- **`rd_kafka_new` failures reported a corrupted message.** The error buffer
  was decoded after the intervening `rd_kafka_conf_destroy` call, by which
  point Mojo had released it and the stack slot had been partly overwritten,
  so a misconfiguration surfaced as

      rd_kafka_new: \xef@\xb71      @st be >= `session.timeout.ms`

  instead of ``​`max.poll.interval.ms`must be >= `session.timeout.ms` ``. The
  tail survived, which made it look like an encoding problem rather than a
  lifetime one. The text is now decoded before the destroy.

- **`AdminClient.create_topic()` faulted instead of raising** when
  `rd_kafka_queue_new` returned NULL — `rd_kafka_CreateTopics` dereferences
  the queue. `Consumer.subscribe()` gained the matching NULL check on
  `rd_kafka_topic_partition_list_new`.

### Changed
- **`Producer` is now safe to drive from more than one thread.** `produce()`,
  `produce_bytes()`, `poll()`, `flush()`, `failures()`, `take_failures()` and
  `delivery_failures()` may all be called concurrently on one producer.

  Two pieces of Mojo state were unsynchronised. The sequence counter is now
  an `Atomic` — a plain `+= 1` loses increments under contention, and two
  messages sharing a token mis-attributes their delivery reports to each
  other, which is a correctness bug rather than a cosmetic one. The failure
  list is now guarded by a spinlock built on `Atomic`, since Mojo 1.0 ships
  no mutex; the delivery-report callback runs on whichever thread called
  `poll` / `flush`, so several threads draining are several writers to it.

  `last_error_kind()` is the one thing that does not become thread-safe,
  and cannot: it is a single slot on the producer, so with two threads
  producing a caller can read the other thread's rejection. That is not a
  data race — the slot is atomic — but it is not attributable either. Branch
  on `DeliveryReport.kind()`, which names its message.

  `test_concurrent_produce_keeps_every_sequence_and_report` drives eight
  real threads through `pthread_create` (Mojo 1.0 has no thread API, and MAX
  is deliberately not a dependency here) and asserts every token is distinct
  and every report survives. It fails 6 runs out of 6 with the
  synchronisation reverted.

- **`Consumer` is safe to drive from more than one thread too**, now that
  `close()` is fixed. Its reading calls — `poll()`, `poll_event()` and the
  control plane — hold no mutable Mojo state and were always fine.
  `subscribe()` now writes the three rebalance handler slots under a latch
  the trampoline reads them through; that one is a **reasoned** fix rather
  than a measured one, and the docstring says so: the slots are aligned and
  pointer-sized, so what the latch actually excludes is a torn *set* — the
  new `on_assign` paired with the previous `on_revoke` — not a torn pointer.

  This does not make one `Consumer` a work-sharing primitive: two threads
  polling one consumer split a single assignment's records between them,
  which is rarely the intent. One consumer per thread, or more members in
  the group.

  The lock itself moved to a new internal `_sync.mojo` now that both clients
  need it, with the two rules that keep every use correct: no FFI call
  inside a critical section, and never raise while holding one.

- **Delivery reports now go through a `dr_msg_cb`** rather than librdkafka's
  event queue. No API change — `failures()`, `take_failures()` and
  `flush()`'s raise-on-rejection behave exactly as before — but the producer
  and the consumer's rebalance handling now use the same mechanism, and
  `flush()` is `rd_kafka_flush()` instead of a hand-written drain loop.

  The event queue was never the preferred design; it was a workaround for
  pre-1.0 Mojo being unable to hand C a function pointer. Measured A/B over
  200k messages, throughput is indistinguishable — the run-to-run spread is
  far wider than the difference between the two paths.

### Changed
- **Rebalance handler slots are single atomic words, and the lock around them
  is gone.** `subscribe()` writes three handler slots that the rebalance
  trampoline reads on another thread. They were `Optional[RebalanceHandler]`
  guarded by a spinlock, justified by the recorded reasoning that a slot was
  "pointer-sized and aligned" and so could not tear.

  Measured, that was wrong: `Optional[RebalanceHandler]` is **16 bytes** — a
  discriminant plus the pointer — against 8 for the bare handler. An
  unguarded read could therefore pair `has_value = True` with the other
  value's payload and call through it, which is worse than the mispaired
  handler the note described.

  The lock excluded it but could not be tested: the trampoline reads a slot
  only during a rebalance, which happens twice in a test against thousands of
  writes, so the window is never hit — four threads hammering `subscribe()`
  misbehaved 0 times in 5 unguarded. The slots now hold the bare code address
  in an `Atomic`, where a single naturally-aligned word cannot tear, and the
  lock is removed from the callback path. `test_a_handler_slot_is_one_word`
  and `test_a_handler_survives_the_round_trip_through_its_slot` guard the
  shape and the round trip, and both fail when broken.

### Changed
- **Examples rewritten.** `producer_basic` / `consumer_basic` / `ml_pipeline`
  are replaced by `produce.mojo`, `pipeline.mojo` and `consume.mojo`, which
  chain into one story: a sensor emits packed `Float32` batches, a pipeline
  reduces them with SIMD over `consume_borrowed()` spans without copying a
  byte, and a reader consumes the summaries. Pixi tasks are now
  `example-produce`, `example-pipeline` and `example-consume`. The old set
  compiled but demonstrated nothing the package could not do on day one.

### Added
- **Zero-copy consume.** `Consumer.consume_borrowed(n)` returns a
  `MessageBatch` that still owns librdkafka's messages and lends
  `BorrowedMessage` views into them — `key()` and `value()` are `Span`s
  pointing at the fetched bytes, copied nowhere. The shape `rust-rdkafka`'s
  `BorrowedMessage` uses, offered **alongside** the owned `Message` rather
  than instead of it.

  **The lifetime is compiler-enforced.** The batch owns the messages and
  destroys them; `BorrowedMessage[origin]` and the `Span`s it returns are
  parameterised by that batch's origin, so holding either keeps the batch
  alive. Threading the origin as a *struct parameter* is what makes this
  work — taking it from a `ref self` borrow instead lets the messages be
  freed before the read, which is a use-after-free that looks correct
  because `free()` does not scrub.

  Measured against C, `rust-rdkafka` and `confluent-kafka` on one topic with
  every client computing the same checksum: **85-87% of C driven directly**
  (152.8ns a record against 129.2ns) and **~2x `rust-rdkafka`'s
  `BorrowedMessage`**. Most of that 2x is API rather than language —
  `rust-rdkafka` exposes no batch consume, which the C peer prices at ~111ns a
  record; measured against C doing the *same* access pattern the honest
  figure is +23.6ns for us against +76.3ns for Rust. See
  `benchmarks/README.md`; these numbers supersede an earlier, less careful
  measurement made before the harness could detect a contaminated run.

  No headers on a borrowed view: each is a separate name and value that
  would have to be copied out, which is the cost this avoids.
  `MessageBatch.reached_end()` reports end-of-partition, without which a
  bounded drain pays a full timeout per pass.

- **Batch consume.** `Consumer.consume(n, timeout_ms)` returns up to `n`
  messages from one `rd_kafka_consume_batch_queue` call, over the queue from
  `rd_kafka_queue_get_consumer` — the same construction `confluent-kafka`'s
  `Consumer.consume()` uses. `consume_events(n)` is the `poll_event()`
  counterpart: it reports end-of-partition instead of dropping it, and
  returns per-entry verdicts instead of raising, so one bad record cannot
  take the good ones with it.

  **Two `consume()` calls on one consumer are refused, not serialised.**
  librdkafka documents concurrent `rd_kafka_consume_batch_queue` on one queue
  as undefined behaviour that "will not be supported in future as well", and
  says so only in `INTRODUCTION.md` — not in the header. It gives no error
  for the violation, so the check is ours; queueing the second caller would
  hide their bug and silently change what their program does. Scale by giving
  each thread its own `Consumer` in the same group, which is what librdkafka
  recommends, or by splitting the returned batch.

  The consumer-queue reference is destroyed before `rd_kafka_consumer_close`
  in both `close()` and the destructor, which librdkafka requires — and
  `consume()` on a closed consumer therefore raises, because
  `rd_kafka_consume_batch_queue` segfaults on the resulting NULL queue rather
  than reporting an error.

  `n` is bounded to 1..1,000,000, matching `confluent-kafka`'s cap on
  `num_messages`: the pointer array is allocated before the call, so an
  unbounded `n` is an unbounded allocation. `consume(0)` raises here where
  `confluent-kafka` returns an empty list — a deliberate divergence, because
  a drain loop around a call that can silently return nothing spins forever.

- **Transactions, producer side.** `Producer.init_transactions()`,
  `begin_transaction()`, `commit_transaction()` and `abort_transaction()` —
  exactly-once for atomic multi-topic writes. The shape follows
  `confluent-kafka`; the state is entirely librdkafka's, and this package
  tracks none of it.

  **They return `Optional[KafkaError]` rather than raising.** `None` is
  success. `confluent-kafka` raises, but its `KafkaException` carries a
  `KafkaError` with `.fatal()`, `.retriable()` and `.txn_requires_abort()`
  on it — and Mojo 1.0's `Error` is text only, so raising here would discard
  exactly the three flags the caller must branch on. Branch with
  `KafkaError.txn_action()`.

  `commit_transaction()` and `abort_transaction()` default to `timeout_ms=-1`,
  which librdkafka calls strongly recommended — it means the transaction's
  remaining time, and other values risk internal state desynchronisation.
  `commit_transaction()` flushes on its own; do not add a `flush()` before it.
  `abort_transaction()` purges anything still queued, and each purged message
  surfaces as an ordinary delivery failure — call `take_failures()` after an
  abort to acknowledge them.

- **`Producer.send_offsets_to_transaction()` and
  `Consumer.consumer_group_metadata()`** — read-process-write exactly-once.
  The consumer's offsets are committed *inside* the producer's transaction,
  so input and output land together or not at all; a failed transaction
  leaves the group uncommitted and the input replays.

  Three librdkafka requirements fail silently and are documented on the
  method: the offsets are the **next** message to consume (which is what
  `Consumer.position()` reports, so hand it the assignment), the consumer
  needs `enable.auto.commit=false`, and invalid offsets are skipped — a call
  with none valid returns success having done nothing. Unlike the other
  transactional calls this one is retriable but **not resumable**: retry with
  identical offsets and metadata.

  `ConsumerGroupMetadata` is not copyable (the handle is freed once) and is a
  snapshot of the group generation, so take it inside the transaction that
  sends the offsets rather than once at startup.

  Note for anyone writing tests against this: librdkafka's **mock broker
  accepts `TxnOffsetCommit` and never serves it back through `OffsetFetch`**,
  so `committed()` there reports `OFFSET_INVALID` whether the transaction
  committed or aborted. Assertions about the committed offset are therefore
  vacuous on the mock and live in the real-broker suite.

- **`MockCluster.push_request_errors()`**, over
  `rd_kafka_mock_push_request_errors_array` — makes the mock broker answer
  chosen requests with chosen error codes. This is how librdkafka tests its
  own transactional error handling, and it is what makes the fatal and
  abortable branches testable without Docker: the flags are set by the
  *client* from the broker's code, so injecting at the mock exercises the
  real classification rather than a stand-in for it.

- **`KafkaError` now carries librdkafka's three error flags** — `is_fatal`,
  `is_retriable` and `txn_requires_abort` — read off `rd_kafka_error_t` in
  `Lib.take_error`, which is the only place they *can* be read: it is what
  destroys the object holding them, and they do not live on the error code.

  `KafkaError.txn_action()` reduces them to the three-way branch every
  transactional call requires, as a `TxnAction` (`TXN_ABORT`, `TXN_RETRY`,
  `TXN_FATAL`). The order is librdkafka's and is not the obvious one: abort
  is tested **before** fatal, because the transactional producer treats most
  of the idempotent producer's fatal errors as abortable — a transaction can
  be aborted and replayed whole — so an error can carry both flags and the
  abort is the correct response. An error with none of the three flags set
  is `TXN_FATAL`, following librdkafka's explicit "treat all other errors as
  fatal".

  A `KafkaError` built from a bare `rd_kafka_resp_err_t` reports all three
  False. That is the absence of an opinion, not a claim the operation was
  survivable, so `txn_action()` is meaningful only on an error returned by a
  transactional call.

  This is step 0 of transactions: without the flags a transactional producer
  cannot be driven correctly, and until now `take_error` read the code and
  the string and threw them away. No public transactional API yet.

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

  This landed as the **consume** half first. The produce half — `produce(
  timestamp=)` — followed in the same release; see its entry above. Together
  they make event time round-trip, which is what either half alone could not.

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

[Unreleased]: https://github.com/dvirarad/mojo-kafka/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/dvirarad/mojo-kafka/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/dvirarad/mojo-kafka/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/dvirarad/mojo-kafka/releases/tag/v0.1.0

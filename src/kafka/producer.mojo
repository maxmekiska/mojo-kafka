"""High-level producer."""

from std.atomic import Atomic

from ._ffi import (
    Lib,
    MSG_ERR,
    MSG_OFFSET,
    MSG_PARTITION,
    MSG_PRIVATE,
    MSG_RKT,
    RD_KAFKA_MSG_F_COPY,
    RD_KAFKA_PARTITION_UA,
    RD_KAFKA_PRODUCER,
    RD_KAFKA_RESP_ERR_NO_ERROR,
    RD_KAFKA_RESP_ERR__TIMED_OUT,
    KIND_OTHER,
    KafkaError,
    KafkaErrorKind,
    kind_of,
    _c_string,
    _load_i32,
    _load_i64,
    _load_word,
    _VuArray,
)
from ._sync import _Latch
from .config import ProducerConfig
from .group import ConsumerGroupMetadata
from .partition import TopicPartition, _build_tpl
from .header import Header

# Let librdkafka choose the partition with the topic's partitioner, which is
# what `produce()` does unless a caller names one. Re-exported under a less
# cryptic name than librdkafka's "UA" (unassigned).
comptime PARTITION_UNASSIGNED: Int32 = RD_KAFKA_PARTITION_UA


@fieldwise_init
struct DeliveryReport(Copyable, Movable, Writable):
    """What the broker said about one produced message.

    `sequence` is the token `produce()` returned, which is what makes a
    verdict addressable: without it a rejection is just a tally and the text
    of whichever failure happened to arrive first.

    Only **failures** are retained -- see `Producer.failures()` for why -- so
    an instance of this always describes a message that did not make it.
    `partition` and `offset` are whatever librdkafka filled in, and for a
    message that never reached the broker they are its unassigned defaults.
    """

    var sequence: Int
    var topic: String
    var partition: Int32
    var offset: Int64
    var error_code: Int32
    var error: String

    def kind(self) -> KafkaErrorKind:
        """The branchable category of this rejection.

        `error_code` keeps the exact librdkafka value; this is the one to
        branch on. A `KIND_TIMED_OUT` report is worth retrying, a
        `KIND_MESSAGE_TOO_LARGE` one never is.
        """
        return kind_of(self.error_code)

    def write_to(self, mut writer: Some[Writer]):
        """One line, naming the message by the token `produce()` returned.

        `Writable` rather than a bespoke `describe()`, so `String(report)`
        and `print(report)` behave like any other Mojo value.
        """
        writer.write(
            "#",
            self.sequence,
            " ",
            self.topic,
            "[",
            self.partition,
            "]: ",
            self.error,
        )


struct _Field(Copyable, Movable):
    """One key or value on its way to C: where it is, how long, and whether
    it is there at all.

    Presence is tracked separately from length because Kafka needs all three
    states -- absent, present-and-empty, present-with-bytes -- and a length
    alone collapses the first two. It exists to describe a buffer the caller
    still owns; it must not outlive the `produce()` call it was built for.

    Deliberately **not** `@fieldwise_init`: that would synthesise
    `_Field(pointer, length, present)`, restoring the bare `(Int, Int, Bool)`
    call this type exists to remove. The only way in is from an `Optional`,
    which is the one place the presence rule can be got right.
    """

    var pointer: Int
    var length: Int
    var present: Bool

    def __init__(out self, field: Optional[String]):
        """Encode text. `byte_length()` because `len()` on a String is UTF-8
        codepoints, and Kafka counts bytes."""
        self.present = field.__bool__()
        self.pointer = Int(field.value().unsafe_ptr()) if self.present else 0
        self.length = field.value().byte_length() if self.present else 0

    def __init__(out self, field: Optional[List[UInt8]]):
        """Encode raw bytes."""
        self.present = field.__bool__()
        self.pointer = Int(field.value().unsafe_ptr()) if self.present else 0
        self.length = len(field.value()) if self.present else 0

    def address(self, placeholder: Int) -> Int:
        """The address C should see: 0 when absent, never 0 when present."""
        if not self.present:
            return 0
        return self.pointer if self.pointer != 0 else placeholder


struct _DrState(Movable):
    """What the delivery-report callback can reach.

    Lives in a one-element `List` on the `Producer`, so its address survives
    anything that moves the producer -- the same heap box `Lib` uses for its
    handle, and `Consumer` for its rebalance state.

    `lock` guards `failures` and nothing else. librdkafka runs the callback
    on whichever thread called `poll` / `flush`, so two threads producing and
    draining are two unsynchronised writers to that list; the lock is what
    makes them one. Not `Copyable`: an `Atomic` cannot be copied, and copying
    a lock would be meaningless anyway.
    """

    var lib: Int
    var lock: _Latch
    var failures: List[DeliveryReport]

    def __init__(out self):
        self.lib = 0
        self.lock = _Latch()
        self.failures = List[DeliveryReport]()


def _delivery_trampoline(rk: Int, msg: Int, opaque: Int) abi("C"):
    """librdkafka's verdict on one produced message.

    `abi("C")` and therefore **thin**: it captures nothing, so `opaque` --
    set with `rd_kafka_conf_set_opaque` -- is the only route back to the
    producer's state. `abi("C")` may not be `raises` either, so the body is
    wrapped, the same discipline `__deinit__` uses.

    Called once per message from inside `poll()` or `flush()`, on the calling
    thread. librdkafka does not invoke callbacks from its background threads,
    which is what makes touching Mojo state here safe at all -- and is also
    why the failure list needs `_Latch`: several threads draining are several
    threads running this body.

    **Only failures are retained.** A report per delivered message would grow
    without bound in a long-running producer that never reads them, and the
    question worth answering is which messages did not make it. The success
    path is therefore a single load and a return, which is what keeps this
    off the produce path's cost.
    """
    try:
        var err = _load_i32(msg + MSG_ERR)
        if err == RD_KAFKA_RESP_ERR_NO_ERROR:
            return
        ref state = Pointer[_DrState, MutAnyOrigin](unsafe_from_address=opaque)[
            unsafe_offset=0
        ]
        ref lib = Pointer[Lib, ImmutAnyOrigin](unsafe_from_address=state.lib)[
            unsafe_offset=0
        ]
        # `_private` is the token handed to produceva as
        # RD_KAFKA_VTYPE_OPAQUE, returned untouched.
        #
        # The report is decoded *before* the lock is taken: decoding crosses
        # into C for the topic name and the error string, and the rule that
        # keeps `_Latch` safe is that no FFI call happens inside a critical
        # section.
        var report = DeliveryReport(
            _load_word(msg + MSG_PRIVATE),
            lib.topic_name(_load_word(msg + MSG_RKT)),
            _load_i32(msg + MSG_PARTITION),
            _load_i64(msg + MSG_OFFSET),
            err,
            String(lib.error(err)),
        )
        # Nothing between these two lines can raise -- `List.append` is not
        # `raises`, which the compiler confirms by rejecting a `try` around
        # it as useless. That matters more than it looks: a lock leaked here
        # would not crash, it would spin the next `flush()` forever.
        state.lock.acquire()
        state.failures.append(report^)
        state.lock.release()
    except:
        # Nothing here is actionable, and librdkafka is mid-teardown of the
        # message. Losing one report is better than aborting the process.
        pass


struct Producer:
    """A producer over librdkafka.

    Construct from a `ProducerConfig` and call `produce(...)` repeatedly.
    `flush()` before drop to wait for in-flight messages to be acked.

    **Delivery is verified, not assumed.** `produce()` only enqueues; the
    broker's verdict arrives later. The producer registers a `dr_msg_cb`,
    which librdkafka calls once per message from inside `poll()` or
    `flush()`, and `flush()` raises if any message was rejected. Without
    that, a message dropped at `message.timeout.ms` leaves the queue empty
    and `flush()` alone would report success over the top of it.

    Messages go out through `rd_kafka_produceva`, which names the topic by
    string. That is what lets this hold no per-topic state: the producer used
    to cache an `rd_kafka_topic_t` per topic name, and that unsynchronised
    `Dict` was the one thing making it unsafe to share across threads when
    librdkafka's own handle is not.

    **Concurrency.** `produce()`, `produce_bytes()`, `poll()`, `flush()`,
    `failures()`, `take_failures()` and `delivery_failures()` may be called
    from more than one thread on the same producer. `rd_kafka_t` is itself
    thread-safe, and the two pieces of Mojo state that were not are now: the
    sequence counter is an `Atomic` (`fetch_add`, so no two messages can
    claim the same token), and the failure list is guarded by `_Latch`.

    Two caveats that are properties of the API rather than of the locking:

    - `last_error_kind()` is a single slot on the producer and cannot be
      attributed to a caller once two threads produce. Read
      `DeliveryReport.kind()` instead, which names its message.
    - `flush()` waits for *every* queued message, not for the ones this
      thread produced, and it raises on any unacknowledged rejection --
      including another thread's.

    Note that the delivery-report callback runs on whichever thread called
    `poll` / `flush`, never on a librdkafka background thread. That is what
    makes it safe for the callback to touch Mojo state at all; it is also
    why two threads draining are two writers to the failure list, which is
    what `_Latch` exists for.

    **Transactions.** With `transactional.id` set on the config, this is an
    exactly-once producer: `init_transactions()` once, then
    `begin_transaction()` / `commit_transaction()` around each batch, and
    `abort_transaction()` to discard one. Unlike everything above, those four
    **return** their error rather than raising it -- see the block comment
    over them for why, and branch on `KafkaError.txn_action()`.

    A transactional producer is not a concurrent one. The four calls are
    sequential state on a single transaction, so one thread drives them; the
    produce path stays thread-safe inside an open transaction, but the
    begin/commit boundary is the caller's to serialise.
    """

    var _lib: Lib
    var _rk: Int
    # One element, on the heap: the C delivery-report callback reaches this
    # by address, and a `List`'s buffer does not move when the `Producer`
    # does. `failures()` and friends read through it.
    var _dr: List[_DrState]
    # Atomic because two threads producing are two writers. `fetch_add`
    # is what makes a token unique rather than merely distinct-looking:
    # a plain `+= 1` loses one under contention and hands two messages the
    # same sequence, which silently mis-attributes their delivery reports.
    var _next_sequence: Atomic[DType.int64]
    # Atomic for well-definedness rather than for meaning -- see
    # `last_error_kind()`, which cannot attribute a kind to a caller once
    # more than one thread is producing.
    var _last_error_kind: Atomic[DType.int32]

    def __init__(out self, cfg: ProducerConfig) raises:
        self._lib = Lib()

        var state = List[_DrState](capacity=1)
        state.append(_DrState())
        self._dr = state^

        var conf = cfg._build(self._lib)
        try:
            self._lib.conf_set_dr_msg_cb(conf, _delivery_trampoline)
            self._lib.conf_set_opaque(conf, Int(self._dr.unsafe_ptr()))
        except e:
            self._lib.conf_destroy(conf)
            raise e
        # rd_kafka_new adopts conf on success and _build/new_client
        # between them free it on every failure path.
        self._rk = self._lib.new_client(RD_KAFKA_PRODUCER, conf)
        # Sequences start at 1, not 0. The opaque travels as a `void *` and
        # comes back as `_private`, where 0 is indistinguishable from a
        # message produced without one.
        self._next_sequence = Atomic[DType.int64](1)
        self._last_error_kind = Atomic[DType.int32](KIND_OTHER._tag)
        # Recorded after construction because it is the address of a field of
        # `self`, which is only final once the producer exists. `Producer` is
        # neither `Copyable` nor `Movable`, so it stays put from here on.
        self._dr[0].lib = Int(Pointer(to=self._lib))

    def __deinit__(deinit self):
        # Destructors cannot raise, and there is nothing useful to do with a
        # teardown failure anyway -- the process is already letting go.
        #
        # The drain below is a **blocking 5s worst case**: dropping a producer
        # that still holds undeliverable messages waits for them to time out,
        # and their failures are then swallowed rather than raised. Call
        # `flush()` before dropping to see the verdict and to choose the wait.
        # The alternative -- a shorter deadline here -- would silently discard
        # messages that were still in flight, which is the worse default for a
        # client whose whole point is that delivery is verified.
        if self._rk != 0:
            try:
                _ = self._drain_until_empty(5000)
                self._lib.destroy(self._rk)
            except:
                pass
        # Keeps the callback's box alive across the drain above, which is
        # where the delivery-report callback fires for anything that times
        # out. Fields are released at their last use *inside* the destructor
        # body, not after it -- `_drain_until_empty` takes `mut self`, so it
        # borrows the whole struct and happens to cover `_dr` today, but that
        # is an accident of its signature. `Consumer.__deinit__` had the same
        # shape without the accident and segfaulted. Pin it explicitly.
        _ = self._dr^

    # -- delivery reports -----------------------------------------------------

    def _drain(mut self, timeout_ms: Int32) raises -> Int:
        """Serve delivery reports, recording any rejections.

        The recording happens in `_delivery_trampoline`, which librdkafka
        calls once per message from inside this. Returns how many events were
        served.
        """
        return Int(self._lib.poll(self._rk, timeout_ms))

    def _drain_until_empty(mut self, timeout_ms: Int32) raises -> Bool:
        """Block until nothing is outstanding, serving reports as they land.

        `rd_kafka_flush` is exactly this, and it is used directly: it polls
        the client itself, so the delivery-report callback runs on this
        thread while it waits. It was unusable while reports were routed to
        the main queue as events -- `rd_kafka_outq_len` counted undrained
        events as outstanding, so it blocked until its timeout regardless --
        which is why this used to be a hand-written loop.
        """
        return (
            self._lib.flush(self._rk, timeout_ms) == RD_KAFKA_RESP_ERR_NO_ERROR
        )

    def _state(self) -> Pointer[_DrState, MutAnyOrigin]:
        """The callback's box, reached the way the callback reaches it.

        `failures()` and `delivery_failures()` take `self` immutably -- they
        are reads, and making them `mut` would break every caller holding a
        `Producer` by reference -- but taking the lock needs mutable access
        to the `Atomic` inside. The box is heap storage whose address is
        already handed to C, so going through it here is the same aliasing
        the `dr_msg_cb` does, not a new one.
        """
        return Pointer[_DrState, MutAnyOrigin](
            unsafe_from_address=Int(self._dr.unsafe_ptr())
        )

    def _raise_if_undelivered(self) raises:
        """Raise if any rejection is still unacknowledged.

        Deliberately does **not** clear: the reports are the useful part, and
        discarding them as the error is raised would leave the caller with a
        count and a string -- exactly what per-message reports exist to
        replace. `take_failures()` is how they are acknowledged.
        """
        ref state = self._state()[unsafe_offset=0]
        # The message is built inside the critical section but raised outside
        # it: a `raise` under the lock would leave it held forever.
        var summary: String
        state.lock.acquire()
        var count = len(state.failures)
        if count != 0:
            summary = String(state.failures[0])
        else:
            summary = String("")
        state.lock.release()

        if count == 0:
            return
        raise Error(
            String(count)
            + " message(s) failed delivery; first was "
            + summary
            + " (call take_failures() for all of them)"
        )

    def last_error_kind(self) -> KafkaErrorKind:
        """Why the most recent `produce()` was rejected, as a branchable kind.

        `produce()` raises, and a Mojo `Error` carries only text, so the kind
        is read here instead of off the exception. The case this exists for is
        backpressure -- `confluent-kafka-python` raises `BufferError` for it
        precisely so callers can drain and retry:

            try:
                _ = p.produce("t", "payload")
            except e:
                if p.last_error_kind() == KIND_QUEUE_FULL:
                    _ = p.poll(100)      # serve reports, free queue space
                else:
                    raise e

        Only meaningful immediately after a `produce()` that raised, **and
        only when one thread is producing.** The kind is one slot on the
        producer, so with two threads producing, a caller can read the kind
        belonging to the other thread's rejection. That is not a data race --
        the slot is atomic -- but it is not attributable either, and no
        arrangement of this API fixes it: Mojo 1.0's `Error` carries only
        text, so the kind cannot travel on the exception where it belongs.
        A concurrent producer should branch on `DeliveryReport.kind()`, which
        names its message.
        """
        return KafkaErrorKind(self._last_error_kind.load())

    # -- transactions -------------------------------------------------------
    #
    # Exactly-once, producer side. The shape follows `confluent-kafka`
    # (`init_transactions` / `begin_transaction` / `commit_transaction` /
    # `abort_transaction`) and the semantics are librdkafka's, which owns all
    # the state -- this package tracks none of it and asks no questions the
    # library already answers. Calling them out of order is librdkafka's
    # `__STATE` to report, not ours to pre-empt.
    #
    # **They return their error instead of raising it, and that is not a
    # style choice.** Raising collapses a `KafkaError` into a `String`,
    # because Mojo 1.0's `Error` carries only text -- which discards exactly
    # the three flags the caller is required to branch on.
    # `confluent-kafka` does raise, but its `KafkaException` carries a
    # `KafkaError` with `.retriable()`, `.txn_requires_abort()` and
    # `.fatal()` on it; a Mojo `Error` has nowhere to put them. Returning the
    # error is how the same information reaches the caller here.
    # `Producer.last_error_kind()` is the side-channel this package already
    # uses to work around that for `produce()`, and it is explicitly not
    # attributable once two threads produce. A transactional call cannot take
    # that compromise: a wrong branch corrupts a transaction rather than
    # mis-reporting one rejection.
    #
    # `None` means success. On an error, branch with `txn_action()`:
    #
    #     var failure = p.commit_transaction()
    #     if failure:
    #         var action = failure.value().txn_action()
    #         if action == TXN_ABORT:
    #             _ = p.abort_transaction()      # then begin a new one
    #         elif action == TXN_RETRY:
    #             _ = p.commit_transaction()     # resumable; call it again
    #         else:
    #             raise Error(String(failure.value()))   # unusable producer

    def init_transactions(
        mut self, timeout_ms: Int32 = -1
    ) raises -> Optional[KafkaError]:
        """Acquire a producer id and fence older instances. Once, first.

        Must be called before `begin_transaction()` or any `produce()`, and
        requires `transactional.id` on the `ProducerConfig`:

            var cfg = ProducerConfig(bootstrap_servers=...)
            cfg.set("transactional.id", "orders-etl-1")

        It completes or aborts whatever a previous producer with the same
        `transactional.id` left behind, which is what makes the id -- not the
        process -- the unit of exactly-once.

        `timeout_ms` of -1 means `2 * transaction.timeout.ms`, librdkafka's
        own default. A `TXN_RETRY` verdict here is **resumable**: the work
        continues in the background and calling again picks it up rather than
        starting over.
        """
        return self._txn_result(
            self._lib.init_transactions(self._rk, timeout_ms)
        )

    def begin_transaction(mut self) raises -> Optional[KafkaError]:
        """Open a transaction. Everything produced after this is in it.

        The only one of the four with no timeout, because it changes local
        state and makes no round trip. After it returns, one of `produce()`,
        `commit_transaction()` or `abort_transaction()` has to happen within
        `transaction.timeout.ms` or the broker times the transaction out.
        """
        return self._txn_result(self._lib.begin_transaction(self._rk))

    def commit_transaction(
        mut self, timeout_ms: Int32 = -1
    ) raises -> Optional[KafkaError]:
        """Flush, then commit everything produced since `begin_transaction()`.

        **Pass -1.** librdkafka calls that "strongly recommended": it means
        the transaction's remaining time, and any other value "risk[s]
        internal state desynchronisation" if a protocol request fails
        mid-commit. The default is -1 for that reason and there is no good
        reason to override it.

        The flush is librdkafka's own and needs nothing from the caller: the
        warning in its docs about having to serve delivery reports on another
        thread applies to `RD_KAFKA_EVENT_DR`, which this package stopped
        using -- a `dr_msg_cb` is served on the flushing thread. Reports for
        messages that failed permanently still land in `failures()`, and the
        commit itself returns `TXN_ABORT` in that case.

        A `TXN_RETRY` verdict is resumable: the commit continues in the
        background and calling again resumes it.
        """
        return self._txn_result(
            self._lib.commit_transaction(self._rk, timeout_ms)
        )

    def abort_transaction(
        mut self, timeout_ms: Int32 = -1
    ) raises -> Optional[KafkaError]:
        """Discard the transaction. Also how you recover from `TXN_ABORT`.

        **Every message still queued is purged**, and each surfaces as a
        delivery failure with `__PURGE_QUEUE` or `__PURGE_INFLIGHT`. That is
        the abort working, not a second problem -- but `failures()` is
        retained until acknowledged, so a later `flush()` raises about
        messages that were discarded on purpose. Call `take_failures()`
        after an abort to acknowledge them, and only inspect them if you
        want to know what was thrown away.

        Pass -1 for the same reason `commit_transaction()` does.
        """
        return self._txn_result(
            self._lib.abort_transaction(self._rk, timeout_ms)
        )

    def send_offsets_to_transaction(
        mut self,
        offsets: List[TopicPartition],
        group_metadata: ConsumerGroupMetadata,
        timeout_ms: Int32 = -1,
    ) raises -> Optional[KafkaError]:
        """Add a consumer's offsets to this transaction. Read-process-write.

        This is what makes exactly-once end to end rather than
        write-only: the offsets are committed to the consumer's group **only
        if the transaction commits**, so a crash mid-transaction leaves the
        input un-consumed and the output un-written together. Call it at the
        end of the loop, before `commit_transaction()`.

        Three things librdkafka requires, and all three fail quietly:

        - **The offsets are the *next* message to consume**, i.e. last
          processed + 1 -- not the offset you just handled. `Consumer.position()`
          already reports exactly that, which is why it is the natural
          argument; hand it the consumer's current assignment.
        - **The consumer must have `enable.auto.commit=false`.** Otherwise it
          commits on its own schedule and the transaction is no longer the
          thing deciding what was consumed.
        - **Invalid offsets are skipped silently.** `OFFSET_INVALID` entries
          are ignored, and if *none* of the offsets are valid this returns
          success having done nothing -- so a `position()` taken before
          anything was read commits nothing and says it worked.

        Unlike the other four, this call is **retriable but not resumable**:
        a retry sends a fresh request, so retry with the *same* offsets and
        the same `group_metadata` or the transaction and your idea of it
        drift apart.
        """
        var list = _build_tpl(self._lib, offsets)
        var err: Int
        try:
            err = self._lib.send_offsets_to_transaction(
                self._rk,
                list,
                group_metadata._address(),
                timeout_ms,
            )
        except e:
            self._lib.topic_partition_list_destroy(list)
            raise e
        self._lib.topic_partition_list_destroy(list)
        return self._txn_result(err)

    def _txn_result(self, err: Int) raises -> Optional[KafkaError]:
        """NULL is success; anything else is ours to read and destroy.

        The reversed polarity `produceva` has. `take_error` does both, and
        is where the three flags are read off the object before it goes.
        """
        if err == 0:
            return None
        return self._lib.take_error(err)

    def delivery_failures(self) -> Int:
        """Rejections tallied since the last `flush()`, without blocking."""
        ref state = self._state()[unsafe_offset=0]
        state.lock.acquire()
        var count = len(state.failures)
        state.lock.release()
        return count

    def failures(self) -> List[DeliveryReport]:
        """Every unacknowledged rejection, each naming the sequence
        `produce()` returned for it. Does not acknowledge them.

        Successful deliveries are deliberately **not** retained. A report per
        message would grow without bound in a long-running producer that
        never reads them, and the question worth answering -- which messages
        did not make it -- needs only the failures. After a `flush()` that
        does not raise, every message produced before it was delivered.
        """
        ref state = self._state()[unsafe_offset=0]
        state.lock.acquire()
        var snapshot = state.failures.copy()
        state.lock.release()
        return snapshot^

    def take_failures(mut self) -> List[DeliveryReport]:
        """Acknowledge every rejection, returning what was outstanding.

        `flush()` keeps raising while any rejection is unacknowledged, so
        this is how a caller says it has handled them:

            try:
                p.flush()
            except e:
                for report in p.take_failures():
                    print(report)
        """
        ref state = self._state()[unsafe_offset=0]
        # Copy and clear under one acquisition, or a report landing between
        # the two is acknowledged without ever being returned.
        state.lock.acquire()
        var taken = state.failures.copy()
        state.failures.clear()
        state.lock.release()
        return taken^

    # -- producing ------------------------------------------------------------

    def _headers_handle(self, headers: List[Header]) raises -> Int:
        """Build a `rd_kafka_headers_t` from `headers`, or 0 if there are none.

        On success the list is handed to `produceva`, which adopts it -- so
        the caller must destroy it only if the produce call never happens or
        comes back with an error. A half-built list is destroyed here.

        Header names are passed as pointer + byte length rather than as C
        strings: `String.unsafe_ptr()` is not NUL-terminated, and an explicit
        length keeps the name off that trap entirely. Header *values* follow
        the same pointer-presence rule as keys and payloads, so they reuse
        `_Field` and its placeholder.
        """
        if len(headers) == 0:
            return 0

        var hdrs = self._lib.headers_new(len(headers))
        if hdrs == 0:
            raise Error("rd_kafka_headers_new returned NULL")

        var placeholder = Array[UInt8, 1](fill=0)
        var somewhere = Int(placeholder.unsafe_ptr())
        try:
            for header in headers:
                var value = _Field(header.value)
                var rc = self._lib.header_add(
                    hdrs,
                    Int(header.name.unsafe_ptr()),
                    header.name.byte_length(),
                    value.address(somewhere),
                    value.length,
                )
                self._lib.raise_if(rc, "header_add(" + header.name + ")")
        except e:
            self._lib.headers_destroy(hdrs)
            raise e
        _ = placeholder^
        return hdrs

    def _enqueue(
        mut self,
        topic: String,
        value: _Field,
        key: _Field,
        headers: List[Header],
        partition: Int32,
        timestamp: Int64,
    ) raises -> Int:
        """Hand one message to librdkafka, null fields and all.

        librdkafka reads a field's **presence from the pointer**, not from
        the length: NULL is a null field, and a non-NULL pointer with length
        0 is a field that is present and empty. That single rule is what
        makes tombstones (null payload) and empty-but-present keys
        expressible, so an absent field must reach C as address 0 and a
        present one must never do.

        An empty Mojo buffer does have a real address, but nothing in the
        language promises it is non-NULL, so a present field is pinned to
        `placeholder` if its own address is 0. Reading zero bytes from it is
        what librdkafka does with it either way.

        Every address in the `vu` array points at a buffer this frame owns,
        and Mojo releases a value after its last *use* rather than at the end
        of the scope -- hence the `_ = x^` line after the call, which is what
        keeps the topic name and the placeholder alive across it.
        """
        # Claimed before anything can fail, so a rejected message still has a
        # token the caller can match its report against. `fetch_add` returns
        # the value before the add, so the first message gets 1.
        var sequence = Int(self._next_sequence.fetch_add(1))

        # Owned from here on: `produceva` adopts the header list only if it
        # succeeds, so every other exit has to destroy it.
        var hdrs = self._headers_handle(headers)

        var err: Int
        try:
            var placeholder = Array[UInt8, 1](fill=0)
            var somewhere = Int(placeholder.unsafe_ptr())
            var topic_c = _c_string(topic)

            # Eight entries, matching the calls below. `_entry` raises
            # rather than overrunning if this is ever left behind -- so a
            # miscount fails on the first produce instead of corrupting the
            # array.
            var vus = _VuArray(8)
            vus.topic(Int(topic_c.unsafe_ptr()))
            vus.partition(partition)
            vus.msgflags(RD_KAFKA_MSG_F_COPY)
            vus.value(value.address(somewhere), value.length)
            vus.key(key.address(somewhere), key.length)
            vus.opaque(sequence)
            vus.timestamp(timestamp)
            if hdrs != 0:
                vus.headers(hdrs)

            err = self._lib.produceva(self._rk, vus.address(), vus.count())
            _ = placeholder^
            _ = topic_c^
            _ = vus^
        except e:
            # Nothing reached librdkafka, so the headers are still ours.
            if hdrs != 0:
                self._lib.headers_destroy(hdrs)
            raise e

        # NULL is success here -- the opposite polarity to the handle-returning
        # calls elsewhere in this package.
        if err == 0:
            return sequence
        if hdrs != 0:
            self._lib.headers_destroy(hdrs)
        var failure = self._lib.take_error(err)
        # Recorded because the raise cannot carry it: Mojo 1.0's `Error` is
        # text, so `except` has no type to match on. See `last_error_kind`.
        self._last_error_kind.store(failure.kind()._tag)
        raise Error("produce(" + topic + "): " + String(failure))

    def produce(
        mut self,
        topic: String,
        value: Optional[String],
        key: Optional[String] = None,
        headers: List[Header] = [],
        partition: Int32 = PARTITION_UNASSIGNED,
        timestamp: Int64 = 0,
    ) raises -> Int:
        """Enqueue a message whose key and value are text.

        Returns as soon as the message is queued; it is handed to the broker
        in the background, and the broker's verdict surfaces at `flush()`.
        librdkafka copies the payload, so `value` and `key` can go out of
        scope immediately.

        Both halves are `Optional` because Kafka's are. `None` writes a
        **null** field, `""` writes one that is present and empty, and the
        two are different messages on the wire:

            p.produce("t", "hello")                # null key
            p.produce("t", "hello", key="")        # empty key, present
            p.produce("t", None, key="k")          # tombstone

        `headers` is a list of pairs rather than a map, because Kafka lets a
        name repeat and keeps the order:

            p.produce("t", "hi", headers=[
                Header("content-type", "text/plain"),
                Header("trace-id", "abc123"),
            ])

        `partition` defaults to `PARTITION_UNASSIGNED`, which leaves the
        choice to the topic's partitioner -- key hashing, normally. Name one
        to bypass it.

        `timestamp` is the record's **CreateTime**, in milliseconds since the
        Unix epoch, and **0 means now** -- librdkafka's own rule, and the
        default `confluent-kafka` documents for the same argument. Set it
        when the event time is not the time you happen to be publishing:
        replaying an archive, or forwarding records from another system.
        It comes back as `Message.timestamp`.

        A broker whose topic is configured `log.message.timestamp.type=
        LogAppendTime` overwrites it, and the consumer then reports
        `TIMESTAMP_LOG_APPEND_TIME` rather than `TIMESTAMP_CREATE_TIME`.

        Returns a **sequence token** identifying this message. It comes back
        on the message's delivery report, so `failures()` can name exactly
        which messages the broker rejected. Ignore it if a count is enough.

        Use `produce_bytes()` for anything that is not valid UTF-8.
        """
        return self._enqueue(
            topic, _Field(value), _Field(key), headers, partition, timestamp
        )

    def produce_bytes(
        mut self,
        topic: String,
        value: Optional[List[UInt8]],
        key: Optional[List[UInt8]] = None,
        headers: List[Header] = [],
        partition: Int32 = PARTITION_UNASSIGNED,
        timestamp: Int64 = 0,
    ) raises -> Int:
        """Enqueue an arbitrary byte payload -- Avro, Protobuf, a compressed blob.

        The same null / empty / present rules as `produce()`, over bytes
        rather than text, and the same `headers`, `partition` and
        `timestamp`. Mojo will not copy a `List` implicitly, so pass an owned
        one (`value^`) or an explicit `value.copy()`.

        Returns the same sequence token as `produce()`.
        """
        return self._enqueue(
            topic, _Field(value), _Field(key), headers, partition, timestamp
        )

    def poll(mut self, timeout_ms: Int32 = 0) raises -> Int32:
        """Serve delivery reports. Returns how many events librdkafka ran.

        The count is events served, which for a producer is delivery reports
        plus any errors -- not a count of rejections. Rejections are tallied
        as they arrive; `flush()` raises on them and `delivery_failures()`
        reads the running count.
        """
        return Int32(self._drain(timeout_ms))

    def flush(mut self, timeout_ms: Int32 = 5000) raises:
        """Block until every queued message is acknowledged.

        Raises if the queue does not drain in time, **and** while any message
        rejection is unacknowledged -- an undelivered message is never
        reported as success. Call `take_failures()` to acknowledge them and
        to see which messages they were.
        """
        if not self._drain_until_empty(timeout_ms):
            var remaining = self._lib.outq_len(self._rk)
            raise Error(
                "flush: "
                + String(self._lib.error(RD_KAFKA_RESP_ERR__TIMED_OUT))
                + " ("
                + String(Int(remaining))
                + " message(s) still queued)"
            )
        self._raise_if_undelivered()

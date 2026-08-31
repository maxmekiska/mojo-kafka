"""High-level consumer."""

from std.atomic import Atomic

from ._ffi import (
    KafkaError,
    Lib,
    RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS,
    RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS,
    MSG_ERR,
    MSG_KEY,
    MSG_KEY_LEN,
    MSG_LEN,
    MSG_OFFSET,
    MSG_PARTITION,
    MSG_PAYLOAD,
    MSG_RKT,
    RD_KAFKA_CONSUMER,
    RD_KAFKA_RESP_ERR_NO_ERROR,
    RD_KAFKA_RESP_ERR__NOENT,
    RD_KAFKA_RESP_ERR__PARTITION_EOF,
    RD_KAFKA_TIMESTAMP_CREATE_TIME,
    RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME,
    RD_KAFKA_TIMESTAMP_NOT_AVAILABLE,
    TP_ERR,
    TP_OFFSET,
    TP_PARTITION,
    TP_TOPIC,
    _load_i32,
    _load_i64,
    _load_word,
    copy_bytes,
    cstr,
)
from .config import ConsumerConfig
from ._sync import _Latch
from .group import ConsumerGroupMetadata
from .header import Header, _text
from .partition import (
    OFFSET_INVALID,
    TopicPartition,
    Watermarks,
    _build_tpl,
)

# rd_kafka_timestamp_type_t, re-exported so `Message.timestamp_type` can be
# compared against something with a name.
# The largest batch `consume()` will accept, matching `confluent-kafka`'s
# documented limit for `num_messages`. The array of message pointers is
# allocated before the call, so this bounds what one bad argument can ask for.
comptime MAX_BATCH: Int = 1_000_000

comptime TIMESTAMP_NOT_AVAILABLE: Int32 = RD_KAFKA_TIMESTAMP_NOT_AVAILABLE
comptime TIMESTAMP_CREATE_TIME: Int32 = RD_KAFKA_TIMESTAMP_CREATE_TIME
comptime TIMESTAMP_LOG_APPEND_TIME: Int32 = RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME


@fieldwise_init
struct Message(Copyable, Movable):
    """One consumed record.

    `key` and `value` are **optional byte arrays**, not strings, because that
    is what Kafka records actually are:

    - Kafka fields are opaque bytes. A `String` cannot hold arbitrary bytes,
      and any text decoding belongs to the caller, who knows the schema.
    - Either half may be **absent**, which Kafka distinguishes from present
      and empty. `None` here means the field was null on the wire; a present
      but empty field is `Some` wrapping an empty list. Conflating the two
      makes compaction tombstones -- non-null key, null value -- invisible.

    `key_text()` / `value_text()` decode for the common case where the
    payload really is text. They collapse null onto their `default`, so read
    `key` / `value` directly when the difference matters.

    `headers` is a **list of pairs, not a map**: Kafka permits a repeated
    header name and preserves order, both of which a `Dict` would quietly
    discard. It is empty for a record written without headers -- which is
    indistinguishable, on the wire, from one written with none.

    `timestamp` is milliseconds since the Unix epoch and is only meaningful
    when `timestamp_type` says so -- see `has_timestamp()`.
    """

    var topic: String
    var partition: Int32
    var offset: Int64
    var key: Optional[List[UInt8]]
    var value: Optional[List[UInt8]]
    var headers: List[Header]
    var timestamp: Int64
    var timestamp_type: Int32

    def has_timestamp(self) -> Bool:
        """True when the record carries a usable `timestamp`.

        Read this rather than testing `timestamp != -1`. A broker older than
        0.10 sends no timestamp at all, and librdkafka reports that as -1
        with a `TIMESTAMP_NOT_AVAILABLE` type -- but -1 is also what an
        arbitrary `int64` millisecond value could legitimately be, so the
        type is the field that actually answers the question.

        `TIMESTAMP_CREATE_TIME` means the producer stamped it (event time);
        `TIMESTAMP_LOG_APPEND_TIME` means the broker did (ingestion time),
        which is a topic-level setting.
        """
        return self.timestamp_type != TIMESTAMP_NOT_AVAILABLE

    def key_text(self, default: String = "") -> String:
        """The key decoded as UTF-8, or `default` if the key was null."""
        return _text(self.key, default)

    def value_text(self, default: String = "") -> String:
        """The value decoded as UTF-8, or `default` if the value was null."""
        return _text(self.value, default)

    def is_tombstone(self) -> Bool:
        """True for a compaction tombstone: a null value, key or not."""
        return not self.value

    def _index_of(self, name: String) -> Int:
        """Position of the first header called `name`, or -1.

        First rather than last because that is the order they were written
        in, and Kafka permits the name to repeat.
        """
        for i in range(len(self.headers)):
            if self.headers[i].name == name:
                return i
        return -1

    def header(self, name: String) -> Optional[List[UInt8]]:
        """The value of the **first** header called `name`, if there is one.

        Returns `None` both when no such header exists and when one exists
        with a null value; the two are different records, so walk `headers`
        directly if that matters.
        """
        var i = self._index_of(name)
        if i < 0:
            return None
        return self.headers[i].value.copy()

    def header_text(self, name: String, default: String = "") -> String:
        """The first `name` header decoded as UTF-8, or `default` if absent.

        Reads the value in place rather than going through `header()`, which
        would copy the bytes only for the decode to throw the copy away.
        """
        var i = self._index_of(name)
        if i < 0:
            return default
        return _text(self.headers[i].value, default)


struct PollEvent(Copyable, Movable):
    """What a single poll turned up: a record, an end-of-partition mark, or
    nothing at all.

    `poll()` collapses the last two onto `None`, which is the right shape for
    a job that follows a topic forever and the wrong one for a job that
    drains it: "I have caught up with the partition" and "nothing has arrived
    yet" are the same answer there, and only the first means stop. This type
    keeps them apart -- at most one of the two fields is ever set, and both
    being empty is the timeout.

        while True:
            var event = consumer.poll_event(1000)
            if event.eof:
                break                       # caught up -- done
            if event.message:
                handle(event.message.value())

    **End-of-partition is opt-in.** librdkafka suppresses it unless the
    consumer was built with `enable_partition_eof=True`, so without that
    `poll_event` reports a timeout where it would otherwise report EOF, and
    the loop above never terminates.
    """

    var message: Optional[Message]
    var eof: Optional[TopicPartition]

    def __init__(out self):
        """A poll that timed out."""
        self.message = None
        self.eof = None

    def __init__(out self, var message: Message):
        self.message = Optional[Message](message^)
        self.eof = None

    def __init__(out self, var eof: TopicPartition):
        """End of partition. `eof.offset` is where the partition ends --
        the offset the next record written to it will get."""
        self.message = None
        self.eof = Optional[TopicPartition](eof^)

    def is_timeout(self) -> Bool:
        """True when the poll turned up neither a record nor an EOF mark."""
        return not self.message and not self.eof


def _decode_tpl(lib: Lib, list: Int) raises -> List[TopicPartition]:
    """Decode a `rd_kafka_topic_partition_list_t` into Mojo values.

    A free function rather than a `Consumer` method because the rebalance
    callback needs it too, and that callback is thin -- it has no `self`.
    The stride is 64; see `TP_STRIDE`. Nothing here owns `list`.
    """
    var out = List[TopicPartition]()
    for i in range(lib.topic_partition_list_count(list)):
        var elem = lib.topic_partition_list_elem(list, i)
        out.append(
            TopicPartition._decoded(
                cstr(_load_word(elem + TP_TOPIC)),
                _load_i32(elem + TP_PARTITION),
                _load_i64(elem + TP_OFFSET),
                _load_i32(elem + TP_ERR),
            )
        )
    return out^


@fieldwise_init
struct Rebalance(Copyable, Movable):
    """The group moved partitions. Handed to an `on_assign` / `on_revoke`
    / `on_lost` handler.

    `confluent-kafka` passes `(consumer, partitions)`; a Mojo handler is
    **thin** -- it captures nothing -- so the two arrive folded into one
    context object instead. `partitions` is the absolute list being assigned
    or revoked, and the methods here are the useful subset of what a handler
    can do to the consumer from inside the callback.

    **Doing nothing is correct.** If a handler does not call `assign()`, the
    default assignment happens anyway, exactly as `confluent-kafka` does it
    -- measured against `confluent-kafka` 2.15, whose `on_assign` need not
    assign for records to flow. Override only to start somewhere else.
    """

    var partitions: List[TopicPartition]
    var lost: Bool
    # Internal: the addresses the methods below reach the consumer through.
    # An instance is only ever built by `_rebalance_trampoline`, and is only
    # valid for the duration of the callback that produced it.
    var _lib: Int
    var _rk: Int
    var _state: Int

    def _settled(self) raises:
        """Record that a handler took the rebalance over.

        Written through a pointer to the whole `_RebalanceState` rather than
        to a hand-computed field offset: the C structs in this package have
        offsets probed against a header, and a *Mojo* struct's layout is the
        compiler's business, not something to reproduce by hand.
        """
        Pointer[_RebalanceState, MutAnyOrigin](unsafe_from_address=self._state)[
            unsafe_offset=0
        ].handled.store(1)

    def _library(self) raises -> Pointer[Lib, ImmutAnyOrigin]:
        if self._lib == 0:
            raise Error("rebalance context used outside its callback")
        return Pointer[Lib, ImmutAnyOrigin](unsafe_from_address=self._lib)

    def assign(self, partitions: List[TopicPartition]) raises:
        """Take `partitions` instead of the default set.

        The point of an `on_assign` handler: read offsets from wherever you
        keep them and start there rather than at the group's commit. Calling
        this marks the rebalance settled, so the default is not also applied.
        """
        ref lib = self._library()[unsafe_offset=0]
        var list = _build_tpl(lib, partitions)
        var rc: Int32
        try:
            rc = lib.assign(self._rk, list)
        except e:
            lib.topic_partition_list_destroy(list)
            raise e
        lib.topic_partition_list_destroy(list)
        lib.raise_if(rc, "rebalance.assign")
        self._settled()

    def unassign(self) raises:
        """Take nothing. Marks the rebalance settled."""
        ref lib = self._library()[unsafe_offset=0]
        lib.raise_if(lib.assign(self._rk, 0), "rebalance.unassign")
        self._settled()

    def commit(self) raises:
        """Synchronously commit the current positions of this assignment.

        The ordinary `on_revoke` action, and the reason that handler exists:
        commit what has been consumed before the partitions belong to
        somebody else. Equivalent to `Consumer.commit()`, but callable from
        inside the callback, which is the only moment it is still true.
        """
        ref lib = self._library()[unsafe_offset=0]
        lib.raise_if(lib.commit(self._rk, 0, 0), "rebalance.commit")

    def commit(self, partitions: List[TopicPartition]) raises:
        """Synchronously commit these offsets before the partitions move.

        The reason `on_revoke` exists: once the rebalance completes the
        partitions belong to another member, and anything consumed but not
        committed is reprocessed there. Commit synchronously -- an
        asynchronous one may not land before the partitions are gone.

        Expect this to fail when `lost` is true: another member may already
        own the partition, which is why lost assignments route to `on_lost`.
        """
        ref lib = self._library()[unsafe_offset=0]
        var list = _build_tpl(lib, partitions)
        var rc: Int32
        try:
            rc = lib.commit(self._rk, list, 0)
        except e:
            lib.topic_partition_list_destroy(list)
            raise e
        lib.topic_partition_list_destroy(list)
        lib.raise_if(rc, "rebalance.commit")

    def protocol(self) raises -> String:
        """ "EAGER" or "COOPERATIVE" -- the group's assignment strategy."""
        ref lib = self._library()[unsafe_offset=0]
        return lib.rebalance_protocol(self._rk)


# A rebalance handler. `thin` because it is reached from a C callback, which
# cannot carry captured state -- so a handler must be a top-level `def`, not
# a closure over local variables. It may raise; the trampoline contains it.
comptime RebalanceHandler = def(Rebalance) thin raises -> None


def _no_handler(event: Rebalance) raises:
    """Seed value for a handler slot being overwritten. Never called."""
    pass


def _handler_word(handler: RebalanceHandler) raises -> Int:
    """The eight bytes of a thin function pointer, as an integer.

    `RebalanceHandler` is `thin`, so its whole representation is one code
    address -- measured at 8 bytes, the same as `PTR_STRIDE`. Reading it as
    an `Int` is what lets a handler slot be a single aligned word, which is
    the entire safety argument in `_RebalanceState`.
    """
    var box = List[RebalanceHandler](capacity=1)
    box.append(handler)
    var word = Pointer[Int, ImmutAnyOrigin](
        unsafe_from_address=Int(box.unsafe_ptr())
    )[unsafe_offset=0]
    _ = box^
    return word


def _handler_from_word(word: Int) raises -> RebalanceHandler:
    """The inverse of `_handler_word`. Only ever given a word it produced."""
    var box = List[RebalanceHandler](capacity=1)
    box.append(_no_handler)
    Pointer[Int, MutAnyOrigin](unsafe_from_address=Int(box.unsafe_ptr()))[
        unsafe_offset=0
    ] = word
    var out = box[0]
    _ = box^
    return out


struct _RebalanceState(Movable):
    """What the C callback can see. Lives in a one-element `List` on the
    `Consumer`, so its address survives anything that moves the consumer.

    `handled` is read back after the handler returns to decide whether the
    default assignment still has to be applied. It is written through its
    address rather than being a `Bool` field because the callback reaches it
    as a raw pointer, and it is `Atomic` because that callback runs on
    whichever thread called `poll` / `close`.

    **The three handler slots are raw code addresses in `Atomic` words, and
    that is the whole point.** `subscribe()` writes them and the trampoline
    reads them, on different threads. They were `Optional[RebalanceHandler]`
    behind a `_Latch`, and the note here claimed a single slot could not tear
    because it was "pointer-sized and aligned". **That was measured and it is
    false**: `Optional[RebalanceHandler]` is **16** bytes -- a discriminant
    plus the pointer -- against 8 for the bare handler. So an unguarded
    reader could pair `has_value = True` with the *other* value's payload and
    call through it. Not a mispaired handler: a call through a wrong address.

    A lock excluded that, but nothing could test the lock. The reader only
    runs during a rebalance, which happens twice in a test and thousands of
    times less often than `subscribe()` can be called, so the window is never
    hit -- a probe hammering four threads saw nothing in 5 runs. A guard that
    cannot fail is worse than none, because it reads as protection.

    Storing the code address instead makes the tearing **impossible rather
    than unlikely**: one naturally-aligned word, one atomic store, one atomic
    load, on the 64-bit targets this package already assumes for
    `PTR_STRIDE`. 0 means "no handler", which is safe because no function has
    address 0. `test_a_handler_slot_is_one_word` fails if anyone puts a
    multi-word type back in these slots.

    What this does *not* exclude is a torn **set** -- a rebalance seeing the
    new `on_assign` beside the previous `on_revoke`. That is unchanged, still
    benign, and now free: a caller racing `subscribe()` against a live
    rebalance has no ordering guarantee to lose in the first place.

    Not `Copyable`: an `Atomic` cannot be copied.
    """

    var lib: Int
    var handled: Atomic[DType.int32]
    var on_assign: Atomic[DType.int64]
    var on_revoke: Atomic[DType.int64]
    var on_lost: Atomic[DType.int64]

    def __init__(out self):
        self.lib = 0
        self.handled = Atomic[DType.int32](0)
        self.on_assign = Atomic[DType.int64](0)
        self.on_revoke = Atomic[DType.int64](0)
        self.on_lost = Atomic[DType.int64](0)

    def _publish(
        mut self,
        on_assign: Optional[RebalanceHandler],
        on_revoke: Optional[RebalanceHandler],
        on_lost: Optional[RebalanceHandler],
    ) raises:
        """Install a set of handlers. One atomic store per slot."""
        self.on_assign.store(
            Int64(_handler_word(on_assign.value())) if on_assign else Int64(0)
        )
        self.on_revoke.store(
            Int64(_handler_word(on_revoke.value())) if on_revoke else Int64(0)
        )
        self.on_lost.store(
            Int64(_handler_word(on_lost.value())) if on_lost else Int64(0)
        )


def _settle(lib: Lib, rk: Int, err: Int32, partitions: Int) raises:
    """Apply librdkafka's own default response to a rebalance.

    Registering a rebalance callback stops librdkafka assigning by itself,
    so **something** has to do this or the consumer quietly stops consuming.
    The correct call depends on the protocol: eager assignors replace the
    whole assignment, cooperative ones add and remove incrementally. Getting
    it wrong stalls the group rather than raising, which is why this is one
    function and not inlined at three call sites.
    """
    var cooperative = lib.rebalance_protocol(rk) == "COOPERATIVE"
    if err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
        if cooperative:
            var e = lib.incremental_assign(rk, partitions)
            if e != 0:
                raise Error(String(lib.take_error(e)))
        else:
            lib.raise_if(lib.assign(rk, partitions), "rebalance assign")
    elif err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
        if cooperative:
            var e = lib.incremental_unassign(rk, partitions)
            if e != 0:
                raise Error(String(lib.take_error(e)))
        else:
            lib.raise_if(lib.assign(rk, 0), "rebalance revoke")
    else:
        # An arbitrary rebalance failure. librdkafka's documented recovery is
        # to clear the assignment and resynchronise.
        lib.raise_if(lib.assign(rk, 0), "rebalance error")


def _rebalance_trampoline(
    rk: Int, err: Int32, partitions: Int, opaque: Int
) abi("C"):
    """The C function librdkafka actually calls.

    `abi("C")` and therefore **thin**: it captures nothing, so `opaque` --
    set with `rd_kafka_conf_set_opaque` -- is the only route back to the
    `Consumer`'s state. `abi("C")` also may not be `raises`, so every path
    here is wrapped, the same discipline `__deinit__` uses.

    Two rules this function exists to hold:

    - **The partition list dies when this returns.** librdkafka destroys it,
      so `_decode_tpl` copies every name out now; a `char *` kept past the
      return reads as garbage.
    - **The rebalance is always settled.** A handler that raises, or that
      simply looks and does nothing, must not leave the assignment unset --
      that is a silent stall, not an error. `handled` records whether the
      handler took over, and `_settle` runs whenever it did not.
    """
    var sp = Pointer[_RebalanceState, MutAnyOrigin](unsafe_from_address=opaque)
    sp[unsafe_offset=0].handled.store(0)

    try:
        ref state = sp[unsafe_offset=0]
        ref lib = Pointer[Lib, ImmutAnyOrigin](unsafe_from_address=state.lib)[
            unsafe_offset=0
        ]
        var lost = lib.assignment_lost(rk)
        var event = Rebalance(
            _decode_tpl(lib, partitions), lost, state.lib, rk, opaque
        )

        # One atomic load picks the handler. There is no lock here any more:
        # a slot is a single aligned word, so the load either sees the whole
        # old address or the whole new one -- see `_RebalanceState`. A lock
        # would also have to be released before the call anyway, because a
        # handler calls straight back into librdkafka and `_Latch` forbids
        # holding it across FFI.
        #
        # A lost assignment goes to `on_lost` when there is one, and falls
        # back to `on_revoke` when there is not -- which is what
        # `confluent-kafka` documents for the same three callbacks.
        var chosen = Int(0)
        if err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
            chosen = Int(state.on_assign.load())
        elif err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            var lost_slot = Int(state.on_lost.load())
            if lost and lost_slot != 0:
                chosen = lost_slot
            else:
                chosen = Int(state.on_revoke.load())

        # 0 is "no handler": no function lives at address 0.
        if chosen != 0:
            _handler_from_word(chosen)(event)
    except:
        # A handler that raised has already had its say. Settling below is
        # what keeps its mistake from stalling the group.
        pass

    try:
        if sp[unsafe_offset=0].handled.load() == 0:
            ref state = sp[unsafe_offset=0]
            ref lib = Pointer[Lib, ImmutAnyOrigin](
                unsafe_from_address=state.lib
            )[unsafe_offset=0]
            _settle(lib, rk, err, partitions)
    except:
        pass


@fieldwise_init
struct BorrowedMessage[origin: Origin[mut=False]](Copyable, Movable):
    """One record, read **in place** in librdkafka's own buffer.

    The zero-copy half of the consume API. `Message` copies every field into
    owned Mojo storage so a record can outlive the fetch that produced it;
    this copies nothing at all -- `key()` and `value()` are `Span`s pointing
    straight into the memory librdkafka received the batch into. It is the
    same shape as `rust-rdkafka`'s `BorrowedMessage`, for the same reason.

    **The lifetime is compiler-enforced, and that is not obvious**, because
    the pointers underneath are fabricated from raw addresses and the
    compiler cannot see their provenance. What it *can* see is `origin`: this
    struct is parameterised by the origin of the `MessageBatch` that lends
    it, so holding a `BorrowedMessage` -- or even just a `Span` taken from
    one -- keeps the batch alive, and the batch is what destroys the
    messages. Verified by instrumenting the batch destructor: transferring
    the batch away mid-scope runs its teardown **after** the last use of a
    span, not at the transfer.

    That last part is why the origin is a **struct parameter** and not
    `origin_of(self)` on the accessor. Probed both: an origin taken from a
    `ref self` borrow ends when the method returns, and the free then happens
    *before* the read -- a real use-after-free that looks fine only because
    `free()` does not scrub. Do not "simplify" these signatures.

    Headers are deliberately absent. Every header is a separate name and
    value that would have to be copied out one at a time, which is the cost
    this type exists to avoid; `confluent-kafka` defers them for the same
    reason. Use `consume()` and `Message.headers` when you need them.
    """

    var _raw: Int
    var _topic_addr: Int
    var _topic_len: Int

    def partition(self) raises -> Int32:
        return _load_i32(self._raw + MSG_PARTITION)

    def offset(self) raises -> Int64:
        return _load_i64(self._raw + MSG_OFFSET)

    def topic(self) -> Span[UInt8, Self.origin]:
        """The topic name's bytes, in librdkafka's memory.

        Resolved once per distinct topic handle by the batch, not per
        message -- `rd_kafka_topic_name` is a crossing, and a batch is nearly
        always one topic.
        """
        return Span[UInt8, Self.origin](
            unsafe_ptr=Pointer[UInt8, Self.origin](
                unsafe_from_address=self._topic_addr
            ),
            length=self._topic_len,
        )

    def key(self) raises -> Optional[Span[UInt8, Self.origin]]:
        """The key, in place. `None` for a null key -- presence is read from
        the **pointer**, exactly as `copy_bytes` does it, because Kafka tells
        a null field from an empty one and so must this."""
        return self._field(MSG_KEY, MSG_KEY_LEN)

    def value(self) raises -> Optional[Span[UInt8, Self.origin]]:
        """The payload, in place. `None` for a tombstone."""
        return self._field(MSG_PAYLOAD, MSG_LEN)

    def _field(
        self, ptr_offset: Int, len_offset: Int
    ) raises -> Optional[Span[UInt8, Self.origin]]:
        var addr = _load_word(self._raw + ptr_offset)
        if addr == 0:
            return None
        var n = _load_word(self._raw + len_offset)
        return Span[UInt8, Self.origin](
            unsafe_ptr=Pointer[UInt8, Self.origin](unsafe_from_address=addr),
            length=n if n > 0 else 0,
        )

    def is_tombstone(self) raises -> Bool:
        return _load_word(self._raw + MSG_PAYLOAD) == 0


struct MessageBatch(Sized):
    """A run of records still owned by librdkafka, lent out in place.

    Returned by `Consumer.consume_borrowed()`. **It owns the raw messages**
    and destroys them in its destructor, which is what makes every
    `BorrowedMessage` it lends valid for exactly as long as it lives -- and
    the compiler enforces that, see `BorrowedMessage`.

        var batch = consumer.consume_borrowed(1000)
        for i in range(len(batch)):
            var record = batch[i]
            if record.value():
                total += process(record.value().value())
        _ = batch^

    Not `Copyable`: two batches destroying the same messages is a double
    free. Not reusable either -- take a new one per fetch.
    """

    var _lib: Lib
    var _raws: List[Int]
    var _topic_addr: Int
    var _topic_len: Int
    var _eof: Bool

    def __init__(
        out self,
        var lib: Lib,
        var raws: List[Int],
        topic_addr: Int,
        topic_len: Int,
        eof: Bool,
    ):
        self._lib = lib^
        self._raws = raws^
        self._topic_addr = topic_addr
        self._topic_len = topic_len
        self._eof = eof

    def __deinit__(deinit self):
        # Destructors cannot raise, and every message here is ours.
        for raw in self._raws:
            if raw != 0:
                try:
                    self._lib.message_destroy(raw)
                except:
                    pass

    def __len__(self) -> Int:
        return len(self._raws)

    def reached_end(self) -> Bool:
        """Whether this fetch ran off the end of a partition.

        The borrowed path drops end-of-partition marks -- they carry no
        payload to lend -- but a bounded drain still has to know, and
        **without this it cannot**: it would have to keep asking until a
        batch came back empty, which costs a full `timeout_ms` every time.
        That is not hypothetical. The first version of this type had no
        `reached_end`, and the benchmark measured the borrowed path at
        19,927 msg/s against 1.2M for the owned one -- 200,000 records in
        10.04 seconds, which is one 10s timeout and nothing else.

        Only meaningful with `enable_partition_eof`, which is off by default
        for the same reason it is on `poll_event()`.
        """
        return self._eof

    def __getitem__(ref self, i: Int) -> BorrowedMessage[origin_of(self)]:
        """Lend record `i`. The result borrows this batch and cannot outlive
        it -- that is the whole safety argument, and it is checked."""
        return BorrowedMessage[origin_of(self)](
            self._raws[i], self._topic_addr, self._topic_len
        )


struct Consumer:
    """A consumer group member over librdkafka.

    **Concurrency.** `rd_kafka_t` is thread-safe, and the reading calls here
    -- `poll()`, `poll_event()` and the control plane -- hold no mutable Mojo
    state, so they may be driven from more than one thread. The two calls
    that mutate are synchronised: `close()` claims its flag with a
    compare-exchange, and `subscribe()` writes the rebalance handler slots
    under a latch the trampoline reads them through.

    `close()` is the one that mattered. It used to check a `Bool` and then
    set it, so every thread calling it could pass the check before any set
    it -- and concurrent `rd_kafka_consumer_close` calls on one handle
    **deadlock**: measured at 8 threads, one returns and seven never do.

    This does not make a single `Consumer` a work-sharing primitive. Two
    threads polling one consumer get interleaved records from a single
    assignment, which is rarely what anyone wants; librdkafka's own guidance
    is one consumer per thread, or more consumers in the group.
    """

    var _lib: Lib
    var _rk: Int
    var _closed: Atomic[DType.int32]
    # One element, on the heap: the C rebalance callback reaches this by
    # address, and a `List`'s buffer does not move when the `Consumer` does.
    var _rebalance: List[_RebalanceState]
    # The queue `consumer_poll` serves, held for `consume()`. Taken at
    # construction rather than lazily so its teardown is unconditional:
    # librdkafka requires it be destroyed **before** `consumer_close`, and a
    # reference that may or may not exist is a rule that may or may not be
    # followed.
    var _queue: Int
    # Not for sharing: `consume()` refuses a second concurrent caller rather
    # than queueing it. See `consume`.
    var _batch: _Latch

    def __init__(out self, cfg: ConsumerConfig) raises:
        self._lib = Lib()

        # The callback has to be installed on the *conf*, before the client
        # exists -- but handlers arrive later, at `subscribe()`. So the
        # trampoline is always registered and the handler slots start empty;
        # with none set it simply applies librdkafka's own default, which is
        # what an unsubscribed or handler-free consumer needs anyway.
        var state = List[_RebalanceState](capacity=1)
        state.append(_RebalanceState())
        self._rebalance = state^

        var conf = cfg._build(self._lib)
        try:
            self._lib.conf_set_rebalance_cb(conf, _rebalance_trampoline)
            self._lib.conf_set_opaque(conf, Int(self._rebalance.unsafe_ptr()))
        except e:
            self._lib.conf_destroy(conf)
            raise e

        self._rk = self._lib.new_client(RD_KAFKA_CONSUMER, conf)
        _ = self._lib.poll_set_consumer(self._rk)
        self._closed = Atomic[DType.int32](0)
        # Recorded now rather than in `__init__`'s field list because it is
        # the address of a field of `self`, which is only final once the
        # consumer is constructed. `Consumer` is neither `Copyable` nor
        # `Movable`, so it stays put from here on.
        self._rebalance[0].lib = Int(Pointer(to=self._lib))
        self._batch = _Latch()
        self._queue = self._lib.queue_get_consumer(self._rk)
        if self._queue == 0:
            raise Error("rd_kafka_queue_get_consumer returned NULL")

    def _release_queue(mut self):
        """Drop the consumer-queue reference. Idempotent.

        Separate from both teardown paths because librdkafka is emphatic
        that it has to happen **before** `rd_kafka_consumer_close`, and
        there are two places that close.
        """
        if self._queue != 0:
            try:
                self._lib.queue_destroy(self._queue)
            except:
                pass
            self._queue = 0

    def __deinit__(deinit self):
        # Destructors cannot raise; a failed close is not actionable here.
        if self._rk != 0:
            try:
                # **Before the close, always.** librdkafka: the consumer
                # queue reference "MUST" be destroyed prior to
                # `rd_kafka_consumer_close`. This is also why `_queue` is an
                # `Int` and not something with a destructor of its own -- a
                # field released at its last use in this body would be freed
                # in the wrong order relative to the close, which is exactly
                # the bug `_rebalance` had.
                self._release_queue()
                # Same compare-exchange as `close()`: if the app
                # already closed, this must not close again.
                var expected = Int32(0)
                if self._closed.compare_exchange(expected, 1):
                    _ = self._lib.consumer_close(self._rk)
                self._lib.destroy(self._rk)
            except:
                pass
        # **Load-bearing, and not obvious.** `consumer_close` fires a final
        # revoke through `_rebalance_trampoline`, which reaches this box by
        # the address handed to `rd_kafka_conf_set_opaque`. Fields are
        # released at their last use *inside the destructor body*, not after
        # it -- so without a use down here, `_rebalance` is freed before
        # `consumer_close` is even called, and the callback then reads a
        # dangling box. That was a reliable segfault for any consumer
        # dropped without an explicit `close()`; the whole test suite missed
        # it because every case closed by hand.
        #
        # `_lib` needs no such line: `consumer_close` and `destroy` are uses
        # of it, so it stays alive across exactly the window that matters.
        _ = self._rebalance^

    def subscribe(
        mut self,
        topics: List[String],
        on_assign: Optional[RebalanceHandler] = None,
        on_revoke: Optional[RebalanceHandler] = None,
        on_lost: Optional[RebalanceHandler] = None,
    ) raises:
        """Join the group and let it hand out partitions.

        The three handlers mirror `confluent-kafka`'s
        `subscribe(topics, on_assign, on_revoke, on_lost)`, and behave the
        same way: **a handler need not assign anything.** Doing nothing gets
        the default assignment, so a handler is an opportunity to intervene
        rather than an obligation to reimplement.

            def start_from_my_store(event: Rebalance) raises:
                var start_at = List[TopicPartition]()
                for tp in event.partitions:
                    start_at.append(
                        TopicPartition(tp.topic, tp.partition, lookup(tp))
                    )
                event.assign(start_at)

            def commit_before_losing_them(event: Rebalance) raises:
                event.commit(event.partitions)

            c.subscribe(["events"],
                        on_assign=start_from_my_store,
                        on_revoke=commit_before_losing_them)

        A handler is **thin**: it is called from a C callback, which carries
        no captured state, so it has to be a top-level `def` rather than a
        closure over local variables. Anything it needs comes off the
        `Rebalance` it is given.

        `on_lost` takes over from `on_revoke` when the partitions were lost
        involuntarily -- another member may already own them, so committing
        is likely to fail. With no `on_lost`, lost assignments fall through
        to `on_revoke`, which is what `confluent-kafka` documents.

        This replaces any previous subscription, handlers included.
        """
        # Three atomic stores, one per slot. The trampoline reads them on
        # whichever thread is polling, so a slot has to be publishable in one
        # write -- which is why it holds a bare code address and not an
        # `Optional`. See `_RebalanceState`; the previous shape needed a lock
        # that no test could ever fail.
        self._rebalance[0]._publish(on_assign, on_revoke, on_lost)

        # The list is owned here, and both the adds and the subscribe can
        # raise.
        var list = self._lib.topic_partition_list_new(Int32(len(topics)))
        if list == 0:
            raise Error("rd_kafka_topic_partition_list_new returned NULL")
        var rc: Int32
        try:
            for topic in topics:
                _ = self._lib.topic_partition_list_add(list, topic, -1)
            rc = self._lib.subscribe(self._rk, list)
        except e:
            self._lib.topic_partition_list_destroy(list)
            raise e
        self._lib.topic_partition_list_destroy(list)
        self._lib.raise_if(rc, "subscribe")

    def poll(self, timeout_ms: Int32 = 1000) raises -> Optional[Message]:
        """Fetch the next message.

        Returns `None` on timeout and at end-of-partition -- neither is a
        message. Any other broker error is raised. Use `poll_event` where
        the difference between the two matters.
        """
        # `take()` rather than a copy: `poll` is the hot path, and the
        # message owns its key, value and headers.
        var event = self.poll_event(timeout_ms)
        if not event.message:
            return None
        return Optional[Message](event.message.take())

    def poll_event(self, timeout_ms: Int32 = 1000) raises -> PollEvent:
        """Fetch the next message, saying which of the three things happened.

        The decode both this and `poll` run; `poll` just drops the EOF half.
        See `PollEvent` for why the distinction exists and what it costs to
        get at (an `enable_partition_eof=True` consumer).
        """
        var raw = self._lib.consumer_poll(self._rk, timeout_ms)
        if raw == 0:
            return PollEvent()

        var err_code = _load_i32(raw + MSG_ERR)
        if err_code == RD_KAFKA_RESP_ERR__PARTITION_EOF:
            # An EOF mark is a message struct with no payload: the topic,
            # partition and offset fields are filled in, and the offset is
            # the end of the partition. Decoding it can raise, so the
            # destroy is on both paths.
            var at: TopicPartition
            try:
                at = TopicPartition(
                    self._lib.topic_name(_load_word(raw + MSG_RKT)),
                    _load_i32(raw + MSG_PARTITION),
                    _load_i64(raw + MSG_OFFSET),
                )
            except e:
                self._lib.message_destroy(raw)
                raise e
            self._lib.message_destroy(raw)
            return PollEvent(at^)
        if err_code != RD_KAFKA_RESP_ERR_NO_ERROR:
            var e = self._lib.error(err_code)
            self._lib.message_destroy(raw)
            raise Error("poll: " + String(e))

        # Every exit from here on has to destroy `raw`, including the ones
        # taken by a raising decode. `_decode` is shared with the batch path,
        # which is why it takes the topic name rather than reading it: there
        # it is usually already known from the previous message.
        var msg: Message
        try:
            msg = self._decode(
                raw, self._lib.topic_name(_load_word(raw + MSG_RKT))
            )
        except e:
            self._lib.message_destroy(raw)
            raise e
        self._lib.message_destroy(raw)
        return PollEvent(msg^)

    def consume(
        mut self, n: Int, timeout_ms: Int32 = 1000
    ) raises -> List[Message]:
        """Fetch up to `n` messages in one call. The batch path.

        `poll()` crosses into C several times per message; this crosses once
        for the fetch and hands back a run of records Mojo can then walk
        without a per-message round trip. That is the reason this exists and
        the reason it is worth more in Mojo than the same call is in Python.

        Returns fewer than `n` -- possibly none -- when the timeout expires
        first. An empty list is a quiet partition, not an error.

        **`timeout_ms` is how long librdkafka will wait to *fill* the batch,
        not how long until it returns what it has.** Measured: 5 available
        records with `n=64` and a 10s timeout come back after the full 10s.
        So size the timeout for the latency you can accept on a partly-full
        batch, not for the fetch -- a large `n` with a long timeout is a
        slow tail on a quiet topic, not a faster one.

        End-of-partition marks are dropped, exactly as `poll()` drops them;
        use `consume_events()` if you want them. A hard error **raises**, and
        that is the one thing to know before choosing between the two: the
        messages that arrived alongside it are lost with it, because a Mojo
        `Error` carries only text and cannot bring them along. A caller who
        cannot afford that uses `consume_events()`, which returns the verdict
        per entry and loses nothing. `poll()` and `poll_event()` are the same
        pair for the same reason.

        **One `consume()` at a time per consumer.** Two concurrent calls on
        one consumer are undefined behaviour in librdkafka -- not slow,
        undefined, and it says the case "will not be supported in future as
        well". A second caller is **refused** with an exception rather than
        queued: serialising would hide the bug and silently change what the
        program does. Scale by giving each thread its own `Consumer` in the
        same group, which is what librdkafka recommends, or fan the returned
        batch out across threads, which is what makes this fast in Mojo.
        """
        var events = self.consume_events(n, timeout_ms)
        var out = List[Message](capacity=len(events))
        for ref event in events:
            if event.message:
                out.append(event.message.value().copy())
        return out^

    def consume_borrowed(
        mut self, n: Int, timeout_ms: Int32 = 1000
    ) raises -> MessageBatch:
        """Fetch up to `n` records and lend them **without copying**.

        The zero-copy counterpart to `consume()`. Where that copies every key,
        value and header into owned Mojo storage, this hands back a
        `MessageBatch` that still owns librdkafka's messages and lends
        `BorrowedMessage` views straight into them -- the shape
        `rust-rdkafka` uses, and the reason a binding can be as fast as the
        library underneath it.

        Use it when the records are consumed **inside the loop**: parsed,
        aggregated, forwarded. Use `consume()` when a record has to outlive
        the batch, or when you need headers -- a borrowed view deliberately
        has none, because copying them per message is the cost this avoids.

        The batch owns the messages, so nothing it lends can outlive it, and
        the compiler enforces that rather than the docs; see
        `BorrowedMessage`. End the scope with `_ = batch^` if the last use of
        a span is not obviously the last use of the batch.

        End-of-partition marks carry no payload to lend, so they are not
        records in the batch -- ask `batch.reached_end()` instead, which is
        what makes a bounded drain possible **without leaving this path**.
        A hard error is likewise not a record; it raises only if the batch
        held nothing else, so one bad entry cannot take the good ones with
        it. That is the same policy `consume_events()` uses.

        Same one-at-a-time rule as `consume()`, enforced the same way.
        """
        if n <= 0 or n > MAX_BATCH:
            raise Error(
                "consume_borrowed: n must be between 1 and "
                + String(MAX_BATCH)
                + ", got "
                + String(n)
            )
        if self._queue == 0:
            raise Error(
                "consume_borrowed(): the consumer is closed. The"
                " consumer-queue reference is released before"
                " rd_kafka_consumer_close, which librdkafka requires, so"
                " there is nothing left to read from."
            )
        if not self._batch.try_acquire():
            raise Error(
                "consume_borrowed(): another thread is already inside"
                " consume() on this consumer. Concurrent batch consumption"
                " is undefined behaviour in librdkafka; give each thread its"
                " own Consumer in the same group, or split the batch this"
                " one returns."
            )

        var batch: MessageBatch
        try:
            batch = self._consume_borrowed_locked(n, timeout_ms)
        except e:
            self._batch.release()
            raise e
        self._batch.release()
        return batch^

    def _consume_borrowed_locked(
        mut self, n: Int, timeout_ms: Int32
    ) raises -> MessageBatch:
        """The body of `consume_borrowed`, with the batch latch held.

        Nothing here is copied out of a message, so nothing here can raise
        after the fetch -- which matters, because a raise would have to
        destroy every message it had not yet handed to the batch.
        """
        var raws = List[Int](capacity=n)
        for _ in range(n):
            raws.append(0)

        var count = self._lib.consume_batch_queue(
            self._queue, timeout_ms, Int(raws.unsafe_ptr()), n
        )
        if count < 0:
            var code = self._lib.last_error()
            _ = raws^
            raise Error("consume_borrowed: " + String(self._lib.error(code)))

        # Keep only the records with a payload to lend, destroying the rest
        # here so the batch never holds a message it cannot describe.
        var kept = List[Int](capacity=count)
        var topic_addr = 0
        var topic_len = 0
        var eof = False
        var failure = Optional[KafkaError](None)
        for i in range(count):
            var raw = raws[i]
            if raw == 0:
                continue
            var err_code = _load_i32(raw + MSG_ERR)
            if err_code == RD_KAFKA_RESP_ERR__PARTITION_EOF:
                # No payload to lend, but the caller still needs to know --
                # see `MessageBatch.reached_end`.
                eof = True
                self._lib.message_destroy(raw)
                continue
            if err_code != RD_KAFKA_RESP_ERR_NO_ERROR:
                if not failure:
                    failure = self._lib.error(err_code)
                self._lib.message_destroy(raw)
                continue
            if topic_addr == 0:
                # Once per batch, not per message: `rd_kafka_topic_name` is a
                # crossing, and the pointer it returns belongs to the topic
                # handle, which outlives every message in this batch.
                topic_addr = self._lib.topic_name_ptr(_load_word(raw + MSG_RKT))
                topic_len = self._lib.cstr_len(topic_addr)
            kept.append(raw)
        _ = raws^
        # Same policy as `consume_events`: an error beside good records is
        # reported through the records the caller already has; one on its own
        # has nowhere else to go.
        if failure and len(kept) == 0:
            raise Error("consume_borrowed: " + String(failure.value()))
        return MessageBatch(Lib(), kept^, topic_addr, topic_len, eof)

    def consume_events(
        mut self, n: Int, timeout_ms: Int32 = 1000
    ) raises -> List[PollEvent]:
        """`consume()` without the lossy parts: one verdict per entry.

        Every entry is a message, an end-of-partition mark, or -- unlike
        `poll_event()` -- nothing at all where a hard error was decoded, and
        the error is raised only if it is the **only** thing in the batch.
        That is the same policy the consumer control plane already uses for
        `position` / `committed` / `offsets_for_times`: one bad partition
        must not hide the good answers beside it.

        Same one-at-a-time rule as `consume()`; see there.
        """
        # The bound is `confluent-kafka`'s, and it is not arbitrary: the
        # array of `rd_kafka_message_t*` is allocated up front, so an
        # unbounded `n` turns one caller's slip into an unbounded allocation
        # before a single message has arrived.
        if n <= 0 or n > MAX_BATCH:
            # **A deliberate divergence at 0.** `confluent-kafka` accepts
            # `consume(0)` and returns an empty list; this raises. A drain
            # loop written around a `consume(n)` that can silently return
            # nothing for a reason that has nothing to do with the topic
            # spins forever, and 0 is only ever reached by accident -- a
            # variable that was meant to hold a batch size and did not.
            raise Error(
                "consume: n must be between 1 and "
                + String(MAX_BATCH)
                + ", got "
                + String(n)
            )

        # **A closed consumer has no queue, and NULL is not a queue
        # librdkafka checks for.** `close()` and the destructor both drop the
        # reference before `rd_kafka_consumer_close`, as librdkafka requires,
        # which leaves `_queue` at 0 -- and handing that to
        # `rd_kafka_consume_batch_queue` faults inside `rd_kafka_consume_batch0`
        # rather than returning an error. `poll()` after `close()` does not
        # crash, so this asymmetry belongs to the batch path alone and has to
        # be caught here.
        if self._queue == 0:
            raise Error(
                "consume(): the consumer is closed. The consumer-queue"
                " reference is released before rd_kafka_consumer_close, which"
                " librdkafka requires, so there is nothing left to read from."
            )

        if not self._batch.try_acquire():
            # Refused, not queued. See `consume`.
            raise Error(
                "consume(): another thread is already inside consume() on"
                " this consumer. Concurrent batch consumption is undefined"
                " behaviour in librdkafka; give each thread its own Consumer"
                " in the same group, or split the batch this one returns."
            )

        var out: List[PollEvent]
        try:
            out = self._consume_locked(n, timeout_ms)
        except e:
            # Released before the raise, never under it -- see `_sync`.
            self._batch.release()
            raise e
        self._batch.release()
        return out^

    def _consume_locked(
        mut self, n: Int, timeout_ms: Int32
    ) raises -> List[PollEvent]:
        """The body of `consume_events`, with the batch latch already held.

        Note that FFI calls happen inside this critical section, which
        `_sync`'s first rule forbids in general. The rule exists because the
        producer's latch is contended by a **callback** that librdkafka
        invokes from inside the very call the holder is making. This latch is
        not: it is contended only by other `consume()` callers, and the one
        callback reachable from here -- the rebalance trampoline -- takes no
        latch at all any more, it reads atomic words. There is no cycle to
        close.
        """
        # One array of `rd_kafka_message_t*`, sized once. `List[Int]` rather
        # than bytes so the pointer alignment comes from the element type,
        # the same reason `_VuArray` is a `List[Int]`.
        var raws = List[Int](capacity=n)
        for _ in range(n):
            raws.append(0)

        var count = self._lib.consume_batch_queue(
            self._queue, timeout_ms, Int(raws.unsafe_ptr()), n
        )
        if count < 0:
            var code = self._lib.last_error()
            _ = raws^
            raise Error("consume: " + String(self._lib.error(code)))

        # Every message from here on is ours to destroy, including on the
        # paths a decode raises on -- so the loop never raises, and the one
        # error worth surfacing is carried out and raised after the sweep.
        var out = List[PollEvent](capacity=count)
        var failure = Optional[KafkaError](None)
        # A batch is nearly always one topic, and `rd_kafka_topic_name` is a
        # crossing per message otherwise. Caching the last handle is what
        # turns 3 crossings per message into 2.
        var last_rkt = 0
        var last_name = String("")

        for i in range(count):
            var raw = raws[i]
            if raw == 0:
                continue
            try:
                var err_code = _load_i32(raw + MSG_ERR)
                var rkt = _load_word(raw + MSG_RKT)
                if rkt != last_rkt:
                    last_name = self._lib.topic_name(rkt)
                    last_rkt = rkt

                if err_code == RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    out.append(
                        PollEvent(
                            TopicPartition(
                                last_name,
                                _load_i32(raw + MSG_PARTITION),
                                _load_i64(raw + MSG_OFFSET),
                            )
                        )
                    )
                elif err_code != RD_KAFKA_RESP_ERR_NO_ERROR:
                    if not failure:
                        failure = self._lib.error(err_code)
                else:
                    out.append(PollEvent(self._decode(raw, last_name)))
            except e:
                if not failure:
                    failure = KafkaError(-1, String(e))
            self._lib.message_destroy(raw)

        _ = raws^
        # An error alongside good records is reported through the records
        # the caller already has; one on its own has nowhere else to go.
        if failure and len(out) == 0:
            raise Error("consume: " + String(failure.value()))
        return out^

    def _decode(self, raw: Int, topic: String) raises -> Message:
        """Build a `Message` from a raw one whose topic name is already known.

        Split out of `poll_event` so the batch path can reuse it *and* skip
        the `rd_kafka_topic_name` crossing when the previous message shared
        the handle.
        """
        # The timestamp is the one field that is not at a fixed offset in
        # `rd_kafka_message_t` -- it lives in librdkafka's private part of
        # the allocation, so it is read through the accessor, which also
        # fills in which clock it came from.
        var tstype = Array[Int32, 1](fill=TIMESTAMP_NOT_AVAILABLE)
        var timestamp = self._lib.message_timestamp(
            raw, Int(tstype.unsafe_ptr())
        )
        return Message(
            topic,
            _load_i32(raw + MSG_PARTITION),
            _load_i64(raw + MSG_OFFSET),
            copy_bytes(
                _load_word(raw + MSG_KEY), _load_word(raw + MSG_KEY_LEN)
            ),
            copy_bytes(
                _load_word(raw + MSG_PAYLOAD), _load_word(raw + MSG_LEN)
            ),
            self._headers_of(raw),
            timestamp,
            tstype[0],
        )

    def _headers_of(self, raw: Int) raises -> List[Header]:
        """Copy a raw message's headers out before the message is destroyed.

        The list `rd_kafka_message_headers` hands back is **borrowed**: it
        belongs to the message and dies with it, so every name and value is
        copied here rather than pointed at. Destroying it would double-free.

        A record with no headers is not an error -- librdkafka reports
        `__NOENT` for it, and so does the iterator once it runs off the end
        of the list. Both mean "stop", not "fail".
        """
        var out = List[Header]()
        var hdrs_out = Array[Int, 1](fill=0)
        var rc = self._lib.message_headers(raw, Int(hdrs_out.unsafe_ptr()))
        if rc == RD_KAFKA_RESP_ERR__NOENT:
            return out^
        self._lib.raise_if(rc, "message_headers")

        var hdrs = hdrs_out[0]
        if hdrs == 0:
            return out^

        # Reused across iterations: `header_get_all` overwrites all three on
        # every call, so rebuilding them per header is pure churn.
        var namep = Array[Int, 1](fill=0)
        var valuep = Array[Int, 1](fill=0)
        var sizep = Array[Int, 1](fill=0)
        var idx = 0
        while True:
            var got = self._lib.header_get_all(
                hdrs,
                idx,
                Int(namep.unsafe_ptr()),
                Int(valuep.unsafe_ptr()),
                Int(sizep.unsafe_ptr()),
            )
            if got == RD_KAFKA_RESP_ERR__NOENT:
                break
            self._lib.raise_if(got, "header_get_all")
            # `copy_bytes` and not a plain read: librdkafka leaves the value
            # pointer NULL for a header whose value is null, which is a
            # different header from one whose value is present and empty.
            out.append(Header(cstr(namep[0]), copy_bytes(valuep[0], sizep[0])))
            idx += 1
        return out^

    # -- control plane ------------------------------------------------------
    #
    # Every call below speaks in `rd_kafka_topic_partition_list_t`, and most
    # of them answer *into* the list they were given rather than through
    # their return code. Two rules follow, and `_control` enforces both in
    # one place so no individual call site has to remember them:
    #
    # - The list is ours from the moment it is created until it is destroyed,
    #   including on every raising path in between.
    # - The return code is only the *request*-level verdict. A per-partition
    #   failure lands in that element's `err`, and reading only the return
    #   code reports a seek or a pause that never happened -- the same trap
    #   `AdminClient.create_topic` has with `rd_kafka_event_error`.

    def consumer_group_metadata(self) raises -> ConsumerGroupMetadata:
        """This consumer's group identity, for exactly-once.

        Hand it to `Producer.send_offsets_to_transaction()` so a transaction
        can commit the offsets this consumer read, atomically with whatever
        the producer wrote. That is the whole read-process-write loop:

            var work = consumer.poll()
            ...
            _ = producer.send_offsets_to_transaction(
                consumer.position(assignment),
                consumer.consumer_group_metadata(),
            )
            _ = producer.commit_transaction()

        Take it **inside the transaction**, not once at startup: it captures
        the group generation, and librdkafka rejects offsets sent under one
        a rebalance has since superseded.

        Raises if this consumer has no `group.id` -- there is no group to
        commit to, and librdkafka answers NULL rather than an error code.
        """
        return ConsumerGroupMetadata(
            self._lib.consumer_group_metadata(self._rk)
        )

    def _tpl_build(self, partitions: List[TopicPartition]) raises -> Int:
        """Marshal `partitions` into a C list. The caller owns the result.

        Shared with the rebalance callback, which cannot reach a method --
        see `_build_tpl`.
        """
        return _build_tpl(self._lib, partitions)

    def _tpl_read(self, list: Int) raises -> List[TopicPartition]:
        """Decode a C list back, per-partition errors included."""
        return _decode_tpl(self._lib, list)

    def _control[
        op: StaticString
    ](
        self, partitions: List[TopicPartition], timeout_ms: Int32 = 0
    ) raises -> List[TopicPartition]:
        """Build a list, make one control call, decode the list, destroy it.

        The only thing that varies between these calls is the middle line, so
        the ownership dance is written once here rather than seven times --
        every one of those copies would be a chance to leak the list on a
        raising path.

        The decoded list is always returned even for the calls that have no
        result to speak of, because that is where their per-partition errors
        are; `_raise_on_partition_error` is what turns those into an
        exception for the calls that return nothing.
        """
        var list = self._tpl_build(partitions)
        var out: List[TopicPartition]
        try:
            comptime if op == "assign":
                self._lib.raise_if(self._lib.assign(self._rk, list), op)
            elif op == "position":
                self._lib.raise_if(self._lib.position(self._rk, list), op)
            elif op == "committed":
                self._lib.raise_if(
                    self._lib.committed(self._rk, list, timeout_ms), op
                )
            elif op == "pause":
                self._lib.raise_if(
                    self._lib.pause_partitions(self._rk, list), op
                )
            elif op == "resume":
                self._lib.raise_if(
                    self._lib.resume_partitions(self._rk, list), op
                )
            elif op == "offsets_for_times":
                self._lib.raise_if(
                    self._lib.offsets_for_times(self._rk, list, timeout_ms), op
                )
            else:
                # `seek`, the odd one out: it hands back a
                # `rd_kafka_error_t*` that is NULL on success -- the reversed
                # polarity `produceva` has -- and the object is ours to
                # destroy, which is what `take_error` does.
                var err = self._lib.seek_partitions(self._rk, list, timeout_ms)
                if err != 0:
                    raise Error(
                        String(op) + ": " + String(self._lib.take_error(err))
                    )
            out = self._tpl_read(list)
        except e:
            self._lib.topic_partition_list_destroy(list)
            raise e
        self._lib.topic_partition_list_destroy(list)
        return out^

    def _raise_on_partition_error(
        self, partitions: List[TopicPartition], context: String
    ) raises:
        """Raise on the first partition librdkafka refused.

        For the calls that return nothing there is nowhere to put a
        per-partition failure, so it has to become an exception or be lost.
        The calls that *do* return a list leave theirs in place instead --
        one bad partition should not hide two good answers -- which is what
        `TopicPartition.has_error()` is for.
        """
        for tp in partitions:
            if tp.has_error():
                raise Error(
                    context
                    + "("
                    + String(tp)
                    + "): "
                    + String(self._lib.error(tp.error_code))
                )

    def assign(self, partitions: List[TopicPartition]) raises:
        """Take these partitions, dropping whatever was assigned before.

        This replaces the whole assignment -- it is not additive. A
        partition given `OFFSET_INVALID` (the default) starts wherever the
        group last committed, falling back to `auto_offset_reset`; name
        `OFFSET_BEGINNING`, `OFFSET_END` or a literal offset to override
        that. Pass an empty list, or call `unassign()`, to consume nothing.

        `assign()` and `subscribe()` are alternatives, not partners: a
        consumer that assigns manually does not take part in the group's
        partition distribution, though it still commits under its `group.id`.
        """
        _ = self._control["assign"](partitions)

    def unassign(self) raises:
        """Drop the assignment entirely and stop consuming."""
        self._lib.raise_if(self._lib.assign(self._rk, 0), "unassign")

    def seek(
        self, partitions: List[TopicPartition], timeout_ms: Int32 = 5000
    ) raises:
        """Move the read position of already-assigned partitions.

        Each entry's `offset` is where to resume: an absolute offset,
        `OFFSET_BEGINNING`, `OFFSET_END`, or `OFFSET_STORED` for the group's
        last commit. The partitions must be assigned already -- seeking one
        that is not is a per-partition error, and this raises on the first.

        Seeking does not commit. A seek followed by a crash resumes from the
        last committed offset, not from the seek target.
        """
        var done = self._control["seek"](partitions, timeout_ms)
        self._raise_on_partition_error(done, "seek")

    def position(
        self, partitions: List[TopicPartition]
    ) raises -> List[TopicPartition]:
        """The next offset this consumer will read from each partition.

        Local and immediate -- no broker round trip. A partition nothing has
        been read from yet comes back `OFFSET_INVALID`, which is not an
        error. Paired with `query_watermark_offsets`, this is the lag
        measurement: `watermarks.high - position.offset`.
        """
        return self._control["position"](partitions)

    def committed(
        self, partitions: List[TopicPartition], timeout_ms: Int32 = 5000
    ) raises -> List[TopicPartition]:
        """The offsets this consumer's group has committed, from the broker.

        A partition the group has never committed comes back
        `OFFSET_INVALID`. This is the group's stored progress, which is not
        the same as `position()`: everything consumed since the last commit
        sits between them.

        Per-partition failures are returned in place rather than raised --
        check `has_error()` on each entry.
        """
        return self._control["committed"](partitions, timeout_ms)

    def pause(self, partitions: List[TopicPartition]) raises:
        """Stop fetching these partitions without giving them up.

        The assignment is unchanged and no rebalance is triggered, so this is
        how a job applies backpressure to one slow partition while the rest
        keep flowing. Messages already fetched into the local queue are still
        returned by `poll()`.
        """
        var done = self._control["pause"](partitions)
        self._raise_on_partition_error(done, "pause")

    def resume(self, partitions: List[TopicPartition]) raises:
        """Start fetching paused partitions again, from where they stopped."""
        var done = self._control["resume"](partitions)
        self._raise_on_partition_error(done, "resume")

    def offsets_for_times(
        self, partitions: List[TopicPartition], timeout_ms: Int32 = 5000
    ) raises -> List[TopicPartition]:
        """Look up offsets by time: replay everything since a wall-clock point.

        Each entry's `offset` goes **in** as a millisecond Unix timestamp and
        comes **out** as the first offset at or after it -- the field is used
        in both directions, which is the whole shape of this API. A partition
        whose records are all older than the timestamp comes back
        `OFFSET_END`, meaning "nothing to replay here".

        Feed the result straight to `assign()` or `seek()`. The broker
        answers from the record timestamps, so which clock those carry
        matters -- see `Message.has_timestamp`.
        """
        return self._control["offsets_for_times"](partitions, timeout_ms)

    def query_watermark_offsets(
        self, topic: String, partition: Int32, timeout_ms: Int32 = 5000
    ) raises -> Watermarks:
        """Ask the broker for a partition's first and last offsets.

        A blocking round trip, which is what makes it accurate. Use
        `get_watermark_offsets` for the cached answer in a hot loop.
        """
        var low = Array[Int64, 1](fill=0)
        var high = Array[Int64, 1](fill=0)
        var rc = self._lib.query_watermark_offsets(
            self._rk,
            topic,
            partition,
            Int(low.unsafe_ptr()),
            Int(high.unsafe_ptr()),
            timeout_ms,
        )
        self._lib.raise_if(
            rc,
            "query_watermark_offsets(" + topic + "[" + String(partition) + "])",
        )
        return Watermarks(low[0], high[0])

    def get_watermark_offsets(
        self, topic: String, partition: Int32
    ) raises -> Watermarks:
        """The watermarks this client already knows -- no broker round trip.

        They are a by-product of fetching, so they are only populated for a
        partition this consumer is actually reading, and `high` is as fresh
        as the last fetch response rather than as fresh as the partition.
        That is the trade: free, and slightly behind. A partition that has
        not been fetched from yet raises.
        """
        var low = Array[Int64, 1](fill=0)
        var high = Array[Int64, 1](fill=0)
        var rc = self._lib.get_watermark_offsets(
            self._rk,
            topic,
            partition,
            Int(low.unsafe_ptr()),
            Int(high.unsafe_ptr()),
        )
        self._lib.raise_if(
            rc,
            "get_watermark_offsets(" + topic + "[" + String(partition) + "])",
        )
        return Watermarks(low[0], high[0])

    def commit(self, asynchronous: Bool = False) raises:
        """Commit the current offsets for this consumer group."""
        var rc = self._lib.commit(
            self._rk, 0, Int32(1) if asynchronous else Int32(0)
        )
        self._lib.raise_if(rc, "commit")

    def close(mut self) raises:
        """Leave the group cleanly. Safe to call more than once, from any
        thread.

        The flag is claimed with a compare-exchange rather than a read
        followed by a write, so exactly one caller ever reaches
        `rd_kafka_consumer_close`. Two threads racing a plain `if
        self._closed` both passed the check and both closed, and the second
        close reports an error for a consumer that had in fact shut down
        cleanly.

        A caller that loses the race returns **immediately** rather than
        waiting for the winner to finish. Leaving a group is a network round
        trip, and spinning a core for the length of one is worse than the
        wrinkle it would fix -- and there is no blocking primitive in Mojo
        1.0 to wait on properly. The close is in progress either way.
        """
        var expected = Int32(0)
        if not self._closed.compare_exchange(expected, 1):
            return
        # librdkafka requires the consumer-queue reference to go first.
        self._release_queue()
        # Claimed before the call, so a close that fails still counts as
        # done -- retrying it would only report the same failure twice.
        self._lib.raise_if(self._lib.consumer_close(self._rk), "close")

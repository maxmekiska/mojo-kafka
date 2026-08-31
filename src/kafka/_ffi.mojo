"""Raw librdkafka FFI declarations.

This is the only file that talks to C directly. Everything else in the
package goes through it, so bumping librdkafka or changing loader
strategy stays local.

Five conventions keep the layers above this file safe:

1. **librdkafka is loaded, not linked.** Bare `external_call` resolves its
   symbols when the JIT materialises the program, which is before anything
   has had a chance to load the library -- under `mojo run` the symbol
   simply does not exist. `OwnedDLHandle` loads it explicitly and works
   under both `mojo run` and `mojo build`.

2. **C pointers cross this boundary as `Int` addresses, never as `Pointer`.**
   Mojo 1.0's `Pointer` is non-nullable by design, but nearly every
   librdkafka call signals failure by returning NULL, so the null check has
   to happen while the value is still an integer.

3. **Foreign memory is read through `ImmutAnyOrigin`.** `ImmStaticOrigin`
   is wrong here and faults at runtime: it lets the optimiser assume the
   data lives in this module's own static storage.

4. **Every symbol is resolved once, in `Lib.__init__`.** `get_function` does
   a `dlsym` by string, which costs more than the C call it wraps. See the
   note above `_bind` for how the resolved callables are held.

5. **No variadic C calls.** Calling a C variadic through a fixed prototype is
   undefined on the SysV and AAPCS ABIs -- the `%al` vector-register count is
   never set -- so `rd_kafka_producev` is off limits however convenient its
   signature looks. Producing goes through `rd_kafka_produceva`, which takes
   an **array** of `rd_kafka_vu_t` and is an ordinary fixed-arity function.
   See `_VuArray`.
"""

from std.ffi import OwnedDLHandle, _DLCallable
from std.os import getenv


comptime RD_KAFKA_PRODUCER: Int32 = 0
comptime RD_KAFKA_CONSUMER: Int32 = 1

comptime RD_KAFKA_RESP_ERR_NO_ERROR: Int32 = 0
comptime RD_KAFKA_RESP_ERR__NOENT: Int32 = -156
comptime RD_KAFKA_RESP_ERR__TIMED_OUT: Int32 = -185
comptime RD_KAFKA_RESP_ERR__PARTITION_EOF: Int32 = -191
# The two codes a rebalance callback branches on. They arrive as the `err`
# argument and are not failures -- they are the whole message.
comptime RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS: Int32 = -175
comptime RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS: Int32 = -174
# The codes `kind_of` groups. Values taken from the installed rdkafka.h.
comptime RD_KAFKA_RESP_ERR__TRANSPORT: Int32 = -195
comptime RD_KAFKA_RESP_ERR__MSG_TIMED_OUT: Int32 = -192
comptime RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION: Int32 = -190
comptime RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC: Int32 = -188
comptime RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN: Int32 = -187
comptime RD_KAFKA_RESP_ERR__QUEUE_FULL: Int32 = -184
comptime RD_KAFKA_RESP_ERR__AUTHENTICATION: Int32 = -169
comptime RD_KAFKA_RESP_ERR__FATAL: Int32 = -150
comptime RD_KAFKA_RESP_ERR__FENCED: Int32 = -144
comptime RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART: Int32 = 3
comptime RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE: Int32 = 10
comptime RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED: Int32 = 29

comptime RD_KAFKA_PARTITION_UA: Int32 = -1

# Tell librdkafka to copy the payload out of our buffer, so the caller's
# key and value can be freed as soon as `produce()` returns. Emitted as a
# `RD_KAFKA_VTYPE_MSGFLAGS` entry -- see `_VuArray.msgflags`.
comptime RD_KAFKA_MSG_F_COPY: Int32 = 0x2

# rd_kafka_message_t field offsets on 64-bit, confirmed with `offsetof`
# against rdkafka.h. `err` and `partition` are followed by padding.
comptime MSG_ERR: Int = 0
comptime MSG_RKT: Int = 8
comptime MSG_PARTITION: Int = 16
comptime MSG_PAYLOAD: Int = 24
comptime MSG_LEN: Int = 32
comptime MSG_KEY: Int = 40
comptime MSG_KEY_LEN: Int = 48
comptime MSG_OFFSET: Int = 56
# `void *_private`: the per-message opaque on the way out, handed back
# unchanged on the delivery report. Verified with offsetof against
# librdkafka 2.15; sizeof(rd_kafka_message_t) is 72.
comptime MSG_PRIVATE: Int = 64

# An array of C pointers -- `sizeof(void *)` on the 64-bit targets this
# package supports.
comptime PTR_STRIDE: Int = 8

# rd_kafka_metadata_t: broker_cnt, brokers*, topic_cnt, topics*
comptime META_TOPIC_CNT: Int = 16
comptime META_TOPICS: Int = 24

# sizeof(rd_kafka_metadata_topic_t) on 64-bit:
#   char *topic; int partition_cnt; <4 pad>; partitions*; err; <4 pad>
# That is 32 bytes, not 24 -- a short stride walks off into unmapped
# memory a few topics in.
comptime META_TOPIC_STRIDE: Int = 32

# rd_kafka_topic_partition_list_t and its elements, probed with
# `offsetof`/`sizeof` against librdkafka 2.15 rather than reasoned about.
#
# The TPL is the currency of the whole consumer control plane: `assign`,
# `seek`, `position`, `committed`, `pause`/`resume` and `offsets_for_times`
# all take one, and most of them *fill it in* -- the answer comes back in the
# same list that carried the question.
comptime TPL_CNT: Int = 0
comptime TPL_SIZE: Int = 4
comptime TPL_ELEMS: Int = 8

# sizeof(rd_kafka_topic_partition_t) is **64**, not the 56 its seven members
# add up to: `err` at 48 is a 4-byte enum, and the struct ends with an
# opaque `void *_private` at 56. This is the metadata stride trap again --
# a 56-byte stride decodes the first element correctly and then walks off.
comptime TP_TOPIC: Int = 0
comptime TP_PARTITION: Int = 8
comptime TP_OFFSET: Int = 16
comptime TP_METADATA: Int = 24
comptime TP_METADATA_SIZE: Int = 32
comptime TP_OPAQUE: Int = 40
comptime TP_ERR: Int = 48
comptime TP_STRIDE: Int = 64

# Offset sentinels. These occupy the same `int64_t` as a real offset, which
# is why an offset field is never just a count -- see `OFFSET_INVALID`, the
# value `position()` reports for a partition nothing has been read from yet.
comptime RD_KAFKA_OFFSET_BEGINNING: Int64 = -2
comptime RD_KAFKA_OFFSET_END: Int64 = -1
comptime RD_KAFKA_OFFSET_STORED: Int64 = -1000
comptime RD_KAFKA_OFFSET_INVALID: Int64 = -1001

# rd_kafka_timestamp_type_t. `NOT_AVAILABLE` is what a broker older than
# 0.10 yields, and it is the reason `Message.timestamp` is paired with a
# type: -1 is a plausible timestamp only if you do not check.
comptime RD_KAFKA_TIMESTAMP_NOT_AVAILABLE: Int32 = 0
comptime RD_KAFKA_TIMESTAMP_CREATE_TIME: Int32 = 1
comptime RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME: Int32 = 2

# rd_kafka_vtype_t discriminants, for the `vu` entries `_VuArray` builds.
# Only the ones this package emits are named; the enum has more.
comptime RD_KAFKA_VTYPE_TOPIC: Int32 = 1
comptime RD_KAFKA_VTYPE_PARTITION: Int32 = 3
comptime RD_KAFKA_VTYPE_VALUE: Int32 = 4
comptime RD_KAFKA_VTYPE_KEY: Int32 = 5
comptime RD_KAFKA_VTYPE_OPAQUE: Int32 = 6
comptime RD_KAFKA_VTYPE_MSGFLAGS: Int32 = 7
comptime RD_KAFKA_VTYPE_TIMESTAMP: Int32 = 8
comptime RD_KAFKA_VTYPE_HEADERS: Int32 = 10

# rd_kafka_vu_t on 64-bit, confirmed with `offsetof`/`sizeof` against
# rdkafka.h rather than reasoned about:
#
#     struct { rd_kafka_vtype_t vtype; union { ...; char _pad[64]; } u; }
#
# The union is 64 bytes because of that `_pad` -- librdkafka sizes it for
# future vtypes -- so an entry is 4 bytes of tag, 4 of padding and 64 of
# union: 72 in total, not the 24 the largest live member would suggest. A
# short stride here has the same failure mode as the metadata one above.
comptime VU_STRIDE: Int = 72
comptime VU_WORDS: Int = 9

# Offsets within one entry. Every vtype reads its arguments from the same
# union, so the slots are named by position rather than by member: ARG0 is
# `u.cstr` / `u.i32` / `u.mem.ptr` / `u.headers` depending on the tag, and
# ARG1 is `u.mem.size`.
comptime VU_VTYPE: Int = 0
comptime VU_ARG0: Int = 8
comptime VU_ARG1: Int = 16


# The signature librdkafka calls a rebalance callback with:
#
#     void (*)(rd_kafka_t *, rd_kafka_resp_err_t,
#              rd_kafka_topic_partition_list_t *, void *)
#
# `thin` because a C callback carries no captured state, and `abi("C")` for
# the platform calling convention -- Mojo 1.0 can supply both, so this needs
# no event-queue workaround. Pointers cross as `Int` here as everywhere else.
comptime RebalanceCallback = def(Int, Int32, Int, Int) thin abi("C") -> None


# The signature librdkafka calls a delivery-report callback with:
#
#     void (*)(rd_kafka_t *, const rd_kafka_message_t *, void *)
#
# One call per produced message, from inside `rd_kafka_poll` /
# `rd_kafka_flush` on the thread that called them -- never from a background
# thread, which is what makes it safe to touch Mojo state from here.
comptime DeliveryCallback = def(Int, Int, Int) thin abi("C") -> None


# --- reading foreign memory -------------------------------------------------


def _load_i32(addr: Int) raises -> Int32:
    return Pointer[Int32, ImmutAnyOrigin](unsafe_from_address=addr)[
        unsafe_offset=0
    ]


def _load_i64(addr: Int) raises -> Int64:
    return Pointer[Int64, ImmutAnyOrigin](unsafe_from_address=addr)[
        unsafe_offset=0
    ]


def _load_word(addr: Int) raises -> Int:
    """Load a pointer-sized value: a `void *` or a `size_t`."""
    return Pointer[Int, ImmutAnyOrigin](unsafe_from_address=addr)[
        unsafe_offset=0
    ]


def _store_i32(addr: Int, value: Int32):
    Pointer[Int32, MutAnyOrigin](unsafe_from_address=addr)[
        unsafe_offset=0
    ] = value


def _store_i64(addr: Int, value: Int64):
    Pointer[Int64, MutAnyOrigin](unsafe_from_address=addr)[
        unsafe_offset=0
    ] = value


def _store_word(addr: Int, value: Int):
    """Store a pointer-sized value: a `void *` or a `size_t`."""
    Pointer[Int, MutAnyOrigin](unsafe_from_address=addr)[
        unsafe_offset=0
    ] = value


def _bytes_to_string(addr: Int, length: Int) raises -> String:
    """Copy `length` bytes at `addr` into an owned Mojo `String`."""
    if addr == 0 or length <= 0:
        return String("")
    var p = Pointer[UInt8, ImmutAnyOrigin](unsafe_from_address=addr)
    return String(
        StringSpan(
            unsafe_from_utf8=Span[UInt8, ImmutAnyOrigin](
                unsafe_ptr=p, length=length
            )
        )
    )


def copy_bytes(addr: Int, length: Int) raises -> Optional[List[UInt8]]:
    """Copy `length` bytes at `addr`, or `None` when the pointer is NULL.

    Presence is read from the **pointer**, not the length. Kafka treats a
    null field as distinct from an empty one -- that distinction is what a
    compaction tombstone is made of -- and librdkafka signals it by leaving
    the pointer NULL, so a NUL pointer must not collapse into an empty list.
    """
    if addr == 0:
        return None
    var p = Pointer[UInt8, ImmutAnyOrigin](unsafe_from_address=addr)
    return List[UInt8](
        Span[UInt8, ImmutAnyOrigin](
            unsafe_ptr=p, length=length if length > 0 else 0
        )
    )


def cstr(addr: Int) raises -> String:
    """Copy a NUL-terminated C string at `addr` into an owned `String`."""
    if addr == 0:
        return String("")
    var p = Pointer[UInt8, ImmutAnyOrigin](unsafe_from_address=addr)
    var n = 0
    while p[unsafe_offset=n] != 0:
        n += 1
    return _bytes_to_string(addr, n)


def _c_string(s: String) raises -> List[UInt8]:
    """Copy `s` into a NUL-terminated buffer for C.

    `String.unsafe_ptr()` is **not** NUL-terminated in Mojo 1.0. Whether the
    byte past the end happens to be zero depends on allocator reuse, so
    passing it straight to a C function that expects a C string works most
    of the time and then intermittently appends garbage -- a topic named
    `events` becomes `events=\xef`, or a broker list picks up a suffix.

    Length-delimited arguments (message keys and payloads) do not need this;
    they are passed as pointer + byte count.
    """
    var buf = List[UInt8](capacity=s.byte_length() + 1)
    for byte in s.as_bytes():
        buf.append(byte)
    buf.append(0)
    return buf^


# --- building a produceva argument list -------------------------------------


struct _VuArray(Movable):
    """An array of `rd_kafka_vu_t` under construction, for `produceva`.

    `rd_kafka_producev` would express the same message far more neatly, but
    it is variadic and convention 5 rules it out. `rd_kafka_produceva` takes
    the same tagged arguments as an ordinary array instead -- so the array has
    to be laid out here, by hand, at the offsets `VU_STRIDE` and friends
    record.

    Two details keep that safe:

    - **The backing store is `List[Int]`, not `List[UInt8]`.** The union holds
      pointers and `size_t`s, so every entry has to be 8-byte aligned; an
      element type of `Int` gets that from the allocator by construction,
      where a byte list would only be aligned by luck.
    - **Capacity is fixed at construction and never grown.** `entry()` hands
      back a raw address into the buffer, and a `List` that reallocated on
      append would leave every address handed out before it dangling. Asking
      for more entries than were reserved raises instead.

    Nothing here is copied: the addresses written into the array point at
    buffers the caller still owns, so an instance must not outlive the
    `produceva` call it was built for.
    """

    var _slots: List[Int]
    var _count: Int
    var _capacity: Int

    def __init__(out self, capacity: Int):
        # Zeroed up front: an entry only writes the fields its vtype uses,
        # and librdkafka reads the whole union.
        self._slots = List[Int](length=capacity * VU_WORDS, fill=0)
        self._count = 0
        self._capacity = capacity

    def _entry(mut self, vtype: Int32) raises -> Int:
        """Append one entry with the given tag; return its byte address."""
        if self._count >= self._capacity:
            raise Error(
                "_VuArray: asked for more than the "
                + String(self._capacity)
                + " entries reserved"
            )
        var addr = Int(self._slots.unsafe_ptr()) + self._count * VU_STRIDE
        self._count += 1
        _store_i32(addr + VU_VTYPE, vtype)
        return addr

    def topic(mut self, name: Int) raises:
        """`RD_KAFKA_VTYPE_TOPIC` -- a NUL-terminated topic name.

        Taking the topic by name rather than by `rd_kafka_topic_t*` is what
        lets `Producer` drop its handle cache; see `Producer._enqueue`.
        """
        _store_word(self._entry(RD_KAFKA_VTYPE_TOPIC) + VU_ARG0, name)

    def partition(mut self, partition: Int32) raises:
        _store_i32(self._entry(RD_KAFKA_VTYPE_PARTITION) + VU_ARG0, partition)

    def msgflags(mut self, flags: Int32) raises:
        """`RD_KAFKA_VTYPE_MSGFLAGS`. Not optional in practice.

        With no flags librdkafka neither copies the payload nor frees it, and
        the caller has to keep the buffer valid until the delivery report --
        which no caller of `produce()` expects. `RD_KAFKA_MSG_F_COPY` is what
        makes returning immediately safe.
        """
        _store_i32(self._entry(RD_KAFKA_VTYPE_MSGFLAGS) + VU_ARG0, flags)

    def value(mut self, pointer: Int, length: Int) raises:
        var e = self._entry(RD_KAFKA_VTYPE_VALUE)
        _store_word(e + VU_ARG0, pointer)
        _store_word(e + VU_ARG1, length)

    def key(mut self, pointer: Int, length: Int) raises:
        var e = self._entry(RD_KAFKA_VTYPE_KEY)
        _store_word(e + VU_ARG0, pointer)
        _store_word(e + VU_ARG1, length)

    def opaque(mut self, token: Int) raises:
        """`RD_KAFKA_VTYPE_OPAQUE` -- the per-message `msg_opaque`.

        librdkafka stores this `void *` and hands it back as `_private` on
        the delivery report without ever dereferencing it, so a plain integer
        token is a legitimate value: it never has to point at anything. That
        is what makes per-message reports reachable without a `dr_msg_cb` at
        all -- which Mojo 1.0 could supply, but this package does not need.
        """
        _store_word(self._entry(RD_KAFKA_VTYPE_OPAQUE) + VU_ARG0, token)

    def timestamp(mut self, milliseconds: Int64) raises:
        """`RD_KAFKA_VTYPE_TIMESTAMP` -- the record's CreateTime, in ms.

        **0 means "stamp it now"**, which is librdkafka's own rule and the
        default `confluent-kafka` documents, so the entry is emitted
        unconditionally rather than only when a caller names a time.

        The vtype is **8**, read off the enum rather than counted: the enum
        starts at `END = 0` and includes `RKT = 2`, which this package never
        emits, so the live vtypes are not densely numbered. 2 would pass an
        `int64` where librdkafka expects a `rd_kafka_topic_t *`.
        """
        _store_i64(
            self._entry(RD_KAFKA_VTYPE_TIMESTAMP) + VU_ARG0, milliseconds
        )

    def headers(mut self, hdrs: Int) raises:
        """`RD_KAFKA_VTYPE_HEADERS` -- a whole `rd_kafka_headers_t`.

        The message **assumes ownership** of the list, but only if the
        `produceva` call succeeds; on failure it is still the caller's to
        destroy. `RD_KAFKA_VTYPE_HEADER` (singular, one header per entry) must
        never be mixed with this one in the same message -- librdkafka rejects
        that with `RD_KAFKA_RESP_ERR__CONFLICT`.
        """
        _store_word(self._entry(RD_KAFKA_VTYPE_HEADERS) + VU_ARG0, hdrs)

    def address(self) -> Int:
        return Int(self._slots.unsafe_ptr())

    def count(self) -> Int:
        return self._count


# --- errors -----------------------------------------------------------------


@fieldwise_init
struct KafkaErrorKind(Copyable, ImplicitlyCopyable, Movable, Writable):
    """A branchable category for a librdkafka error code.

    Deliberately **small**. This is not a mirror of librdkafka's error table --
    that table has hundreds of entries and `KafkaError.code` already carries
    the exact one. These are the cases a caller writes different code for, and
    the list is meant to stay short enough to read.

    Modelled on what `confluent-kafka-python` actually does rather than on the
    error table: its `Producer.c` gives exactly one code a bespoke exception
    type, `RD_KAFKA_RESP_ERR__QUEUE_FULL` -> `BufferError`, because that is the
    one a caller must handle differently -- poll to drain, then retry. Every
    other code arrives there as a generic wrapper around the number.

    Mojo 1.0 cannot carry a typed exception: a caught `Error` yields only its
    text, so `except` cannot branch on a type. The kind therefore lives on
    **values** -- `KafkaError.kind()`, `DeliveryReport.kind()` and
    `Producer.last_error_kind()` -- rather than on what gets raised.
    """

    var _tag: Int32

    def __eq__(self, other: Self) -> Bool:
        return self._tag == other._tag

    def __ne__(self, other: Self) -> Bool:
        return self._tag != other._tag

    def write_to(self, mut writer: Some[Writer]):
        writer.write(self._name())

    def _name(self) -> StaticString:
        if self._tag == 1:
            return "QUEUE_FULL"
        if self._tag == 2:
            return "TIMED_OUT"
        if self._tag == 3:
            return "TRANSPORT"
        if self._tag == 4:
            return "UNKNOWN_TOPIC_OR_PARTITION"
        if self._tag == 5:
            return "MESSAGE_TOO_LARGE"
        if self._tag == 6:
            return "AUTHORIZATION"
        if self._tag == 7:
            return "FATAL"
        return "OTHER"


# The tags. Kept as module-level constants because a struct cannot hold a
# comptime alias of its own type.
comptime KIND_OTHER = KafkaErrorKind(0)
comptime KIND_QUEUE_FULL = KafkaErrorKind(1)
comptime KIND_TIMED_OUT = KafkaErrorKind(2)
comptime KIND_TRANSPORT = KafkaErrorKind(3)
comptime KIND_UNKNOWN_TOPIC_OR_PARTITION = KafkaErrorKind(4)
comptime KIND_MESSAGE_TOO_LARGE = KafkaErrorKind(5)
comptime KIND_AUTHORIZATION = KafkaErrorKind(6)
comptime KIND_FATAL = KafkaErrorKind(7)


def kind_of(code: Int32) -> KafkaErrorKind:
    """Classify a `rd_kafka_resp_err_t`.

    Several librdkafka codes collapse onto one kind on purpose -- a caller
    retrying a transient connectivity failure does not care whether it was
    `__TRANSPORT` or `__ALL_BROKERS_DOWN`, and one that mis-routed a record
    does not care whether the broker said the topic or the partition was the
    unknown one. `KafkaError.code` still has the exact value when it matters.
    """
    if code == RD_KAFKA_RESP_ERR__QUEUE_FULL:
        return KIND_QUEUE_FULL
    if (
        code == RD_KAFKA_RESP_ERR__TIMED_OUT
        or code == RD_KAFKA_RESP_ERR__MSG_TIMED_OUT
    ):
        return KIND_TIMED_OUT
    if (
        code == RD_KAFKA_RESP_ERR__TRANSPORT
        or code == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN
    ):
        return KIND_TRANSPORT
    if (
        code == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC
        or code == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION
        or code == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART
    ):
        return KIND_UNKNOWN_TOPIC_OR_PARTITION
    if code == RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
        return KIND_MESSAGE_TOO_LARGE
    if (
        code == RD_KAFKA_RESP_ERR__AUTHENTICATION
        or code == RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED
    ):
        return KIND_AUTHORIZATION
    if code == RD_KAFKA_RESP_ERR__FATAL or code == RD_KAFKA_RESP_ERR__FENCED:
        return KIND_FATAL
    return KIND_OTHER


@fieldwise_init
struct KafkaError(Copyable, Movable, Writable):
    """A `rd_kafka_resp_err_t` with the description librdkafka gives it.

    `Writable` rather than a `describe()` of its own, so `String(err)` and
    `print(err)` work the way they do for every other Mojo type.
    """

    var code: Int32
    var message: String

    def kind(self) -> KafkaErrorKind:
        """The branchable category, for handling rather than reporting."""
        return kind_of(self.code)

    def write_to(self, mut writer: Some[Writer]):
        writer.write("KafkaError(", Int(self.code), "): ", self.message)


# --- the library handle -----------------------------------------------------


def _open_librdkafka() raises -> OwnedDLHandle:
    """Load librdkafka, preferring an explicit override if one is set."""
    var override = getenv("MOJO_KAFKA_LIBRDKAFKA")
    if override != "":
        return OwnedDLHandle(override)

    var candidates = [
        String("librdkafka.so.1"),
        String("librdkafka.so"),
        String("librdkafka.1.dylib"),
        String("librdkafka.dylib"),
    ]
    for name in candidates:
        try:
            return OwnedDLHandle(name)
        except:
            continue
    raise Error(
        "could not load librdkafka -- install it (conda-forge `librdkafka`,"
        " `apt install librdkafka-dev`, `brew install librdkafka`) or point"
        " MOJO_KAFKA_LIBRDKAFKA at the shared library."
    )


def _bind[
    R: RegisterPassable
](box: List[OwnedDLHandle], name: StaticString) raises -> _DLCallable[
    R, ImmUntrackedOrigin
]:
    """Resolve one C symbol now, so no call site pays a `dlsym` again.

    `OwnedDLHandle.get_function` looks the symbol up by string every time it
    is called, and that lookup costs more than the C call it wraps. Measured
    against librdkafka 2.15 on this machine: 45-55 ns/call resolving per call
    against 1.2-1.5 ns/call resolving once, and through the wrappers below an
    idle `Producer.poll(0)` -- one crossing and nothing else -- went from
    97-110 ns to 54-56 ns. `Consumer.poll()` crosses four times per message
    (`consumer_poll`, `topic_name`, `message_timestamp`, `message_destroy`),
    so name lookup was most of what this binding charged per message.

    Two details make holding the result possible:

    - `get_function` returns a `_DLCallable` whose origin is borrowed from the
      handle, and Mojo will not let a struct field name the origin of one of
      its own fields. The resolved symbol is re-wrapped with
      `ImmUntrackedOrigin` -- the escape hatch the compiler itself suggests --
      which moves the lifetime argument out of the type system and into this
      comment: a callable is only ever a field of the `Lib` that resolved it,
      so it cannot outlive the handle it came from.
    - `box` is that handle in a one-element `List`. `Lib` is movable, and a
      bare field would move out from under the pointer stored here; the
      `List`'s heap buffer does not move with it.

    Symbols are bound eagerly in `Lib.__init__` **except** the four
    `rd_kafka_mock_*` ones, which stay lazy. They are cold -- test setup, once
    per cluster -- and binding them eagerly would make every client fail to
    construct against a librdkafka built without the mock broker, rather than
    only `MockCluster`.
    """
    return _DLCallable[R, ImmUntrackedOrigin](
        box[0].get_function[R](name)._opaque,
        Pointer[OwnedDLHandle, ImmUntrackedOrigin](
            unsafe_from_address=Int(box.unsafe_ptr())
        ),
    )


struct Lib(Movable):
    """An open handle on librdkafka plus typed wrappers over its symbols.

    Every client owns one. `dlopen` refcounts, so the library is mapped
    once however many clients exist, and it stays mapped until the last
    one is dropped -- which is what keeps `rd_kafka_destroy` legal in a
    client's destructor.
    """

    # One handle, in a one-element `List` rather than a bare field: `Lib` is
    # movable, and a heap box keeps the handle's address stable so the
    # callables below can point at it across a move.
    var _box: List[OwnedDLHandle]

    var _version_str: _DLCallable[Int, ImmUntrackedOrigin]
    var _err2str: _DLCallable[Int, ImmUntrackedOrigin]
    var _last_error: _DLCallable[Int32, ImmUntrackedOrigin]
    var _conf_new: _DLCallable[Int, ImmUntrackedOrigin]
    var _conf_destroy: _DLCallable[NoneType, ImmUntrackedOrigin]
    var _conf_set: _DLCallable[Int32, ImmUntrackedOrigin]
    var _new: _DLCallable[Int, ImmUntrackedOrigin]
    var _destroy: _DLCallable[NoneType, ImmUntrackedOrigin]
    var _outq_len: _DLCallable[Int32, ImmUntrackedOrigin]
    var _topic_name: _DLCallable[Int, ImmUntrackedOrigin]
    var _produceva: _DLCallable[Int, ImmUntrackedOrigin]
    var _error_code: _DLCallable[Int32, ImmUntrackedOrigin]
    var _error_string: _DLCallable[Int, ImmUntrackedOrigin]
    var _error_destroy: _DLCallable[NoneType, ImmUntrackedOrigin]
    var _headers_new: _DLCallable[Int, ImmUntrackedOrigin]
    var _headers_destroy: _DLCallable[NoneType, ImmUntrackedOrigin]
    var _header_add: _DLCallable[Int32, ImmUntrackedOrigin]
    var _message_headers: _DLCallable[Int32, ImmUntrackedOrigin]
    var _header_get_all: _DLCallable[Int32, ImmUntrackedOrigin]
    var _poll_set_consumer: _DLCallable[Int32, ImmUntrackedOrigin]
    var _subscribe: _DLCallable[Int32, ImmUntrackedOrigin]
    var _consumer_poll: _DLCallable[Int, ImmUntrackedOrigin]
    var _message_destroy: _DLCallable[NoneType, ImmUntrackedOrigin]
    var _consumer_close: _DLCallable[Int32, ImmUntrackedOrigin]
    var _commit: _DLCallable[Int32, ImmUntrackedOrigin]
    var _assign: _DLCallable[Int32, ImmUntrackedOrigin]
    var _seek_partitions: _DLCallable[Int, ImmUntrackedOrigin]
    var _position: _DLCallable[Int32, ImmUntrackedOrigin]
    var _committed: _DLCallable[Int32, ImmUntrackedOrigin]
    var _pause_partitions: _DLCallable[Int32, ImmUntrackedOrigin]
    var _resume_partitions: _DLCallable[Int32, ImmUntrackedOrigin]
    var _query_watermark_offsets: _DLCallable[Int32, ImmUntrackedOrigin]
    var _get_watermark_offsets: _DLCallable[Int32, ImmUntrackedOrigin]
    var _offsets_for_times: _DLCallable[Int32, ImmUntrackedOrigin]
    var _message_timestamp: _DLCallable[Int64, ImmUntrackedOrigin]
    var _conf_set_rebalance_cb: _DLCallable[NoneType, ImmUntrackedOrigin]
    var _conf_set_dr_msg_cb: _DLCallable[NoneType, ImmUntrackedOrigin]
    var _poll: _DLCallable[Int32, ImmUntrackedOrigin]
    var _flush: _DLCallable[Int32, ImmUntrackedOrigin]
    var _conf_set_opaque: _DLCallable[NoneType, ImmUntrackedOrigin]
    var _incremental_assign: _DLCallable[Int, ImmUntrackedOrigin]
    var _incremental_unassign: _DLCallable[Int, ImmUntrackedOrigin]
    var _rebalance_protocol: _DLCallable[Int, ImmUntrackedOrigin]
    var _assignment_lost: _DLCallable[Int32, ImmUntrackedOrigin]
    var _topic_partition_list_new: _DLCallable[Int, ImmUntrackedOrigin]
    var _topic_partition_list_add: _DLCallable[Int, ImmUntrackedOrigin]
    var _topic_partition_list_destroy: _DLCallable[NoneType, ImmUntrackedOrigin]
    var _newtopic_new: _DLCallable[Int, ImmUntrackedOrigin]
    var _newtopic_destroy: _DLCallable[NoneType, ImmUntrackedOrigin]
    var _queue_new: _DLCallable[Int, ImmUntrackedOrigin]
    var _queue_destroy: _DLCallable[NoneType, ImmUntrackedOrigin]
    var _queue_poll: _DLCallable[Int, ImmUntrackedOrigin]
    var _event_destroy: _DLCallable[NoneType, ImmUntrackedOrigin]
    var _event_error: _DLCallable[Int32, ImmUntrackedOrigin]
    var _event_error_string: _DLCallable[Int, ImmUntrackedOrigin]
    var _createtopics: _DLCallable[NoneType, ImmUntrackedOrigin]
    var _event_createtopics_result: _DLCallable[Int, ImmUntrackedOrigin]
    var _createtopics_result_topics: _DLCallable[Int, ImmUntrackedOrigin]
    var _topic_result_error: _DLCallable[Int32, ImmUntrackedOrigin]
    var _topic_result_error_string: _DLCallable[Int, ImmUntrackedOrigin]
    var _topic_result_name: _DLCallable[Int, ImmUntrackedOrigin]
    var _metadata: _DLCallable[Int32, ImmUntrackedOrigin]
    var _metadata_destroy: _DLCallable[NoneType, ImmUntrackedOrigin]

    def __init__(out self) raises:
        var box = List[OwnedDLHandle](capacity=1)
        box.append(_open_librdkafka())
        self._box = box^

        self._version_str = _bind[Int](self._box, "rd_kafka_version_str")
        self._err2str = _bind[Int](self._box, "rd_kafka_err2str")
        self._last_error = _bind[Int32](self._box, "rd_kafka_last_error")
        self._conf_new = _bind[Int](self._box, "rd_kafka_conf_new")
        self._conf_destroy = _bind[NoneType](self._box, "rd_kafka_conf_destroy")
        self._conf_set = _bind[Int32](self._box, "rd_kafka_conf_set")
        self._new = _bind[Int](self._box, "rd_kafka_new")
        self._destroy = _bind[NoneType](self._box, "rd_kafka_destroy")
        self._outq_len = _bind[Int32](self._box, "rd_kafka_outq_len")
        self._topic_name = _bind[Int](self._box, "rd_kafka_topic_name")
        self._produceva = _bind[Int](self._box, "rd_kafka_produceva")
        self._error_code = _bind[Int32](self._box, "rd_kafka_error_code")
        self._error_string = _bind[Int](self._box, "rd_kafka_error_string")
        self._error_destroy = _bind[NoneType](
            self._box, "rd_kafka_error_destroy"
        )
        self._headers_new = _bind[Int](self._box, "rd_kafka_headers_new")
        self._headers_destroy = _bind[NoneType](
            self._box, "rd_kafka_headers_destroy"
        )
        self._header_add = _bind[Int32](self._box, "rd_kafka_header_add")
        self._message_headers = _bind[Int32](
            self._box, "rd_kafka_message_headers"
        )
        self._header_get_all = _bind[Int32](
            self._box, "rd_kafka_header_get_all"
        )
        self._poll_set_consumer = _bind[Int32](
            self._box, "rd_kafka_poll_set_consumer"
        )
        self._subscribe = _bind[Int32](self._box, "rd_kafka_subscribe")
        self._consumer_poll = _bind[Int](self._box, "rd_kafka_consumer_poll")
        self._message_destroy = _bind[NoneType](
            self._box, "rd_kafka_message_destroy"
        )
        self._consumer_close = _bind[Int32](
            self._box, "rd_kafka_consumer_close"
        )
        self._commit = _bind[Int32](self._box, "rd_kafka_commit")
        self._assign = _bind[Int32](self._box, "rd_kafka_assign")
        # `rd_kafka_seek_partitions`, not the per-topic `rd_kafka_seek`:
        # that one is deprecated, needs a topic handle this package no
        # longer keeps, and cannot report a per-partition verdict.
        self._seek_partitions = _bind[Int](
            self._box, "rd_kafka_seek_partitions"
        )
        self._position = _bind[Int32](self._box, "rd_kafka_position")
        self._committed = _bind[Int32](self._box, "rd_kafka_committed")
        # Note the `_partitions` suffix on both: `rd_kafka_pause` and
        # `rd_kafka_resume` do not exist.
        self._pause_partitions = _bind[Int32](
            self._box, "rd_kafka_pause_partitions"
        )
        self._resume_partitions = _bind[Int32](
            self._box, "rd_kafka_resume_partitions"
        )
        self._query_watermark_offsets = _bind[Int32](
            self._box, "rd_kafka_query_watermark_offsets"
        )
        self._get_watermark_offsets = _bind[Int32](
            self._box, "rd_kafka_get_watermark_offsets"
        )
        self._offsets_for_times = _bind[Int32](
            self._box, "rd_kafka_offsets_for_times"
        )
        self._message_timestamp = _bind[Int64](
            self._box, "rd_kafka_message_timestamp"
        )
        self._conf_set_rebalance_cb = _bind[NoneType](
            self._box, "rd_kafka_conf_set_rebalance_cb"
        )
        self._conf_set_dr_msg_cb = _bind[NoneType](
            self._box, "rd_kafka_conf_set_dr_msg_cb"
        )
        self._poll = _bind[Int32](self._box, "rd_kafka_poll")
        self._flush = _bind[Int32](self._box, "rd_kafka_flush")
        self._conf_set_opaque = _bind[NoneType](
            self._box, "rd_kafka_conf_set_opaque"
        )
        self._incremental_assign = _bind[Int](
            self._box, "rd_kafka_incremental_assign"
        )
        self._incremental_unassign = _bind[Int](
            self._box, "rd_kafka_incremental_unassign"
        )
        self._rebalance_protocol = _bind[Int](
            self._box, "rd_kafka_rebalance_protocol"
        )
        self._assignment_lost = _bind[Int32](
            self._box, "rd_kafka_assignment_lost"
        )
        self._topic_partition_list_new = _bind[Int](
            self._box, "rd_kafka_topic_partition_list_new"
        )
        self._topic_partition_list_add = _bind[Int](
            self._box, "rd_kafka_topic_partition_list_add"
        )
        self._topic_partition_list_destroy = _bind[NoneType](
            self._box, "rd_kafka_topic_partition_list_destroy"
        )
        self._newtopic_new = _bind[Int](self._box, "rd_kafka_NewTopic_new")
        self._newtopic_destroy = _bind[NoneType](
            self._box, "rd_kafka_NewTopic_destroy"
        )
        self._queue_new = _bind[Int](self._box, "rd_kafka_queue_new")
        self._queue_destroy = _bind[NoneType](
            self._box, "rd_kafka_queue_destroy"
        )
        self._queue_poll = _bind[Int](self._box, "rd_kafka_queue_poll")
        self._event_destroy = _bind[NoneType](
            self._box, "rd_kafka_event_destroy"
        )
        self._event_error = _bind[Int32](self._box, "rd_kafka_event_error")
        self._event_error_string = _bind[Int](
            self._box, "rd_kafka_event_error_string"
        )
        self._createtopics = _bind[NoneType](self._box, "rd_kafka_CreateTopics")
        self._event_createtopics_result = _bind[Int](
            self._box, "rd_kafka_event_CreateTopics_result"
        )
        self._createtopics_result_topics = _bind[Int](
            self._box, "rd_kafka_CreateTopics_result_topics"
        )
        self._topic_result_error = _bind[Int32](
            self._box, "rd_kafka_topic_result_error"
        )
        self._topic_result_error_string = _bind[Int](
            self._box, "rd_kafka_topic_result_error_string"
        )
        self._topic_result_name = _bind[Int](
            self._box, "rd_kafka_topic_result_name"
        )
        self._metadata = _bind[Int32](self._box, "rd_kafka_metadata")
        self._metadata_destroy = _bind[NoneType](
            self._box, "rd_kafka_metadata_destroy"
        )

    # -- version ------------------------------------------------------------

    def version(self) raises -> String:
        return cstr(self._version_str())

    def err2str(self, code: Int32) raises -> String:
        return cstr(self._err2str(code))

    def error(self, code: Int32) raises -> KafkaError:
        return KafkaError(code, self.err2str(code))

    def raise_if(self, code: Int32, context: String) raises:
        if code != RD_KAFKA_RESP_ERR_NO_ERROR:
            raise Error(context + ": " + String(self.error(code)))

    def last_error(self) raises -> Int32:
        return self._last_error()

    # -- rd_kafka_conf_t ----------------------------------------------------

    def conf_new(self) raises -> Int:
        return self._conf_new()

    def conf_destroy(self, conf: Int) raises:
        _ = self._conf_destroy(conf)

    def conf_set(self, conf: Int, name: String, value: String) raises:
        """Set one librdkafka property. Raises if the pair is rejected."""
        var errbuf = Array[UInt8, 512](fill=0)
        var name_c = _c_string(name)
        var value_c = _c_string(value)
        var rc = self._conf_set(
            conf,
            name_c.unsafe_ptr(),
            value_c.unsafe_ptr(),
            errbuf.unsafe_ptr(),
            512,
        )
        # Mojo frees a value after its last *use*; without these the buffers
        # could in principle be released before the call returns.
        _ = name_c^
        _ = value_c^
        if rc != 0:
            var msg = cstr(Int(errbuf.unsafe_ptr()))
            raise Error("rd_kafka_conf_set(" + name + "=" + value + "): " + msg)

    # -- rd_kafka_t ---------------------------------------------------------

    def new_client(self, kind: Int32, conf: Int) raises -> Int:
        """Create a client. Takes ownership of `conf` only on success."""
        var errbuf = Array[UInt8, 512](fill=0)
        var rk = self._new(kind, conf, errbuf.unsafe_ptr(), 512)
        if rk == 0:
            # librdkafka only adopts conf when it succeeds, so on failure
            # the conf is still ours to free.
            self.conf_destroy(conf)
            raise Error("rd_kafka_new: " + cstr(Int(errbuf.unsafe_ptr())))
        return rk

    def destroy(self, rk: Int) raises:
        _ = self._destroy(rk)

    def outq_len(self, rk: Int) raises -> Int32:
        return self._outq_len(rk)

    def poll(self, rk: Int, timeout_ms: Int32) raises -> Int32:
        """Serve the client's callbacks. Returns how many events it ran.

        Delivery reports arrive through this, on **this** thread -- librdkafka
        never invokes a callback from one of its background threads.
        """
        return self._poll(rk, timeout_ms)

    def flush(self, rk: Int, timeout_ms: Int32) raises -> Int32:
        """Block until every produced message has been acknowledged.

        Serves delivery-report callbacks while it waits, so it needs no
        second thread. This was unusable while delivery reports were routed
        to the main queue as events -- `rd_kafka_outq_len` counted undrained
        events as outstanding, so it sat until its timeout -- and became
        correct again when they moved to a `dr_msg_cb`.

        Returns `__TIMED_OUT` if anything is still queued at the deadline.
        """
        return self._flush(rk, timeout_ms)

    # -- topics -------------------------------------------------------------

    def topic_name(self, rkt: Int) raises -> String:
        if rkt == 0:
            return String("")
        return cstr(self._topic_name(rkt))

    # -- producing ----------------------------------------------------------

    def produceva(self, rk: Int, vus: Int, cnt: Int) raises -> Int:
        """`rd_kafka_produceva` -- the non-variadic form of `producev`.

        Returns a `rd_kafka_error_t*`, which is **NULL on success**; that is
        the opposite polarity to most of this API, where 0 means failure.
        Pass a non-NULL result to `take_error`, which reads it and destroys
        it. The whole array is described by `_VuArray`.
        """
        return self._produceva(rk, vus, cnt)

    # -- rd_kafka_error_t ---------------------------------------------------

    def take_error(self, err: Int) raises -> KafkaError:
        """Read a `rd_kafka_error_t*` and destroy it.

        The object is heap-allocated and the caller owns it, so reading it
        without destroying it leaks. Both reads are guarded so the destroy
        still happens if decoding the string raises.

        `rd_kafka_error_string` is deliberately preferred over `err2str` on
        the code: it carries what actually went wrong ("Unknown topic ...")
        where `err2str` only names the code's category.
        """
        var code: Int32
        var message: String
        try:
            code = self._error_code(err)
            message = cstr(self._error_string(err))
        except e:
            _ = self._error_destroy(err)
            raise e
        _ = self._error_destroy(err)
        return KafkaError(code, message)

    # -- record headers -----------------------------------------------------

    def headers_new(self, count: Int) raises -> Int:
        """An empty header list sized for `count` entries; NULL on failure."""
        return self._headers_new(count)

    def headers_destroy(self, hdrs: Int) raises:
        _ = self._headers_destroy(hdrs)

    def header_add(
        self,
        hdrs: Int,
        name: Int,
        name_size: Int,
        value: Int,
        value_size: Int,
    ) raises -> Int32:
        """Append one header. Both name and value are copied.

        `name_size` is passed explicitly rather than as -1: librdkafka would
        otherwise `strlen` the pointer, and a Mojo `String`'s buffer is not
        NUL-terminated. A length keeps the name off the `_c_string` path
        entirely.

        `value` follows the same presence rule as a message key or payload --
        NULL is a null header value, and a non-NULL pointer with `value_size`
        0 is one that is present and empty.
        """
        return self._header_add(hdrs, name, name_size, value, value_size)

    def message_headers(self, msg: Int, out_ptr: Int) raises -> Int32:
        """Borrow a message's headers. `__NOENT` when it has none.

        The list belongs to the message and is freed with it -- it must not
        be destroyed here, and it must not be read after the message is.
        """
        return self._message_headers(msg, out_ptr)

    def header_get_all(
        self, hdrs: Int, idx: Int, namep: Int, valuep: Int, sizep: Int
    ) raises -> Int32:
        """Read header `idx`. `__NOENT` once the index runs past the end.

        The name and value point into the header list and are not copies.
        `valuep` is left NULL for a header whose value is null, which is why
        it goes through `copy_bytes` rather than being read as bytes.
        """
        return self._header_get_all(hdrs, idx, namep, valuep, sizep)

    # -- consuming ----------------------------------------------------------

    def poll_set_consumer(self, rk: Int) raises -> Int32:
        return self._poll_set_consumer(rk)

    def subscribe(self, rk: Int, list: Int) raises -> Int32:
        return self._subscribe(rk, list)

    def consumer_poll(self, rk: Int, timeout_ms: Int32) raises -> Int:
        return self._consumer_poll(rk, timeout_ms)

    def message_destroy(self, msg: Int) raises:
        _ = self._message_destroy(msg)

    def consumer_close(self, rk: Int) raises -> Int32:
        return self._consumer_close(rk)

    def commit(self, rk: Int, offsets: Int, async_: Int32) raises -> Int32:
        """Commit `offsets`, or the current assignment's when it is 0."""
        return self._commit(rk, offsets, async_)

    # -- topic partition lists ----------------------------------------------

    def topic_partition_list_new(self, size: Int32) raises -> Int:
        return self._topic_partition_list_new(size)

    def topic_partition_list_add(
        self,
        list: Int,
        topic: String,
        partition: Int32,
        offset: Int64 = RD_KAFKA_OFFSET_INVALID,
    ) raises -> Int:
        """Append one topic+partition and return its element address.

        `offset` is written straight into the new element rather than
        through `rd_kafka_topic_partition_list_set_offset`, which would only
        look the element back up by name. `RD_KAFKA_OFFSET_INVALID` is what
        librdkafka initialises the field to, so the default is a no-op.

        **The returned address is only valid until the next add.** The list
        grows by reallocating its `elems` array, so an address kept across a
        second add dangles -- the `_VuArray` trap one library down. Write to
        the element now, or size the list with `topic_partition_list_new`
        and re-derive the address with `topic_partition_list_elem`.
        """
        var topic_c = _c_string(topic)
        var tp = self._topic_partition_list_add(
            list, topic_c.unsafe_ptr(), partition
        )
        _ = topic_c^
        if tp != 0 and offset != RD_KAFKA_OFFSET_INVALID:
            _store_i64(tp + TP_OFFSET, offset)
        return tp

    def topic_partition_list_destroy(self, list: Int) raises:
        _ = self._topic_partition_list_destroy(list)

    def topic_partition_list_count(self, list: Int) raises -> Int:
        """How many elements the list holds. 0 for a NULL list."""
        if list == 0:
            return 0
        return Int(_load_i32(list + TPL_CNT))

    def topic_partition_list_elem(self, list: Int, i: Int) raises -> Int:
        """Address of element `i`, or 0 if the list is NULL.

        The stride is 64 and not the 56 the members add up to -- see
        `TP_STRIDE`. Nothing bounds-checks `i`; callers walk to
        `topic_partition_list_count`.
        """
        if list == 0:
            return 0
        return _load_word(list + TPL_ELEMS) + i * TP_STRIDE

    # -- consumer control plane ---------------------------------------------
    #
    # Every call here takes a `rd_kafka_topic_partition_list_t*`, and most of
    # them answer *into it* rather than through their return value: the
    # return code says whether the request as a whole worked, while the
    # per-partition verdict lands in each element's `err`. Reading only the
    # return code is the same mistake as reading only `rd_kafka_event_error`
    # on a CreateTopics reply.

    def assign(self, rk: Int, list: Int) raises -> Int32:
        """Set the whole assignment. `list` of 0 clears it."""
        return self._assign(rk, list)

    def seek_partitions(
        self, rk: Int, list: Int, timeout_ms: Int32
    ) raises -> Int:
        """Returns a `rd_kafka_error_t*` -- **NULL on success**.

        Same reversed polarity as `produceva`, and the same disposal: hand a
        non-NULL result to `take_error`. Per-partition results are written
        back into `list`.
        """
        return self._seek_partitions(rk, list, timeout_ms)

    def position(self, rk: Int, list: Int) raises -> Int32:
        return self._position(rk, list)

    def committed(self, rk: Int, list: Int, timeout_ms: Int32) raises -> Int32:
        return self._committed(rk, list, timeout_ms)

    def pause_partitions(self, rk: Int, list: Int) raises -> Int32:
        return self._pause_partitions(rk, list)

    def resume_partitions(self, rk: Int, list: Int) raises -> Int32:
        return self._resume_partitions(rk, list)

    def query_watermark_offsets(
        self,
        rk: Int,
        topic: String,
        partition: Int32,
        low_out: Int,
        high_out: Int,
        timeout_ms: Int32,
    ) raises -> Int32:
        """Ask the broker for the partition's low and high offsets."""
        var topic_c = _c_string(topic)
        var rc = self._query_watermark_offsets(
            rk, topic_c.unsafe_ptr(), partition, low_out, high_out, timeout_ms
        )
        _ = topic_c^
        return rc

    def get_watermark_offsets(
        self,
        rk: Int,
        topic: String,
        partition: Int32,
        low_out: Int,
        high_out: Int,
    ) raises -> Int32:
        """The watermarks this client already cached -- no broker round trip."""
        var topic_c = _c_string(topic)
        var rc = self._get_watermark_offsets(
            rk, topic_c.unsafe_ptr(), partition, low_out, high_out
        )
        _ = topic_c^
        return rc

    def offsets_for_times(
        self, rk: Int, list: Int, timeout_ms: Int32
    ) raises -> Int32:
        """Each element's `offset` is a millisecond timestamp on the way in
        and the first offset at or after it on the way out."""
        return self._offsets_for_times(rk, list, timeout_ms)

    def message_timestamp(self, msg: Int, tstype_out: Int) raises -> Int64:
        """Milliseconds since the epoch, or -1 when the broker sent none."""
        return self._message_timestamp(msg, tstype_out)

    # -- rebalance ----------------------------------------------------------
    #
    # These are set on the *conf*, before `rd_kafka_new`, and only ever fire
    # for a consumer that `subscribe`d -- a manual `assign` never rebalances.
    #
    # Registering a callback **takes the assignment over**: librdkafka stops
    # assigning by itself the moment one is set, so the callback must settle
    # every rebalance or the consumer silently stops consuming. That failure
    # is a stall, not an exception. See `_rebalance_trampoline`.

    def conf_set_dr_msg_cb(self, conf: Int, callback: DeliveryCallback) raises:
        """Install the C delivery-report callback.

        One call per produced message, delivered from inside `poll` or
        `flush`. See `_delivery_trampoline` in `producer.mojo`.
        """
        _ = self._conf_set_dr_msg_cb(conf, callback)

    def conf_set_rebalance_cb(
        self, conf: Int, callback: RebalanceCallback
    ) raises:
        """Install the C rebalance callback.

        `callback` is an `abi("C")` Mojo function, passed as a value rather
        than an address -- Mojo 1.0 can supply one, so this needs no
        event-queue workaround. See "C callbacks are possible in 1.0" in
        CLAUDE.md.
        """
        _ = self._conf_set_rebalance_cb(conf, callback)

    def conf_set_opaque(self, conf: Int, opaque: Int) raises:
        """The `void *` handed back to the callback.

        A thin C callback captures nothing, so this pointer is the only
        route from inside one back to Mojo state.
        """
        _ = self._conf_set_opaque(conf, opaque)

    def incremental_assign(self, rk: Int, list: Int) raises -> Int:
        """Add partitions to the assignment -- COOPERATIVE protocol only.

        Returns a `rd_kafka_error_t*`, NULL on success.
        """
        return self._incremental_assign(rk, list)

    def incremental_unassign(self, rk: Int, list: Int) raises -> Int:
        """Remove partitions from the assignment -- COOPERATIVE only."""
        return self._incremental_unassign(rk, list)

    def rebalance_protocol(self, rk: Int) raises -> String:
        """ "EAGER", "COOPERATIVE", or "NONE" when there is no group."""
        return cstr(self._rebalance_protocol(rk))

    def assignment_lost(self, rk: Int) raises -> Bool:
        """True when partitions were lost involuntarily.

        Committing offsets for a lost partition may fail -- another member
        may already own it -- which is why it routes to `on_lost` rather
        than `on_revoke`.
        """
        return self._assignment_lost(rk) != 0

    # -- admin --------------------------------------------------------------

    def new_topic_new(
        self, name: String, num_partitions: Int32, replication_factor: Int32
    ) raises -> Int:
        var errbuf = Array[UInt8, 512](fill=0)
        var name_c = _c_string(name)
        var nt = self._newtopic_new(
            name_c.unsafe_ptr(),
            num_partitions,
            replication_factor,
            errbuf.unsafe_ptr(),
            512,
        )
        _ = name_c^
        if nt == 0:
            raise Error(
                "rd_kafka_NewTopic_new: " + cstr(Int(errbuf.unsafe_ptr()))
            )
        return nt

    def new_topic_destroy(self, nt: Int) raises:
        _ = self._newtopic_destroy(nt)

    def queue_new(self, rk: Int) raises -> Int:
        return self._queue_new(rk)

    def queue_destroy(self, q: Int) raises:
        _ = self._queue_destroy(q)

    def queue_poll(self, q: Int, timeout_ms: Int32) raises -> Int:
        return self._queue_poll(q, timeout_ms)

    def event_destroy(self, ev: Int) raises:
        _ = self._event_destroy(ev)

    def event_error(self, ev: Int) raises -> Int32:
        return self._event_error(ev)

    def event_error_string(self, ev: Int) raises -> String:
        return cstr(self._event_error_string(ev))

    def create_topics(self, rk: Int, topics: Int, cnt: Int, queue: Int) raises:
        _ = self._createtopics(rk, topics, cnt, 0, queue)

    def event_create_topics_result(self, ev: Int) raises -> Int:
        """Cast a DR-style event to a CreateTopics result; 0 if it is not one.
        """
        return self._event_createtopics_result(ev)

    def create_topics_result_topics(
        self, result: Int, cnt_out: Int
    ) raises -> Int:
        """Array of `rd_kafka_topic_result_t*`; `cnt_out` receives the count.

        The per-topic verdict lives here, not in `rd_kafka_event_error` --
        that one only reports whether the *request* itself failed.
        """
        return self._createtopics_result_topics(result, cnt_out)

    def topic_result_error(self, tr: Int) raises -> Int32:
        return self._topic_result_error(tr)

    def topic_result_error_string(self, tr: Int) raises -> String:
        return cstr(self._topic_result_error_string(tr))

    def topic_result_name(self, tr: Int) raises -> String:
        return cstr(self._topic_result_name(tr))

    def metadata(
        self, rk: Int, all_topics: Int32, out_ptr: Int, timeout_ms: Int32
    ) raises -> Int32:
        return self._metadata(rk, all_topics, 0, out_ptr, timeout_ms)

    def metadata_destroy(self, meta: Int) raises:
        _ = self._metadata_destroy(meta)

    # -- mock cluster -------------------------------------------------------
    #
    # librdkafka ships an in-process broker (rdkafka_mock.h). It speaks the
    # real wire protocol over a real socket, so clients are ordinary clients
    # and no Docker is involved. Note that CreateTopics is NOT implemented by
    # the mock -- topics are made with `mock_topic_create` instead.

    def mock_cluster_new(self, rk: Int, broker_count: Int32) raises -> Int:
        return self._box[0].get_function[Int]("rd_kafka_mock_cluster_new")(
            rk, broker_count
        )

    def mock_cluster_destroy(self, mcluster: Int) raises:
        _ = self._box[0].get_function[NoneType](
            "rd_kafka_mock_cluster_destroy"
        )(mcluster)

    def mock_cluster_bootstraps(self, mcluster: Int) raises -> String:
        return cstr(
            self._box[0].get_function[Int]("rd_kafka_mock_cluster_bootstraps")(
                mcluster
            )
        )

    def mock_topic_create(
        self,
        mcluster: Int,
        topic: String,
        partition_count: Int32,
        replication_factor: Int32,
    ) raises -> Int32:
        var topic_c = _c_string(topic)
        var rc = self._box[0].get_function[Int32]("rd_kafka_mock_topic_create")(
            mcluster,
            topic_c.unsafe_ptr(),
            partition_count,
            replication_factor,
        )
        _ = topic_c^
        return rc


def librdkafka_version() raises -> String:
    """Version string of the librdkafka this process loaded."""
    return Lib().version()

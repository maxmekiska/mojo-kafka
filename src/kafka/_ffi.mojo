"""Raw librdkafka FFI declarations.

This is the only file that talks to C directly. Everything else in the
package goes through it, so bumping librdkafka or changing loader
strategy stays local.

Three conventions keep the layers above this file safe:

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
"""

from std.ffi import OwnedDLHandle
from std.os import getenv


comptime RD_KAFKA_PRODUCER: Int32 = 0
comptime RD_KAFKA_CONSUMER: Int32 = 1

comptime RD_KAFKA_RESP_ERR_NO_ERROR: Int32 = 0
comptime RD_KAFKA_RESP_ERR__TIMED_OUT: Int32 = -185
comptime RD_KAFKA_RESP_ERR__PARTITION_EOF: Int32 = -191

comptime RD_KAFKA_PARTITION_UA: Int32 = -1

# Tell librdkafka to copy the payload out of our buffer, so Mojo-side
# strings can be freed as soon as `produce()` returns.
comptime RD_KAFKA_MSG_F_COPY: Int32 = 0x2

# Delivery reports as *events* on the main queue rather than through a C
# callback. Mojo cannot hand librdkafka a C function pointer, but event
# sourcing needs none -- `queue_poll` returns the batch and we walk it.
comptime RD_KAFKA_EVENT_DR: Int32 = 0x1

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


def bytes_to_string(addr: Int, length: Int) raises -> String:
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


def cstr(addr: Int) raises -> String:
    """Copy a NUL-terminated C string at `addr` into an owned `String`."""
    if addr == 0:
        return String("")
    var p = Pointer[UInt8, ImmutAnyOrigin](unsafe_from_address=addr)
    var n = 0
    while p[unsafe_offset=n] != 0:
        n += 1
    return bytes_to_string(addr, n)


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


# --- errors -----------------------------------------------------------------


@fieldwise_init
struct KafkaError(Copyable, Movable):
    """A `rd_kafka_resp_err_t` with the description librdkafka gives it."""

    var code: Int32
    var message: String

    def describe(self) raises -> String:
        return "KafkaError(" + String(Int(self.code)) + "): " + self.message


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


struct Lib(Movable):
    """An open handle on librdkafka plus typed wrappers over its symbols.

    Every client owns one. `dlopen` refcounts, so the library is mapped
    once however many clients exist, and it stays mapped until the last
    one is dropped -- which is what keeps `rd_kafka_destroy` legal in a
    client's destructor.
    """

    var _h: OwnedDLHandle

    def __init__(out self) raises:
        self._h = _open_librdkafka()

    # -- version ------------------------------------------------------------

    def version(self) raises -> String:
        return cstr(self._h.get_function[Int]("rd_kafka_version_str")())

    def err2str(self, code: Int32) raises -> String:
        return cstr(self._h.get_function[Int]("rd_kafka_err2str")(code))

    def error(self, code: Int32) raises -> KafkaError:
        return KafkaError(code, self.err2str(code))

    def raise_if(self, code: Int32, context: String) raises:
        if code != RD_KAFKA_RESP_ERR_NO_ERROR:
            raise Error(context + ": " + self.error(code).describe())

    def last_error(self) raises -> Int32:
        return self._h.get_function[Int32]("rd_kafka_last_error")()

    # -- rd_kafka_conf_t ----------------------------------------------------

    def conf_new(self) raises -> Int:
        return self._h.get_function[Int]("rd_kafka_conf_new")()

    def conf_destroy(self, conf: Int) raises:
        _ = self._h.get_function[NoneType]("rd_kafka_conf_destroy")(conf)

    def conf_set(self, conf: Int, name: String, value: String) raises:
        """Set one librdkafka property. Raises if the pair is rejected."""
        var errbuf = Array[UInt8, 512](fill=0)
        var name_c = _c_string(name)
        var value_c = _c_string(value)
        var rc = self._h.get_function[Int32]("rd_kafka_conf_set")(
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

    def conf_set_events(self, conf: Int, events: Int32) raises:
        """Route the given event types to the main queue instead of callbacks.
        """
        _ = self._h.get_function[NoneType]("rd_kafka_conf_set_events")(
            conf, events
        )

    # -- rd_kafka_t ---------------------------------------------------------

    def new_client(self, kind: Int32, conf: Int) raises -> Int:
        """Create a client. Takes ownership of `conf` only on success."""
        var errbuf = Array[UInt8, 512](fill=0)
        var rk = self._h.get_function[Int]("rd_kafka_new")(
            kind, conf, errbuf.unsafe_ptr(), 512
        )
        if rk == 0:
            # librdkafka only adopts conf when it succeeds, so on failure
            # the conf is still ours to free.
            self.conf_destroy(conf)
            raise Error("rd_kafka_new: " + cstr(Int(errbuf.unsafe_ptr())))
        return rk

    def destroy(self, rk: Int) raises:
        _ = self._h.get_function[NoneType]("rd_kafka_destroy")(rk)

    def poll(self, rk: Int, timeout_ms: Int32) raises -> Int32:
        return self._h.get_function[Int32]("rd_kafka_poll")(rk, timeout_ms)

    def flush(self, rk: Int, timeout_ms: Int32) raises -> Int32:
        return self._h.get_function[Int32]("rd_kafka_flush")(rk, timeout_ms)

    def outq_len(self, rk: Int) raises -> Int32:
        return self._h.get_function[Int32]("rd_kafka_outq_len")(rk)

    # -- topics -------------------------------------------------------------

    def topic_new(self, rk: Int, name: String) raises -> Int:
        var name_c = _c_string(name)
        var rkt = self._h.get_function[Int]("rd_kafka_topic_new")(
            rk, name_c.unsafe_ptr(), 0
        )
        _ = name_c^
        if rkt == 0:
            raise Error(
                "rd_kafka_topic_new("
                + name
                + "): "
                + self.error(self.last_error()).describe()
            )
        return rkt

    def topic_destroy(self, rkt: Int) raises:
        _ = self._h.get_function[NoneType]("rd_kafka_topic_destroy")(rkt)

    def topic_name(self, rkt: Int) raises -> String:
        if rkt == 0:
            return String("")
        return cstr(self._h.get_function[Int]("rd_kafka_topic_name")(rkt))

    # -- producing ----------------------------------------------------------

    def produce(
        self,
        rkt: Int,
        partition: Int32,
        payload: Int,
        payload_len: Int,
        key: Int,
        key_len: Int,
    ) raises -> Int32:
        """`rd_kafka_produce`, which unlike `producev` is not variadic.

        Calling a C variadic through a fixed prototype is undefined on the
        SysV and AAPCS ABIs, so the older non-variadic entry point is the
        one worth binding. Returns -1 on failure; the code is then in
        `last_error()`.
        """
        return self._h.get_function[Int32]("rd_kafka_produce")(
            rkt,
            partition,
            RD_KAFKA_MSG_F_COPY,
            payload,
            payload_len,
            key,
            key_len,
            0,
        )

    # -- consuming ----------------------------------------------------------

    def poll_set_consumer(self, rk: Int) raises -> Int32:
        return self._h.get_function[Int32]("rd_kafka_poll_set_consumer")(rk)

    def subscribe(self, rk: Int, list: Int) raises -> Int32:
        return self._h.get_function[Int32]("rd_kafka_subscribe")(rk, list)

    def consumer_poll(self, rk: Int, timeout_ms: Int32) raises -> Int:
        return self._h.get_function[Int]("rd_kafka_consumer_poll")(
            rk, timeout_ms
        )

    def message_destroy(self, msg: Int) raises:
        _ = self._h.get_function[NoneType]("rd_kafka_message_destroy")(msg)

    def consumer_close(self, rk: Int) raises -> Int32:
        return self._h.get_function[Int32]("rd_kafka_consumer_close")(rk)

    def commit(self, rk: Int, async_: Int32) raises -> Int32:
        return self._h.get_function[Int32]("rd_kafka_commit")(rk, 0, async_)

    # -- topic partition lists ----------------------------------------------

    def topic_partition_list_new(self, size: Int32) raises -> Int:
        return self._h.get_function[Int]("rd_kafka_topic_partition_list_new")(
            size
        )

    def topic_partition_list_add(
        self, list: Int, topic: String, partition: Int32
    ) raises -> Int:
        var topic_c = _c_string(topic)
        var tp = self._h.get_function[Int]("rd_kafka_topic_partition_list_add")(
            list, topic_c.unsafe_ptr(), partition
        )
        _ = topic_c^
        return tp

    def topic_partition_list_destroy(self, list: Int) raises:
        _ = self._h.get_function[NoneType](
            "rd_kafka_topic_partition_list_destroy"
        )(list)

    # -- admin --------------------------------------------------------------

    def new_topic_new(
        self, name: String, num_partitions: Int32, replication_factor: Int32
    ) raises -> Int:
        var errbuf = Array[UInt8, 512](fill=0)
        var name_c = _c_string(name)
        var nt = self._h.get_function[Int]("rd_kafka_NewTopic_new")(
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
        _ = self._h.get_function[NoneType]("rd_kafka_NewTopic_destroy")(nt)

    def queue_new(self, rk: Int) raises -> Int:
        return self._h.get_function[Int]("rd_kafka_queue_new")(rk)

    def queue_destroy(self, q: Int) raises:
        _ = self._h.get_function[NoneType]("rd_kafka_queue_destroy")(q)

    def queue_poll(self, q: Int, timeout_ms: Int32) raises -> Int:
        return self._h.get_function[Int]("rd_kafka_queue_poll")(q, timeout_ms)

    def event_destroy(self, ev: Int) raises:
        _ = self._h.get_function[NoneType]("rd_kafka_event_destroy")(ev)

    def queue_get_main(self, rk: Int) raises -> Int:
        """A new reference to the main queue -- destroy it before the client."""
        return self._h.get_function[Int]("rd_kafka_queue_get_main")(rk)

    def event_type(self, ev: Int) raises -> Int32:
        return self._h.get_function[Int32]("rd_kafka_event_type")(ev)

    def event_message_count(self, ev: Int) raises -> Int:
        return self._h.get_function[Int]("rd_kafka_event_message_count")(ev)

    def event_message_next(self, ev: Int) raises -> Int:
        """Next `rd_kafka_message_t*` in a DR batch; owned by the event."""
        return self._h.get_function[Int]("rd_kafka_event_message_next")(ev)

    def event_error(self, ev: Int) raises -> Int32:
        return self._h.get_function[Int32]("rd_kafka_event_error")(ev)

    def event_error_string(self, ev: Int) raises -> String:
        return cstr(
            self._h.get_function[Int]("rd_kafka_event_error_string")(ev)
        )

    def create_topics(self, rk: Int, topics: Int, cnt: Int, queue: Int) raises:
        _ = self._h.get_function[NoneType]("rd_kafka_CreateTopics")(
            rk, topics, cnt, 0, queue
        )

    def event_create_topics_result(self, ev: Int) raises -> Int:
        """Cast a DR-style event to a CreateTopics result; 0 if it is not one.
        """
        return self._h.get_function[Int]("rd_kafka_event_CreateTopics_result")(
            ev
        )

    def create_topics_result_topics(
        self, result: Int, cnt_out: Int
    ) raises -> Int:
        """Array of `rd_kafka_topic_result_t*`; `cnt_out` receives the count.

        The per-topic verdict lives here, not in `rd_kafka_event_error` --
        that one only reports whether the *request* itself failed.
        """
        return self._h.get_function[Int]("rd_kafka_CreateTopics_result_topics")(
            result, cnt_out
        )

    def topic_result_error(self, tr: Int) raises -> Int32:
        return self._h.get_function[Int32]("rd_kafka_topic_result_error")(tr)

    def topic_result_error_string(self, tr: Int) raises -> String:
        return cstr(
            self._h.get_function[Int]("rd_kafka_topic_result_error_string")(tr)
        )

    def topic_result_name(self, tr: Int) raises -> String:
        return cstr(self._h.get_function[Int]("rd_kafka_topic_result_name")(tr))

    def metadata(
        self, rk: Int, all_topics: Int32, out_ptr: Int, timeout_ms: Int32
    ) raises -> Int32:
        return self._h.get_function[Int32]("rd_kafka_metadata")(
            rk, all_topics, 0, out_ptr, timeout_ms
        )

    def metadata_destroy(self, meta: Int) raises:
        _ = self._h.get_function[NoneType]("rd_kafka_metadata_destroy")(meta)

    # -- mock cluster -------------------------------------------------------
    #
    # librdkafka ships an in-process broker (rdkafka_mock.h). It speaks the
    # real wire protocol over a real socket, so clients are ordinary clients
    # and no Docker is involved. Note that CreateTopics is NOT implemented by
    # the mock -- topics are made with `mock_topic_create` instead.

    def mock_cluster_new(self, rk: Int, broker_count: Int32) raises -> Int:
        return self._h.get_function[Int]("rd_kafka_mock_cluster_new")(
            rk, broker_count
        )

    def mock_cluster_destroy(self, mcluster: Int) raises:
        _ = self._h.get_function[NoneType]("rd_kafka_mock_cluster_destroy")(
            mcluster
        )

    def mock_cluster_bootstraps(self, mcluster: Int) raises -> String:
        return cstr(
            self._h.get_function[Int]("rd_kafka_mock_cluster_bootstraps")(
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
        var rc = self._h.get_function[Int32]("rd_kafka_mock_topic_create")(
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

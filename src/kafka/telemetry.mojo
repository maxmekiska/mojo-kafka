"""Errors, statistics and log lines from librdkafka's background threads.

A failure on a librdkafka background thread -- all brokers down, an
authentication rejection, a fenced producer -- used to reach the
application only if a `poll()` or `flush()` happened to report it, and
there was no metrics hook at all. This module is the missing half: three
C callbacks, installed on every client at construction, that **retain**
what librdkafka reports so the application can read it when it likes.

    var down = producer.errors()          # most recent 256, oldest first
    var fatal = producer.fatal_error()    # the one that ended the client
    var stats = consumer.latest_stats()   # the last statistics document
    var lines = consumer.take_logs()      # only with capture_logs=True

**Retain, do not call back.** `confluent-kafka` hands these to Python
callables; a Mojo handler reached from C is *thin* -- it captures nothing
-- so a callback API here would force every user into the `setenv` tricks
the lost-rebalance tests use to smuggle state out. Retaining matches
`failures()` / `take_failures()`, which the delivery path already does for
the same reason.

**Served on the polling thread, like every callback in this package.** The
error and statistics callbacks are always; the log callback only with
`log.queue=true`, which is why `capture_logs` on a config sets both
together. That is what makes touching Mojo state from them safe, and it is
also why a client that is never polled reports nothing -- the reports are
queued until something serves them.

**Bounded.** `errors()` and `logs()` keep the most recent `RETAINED`
entries and count what they dropped, so a client whose broker flaps for a
week does not grow without bound in a job that never reads them. The
statistics document is one slot, overwritten every interval.
"""

from ._ffi import KafkaError, _OpaqueReader, _bytes_to_string, cstr
from ._sync import _Latch

# Most recent entries kept per client by `errors()` and `logs()`. Older ones
# are dropped and counted -- see `_Telemetry.record_error`.
comptime RETAINED: Int = 256


@fieldwise_init
struct LogLine(Copyable, Movable, Writable):
    """One line librdkafka logged.

    `level` is the syslog level librdkafka uses -- 3 is an error, 4 a
    warning, 7 debug -- and `facility` is its short tag for the subsystem
    (`FAIL`, `CONNECT`, `BROKERFAIL`). What gets captured is bounded by the
    client's `log_level` property, set through `ConsumerConfig.set(
    "log_level", "7")` and friends; the default of 6 keeps debug lines out.
    """

    var level: Int32
    var facility: String
    var message: String

    def write_to(self, mut writer: Some[Writer]):
        writer.write("[", self.level, "] ", self.facility, ": ", self.message)


struct _Telemetry(Movable):
    """What the three observability callbacks write and the accessors read.

    Lives inside the one heap box each client already hands to
    `rd_kafka_conf_set_opaque` -- `_DrState` on the producer,
    `_RebalanceState` on the consumer -- so no second opaque is needed and
    the box's lifetime rules cover it. A `_Latch` guards all of it: the
    callbacks run on whichever thread polls, and two threads polling are
    two writers.

    The two rules `_sync` sets are kept here by construction. Nothing under
    the lock raises (`List.append` and `pop` do not), and nothing under the
    lock crosses into C -- the trampolines decode every string *before*
    they call in. Not `Copyable`: an `Atomic` cannot be copied.
    """

    var lock: _Latch
    var errors: List[KafkaError]
    var dropped_errors: Int
    var stats: Optional[String]
    var logs: List[LogLine]
    var dropped_logs: Int

    def __init__(out self):
        self.lock = _Latch()
        self.errors = List[KafkaError]()
        self.dropped_errors = 0
        self.stats = None
        self.logs = List[LogLine]()
        self.dropped_logs = 0

    # -- writers, from the trampolines ---------------------------------------

    def record_error(mut self, var error: KafkaError):
        """Append, dropping the oldest once `RETAINED` are held."""
        self.lock.acquire()
        if len(self.errors) >= RETAINED:
            _ = self.errors.pop(0)
            self.dropped_errors += 1
        self.errors.append(error^)
        self.lock.release()

    def record_stats(mut self, var document: String):
        """Replace the last statistics document."""
        self.lock.acquire()
        self.stats = Optional[String](document^)
        self.lock.release()

    def record_log(mut self, var line: LogLine):
        """Append, dropping the oldest once `RETAINED` are held."""
        self.lock.acquire()
        if len(self.logs) >= RETAINED:
            _ = self.logs.pop(0)
            self.dropped_logs += 1
        self.logs.append(line^)
        self.lock.release()

    # -- readers, from the clients -------------------------------------------

    def snapshot_errors(mut self) -> List[KafkaError]:
        self.lock.acquire()
        var out = self.errors.copy()
        self.lock.release()
        return out^

    def take_errors(mut self) -> List[KafkaError]:
        # Copy and clear under one acquisition, or an error landing between
        # the two is acknowledged without ever being returned.
        self.lock.acquire()
        var out = self.errors.copy()
        self.errors.clear()
        self.lock.release()
        return out^

    def errors_dropped(mut self) -> Int:
        self.lock.acquire()
        var n = self.dropped_errors
        self.lock.release()
        return n

    def latest_stats(mut self) -> Optional[String]:
        self.lock.acquire()
        var out = self.stats.copy()
        self.lock.release()
        return out^

    def snapshot_logs(mut self) -> List[LogLine]:
        self.lock.acquire()
        var out = self.logs.copy()
        self.lock.release()
        return out^

    def take_logs(mut self) -> List[LogLine]:
        self.lock.acquire()
        var out = self.logs.copy()
        self.logs.clear()
        self.lock.release()
        return out^

    def logs_dropped(mut self) -> Int:
        self.lock.acquire()
        var n = self.dropped_logs
        self.lock.release()
        return n


trait _Observed:
    """A callback state box that carries a `_Telemetry`.

    Both clients' boxes conform, which is what lets one trampoline serve
    both: the trampolines below are parameterised on the box type, and
    `_error_trampoline[_DrState]` is a plain C function pointer exactly as
    `_delivery_trampoline` is -- probed, not assumed. The alternative, a
    `_Telemetry` at a known offset inside each box, would rest on a Mojo
    struct layout this package deliberately never reasons about.
    """

    def telemetry_ptr(self) -> Pointer[_Telemetry, MutAnyOrigin]:
        """The box's `_Telemetry`, by address.

        A pointer rather than a `ref` because the accessors need mutable
        access to the latch through a box the callers hold immutably -- the
        same aliasing `Producer._state()` does for the failure list.
        """
        ...


def _error_trampoline[
    S: _Observed
](rk: Int, err: Int32, reason: Int, opaque: Int) abi("C"):
    """librdkafka's error callback, for a client whose opaque is an `S`.

    `abi("C")` and therefore **thin**, and may not `raises` -- the same
    discipline `_delivery_trampoline` follows. `reason` is the description
    librdkafka wrote, which says more than `err2str` would ("Connect to
    ipv4#127.0.0.1:9 failed: Connection refused" against "Local:
    Broker transport failure"), so the error is built from it and no `Lib`
    is needed here at all.

    An `err` of `RD_KAFKA_RESP_ERR__FATAL` is a notification that a fatal
    error was raised; `fatal_error()` on the client has the underlying one.
    It is retained like any other, so `errors()` shows *that* the client
    died even after `fatal_error()` has been read.
    """
    try:
        # Decoded before the lock: `cstr` reads foreign memory, and the
        # `_Latch` rule is that nothing under it touches C.
        var message = cstr(reason)
        ref state = Pointer[S, ImmutAnyOrigin](unsafe_from_address=opaque)[
            unsafe_offset=0
        ]
        state.telemetry_ptr()[unsafe_offset=0].record_error(
            KafkaError(err, message^)
        )
    except:
        # Nothing here is actionable. Losing one report is better than
        # aborting the process from inside a C callback.
        pass


def _stats_trampoline[
    S: _Observed
](rk: Int, json: Int, json_len: Int, opaque: Int) abi("C") -> Int32:
    """librdkafka's statistics callback, for a client whose opaque is an `S`.

    **Returns 0**, which tells librdkafka to free `json` on return -- so the
    document is copied here first. Returning 1 would make it ours to free,
    with a `free()` this package has no binding for.
    """
    try:
        var document = _bytes_to_string(json, json_len)
        ref state = Pointer[S, ImmutAnyOrigin](unsafe_from_address=opaque)[
            unsafe_offset=0
        ]
        state.telemetry_ptr()[unsafe_offset=0].record_stats(document^)
    except:
        pass
    return 0


def _log_trampoline[
    S: _Observed
](rk: Int, level: Int32, fac: Int, buf: Int) abi("C"):
    """librdkafka's log callback, for a client whose opaque is an `S`.

    Two things distinguish it from the other two, and both are handled here:

    - **It carries no opaque.** The client's state is reached through
      `rd_kafka_opaque(rk)`, which needs a resolved symbol a thin callback
      cannot hold -- so `_OpaqueReader` opens one per line. See there for
      what that costs and why it is acceptable for an opt-in hook.
    - **`rk` may be NULL.** librdkafka logs from `rd_kafka_conf_set`
      validation and a few other places with no client yet; those lines
      have no state to land in and are dropped.

    Installed only under `capture_logs=True`, which also sets
    `log.queue=true`: without that librdkafka calls this from its internal
    threads, and there is no safe thing a Mojo callback can do there.
    """
    try:
        if rk == 0:
            return
        var line = LogLine(level, cstr(fac), cstr(buf))
        var opaque = _OpaqueReader().opaque(rk)
        if opaque == 0:
            return
        ref state = Pointer[S, ImmutAnyOrigin](unsafe_from_address=opaque)[
            unsafe_offset=0
        ]
        state.telemetry_ptr()[unsafe_offset=0].record_log(line^)
    except:
        pass

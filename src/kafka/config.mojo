"""Typed configuration objects.

Thin builders that emit a `rd_kafka_conf_t*` on demand. The named fields are
Mojo-idiomatic -- `bootstrap_servers` for `bootstrap.servers` -- and map onto
librdkafka's documented keys explicitly, one at a time, in `_build`.

Keys given to `set()` are passed to librdkafka **verbatim**. An earlier
version rewrote `_` to `.` there, which put librdkafka's real `log_level`
property permanently out of reach as an invalid `log.level`.
"""

from ._ffi import Lib


def _build_conf(
    lib: Lib, named: List[Tuple[String, String]], extra: Dict[String, String]
) raises -> Int:
    """Apply `named` then `extra` to a fresh `rd_kafka_conf_t`.

    Shared by both config types so that librdkafka's ownership rule is
    written once: the conf is **ours** until `rd_kafka_new` adopts it, and it
    adopts it only on success -- so a `conf_set` rejected partway through has
    to destroy it here or it leaks. `Lib.new_client` covers the other half.

    `extra` is applied after the named fields, which is the order both config
    types used before this was factored out. Keys given to `set()` are passed
    to librdkafka verbatim -- see the module docstring for why.
    """
    var conf = lib.conf_new()
    try:
        for pair in named:
            lib.conf_set(conf, pair[0], pair[1])
        for entry in extra.items():
            lib.conf_set(conf, entry.key, entry.value)
    except e:
        lib.conf_destroy(conf)
        raise e
    return conf


def _observability(
    statistics_interval_ms: Int, capture_logs: Bool
) -> List[Tuple[String, String]]:
    """The conf keys behind `statistics_interval_ms` and `capture_logs`.

    Shared by both config types because the rule is the same on both:
    statistics need an interval to fire at all, and a log hook needs
    `log.queue=true` or librdkafka calls it from its own threads, where a
    Mojo callback has no safe thing to do. `capture_logs` sets the key and
    installs the callback together so neither can happen without the other.
    """
    var out: List[Tuple[String, String]] = [
        ("statistics.interval.ms", String(statistics_interval_ms)),
    ]
    if capture_logs:
        out.append((String("log.queue"), String("true")))
    return out^


@fieldwise_init
struct ProducerConfig(Copyable, Movable):
    var bootstrap_servers: String
    var client_id: String
    var compression_type: String
    var linger_ms: Int
    var acks: String
    var statistics_interval_ms: Int
    var capture_logs: Bool
    var extra: Dict[String, String]

    def __init__(
        out self,
        bootstrap_servers: String,
        client_id: String = "mojo-kafka",
        compression_type: String = "none",
        linger_ms: Int = 0,
        acks: String = "all",
        statistics_interval_ms: Int = 0,
        capture_logs: Bool = False,
    ):
        """`statistics_interval_ms` is how often `Producer.latest_stats()`
        is refreshed, and 0 -- the default -- never. `capture_logs` retains
        librdkafka's log lines for `Producer.logs()`; it is off by default
        because it forces `log.queue=true`, which stops librdkafka logging
        to stderr on its own and makes the lines the caller's to read.
        """
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.compression_type = compression_type
        self.linger_ms = linger_ms
        self.acks = acks
        self.statistics_interval_ms = statistics_interval_ms
        self.capture_logs = capture_logs
        self.extra = Dict[String, String]()

    def set(mut self, key: String, value: String):
        """Escape hatch for any librdkafka key not exposed as a field.

        `key` is the librdkafka property name exactly as documented --
        `"message.max.bytes"`, `"log_level"` -- and is passed through
        unchanged.
        """
        self.extra[key] = value

    def _build(self, lib: Lib) raises -> Int:
        var named: List[Tuple[String, String]] = [
            ("bootstrap.servers", self.bootstrap_servers),
            ("client.id", self.client_id),
            ("compression.type", self.compression_type),
            ("linger.ms", String(self.linger_ms)),
            ("acks", self.acks),
        ]
        named.extend(
            _observability(self.statistics_interval_ms, self.capture_logs)
        )
        return _build_conf(lib, named, self.extra)


@fieldwise_init
struct ConsumerConfig(Copyable, Movable):
    var bootstrap_servers: String
    var group_id: String
    var client_id: String
    var auto_offset_reset: String
    var enable_auto_commit: Bool
    var enable_partition_eof: Bool
    var statistics_interval_ms: Int
    var capture_logs: Bool
    var extra: Dict[String, String]

    def __init__(
        out self,
        bootstrap_servers: String,
        group_id: String,
        client_id: String = "mojo-kafka",
        auto_offset_reset: String = "latest",
        enable_auto_commit: Bool = True,
        enable_partition_eof: Bool = False,
        statistics_interval_ms: Int = 0,
        capture_logs: Bool = False,
    ):
        """`enable_partition_eof` defaults to `False`, matching librdkafka.

        Turn it on for a job that drains a partition and stops: it is what
        makes `Consumer.poll_event()` able to report end-of-partition, and
        without it "caught up" is indistinguishable from "nothing arrived".
        A tail-following job wants it off -- it would otherwise get an EOF
        mark every time it caught up with the log.

        `statistics_interval_ms` and `capture_logs` are as on
        `ProducerConfig`: the first refreshes `Consumer.latest_stats()` and
        0 never does; the second retains log lines for `Consumer.logs()` at
        the price of `log.queue=true`.
        """
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.client_id = client_id
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.enable_partition_eof = enable_partition_eof
        self.statistics_interval_ms = statistics_interval_ms
        self.capture_logs = capture_logs
        self.extra = Dict[String, String]()

    def set(mut self, key: String, value: String):
        """Escape hatch for any librdkafka key, passed through verbatim."""
        self.extra[key] = value

    def _build(self, lib: Lib) raises -> Int:
        var named: List[Tuple[String, String]] = [
            ("bootstrap.servers", self.bootstrap_servers),
            ("group.id", self.group_id),
            ("client.id", self.client_id),
            ("auto.offset.reset", self.auto_offset_reset),
            (
                "enable.auto.commit",
                String("true") if self.enable_auto_commit else String("false"),
            ),
            (
                "enable.partition.eof",
                String("true") if self.enable_partition_eof else String(
                    "false"
                ),
            ),
        ]
        named.extend(
            _observability(self.statistics_interval_ms, self.capture_logs)
        )
        return _build_conf(lib, named, self.extra)

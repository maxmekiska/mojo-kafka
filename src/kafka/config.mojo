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


@fieldwise_init
struct ProducerConfig(Copyable, Movable):
    var bootstrap_servers: String
    var client_id: String
    var compression_type: String
    var linger_ms: Int
    var acks: String
    var extra: Dict[String, String]

    def __init__(
        out self,
        bootstrap_servers: String,
        client_id: String = "mojo-kafka",
        compression_type: String = "none",
        linger_ms: Int = 0,
        acks: String = "all",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.compression_type = compression_type
        self.linger_ms = linger_ms
        self.acks = acks
        self.extra = Dict[String, String]()

    def set(mut self, key: String, value: String):
        """Escape hatch for any librdkafka key not exposed as a field.

        `key` is the librdkafka property name exactly as documented --
        `"message.max.bytes"`, `"log_level"` -- and is passed through
        unchanged.
        """
        self.extra[key] = value

    def _build(self, lib: Lib) raises -> Int:
        return _build_conf(
            lib,
            [
                ("bootstrap.servers", self.bootstrap_servers),
                ("client.id", self.client_id),
                ("compression.type", self.compression_type),
                ("linger.ms", String(self.linger_ms)),
                ("acks", self.acks),
            ],
            self.extra,
        )


@fieldwise_init
struct ConsumerConfig(Copyable, Movable):
    var bootstrap_servers: String
    var group_id: String
    var client_id: String
    var auto_offset_reset: String
    var enable_auto_commit: Bool
    var extra: Dict[String, String]

    def __init__(
        out self,
        bootstrap_servers: String,
        group_id: String,
        client_id: String = "mojo-kafka",
        auto_offset_reset: String = "latest",
        enable_auto_commit: Bool = True,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.client_id = client_id
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.extra = Dict[String, String]()

    def set(mut self, key: String, value: String):
        """Escape hatch for any librdkafka key, passed through verbatim."""
        self.extra[key] = value

    def _build(self, lib: Lib) raises -> Int:
        return _build_conf(
            lib,
            [
                ("bootstrap.servers", self.bootstrap_servers),
                ("group.id", self.group_id),
                ("client.id", self.client_id),
                ("auto.offset.reset", self.auto_offset_reset),
                (
                    "enable.auto.commit",
                    String("true") if self.enable_auto_commit else String(
                        "false"
                    ),
                ),
            ],
            self.extra,
        )

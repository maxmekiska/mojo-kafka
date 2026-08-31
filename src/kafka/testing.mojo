"""In-process Kafka for tests -- no broker process, no Docker.

`librdkafka` ships a mock broker that speaks the real wire protocol over a
real socket. Clients pointed at it are ordinary clients: the produce and
consume paths under test are the same ones used against a real cluster.

    from kafka import Consumer, ConsumerConfig, Producer, ProducerConfig
    from kafka.testing import MockCluster

    var cluster = MockCluster()
    cluster.create_topic("events", partition_count=1)
    var p = Producer(ProducerConfig(
        bootstrap_servers=cluster.bootstrap_servers()))
    ...
    _ = cluster^      # keep the cluster alive until here -- see below

What the mock does **not** implement is the Topic Admin API, so
`AdminClient.create_topic()` cannot be exercised here -- use
`MockCluster.create_topic` to set topics up, and cover `AdminClient` against
a real broker.
"""

from ._ffi import Lib, RD_KAFKA_PRODUCER


struct MockCluster:
    """An in-process Kafka cluster backed by librdkafka's mock broker.

    **Keep the cluster alive explicitly.** Mojo destroys a value after its
    last use, not at the end of the scope, so a cluster that is only touched
    while setting topics up is torn down before the first `produce()` runs.
    The symptom is not obvious -- clients log `1/1 brokers are down` and
    `flush()` eventually raises a timeout. End the scope with:

        _ = cluster^

    which consumes the cluster at that point and keeps the broker up for
    everything above it.
    """

    var _lib: Lib
    var _host: Int
    var _cluster: Int

    def __init__(out self, broker_count: Int32 = 1) raises:
        self._lib = Lib()
        var conf = self._lib.conf_new()
        try:
            self._lib.conf_set(conf, "client.id", "mojo-kafka-mock")
            # The host handle deliberately has no bootstrap.servers -- it
            # exists only for librdkafka's book keeping -- so quiet the
            # warning it would otherwise log about that.
            self._lib.conf_set(conf, "log_level", "3")
        except e:
            self._lib.conf_destroy(conf)
            raise e

        self._host = self._lib.new_client(RD_KAFKA_PRODUCER, conf)
        self._cluster = self._lib.mock_cluster_new(self._host, broker_count)
        if self._cluster == 0:
            self._lib.destroy(self._host)
            self._host = 0
            raise Error("rd_kafka_mock_cluster_new returned NULL")

    def __deinit__(deinit self):
        # Destructors cannot raise. Order matters: the cluster is torn down
        # before the handle that hosts it.
        try:
            if self._cluster != 0:
                self._lib.mock_cluster_destroy(self._cluster)
            if self._host != 0:
                self._lib.destroy(self._host)
        except:
            pass

    def bootstrap_servers(self) raises -> String:
        """The address to point `ProducerConfig` / `ConsumerConfig` at."""
        return self._lib.mock_cluster_bootstraps(self._cluster)

    def create_topic(
        self,
        name: String,
        partition_count: Int32 = 1,
        replication_factor: Int32 = 1,
    ) raises:
        """Create a topic directly on the mock.

        The mock broker does not implement CreateTopics, so this is the way
        to set up topics -- not `AdminClient.create_topic()`.
        """
        var rc = self._lib.mock_topic_create(
            self._cluster, name, partition_count, replication_factor
        )
        self._lib.raise_if(rc, "mock_topic_create(" + name + ")")

    def push_request_errors(self, api_key: Int16, errors: List[Int32]) raises:
        """Make the next requests of `api_key` fail with `errors`, in order.

        The mock answers the matching requests with these codes and then
        goes back to behaving normally, one error consumed per request.
        `api_key` is a Kafka wire-protocol request type -- use the
        `API_KEY_*` constants:

            cluster.push_request_errors(
                API_KEY_INIT_PRODUCER_ID,
                [RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED],
            )

        This is how librdkafka tests its own transactional error handling,
        and it is the only way to reach the fatal and abortable branches of
        `KafkaError.txn_action()` deterministically. It drives the real
        classification path rather than simulating it: the mock returns the
        broker code, and the *client* decides from it whether the error is
        fatal, abortable or retriable -- which is precisely the logic under
        test.

        A code queued and never matched is simply never used; it does not
        leak and does not fail the next test, because the cluster does not
        outlive it.
        """
        self._lib.mock_push_request_errors(self._cluster, api_key, errors)

/* The native ceiling: librdkafka driven directly, no binding in the way.
 *
 *   bench_consume_c <bootstrap> <topic> <group> <mode> <batch> <prefetch_ms>
 *                   <repeats>
 *
 * Prints one line per repeat, the same shape every peer prints:
 *
 *   RESULT c <mode> <repeat> <messages> <nanoseconds> <stalls> <checksum>
 *
 * This exists to answer one question the cross-client table cannot: how much
 * of the remaining time is the binding, and how much is librdkafka. Every
 * other peer is measured against it, so it does exactly what they do --
 * same config, same assign/seek replay, same zero-timeout drain, same first
 * payload byte summed into a checksum -- and nothing they do not.
 *
 * `mode` is poll, batch, or batchhdr. The last one is batch plus a
 * rd_kafka_message_headers() call per record, which is what a binding that
 * populates a headers field eagerly has to pay; it is here because that call
 * turned out to cost more than everything else the decode does.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <librdkafka/rdkafka.h>

static long long now_ns(void) {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static void set(rd_kafka_conf_t *conf, const char *k, const char *v) {
        char err[512];
        if (rd_kafka_conf_set(conf, k, v, err, sizeof(err)) !=
            RD_KAFKA_CONF_OK) {
                fprintf(stderr, "conf %s=%s: %s\n", k, v, err);
                exit(1);
        }
}

int main(int argc, char **argv) {
        if (argc != 8) {
                fprintf(stderr,
                        "usage: %s <bootstrap> <topic> <group> "
                        "<poll|batch|batchhdr> <batch> <prefetch_ms> "
                        "<repeats>\n",
                        argv[0]);
                return 2;
        }
        const char *bootstrap = argv[1], *topic = argv[2], *group = argv[3];
        const char *mode = argv[4];
        int batch = atoi(argv[5]);
        int prefetch_ms = atoi(argv[6]);
        int repeats = atoi(argv[7]);

        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        set(conf, "bootstrap.servers", bootstrap);
        set(conf, "group.id", group);
        set(conf, "auto.offset.reset", "earliest");
        set(conf, "enable.partition.eof", "true");
        set(conf, "enable.auto.commit", "false");
        set(conf, "fetch.max.bytes", "52428800");
        set(conf, "queued.max.messages.kbytes", "2097151");
        set(conf, "queued.min.messages", "10000000");
        set(conf, "fetch.wait.max.ms", "10");
        set(conf, "log_level", "3");

        char err[512];
        rd_kafka_t *rk =
            rd_kafka_new(RD_KAFKA_CONSUMER, conf, err, sizeof(err));
        if (!rk) {
                fprintf(stderr, "rd_kafka_new: %s\n", err);
                return 1;
        }
        rd_kafka_poll_set_consumer(rk);

        rd_kafka_topic_partition_list_t *tpl =
            rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(tpl, topic, 0)->offset =
            RD_KAFKA_OFFSET_BEGINNING;
        rd_kafka_assign(rk, tpl);

        rd_kafka_queue_t *queue = rd_kafka_queue_get_consumer(rk);
        rd_kafka_message_t **buf =
            malloc(sizeof(rd_kafka_message_t *) * (size_t)batch);

        for (int r = 0; r < repeats; r++) {
                rd_kafka_topic_partition_list_t *seek =
                    rd_kafka_topic_partition_list_new(1);
                rd_kafka_topic_partition_list_add(seek, topic, 0)->offset = 0;
                rd_kafka_error_t *serr = rd_kafka_seek_partitions(rk, seek, -1);
                if (serr) {
                        fprintf(stderr, "seek: %s\n",
                                rd_kafka_error_string(serr));
                        rd_kafka_error_destroy(serr);
                        return 1;
                }
                rd_kafka_topic_partition_list_destroy(seek);

                struct timespec nap = {prefetch_ms / 1000,
                                       (long)(prefetch_ms % 1000) * 1000000L};
                nanosleep(&nap, NULL);

                long long seen = 0, checksum = 0, stalls = 0;
                int done = 0;
                long long started = now_ns();

                if (!strcmp(mode, "poll")) {
                        while (!done) {
                                rd_kafka_message_t *m =
                                    rd_kafka_consumer_poll(rk, 0);
                                if (!m) {
                                        if (++stalls > 5000000)
                                                done = 1;
                                        continue;
                                }
                                if (m->err ==
                                    RD_KAFKA_RESP_ERR__PARTITION_EOF)
                                        done = 1;
                                else if (!m->err) {
                                        if (m->payload)
                                                checksum +=
                                                    ((unsigned char *)
                                                         m->payload)[0];
                                        seen++;
                                }
                                rd_kafka_message_destroy(m);
                        }
                } else {
                        int want_hdrs = !strcmp(mode, "batchhdr");
                        while (!done) {
                                ssize_t n = rd_kafka_consume_batch_queue(
                                    queue, 0, buf, (size_t)batch);
                                if (n < 0) {
                                        fprintf(stderr, "batch: %s\n",
                                                rd_kafka_err2str(
                                                    rd_kafka_last_error()));
                                        return 1;
                                }
                                if (n == 0) {
                                        if (++stalls > 5000000)
                                                done = 1;
                                        continue;
                                }
                                for (ssize_t i = 0; i < n; i++) {
                                        rd_kafka_message_t *m = buf[i];
                                        if (m->err ==
                                            RD_KAFKA_RESP_ERR__PARTITION_EOF)
                                                done = 1;
                                        else if (!m->err) {
                                                if (m->payload)
                                                        checksum +=
                                                            ((unsigned char *)
                                                                 m->payload)[0];
                                                if (want_hdrs) {
                                                        rd_kafka_headers_t *h;
                                                        checksum +=
                                                            rd_kafka_message_headers(
                                                                m, &h) &
                                                            1;
                                                }
                                                seen++;
                                        }
                                        rd_kafka_message_destroy(m);
                                }
                        }
                }
                long long elapsed = now_ns() - started;
                printf("RESULT c %s %d %lld %lld %lld %lld\n", mode, r, seen,
                       elapsed, stalls, checksum);
                fflush(stdout);
        }

        free(buf);
        rd_kafka_queue_destroy(queue);
        rd_kafka_consumer_close(rk);
        rd_kafka_topic_partition_list_destroy(tpl);
        rd_kafka_destroy(rk);
        return 0;
}

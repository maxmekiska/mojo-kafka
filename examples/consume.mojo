"""Consume: read the summaries `pipeline.mojo` writes.

    pixi run example-consume

`consume(n)` returns a run of records from one call into librdkafka, and
hands back owned `Message`s -- safe to keep, unlike the borrowed views in
`pipeline.mojo`. That is the trade: copy once, keep forever.

It drops end-of-partition marks the way `poll()` does, so `reached_end()` is
how a bounded drain like this one knows to stop. `consume_events()` is the
other way -- it returns the mark as an entry -- but it costs about 1.7x for
the privilege, so reach for it when you need a verdict per record rather
than by default.
"""

from kafka import Consumer, ConsumerConfig

comptime TOPIC = "sensor.summary"


def main() raises:
    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers="localhost:9092",
            group_id="sensor-reader",
            auto_offset_reset="earliest",
            enable_partition_eof=True,
        )
    )
    consumer.subscribe([TOPIC])

    var seen = 0
    while seen < 20:
        # One crossing into C for the whole batch, not one per record.
        var records = consumer.consume(32, timeout_ms=1000)
        for ref m in records:
            print(
                m.topic,
                "[",
                m.partition,
                "] @",
                m.offset,
                m.key_text(default="<null>"),
                m.value_text(default="<null>"),
            )
            seen += 1
        if consumer.reached_end():
            print("reached the end of", TOPIC)
            consumer.close()
            return

    # Commit where we stopped, so a restart resumes rather than replays.
    consumer.commit()
    consumer.close()

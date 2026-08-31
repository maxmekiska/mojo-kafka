"""Consume: read the summaries `pipeline.mojo` writes.

    pixi run example-consume

`consume(n)` returns a run of records from one call into librdkafka, and
hands back owned `Message`s -- safe to keep, unlike the borrowed views in
`pipeline.mojo`. That is the trade: copy once, keep forever.
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
        var events = consumer.consume_events(32, timeout_ms=1000)
        for ref event in events:
            if event.eof:
                print("reached the end of", TOPIC)
                consumer.close()
                return
            if not event.message:
                continue
            ref m = event.message.value()
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

    # Commit where we stopped, so a restart resumes rather than replays.
    consumer.commit()
    consumer.close()

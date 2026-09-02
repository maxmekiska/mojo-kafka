//! The `rust-rdkafka` peer: a zero-copy binding in a systems language.
//!
//!   bench_consume_rs <bootstrap> <topic> <group> <mode> <batch> <prefetch_ms>
//!                    <repeats>
//!
//! Prints one line per repeat, the shape every peer prints:
//!
//!   RESULT rust <mode> <repeat> <messages> <nanoseconds> <stalls> <checksum>
//!
//! This is the peer that makes the borrowed-view claim falsifiable.
//! `confluent-kafka` copies every key and value into `PyBytes`, so beating it
//! with a span costs nothing to explain; `rust-rdkafka`'s `BorrowedMessage`
//! does exactly what `Consumer.consume_borrowed()` does, against the same
//! librdkafka, so it is the number that says whether the Mojo layer is
//! actually free.
//!
//! Three modes, each matching something on our side:
//!
//!   borrowed  BorrowedMessage, payload read in place  <- consume_borrowed()
//!   owned     .detach() to an OwnedMessage            <- consume()
//!   ownedhdr  .detach() plus a headers() call         <- consume() as shipped
//!
//! `batch` is accepted and ignored: rust-rdkafka exposes no batch consume, so
//! every mode here is one message per call. That is itself part of the
//! result.

use std::time::{Duration, Instant};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 8 {
        eprintln!(
            "usage: {} <bootstrap> <topic> <group> \
             <borrowed|owned|ownedhdr> <batch> <prefetch_ms> <repeats>",
            args[0]
        );
        std::process::exit(2);
    }
    let (bootstrap, topic, group, mode) =
        (&args[1], &args[2], &args[3], &args[4]);
    let prefetch_ms: u64 = args[6].parse().expect("prefetch_ms");
    let repeats: usize = args[7].parse().expect("repeats");

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "true")
        .set("enable.auto.commit", "false")
        .set("fetch.max.bytes", "52428800")
        .set("queued.max.messages.kbytes", "2097151")
        .set("queued.min.messages", "10000000")
        .set("fetch.wait.max.ms", "10")
        .set("log_level", "3")
        .create()
        .expect("consumer");

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, Offset::Beginning)
        .expect("assign offset");
    consumer.assign(&tpl).expect("assign");

    for r in 0..repeats {
        consumer
            .seek(topic, 0, Offset::Offset(0), Duration::from_secs(10))
            .expect("seek");
        std::thread::sleep(Duration::from_millis(prefetch_ms));

        let mut seen: u64 = 0;
        let mut checksum: u64 = 0;
        let mut stalls: u64 = 0;
        let mut done = false;
        let started = Instant::now();

        while !done {
            match consumer.poll(Duration::from_millis(0)) {
                None => {
                    stalls += 1;
                    if stalls > 5_000_000 {
                        done = true;
                    }
                }
                Some(Err(KafkaError::PartitionEOF(_))) => done = true,
                Some(Err(e)) => {
                    eprintln!("poll: {e}");
                    std::process::exit(1);
                }
                Some(Ok(m)) => {
                    match mode.as_str() {
                        "borrowed" => {
                            if let Some(p) = m.payload() {
                                checksum += p[0] as u64;
                            }
                        }
                        "owned" | "ownedhdr" => {
                            if mode == "ownedhdr" {
                                // The call both other clients make per
                                // record; counted the same way so the row
                                // is comparable.
                                if let Some(h) = m.headers() {
                                    checksum += h.count() as u64;
                                }
                            }
                            let owned = m.detach();
                            if let Some(p) = owned.payload() {
                                checksum += p[0] as u64;
                            }
                            std::hint::black_box(&owned);
                        }
                        other => {
                            eprintln!("bad mode {other}");
                            std::process::exit(2);
                        }
                    }
                    seen += 1;
                }
            }
        }

        let ns = started.elapsed().as_nanos();
        println!(
            "RESULT rust {mode} {r} {seen} {ns} {stalls} {checksum}"
        );
    }
}

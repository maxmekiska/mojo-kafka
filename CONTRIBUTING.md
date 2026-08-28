# Contributing

Thanks for considering a contribution. `mojo-kafka` is small and the bar for getting changes in is "does it move the alpha closer to a stable v0.1".

## Areas where help is especially welcome

- **Header support on the consumer side.** `Consumer.poll` skips headers — we need the C struct walk and a Mojo-side `Dict[String, String]` exposed on `Message`.
- **Binary payloads.** `Message.key` / `.value` are `String`, so non-UTF-8 payloads (Avro, Protobuf, compressed blobs) cannot round-trip. Moving to a byte span is the biggest remaining API decision.
- **Error mapping.** `KafkaError.code` is the raw int. A `enum KafkaErrorKind` over the common cases would be nicer to pattern-match against.
- **Transactional producer.** `init_transactions / begin / commit / abort` aren't wrapped yet.
- **Schema registry / Avro / Protobuf.** Out of scope for v0.1 but would make a great second package.

## Workflow

1. Fork, branch off `main`.
2. Run `pixi run test && pixi run test-mock` locally. Neither needs Docker —
   the integration suite uses librdkafka's in-process mock broker. If you
   touch `AdminClient` topic creation, also run `pixi run broker-up &&
   pixi run test-broker` (the mock does not implement CreateTopics).
3. If you change the public API, update `README.md` and `examples/`.
4. Open a PR. Small focused PRs > big ones.

## Style

- Prefer Mojo stdlib types over custom ones.
- Keep `_ffi.mojo` the only file that touches C. Everything above it works
  in typed Mojo values, never raw addresses.
- One public symbol per concept (`Producer`, not `KafkaProducer` — the module already says `kafka`).

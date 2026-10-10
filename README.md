# dingding

Rust SDK and bot framework for DingTalk, powered by `reqx`.

- Webhook and enterprise robot messages, interactive cards, and media.
- Typed bot routing, Stream callbacks, shared state, and graceful shutdown.
- Message status, pagination, recall, and persistent destinations for background jobs.
- Bounded processing, deduplication, and structured errors.

## Documentation

Integration examples, feature selection, runtime contracts, and API documentation live in
[rustdoc](https://docs.rs/dingding). Build documentation for the working tree with:

```sh
cargo doc -p dingding --no-deps --open
```

Runnable applications are in [`crates/dingding/examples`](crates/dingding/examples).

## Development

```sh
just ci
```

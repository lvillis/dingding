set shell := ["bash", "-euo", "pipefail", "-c"]

patch:
    cargo release patch --no-publish --execute

publish:
    cargo publish --workspace

ci:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo check -p dingding --no-default-features --features async-tls-rustls-ring,webhook
    cargo check -p dingding --no-default-features --features async-tls-rustls-ring,openapi
    cargo check -p dingding --no-default-features --features async-tls-rustls-ring,stream,macros
    RUSTDOCFLAGS='-D warnings' cargo doc -p dingding --no-deps
    RUSTDOCFLAGS='-D warnings' cargo doc -p dingding-macros --no-deps
    cargo test --doc --workspace
    cargo nextest run --workspace

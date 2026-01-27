# Hegel Rust SDK

Hegel rust SDK.

## Installation

Add to your `Cargo.toml`:

```toml
[dev-dependencies]
hegel = { git = "ssh://git@github.com/antithesishq/hegel-rust" }
```

The SDK automatically installs the Hegel CLI at compile time if not already on PATH.

## Quick Start

```rust
use hegel::gen::{self, Generate};

#[test]
fn test_addition_commutative() {
    hegel::hegel(|| {
        let x = gen::integers::<i32>().generate();
        let y = gen::integers::<i32>().generate();
        assert_eq!(x + y, y + x);
    });
}
```

Run with `cargo test`.

## Documentation

`just docs` to build and open the docs locally.

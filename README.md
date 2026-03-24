# Hegel for Rust

> [!IMPORTANT]
> We're excited you're checking out Hegel! Hegel is in beta, and we'd love for you to try it and [give any feedback you have](https://github.com/hegeldev/hegel-rust/issues/new).
>
> As part of our beta, we may make breaking changes if it makes Hegel a better property-based testing library. If that instability bothers you, please check back in a few months for a stable release!
>
> See https://hegel.dev/compatibility for more details.

* [Documentation](https://docs.rs/hegeltest)
* [Hegel website](https://hegel.dev)

`hegel-rust` is a property-based testing library for Rust. `hegel-rust` is based on [Hypothesis](https://github.com/hypothesisworks/hypothesis), using the [Hegel](https://hegel.dev/) protocol.

## Installation

To install: `cargo add --dev hegeltest`.

## Quick Start

A simple Hegel test:

```rust
use hegel::generators::integers;
use hegel::TestCase;

#[hegel::test]
fn test_addition_commutative(tc: TestCase) {
    let x = tc.draw(integers::<i32>());
    let y = tc.draw(integers::<i32>());
    assert_eq!(x + y, y + x);
}
```

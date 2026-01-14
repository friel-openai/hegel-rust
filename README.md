# Hegel Rust SDK

A Rust SDK for property-based testing with Hegel. Provides a Hypothesis-like API for generating test data via JSON Schema.

## Prerequisites

This SDK requires the `hegel` CLI tool to be installed. Install it via pip:

```bash
pip install git+ssh://git@github.com/antithesishq/hegel.git
```

Or if you have access to the repository locally:

```bash
pip install /path/to/hegel
```

Verify installation:

```bash
hegel --version
```

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
hegel = { git = "https://github.com/antithesishq/hegel-rust.git" }
serde = { version = "1.0", features = ["derive"] }
```

Or for local development:

```toml
[dependencies]
hegel = { path = "/path/to/hegel-rust" }
serde = { version = "1.0", features = ["derive"] }
```

## Quick Start

```rust
use hegel::gen::{self, Generate};

fn main() {
    // Generate integers in a range
    let gen = gen::integers::<i32>().with_min(0).with_max(100);
    let value: i32 = gen.generate();

    // Generate vectors
    let vec_gen = gen::vecs(gen::integers::<i32>())
        .with_min_size(1)
        .with_max_size(10);
    let values: Vec<i32> = vec_gen.generate();
}
```

## Running with Hegel

The SDK requires the Hegel backend. Tests are executed via the `hegel` command:

```bash
hegel ./target/debug/my_test --test-cases=100
```

## API Reference

### The Generate Trait

All generators implement `Generate<T>`:

```rust
pub trait Generate<T> {
    fn generate(&self) -> T;
    fn schema(&self) -> Option<Value>;

    fn map<U, F>(self, f: F) -> Mapped<...>;
    fn flat_map<U, G, F>(self, f: F) -> FlatMapped<...>;
    fn filter<F>(self, predicate: F, max_attempts: usize) -> Filtered<...>;
    fn boxed(self) -> BoxedGenerator<T>;
}
```

### Primitives

```rust
use hegel::gen::{self, Generate};

// Unit
let unit_gen = gen::units();
let _: () = unit_gen.generate();

// Booleans
let bool_gen = gen::booleans();
let b: bool = bool_gen.generate();

// Constants (with schema support)
let const_gen = gen::just(42);
let s_gen = gen::just("hello".to_string());

// Constants (without schema, for non-Serialize types)
let any_gen = gen::just_any(my_value);
```

### Numbers

```rust
// Integers - bounds default to type limits
let int_gen = gen::integers::<i32>();
let bounded = gen::integers::<i32>().with_min(0).with_max(100);
let u8_gen = gen::integers::<u8>();  // automatically 0-255

// Floating point
let float_gen = gen::floats::<f64>();
let bounded_float = gen::floats::<f64>()
    .with_min(0.0)
    .with_max(1.0)
    .exclude_min()  // exclusive bounds
    .exclude_max();
```

### Strings

```rust
// Text with optional length constraints
let text_gen = gen::text();
let bounded = gen::text().with_min_size(1).with_max_size(100);

// Regex patterns (auto-anchored with ^ and $)
let pattern = gen::from_regex(r"[a-z]{3}-[0-9]{3}");

// Format strings
let email = gen::emails();
let url = gen::urls();
let domain = gen::domains().with_max_length(63);
let ip = gen::ip_addresses();      // IPv4 or IPv6
let ipv4 = gen::ip_addresses().v4();
let ipv6 = gen::ip_addresses().v6();

// Date/time (ISO 8601)
let date = gen::dates();       // YYYY-MM-DD
let time = gen::times();       // HH:MM:SS
let datetime = gen::datetimes();
```

### Collections

```rust
// Vectors
let vec_gen = gen::vecs(gen::integers::<i32>());
let bounded = gen::vecs(gen::integers::<i32>())
    .with_min_size(1)
    .with_max_size(10)
    .unique();  // no duplicates

// HashSets
let set_gen = gen::hashsets(gen::integers::<i32>())
    .with_min_size(1)
    .with_max_size(5);

// HashMaps (string keys only, JSON limitation)
let map_gen = gen::hashmaps(gen::integers::<i32>())
    .with_min_size(1)
    .with_max_size(3);
```

### Tuples

```rust
let pair = gen::tuples(gen::integers::<i32>(), gen::text());
let triple = gen::tuples3(gen::booleans(), gen::integers::<i32>(), gen::floats::<f64>());
```

### Combinators

```rust
// Sample from fixed set
let color = gen::sampled_from(vec!["red", "green", "blue"]);
let nums = gen::sampled_from(vec![1, 2, 3, 4, 5]);

// Choose from multiple generators
let range = hegel::one_of!(
    gen::integers::<i32>().with_min(0).with_max(10),
    gen::integers::<i32>().with_min(100).with_max(110),
);

// Optional values
let opt = gen::optional(gen::integers::<i32>());
```

### map, flat_map, filter

```rust
// Transform values
let squared = gen::integers::<i32>()
    .with_min(1)
    .with_max(10)
    .map(|x| x * x);

// Filter values (rejects after max_attempts failures)
let even = gen::integers::<i32>()
    .with_min(0)
    .with_max(100)
    .filter(|x| x % 2 == 0, 10);

// Dependent generation
let sized_string = gen::integers::<usize>()
    .with_min(1)
    .with_max(10)
    .flat_map(|len| gen::text().with_min_size(len).with_max_size(len));
```

### Struct Generation with Derive Macro

For types defined in your crate, use the derive macro:

```rust
use hegel::Generate;
use hegel::gen::{self, Generate as _};
use serde::{Deserialize, Serialize};

#[derive(Generate, Debug, Serialize, Deserialize)]
struct Person {
    name: String,
    age: u32,
}

fn main() {
    // Use generated PersonGenerator with builder methods
    let gen = PersonGenerator::new()
        .with_name(gen::text().with_min_size(1).with_max_size(50))
        .with_age(gen::integers::<u32>().with_min(0).with_max(120));

    let person: Person = gen.generate();
}
```

### Struct Generation for External Types

For types from other crates, use the `derive_generator!` macro:

```rust
use hegel::derive_generator;
use hegel::gen::{self, Generate};

// Type from another crate
use external_crate::Point;

derive_generator!(Point {
    x: f64,
    y: f64,
});

fn main() {
    let gen = PointGenerator::new()
        .with_x(gen::floats::<f64>().with_min(-100.0).with_max(100.0))
        .with_y(gen::floats::<f64>().with_min(-100.0).with_max(100.0));

    let point: Point = gen.generate();
}
```

### Fixed Dictionaries

Generate JSON objects with specific fields:

```rust
use serde_json::Value;

let gen = gen::fixed_dicts()
    .field("name", gen::text().with_min_size(1))
    .field("age", gen::integers::<u32>())
    .field("active", gen::booleans())
    .build();

let value: Value = gen.generate();
```

### Rejecting Invalid Inputs

Use `reject()` when generated data doesn't meet preconditions:

```rust
use hegel::reject;

let person = person_gen.generate();
if person.age < 18 {
    reject("Person must be an adult");
}
```

This tells Hegel to try different input data rather than counting as a test failure.

## Environment Variables

- `HEGEL_SOCKET` - Path to Unix socket (set by hegel)
- `HEGEL_REJECT_CODE` - Exit code for `reject()` calls (set by hegel)
- `HEGEL_DEBUG` - Enable debug logging of requests/responses

## Complete Example

```rust
use hegel::gen::{self, Generate};
use hegel::Generate as DeriveGenerate;
use hegel::reject;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(DeriveGenerate, Debug, Serialize, Deserialize)]
struct Order {
    id: String,
    items: Vec<String>,
    total: f64,
}

// Test registry pattern - let Hegel explore different tests
fn main() {
    let tests: HashMap<&str, fn()> = [
        ("test_order_creation", test_order_creation as fn()),
        ("test_order_total", test_order_total),
    ].into_iter().collect();

    let test_names: Vec<&str> = tests.keys().copied().collect();
    let selected = gen::sampled_from(test_names).generate();

    println!("Running: {}", selected);
    tests[selected]();
    println!("PASSED: {}", selected);
}

fn test_order_creation() {
    let gen = OrderGenerator::new()
        .with_id(gen::from_regex(r"ORD-[0-9]{6}"))
        .with_items(gen::vecs(gen::text().with_min_size(1)).with_min_size(1))
        .with_total(gen::floats::<f64>().with_min(0.0).with_max(10000.0));

    let order: Order = gen.generate();

    assert!(order.id.starts_with("ORD-"));
    assert!(!order.items.is_empty());
}

fn test_order_total() {
    let gen = OrderGenerator::new()
        .with_total(gen::floats::<f64>().with_min(0.0));

    let order: Order = gen.generate();

    if order.total < 0.0 {
        reject("Order total cannot be negative");
    }

    // Test logic...
}
```

## DefaultGenerator Trait

Types implementing `DefaultGenerator` can be generated without explicit configuration:

```rust
// Built-in implementations for:
// - bool, String
// - i8, i16, i32, i64, u8, u16, u32, u64
// - f32, f64
// - Option<T> where T: DefaultGenerator
// - Vec<T> where T: DefaultGenerator
```

The derive macro automatically uses `DefaultGenerator` for fields when you call `::new()`.

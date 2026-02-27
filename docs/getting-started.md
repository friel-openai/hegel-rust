# Getting Started with Hegel for Rust

## Install Hegel

Add Hegel to your `Cargo.toml` as a dev dependency:

```toml
[dev-dependencies]
hegel = { git = "ssh://git@github.com/antithesishq/hegel-rust" }
```

The SDK requires the `hegel` CLI on your PATH:

```bash
pip install "git+ssh://git@github.com/antithesishq/hegel-core.git"
```

If you are working inside this repository, `just setup` handles this.

## Write your first test

```rust
use hegel::generators::{self, Generate};

#[test]
fn test_integers() {
    hegel::hegel(|| {
        let n = generators::integers::<i64>().generate();
        println!("called with {n}");
        assert_eq!(n, n); // integers are always equal to themselves
    });
}
```

`hegel::hegel` runs your closure many times with different generated inputs.
Inside the body, call `.generate()` on a generator to produce a value. If any
assertion fails, Hegel shrinks the inputs to a minimal counterexample.

By default Hegel runs **100 test cases**. Use the builder API to override this:

```rust
use hegel::Hegel;
use hegel::generators::{self, Generate};

Hegel::new(|| {
    let n = generators::integers::<i64>().generate();
    assert_eq!(n, n);
}).test_cases(500).run();
```

## Running in a test suite

Hegel tests are ordinary `#[test]` functions:

```rust
use hegel::generators::{self, Generate};

#[test]
fn test_bounded_integers() {
    hegel::hegel(|| {
        let n = generators::integers::<i32>()
            .with_min(0).with_max(200)
            .generate();
        assert!(n < 50); // this will fail!
    });
}
```

When the test fails, Hegel finds the smallest counterexample — in this case,
`n = 50`.

## Generating multiple values

Call `.generate()` multiple times to produce multiple values in a single test:

```rust
use hegel::generators::{self, Generate};

#[test]
fn test_multiple_values() {
    hegel::hegel(|| {
        let n = generators::integers::<i64>().generate();
        let s = generators::text().generate();
        assert_eq!(n, n);
        assert!(s.len() >= 0);
    });
}
```

Because generation is imperative, you can generate values at any point —
including conditionally or inside loops.

## Filtering

Use `.filter()` for simple conditions on a generator:

```rust
use hegel::generators::{self, Generate};

#[test]
fn test_even_integers() {
    hegel::hegel(|| {
        let n = generators::integers::<i64>()
            .filter(|x| x % 2 == 0)
            .generate();
        assert!(n % 2 == 0);
    });
}
```

When the constraint spans multiple values, use `hegel::assume` inside the
test body:

```rust
use hegel::generators::{self, Generate};

#[test]
fn test_division() {
    hegel::hegel(|| {
        let n1 = generators::integers::<i64>().generate();
        let n2 = generators::integers::<i64>().generate();
        hegel::assume(n2 != 0);
        // n2 is guaranteed non-zero here
        let q = n1 / n2;
        let r = n1 % n2;
        assert_eq!(n1, q * n2 + r);
    });
}
```

Using bounds and `.map()` is more efficient than `.filter()` or `hegel::assume()`
because they avoid generating values that will be rejected.

## Transforming generated values

Use `.map()` to transform values after generation:

```rust
use hegel::generators::{self, Generate};

#[test]
fn test_string_integers() {
    hegel::hegel(|| {
        let s = generators::integers::<i32>()
            .with_min(0).with_max(100)
            .map(|n| n.to_string())
            .generate();
        assert!(s.parse::<i32>().unwrap() >= 0);
    });
}
```

## Dependent generation

Because generation is imperative in Hegel, you can use earlier results to
configure later generators directly:

```rust
use hegel::generators::{self, Generate};

#[test]
fn test_list_with_valid_index() {
    hegel::hegel(|| {
        let n = generators::integers::<usize>()
            .with_min(1).with_max(10)
            .generate();
        let lst: Vec<i32> = generators::vecs(generators::integers())
            .with_min_size(n).with_max_size(n)
            .generate();
        let index = generators::integers::<usize>()
            .with_min(0).with_max(n - 1)
            .generate();
        assert!(index < lst.len());
    });
}
```

You can also use `.flat_map()` for dependent generation within a single
generator expression:

```rust
use hegel::generators::{self, Generate};

#[test]
fn test_flatmap_example() {
    hegel::hegel(|| {
        let (n, lst) = generators::integers::<usize>()
            .with_min(1).with_max(5)
            .flat_map(|n| {
                generators::vecs(generators::integers::<i32>())
                    .with_min_size(n).with_max_size(n)
                    .map(move |lst| (n, lst))
            }).generate();
        assert_eq!(lst.len(), n);
    });
}
```

## What you can generate

### Primitive types

```rust
use hegel::generators::{self, Generate};

# hegel::hegel(|| {
let b: bool = generators::booleans().generate();
let n: i32 = generators::integers::<i32>().generate();    // also i8-i64, u8-u64, usize
let f: f64 = generators::floats::<f64>().generate();      // also f32
let s: String = generators::text().generate();
let bytes: Vec<u8> = generators::binary().generate();
# });
```

All numeric generators support `.with_min()` and `.with_max()`. Floats also
support `.exclude_min()`, `.exclude_max()`, `.allow_nan(bool)`, and
`.allow_infinity(bool)`. Text and binary accept `.with_min_size()`/`.with_max_size()`.

### Constants and choices

```rust
use hegel::generators::{self, Generate};

# hegel::hegel(|| {
let always_42 = generators::just(42).generate();
let suit = generators::sampled_from(vec!["hearts", "diamonds", "clubs", "spades"])
    .generate();
# });
```

### Collections

```rust
use hegel::generators::{self, Generate};
use std::collections::{HashSet, HashMap};

# hegel::hegel(|| {
let v: Vec<i32> = generators::vecs(generators::integers())
    .with_min_size(1).with_max_size(10).generate();
let s: HashSet<i32> = generators::hashsets(generators::integers())
    .with_max_size(5).generate();
let m: HashMap<String, i32> = generators::hashmaps(
    generators::text().with_max_size(10), generators::integers(),
).with_max_size(5).generate();
# });
```

### Combinators

```rust
use hegel::generators::{self, Generate};

# hegel::hegel(|| {
let pair: (i32, String) = generators::tuples(
    generators::integers(), generators::text(),
).generate();
let triple: (bool, i32, f64) = generators::tuples3(
    generators::booleans(), generators::integers(), generators::floats(),
).generate();
let maybe: Option<i32> = generators::optional(generators::integers()).generate();

// Choose between generators (type-erased via one_of! macro)
let n: i32 = hegel::one_of!(
    generators::just(0),
    generators::integers::<i32>().with_min(1).with_max(100),
    generators::integers::<i32>().with_min(-100).with_max(-1),
).generate();
# });
```

### Formats and patterns

```rust
use hegel::generators::{self, Generate};

# hegel::hegel(|| {
let email: String = generators::emails().generate();
let url: String = generators::urls().generate();
let domain: String = generators::domains().with_max_length(50).generate();
let date: String = generators::dates().generate();     // YYYY-MM-DD
let time: String = generators::times().generate();      // HH:MM:SS
let dt: String = generators::datetimes().generate();
let ipv4: String = generators::ip_addresses().v4().generate();
let ipv6: String = generators::ip_addresses().v6().generate();
let pattern: String = generators::from_regex(r"[A-Z]{2}-[0-9]{4}").fullmatch().generate();
# });
```

## Type-directed derivation

`#[derive(Generate)]` creates a builder struct named `<Type>Generator` with
`.new()` and `.with_<field>()` methods:

```rust
use hegel::Generate;
use hegel::generators::{self, Generate as _};

#[derive(Generate, Debug)]
struct User { name: String, age: u32, active: bool }

#[test]
fn test_derived_user() {
    hegel::hegel(|| {
        let user: User = UserGenerator::new()
            .with_age(generators::integers().with_min(18).with_max(120))
            .with_name(generators::from_regex(r"[A-Z][a-z]{2,15}").fullmatch())
            .generate();
        assert!(user.age >= 18 && user.age <= 120);
    });
}
```

For external types, use `derive_generator!` to generate the same builder:

```rust
use hegel::{derive_generator};
use hegel::generators::{self, Generate};

struct Point { x: f64, y: f64 }
derive_generator!(Point { x: f64, y: f64 });
// Now PointGenerator::new().with_x(...).with_y(...).generate() works
```

## Debugging with note()

Use `hegel::note()` to attach debug information. Notes only appear when Hegel
replays the minimal failing example:

```rust
use hegel::generators::{self, Generate};

#[test]
fn test_with_notes() {
    hegel::hegel(|| {
        let x = generators::integers::<i64>().generate();
        let y = generators::integers::<i64>().generate();
        hegel::note(&format!("trying x={x}, y={y}"));
        assert_eq!(x + y, y + x); // commutativity -- always true
    });
}
```

## Guiding generation with target()

> `target()` is not yet available in the Rust SDK. In other Hegel SDKs,
> `target(value, label)` guides the generator toward higher values of a
> numeric metric, useful for finding worst-case inputs. It is planned for
> a future release.

## Next steps

- Run `just docs` to build and browse the full API documentation locally.
- Look at `tests/` for more usage patterns.
- Combine `#[derive(Generate)]` with `.with_<field>()` to generate realistic domain objects.

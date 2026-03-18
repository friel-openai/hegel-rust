RELEASE_TYPE: patch

Add a `#[hegel::composite]` macro to define composite generators:


```rust
use hegel::{TestCase, composite, generators};

#[derive(Debug)]
struct Person {
    age: i32,
    has_drivers_license: bool,
}

#[composite]
fn persons(tc: TestCase) -> Person {
    let age: i32 = tc.draw(generators::integers().min_value(0).max_value(100));
    let has_drivers_license = age > 18 && tc.draw(generators::booleans());
    Person { age, has_drivers_license }
}
```

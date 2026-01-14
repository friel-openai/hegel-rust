//! Example of using Hegel in embedded mode.
//!
//! Run with: cargo run --example embedded_test
//!
//! This example demonstrates:
//! - Running property-based tests without an external hegel process
//! - Using the note() function for debug output
//! - Configuring test options

use hegel::gen::{self, Generate};
use hegel::note;

fn main() {
    println!("Running embedded mode tests...");

    // Test 1: Addition is commutative
    hegel::hegel(
        || {
            let x = gen::integers::<i32>().generate();
            let y = gen::integers::<i32>().generate();
            note(&format!("Testing commutativity: {} + {}", x, y));
            assert_eq!(x.wrapping_add(y), y.wrapping_add(x));
        });
    println!("Test 1 passed: addition is commutative");

    // Test 2: Vector length is within bounds
    hegel::hegel(
        || {
            let v: Vec<i32> = gen::vecs(gen::integers::<i32>())
                .with_min_size(1)
                .with_max_size(10)
                .generate();
            note(&format!("Testing vector length: {}", v.len()));
            assert!(!v.is_empty());
            assert!(v.len() <= 10);
        });
    println!("Test 2 passed: vector length within bounds");

    // Test 3: String generation
    hegel::hegel(
        || {
            let s = gen::text().with_max_size(50).generate();
            note(&format!("Generated string: {:?}", s));
            assert!(s.len() <= 200); // UTF-8 bytes can be more than codepoints
        });
    println!("Test 3 passed: string generation works");

    println!("All tests passed!");
}

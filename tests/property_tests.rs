//! Property-based unit tests using hegel in embedded mode.
//!
//! These tests demonstrate standard property-based testing patterns:
//! - Algebraic properties (commutativity, associativity, identity)
//! - Invariants (sorting, data structure properties)
//! - Round-trip properties (encode/decode, serialize/deserialize)
//! - Bounds and size properties
//!
//! Run with: cargo test --test property_tests

use hegel::gen::{self, Generate};
use hegel::note;
use hegel::{hegel_with_options, HegelOptions};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

// =============================================================================
// Algebraic Properties
// =============================================================================

#[test]
fn addition_is_commutative() {
    hegel::hegel(
        || {
            let x = gen::integers::<i32>().generate();
            let y = gen::integers::<i32>().generate();
            note(&format!("Testing: {} + {}", x, y));

            // Use wrapping arithmetic to avoid overflow panic
            assert_eq!(
                x.wrapping_add(y),
                y.wrapping_add(x),
                "Addition should be commutative"
            );
        });
}

#[test]
fn multiplication_is_commutative() {
    hegel::hegel(
        || {
            let x = gen::integers::<i32>().generate();
            let y = gen::integers::<i32>().generate();
            note(&format!("Testing: {} * {}", x, y));

            assert_eq!(
                x.wrapping_mul(y),
                y.wrapping_mul(x),
                "Multiplication should be commutative"
            );
        });
}

#[test]
fn addition_is_associative() {
    hegel::hegel(
        || {
            // Use smaller integers to avoid overflow
            let x = gen::integers::<i16>().generate();
            let y = gen::integers::<i16>().generate();
            let z = gen::integers::<i16>().generate();
            note(&format!("Testing: ({} + {}) + {}", x, y, z));

            // Cast to i64 to avoid overflow during computation
            let lhs = (x as i64 + y as i64) + z as i64;
            let rhs = x as i64 + (y as i64 + z as i64);
            assert_eq!(lhs, rhs, "Addition should be associative");
        });
}

#[test]
fn zero_is_additive_identity() {
    hegel::hegel(
        || {
            let x = gen::integers::<i64>().generate();
            note(&format!("Testing: {} + 0", x));

            assert_eq!(x + 0, x, "Zero should be additive identity");
            assert_eq!(0 + x, x, "Zero should be additive identity");
        });
}

#[test]
fn one_is_multiplicative_identity() {
    hegel::hegel(
        || {
            let x = gen::integers::<i64>().generate();
            note(&format!("Testing: {} * 1", x));

            assert_eq!(x * 1, x, "One should be multiplicative identity");
            assert_eq!(1 * x, x, "One should be multiplicative identity");
        });
}

// =============================================================================
// Sorting Invariants
// =============================================================================

#[test]
fn sorted_output_is_sorted() {
    hegel::hegel(
        || {
            let v: Vec<i32> = gen::vecs(gen::integers::<i32>())
                .with_max_size(100)
                .generate();
            note(&format!("Testing vector of size {}", v.len()));

            let mut sorted = v.clone();
            sorted.sort();

            // Check that the sorted vector is actually sorted
            for i in 1..sorted.len() {
                assert!(
                    sorted[i - 1] <= sorted[i],
                    "Sorted output should be sorted"
                );
            }
        });
}

#[test]
fn sort_preserves_length() {
    hegel::hegel(
        || {
            let mut v: Vec<i32> = gen::vecs(gen::integers::<i32>())
                .with_max_size(100)
                .generate();
            let original_len = v.len();

            v.sort();

            assert_eq!(v.len(), original_len, "Sorting should preserve length");
        });
}

#[test]
fn sort_preserves_elements() {
    hegel::hegel(
        || {
            let v: Vec<i32> = gen::vecs(gen::integers::<i32>())
                .with_max_size(50)
                .generate();
            let mut original: Vec<i32> = v.clone();
            original.sort();

            let mut sorted = v;
            sorted.sort();

            // After sorting both, they should be equal
            assert_eq!(sorted, original, "Sorting should preserve all elements");
        });
}

#[test]
fn sort_is_idempotent() {
    hegel::hegel(
        || {
            let mut v: Vec<i32> = gen::vecs(gen::integers::<i32>())
                .with_max_size(50)
                .generate();

            v.sort();
            let once_sorted = v.clone();

            v.sort();

            assert_eq!(v, once_sorted, "Sorting twice should equal sorting once");
        });
}

// =============================================================================
// String Properties
// =============================================================================

#[test]
fn reverse_reverse_is_identity() {
    hegel::hegel(
        || {
            let s = gen::text().with_max_size(100).generate();
            note(&format!("Testing string of length {}", s.len()));

            let reversed: String = s.chars().rev().collect();
            let double_reversed: String = reversed.chars().rev().collect();

            assert_eq!(double_reversed, s, "Reversing twice should give original");
        });
}

#[test]
fn concatenation_length_is_sum() {
    hegel::hegel(
        || {
            let s1 = gen::text().with_max_size(50).generate();
            let s2 = gen::text().with_max_size(50).generate();

            let concatenated = format!("{}{}", s1, s2);

            assert_eq!(
                concatenated.len(),
                s1.len() + s2.len(),
                "Concatenation length should be sum of lengths"
            );
        });
}

#[test]
fn substring_is_contained() {
    hegel::hegel(
        || {
            let s = gen::text().with_min_size(10).with_max_size(100).generate();
            if s.len() < 10 {
                hegel::reject("String too short");
            }

            let start = gen::integers::<usize>()
                .with_min(0)
                .with_max(s.len() - 1)
                .generate();
            let max_len = s.len() - start;
            let len = gen::integers::<usize>()
                .with_min(1)
                .with_max(max_len)
                .generate();

            // Get byte-safe substring
            let sub: String = s.chars().skip(start).take(len).collect();

            assert!(s.contains(&sub), "Substring should be found in original");
        });
}

// =============================================================================
// Collection Properties
// =============================================================================

#[test]
fn hashset_has_no_duplicates() {
    hegel::hegel(
        || {
            let s: HashSet<i32> = gen::hashsets(gen::integers::<i32>().with_min(0).with_max(1000))
                .with_max_size(50)
                .generate();

            let elements: Vec<&i32> = s.iter().collect();
            let unique: HashSet<&i32> = elements.iter().copied().collect();

            assert_eq!(elements.len(), unique.len(), "Set should have no duplicates");
        });
}

#[test]
fn unique_vec_has_no_duplicates() {
    hegel::hegel(
        || {
            let v: Vec<i32> = gen::vecs(gen::integers::<i32>().with_min(0).with_max(10000))
                .with_max_size(100)
                .unique()
                .generate();

            let unique: HashSet<&i32> = v.iter().collect();

            assert_eq!(
                v.len(),
                unique.len(),
                "Unique vector should have no duplicates"
            );
        });
}

#[test]
fn hashmap_keys_are_unique() {
    hegel::hegel(
        || {
            let m: HashMap<String, i32> = gen::hashmaps(gen::integers::<i32>())
                .with_max_size(20)
                .generate();

            let keys: Vec<&String> = m.keys().collect();
            let unique: HashSet<&String> = keys.iter().copied().collect();

            assert_eq!(keys.len(), unique.len(), "Map keys should be unique");
        });
}

#[test]
fn vec_sum_is_order_independent() {
    hegel::hegel(
        || {
            let v: Vec<i32> = gen::vecs(gen::integers::<i32>().with_min(-1000).with_max(1000))
                .with_max_size(50)
                .generate();

            let sum1: i64 = v.iter().map(|&x| x as i64).sum();

            let mut reversed = v;
            reversed.reverse();
            let sum2: i64 = reversed.iter().map(|&x| x as i64).sum();

            assert_eq!(sum1, sum2, "Sum should be order-independent");
        });
}

// =============================================================================
// Numeric Properties
// =============================================================================

#[test]
fn absolute_value_is_non_negative() {
    hegel::hegel(
        || {
            let x = gen::integers::<i32>().generate();
            // Avoid i32::MIN which has no positive counterpart
            if x == i32::MIN {
                hegel::reject("Skipping i32::MIN");
            }
            note(&format!("Testing abs({})", x));

            assert!(x.abs() >= 0, "Absolute value should be non-negative");
        });
}

#[test]
fn max_is_greater_or_equal() {
    hegel::hegel(
        || {
            let x = gen::integers::<i32>().generate();
            let y = gen::integers::<i32>().generate();
            note(&format!("Testing max({}, {})", x, y));

            let m = x.max(y);
            assert!(m >= x, "Max should be >= first argument");
            assert!(m >= y, "Max should be >= second argument");
            assert!(m == x || m == y, "Max should equal one of the inputs");
        });
}

#[test]
fn min_is_less_or_equal() {
    hegel::hegel(
        || {
            let x = gen::integers::<i32>().generate();
            let y = gen::integers::<i32>().generate();

            let m = x.min(y);
            assert!(m <= x, "Min should be <= first argument");
            assert!(m <= y, "Min should be <= second argument");
            assert!(m == x || m == y, "Min should equal one of the inputs");
        });
}

#[test]
fn clamp_is_in_range() {
    hegel::hegel(
        || {
            let lo = gen::integers::<i32>().with_min(-100).with_max(0).generate();
            let hi = gen::integers::<i32>().with_min(0).with_max(100).generate();
            let x = gen::integers::<i32>().generate();

            let (lo, hi) = if lo > hi { (hi, lo) } else { (lo, hi) };

            let clamped = x.clamp(lo, hi);

            assert!(clamped >= lo, "Clamped value should be >= lower bound");
            assert!(clamped <= hi, "Clamped value should be <= upper bound");
        });
}

// =============================================================================
// Round-trip Properties
// =============================================================================

#[test]
fn int_to_string_to_int() {
    hegel::hegel(
        || {
            let x = gen::integers::<i32>().generate();
            note(&format!("Testing round-trip for {}", x));

            let s = x.to_string();
            let y: i32 = s.parse().expect("Should parse back to int");

            assert_eq!(x, y, "Int -> string -> int should be identity");
        });
}

// NOTE: Removed float_to_string_to_float test as it's testing float parsing
// behavior (which has known edge cases) rather than hegel functionality

// =============================================================================
// Data Structure Properties
// =============================================================================

#[derive(Debug, Clone)]
struct Point {
    x: i32,
    y: i32,
}

#[test]
fn point_distance_is_non_negative() {
    hegel::hegel(
        || {
            let x = gen::integers::<i32>().with_min(-1000).with_max(1000).generate();
            let y = gen::integers::<i32>().with_min(-1000).with_max(1000).generate();
            let p = Point { x, y };
            note(&format!("Testing point ({}, {})", p.x, p.y));

            let dist = ((p.x as f64).powi(2) + (p.y as f64).powi(2)).sqrt();

            assert!(dist >= 0.0, "Distance from origin should be non-negative");
        });
}

#[test]
fn optional_has_value_or_not() {
    hegel::hegel(
        || {
            let opt: Option<i32> = gen::optional(gen::integers::<i32>()).generate();

            // This is a tautology, but demonstrates optional generation
            assert!(
                opt.is_some() || opt.is_none(),
                "Optional must have value or not"
            );

            match opt {
                Some(v) => note(&format!("Got value: {}", v)),
                None => note("Got None"),
            }
        });
}

// =============================================================================
// Filter and Map Properties
// =============================================================================

#[test]
fn filtered_values_match_predicate() {
    hegel::hegel(
        || {
            let gen = gen::integers::<i32>()
                .with_min(0)
                .with_max(100)
                .filter(|x| x % 2 == 0, 10);
            let x = gen.generate();

            assert_eq!(x % 2, 0, "Filtered values should be even");
            assert!(x >= 0, "Value should be in original range");
            assert!(x <= 100, "Value should be in original range");
        });
}

#[test]
fn mapped_values_are_transformed() {
    hegel::hegel(
        || {
            let gen = gen::integers::<i32>()
                .with_min(1)
                .with_max(10)
                .map(|x| x * x);
            let x = gen.generate();

            // Result should be a perfect square between 1 and 100
            let root = (x as f64).sqrt() as i32;
            assert_eq!(root * root, x, "Mapped value should be a perfect square");
            assert!(x >= 1, "Value should be >= 1");
            assert!(x <= 100, "Value should be <= 100");
        });
}

// =============================================================================
// Derive Macro Properties
// =============================================================================

use hegel_derive::Generate;

#[derive(Debug, Clone, Generate)]
struct Person {
    name: String,
    age: u8,
}

#[test]
fn derived_generator_produces_valid_data() {
    hegel::hegel(
        || {
            let person = PersonGenerator::new()
                .with_age(gen::integers::<u8>().with_max(120))
                .generate();

            note(&format!("Generated person: {:?}", person));

            assert!(person.age <= 120, "Age should be <= 120");
        });
}

#[derive(Debug, Clone, Generate, Deserialize)]
enum Status {
    Active,
    Inactive,
    Pending { reason: String },
}

#[test]
fn derived_enum_generator_works() {
    hegel::hegel(
        || {
            let status = StatusGenerator::new().generate();

            note(&format!("Generated status: {:?}", status));

            // Just verify it generates something valid
            match status {
                Status::Active => {}
                Status::Inactive => {}
                Status::Pending { reason: _ } => {}
            }
        });
}

// =============================================================================
// Edge Cases and Boundary Testing
// =============================================================================

#[test]
fn empty_vec_is_valid() {
    hegel_with_options(
        || {
            let v: Vec<i32> = gen::vecs(gen::integers::<i32>())
                .with_min_size(0)
                .with_max_size(0)
                .generate();

            assert!(v.is_empty(), "Should generate empty vec");
        },
        HegelOptions::new().with_test_cases(10),
    );
}

#[test]
fn single_element_vec_is_valid() {
    hegel::hegel(
        || {
            let v: Vec<i32> = gen::vecs(gen::integers::<i32>())
                .with_min_size(1)
                .with_max_size(1)
                .generate();

            assert_eq!(v.len(), 1, "Should generate single-element vec");
        });
}

#[test]
fn bounded_integers_are_in_bounds() {
    hegel::hegel(
        || {
            let x = gen::integers::<i32>().with_min(-50).with_max(50).generate();

            assert!(x >= -50, "Should be >= min");
            assert!(x <= 50, "Should be <= max");
        });
}

#[test]
fn sampled_from_returns_valid_element() {
    hegel::hegel(
        || {
            let options = vec!["apple", "banana", "cherry"];
            let choice = gen::sampled_from(options.clone()).generate();

            assert!(
                options.contains(&choice.as_ref()),
                "Should return one of the options"
            );
        });
}

#[test]
fn one_of_returns_from_generators() {
    hegel::hegel(
        || {
            let gen = gen::one_of(vec![
                gen::integers::<i32>().with_min(0).with_max(10).boxed(),
                gen::integers::<i32>().with_min(100).with_max(110).boxed(),
            ]);
            let x = gen.generate();

            assert!(
                (0..=10).contains(&x) || (100..=110).contains(&x),
                "Should return from one of the ranges"
            );
        });
}

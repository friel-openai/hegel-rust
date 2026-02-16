//! Tests that generators work correctly with non-'static (borrowed) types.
//!
//! These tests exercise the lifetime logic in BasicGenerator<'a, T> and
//! the phantom type parameters on composite generators.

use hegel::gen::{self, Generate};

#[test]
fn test_sampled_from_references() {
    hegel::hegel(|| {
        let options = [10, 20, 30, 40, 50];
        let refs: Vec<&i32> = options.iter().collect();
        let value: &i32 = gen::sampled_from(refs).generate();
        assert!(options.contains(value));
    });
}

#[test]
fn test_sampled_from_str_references() {
    hegel::hegel(|| {
        let strings = ["hello", "world", "foo", "bar"];
        let value: &str = gen::sampled_from(strings.to_vec()).generate();
        assert!(strings.contains(&value));
    });
}

#[test]
fn test_tuple_of_references() {
    hegel::hegel(|| {
        let xs = [1, 2, 3];
        let ys = ["a", "b", "c"];
        let x_refs: Vec<&i32> = xs.iter().collect();
        let y_refs: Vec<&&str> = ys.iter().collect();
        let (x, y): (&i32, &&str) =
            gen::tuples(gen::sampled_from(x_refs), gen::sampled_from(y_refs)).generate();
        assert!(xs.contains(x));
        assert!(ys.contains(y));
    });
}

#[test]
fn test_optional_of_references() {
    hegel::hegel(|| {
        let values = [100, 200, 300];
        let refs: Vec<&i32> = values.iter().collect();
        let result: Option<&i32> = gen::optional(gen::sampled_from(refs)).generate();
        if let Some(v) = result {
            assert!(values.contains(v));
        }
    });
}

#[test]
fn test_one_of_with_references() {
    hegel::hegel(|| {
        let small = [1, 2, 3];
        let big = [100, 200, 300];
        let small_refs: Vec<&i32> = small.iter().collect();
        let big_refs: Vec<&i32> = big.iter().collect();
        let value: &i32 =
            hegel::one_of!(gen::sampled_from(small_refs), gen::sampled_from(big_refs),).generate();
        assert!(small.contains(value) || big.contains(value));
    });
}

#[test]
fn test_vec_of_references() {
    hegel::hegel(|| {
        let options = [10, 20, 30];
        let refs: Vec<&i32> = options.iter().collect();
        let result: Vec<&i32> = gen::vecs(gen::sampled_from(refs))
            .with_min_size(1)
            .with_max_size(5)
            .generate();
        assert!(!result.is_empty());
        for v in &result {
            assert!(options.contains(v));
        }
    });
}

#[test]
fn test_map_over_references() {
    hegel::hegel(|| {
        let values = [10, 20, 30];
        let refs: Vec<&i32> = values.iter().collect();
        let doubled: i32 = gen::sampled_from(refs).map(|r| r * 2).generate();
        assert!([20, 40, 60].contains(&doubled));
    });
}

#[test]
fn test_tuple3_of_references() {
    hegel::hegel(|| {
        let xs = [1, 2];
        let ys = ["a", "b"];
        let zs = [true, false];
        let xr: Vec<&i32> = xs.iter().collect();
        let yr: Vec<&&str> = ys.iter().collect();
        let zr: Vec<&bool> = zs.iter().collect();
        let (x, y, z): (&i32, &&str, &bool) = gen::tuples3(
            gen::sampled_from(xr),
            gen::sampled_from(yr),
            gen::sampled_from(zr),
        )
        .generate();
        assert!(xs.contains(x));
        assert!(ys.contains(y));
        assert!(zs.contains(z));
    });
}

#[test]
fn test_nested_optional_tuple_of_references() {
    hegel::hegel(|| {
        let names = ["alice", "bob", "carol"];
        let ages = [25u32, 30, 35];
        let name_refs: Vec<&&str> = names.iter().collect();
        let age_refs: Vec<&u32> = ages.iter().collect();
        let result: Option<(&&str, &u32)> = gen::optional(gen::tuples(
            gen::sampled_from(name_refs),
            gen::sampled_from(age_refs),
        ))
        .generate();
        if let Some((name, age)) = result {
            assert!(names.contains(name));
            assert!(ages.contains(age));
        }
    });
}

#[test]
fn test_vec_of_tuples_of_references() {
    hegel::hegel(|| {
        let keys = [1, 2, 3];
        let vals = ["x", "y", "z"];
        let kr: Vec<&i32> = keys.iter().collect();
        let vr: Vec<&&str> = vals.iter().collect();
        let result: Vec<(&i32, &&str)> =
            gen::vecs(gen::tuples(gen::sampled_from(kr), gen::sampled_from(vr)))
                .with_max_size(5)
                .generate();
        for (k, v) in &result {
            assert!(keys.contains(k));
            assert!(vals.contains(v));
        }
    });
}

#[test]
fn test_one_of_mapped_references() {
    hegel::hegel(|| {
        let positives = [1, 2, 3];
        let negatives = [-1, -2, -3];
        let pos_refs: Vec<&i32> = positives.iter().collect();
        let neg_refs: Vec<&i32> = negatives.iter().collect();
        let description: String = hegel::one_of!(
            gen::sampled_from(pos_refs).map(|r| format!("positive: {}", r)),
            gen::sampled_from(neg_refs).map(|r| format!("negative: {}", r)),
        )
        .generate();
        assert!(description.starts_with("positive:") || description.starts_with("negative:"));
    });
}

#[test]
fn test_boxed_generator_with_references() {
    hegel::hegel(|| {
        let options = [10, 20, 30];
        let refs: Vec<&i32> = options.iter().collect();
        let gen = gen::sampled_from(refs).boxed();
        let value: &i32 = gen.generate();
        assert!(options.contains(value));
    });
}

#[test]
fn test_deeply_nested_reference_composition() {
    // References flowing through: sampled_from -> tuple -> optional -> vec -> map
    hegel::hegel(|| {
        let xs = [1i32, 2, 3];
        let ys = [4i32, 5, 6];
        let xr: Vec<&i32> = xs.iter().collect();
        let yr: Vec<&i32> = ys.iter().collect();

        let result: Vec<i32> = gen::vecs(
            gen::optional(gen::tuples(gen::sampled_from(xr), gen::sampled_from(yr))).map(|opt| {
                match opt {
                    Some((a, b)) => a + b,
                    None => 0,
                }
            }),
        )
        .with_max_size(5)
        .generate();

        for v in &result {
            assert!(
                *v == 0 || (5..=9).contains(v),
                "Expected 0 or 5..=9, got {}",
                v
            );
        }
    });
}

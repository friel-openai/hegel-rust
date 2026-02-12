mod common;

use common::utils::find_any;
use hegel::gen::{self, Generate};

#[test]
fn test_sampled_from_returns_element_from_list() {
    hegel::hegel(|| {
        let options = gen::vecs(gen::integers::<i32>()).generate();
        let value = gen::sampled_from(options.clone()).generate();
        assert!(options.contains(&value));
    });
}

#[test]
fn test_sampled_from_strings() {
    hegel::hegel(|| {
        let options = gen::vecs(gen::text()).generate();
        let value = gen::sampled_from(options.clone()).generate();
        assert!(options.contains(&value));
    });
}

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
fn test_optional_can_generate_some() {
    find_any(gen::optional(gen::integers::<i32>()), |v| v.is_some());
}

#[test]
fn test_optional_can_generate_none() {
    find_any(gen::optional(gen::integers::<i32>()), |v| v.is_none());
}

#[test]
fn test_optional_respects_inner_generator_bounds() {
    hegel::hegel(|| {
        let value: Option<i32> =
            gen::optional(gen::integers().with_min(10).with_max(20)).generate();
        if let Some(n) = value {
            assert!((10..=20).contains(&n));
        }
    });
}

#[test]
fn test_one_of_returns_value_from_one_generator() {
    hegel::hegel(|| {
        let value: i32 = hegel::one_of!(
            gen::integers().with_min(0).with_max(10),
            gen::integers().with_min(100).with_max(110),
        )
        .generate();
        assert!((0..=10).contains(&value) || (100..=110).contains(&value));
    });
}

#[test]
fn test_one_of_with_different_types_via_map() {
    hegel::hegel(|| {
        let value: String = hegel::one_of!(
            gen::integers::<i32>()
                .with_min(0)
                .with_max(100)
                .map(|n| format!("number: {}", n)),
            gen::text()
                .with_min_size(1)
                .with_max_size(10)
                .map(|s| format!("text: {}", s)),
        )
        .generate();
        assert!(value.starts_with("number: ") || value.starts_with("text: "));
    });
}

#[test]
fn test_one_of_many() {
    hegel::hegel(|| {
        let generators: Vec<_> = (0..10).map(|i| gen::just(i).boxed()).collect();
        let value: i32 = gen::one_of(generators).generate();
        assert!((0..10).contains(&value));
    });
}

#[test]
fn test_flat_map() {
    hegel::hegel(|| {
        let value: String = gen::integers::<usize>()
            .with_min(1)
            .with_max(5)
            .flat_map(|len| gen::text().with_min_size(len).with_max_size(len))
            .generate();
        assert!(!value.is_empty());
        assert!(value.chars().count() <= 5);
    });
}

#[test]
fn test_filter() {
    hegel::hegel(|| {
        let value: i32 = gen::integers::<i32>()
            .with_min(0)
            .with_max(100)
            .filter(|n| n % 2 == 0)
            .generate();
        assert!(value % 2 == 0);
        assert!((0..=100).contains(&value));
    });
}

#[test]
fn test_boxed_generator_clone() {
    hegel::hegel(|| {
        let gen1 = gen::integers::<i32>().with_min(0).with_max(10).boxed();
        let gen2 = gen1.clone();
        let v1 = gen1.generate();
        let v2 = gen2.generate();
        assert!((0..=10).contains(&v1));
        assert!((0..=10).contains(&v2));
    });
}

#[test]
fn test_boxed_generator_double_boxed() {
    hegel::hegel(|| {
        // Calling .boxed() on an already-boxed generator should not re-wrap
        let gen1 = gen::integers::<i32>().with_min(0).with_max(10).boxed();
        let gen2 = gen1.boxed();
        let value = gen2.generate();
        assert!((0..=10).contains(&value));
    });
}

#[test]
fn test_sampled_from_non_primitive() {
    #[derive(Clone, Debug, PartialEq, serde::Serialize)]
    struct Point {
        x: i32,
        y: i32,
    }

    hegel::hegel(|| {
        let options = vec![
            Point { x: 1, y: 2 },
            Point { x: 3, y: 4 },
            Point { x: 5, y: 6 },
        ];
        let value = gen::sampled_from(options.clone()).generate();
        assert!(options.contains(&value));
    });
}

#[test]
fn test_optional_mapped() {
    hegel::hegel(|| {
        let value: Option<String> = gen::optional(
            gen::integers::<i32>()
                .with_min(0)
                .with_max(100)
                .map(|n| format!("value: {}", n)),
        )
        .generate();
        if let Some(s) = value {
            assert!(s.starts_with("value: "));
        }
    });

    find_any(
        gen::optional(gen::integers::<i32>().map(|n| n.wrapping_mul(2))),
        |v| v.is_some(),
    );

    find_any(
        gen::optional(gen::integers::<i32>().map(|n| n.wrapping_mul(2))),
        |v| v.is_none(),
    );
}

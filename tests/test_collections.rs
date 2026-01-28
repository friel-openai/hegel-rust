use hegel::gen::{self, Generate};
use std::collections::{HashMap, HashSet};

#[test]
fn test_vec_with_max_size() {
    hegel::hegel(|| {
        let max_size: usize = gen::integers().with_min(0).with_max(20).generate();
        let vec: Vec<i32> = gen::vecs(gen::integers::<i32>())
            .with_max_size(max_size)
            .generate();
        assert!(vec.len() <= max_size);
    });
}

#[test]
fn test_vec_with_min_size() {
    hegel::hegel(|| {
        let min_size: usize = gen::integers().with_min(0).with_max(20).generate();
        let vec: Vec<i32> = gen::vecs(gen::integers::<i32>())
            .with_min_size(min_size)
            .generate();
        assert!(vec.len() >= min_size);
    });
}

#[test]
fn test_vec_with_min_and_max_size() {
    hegel::hegel(|| {
        let min_size: usize = gen::integers().with_min(0).with_max(10).generate();
        let max_size = gen::integers().with_min(min_size).generate();
        let vec: Vec<i32> = gen::vecs(gen::integers::<i32>())
            .with_min_size(min_size)
            .with_max_size(max_size)
            .generate();
        assert!(vec.len() >= min_size && vec.len() <= max_size);
    });
}

#[test]
fn test_vec_unique() {
    hegel::hegel(|| {
        let max_size: usize = gen::integers().with_min(0).with_max(50).generate();
        let vec: Vec<i32> = gen::vecs(gen::integers::<i32>())
            .with_max_size(max_size)
            .unique()
            .generate();

        let set: HashSet<_> = vec.iter().collect();
        assert_eq!(set.len(), vec.len());
    });
}

#[test]
fn test_vec_unique_with_min_size() {
    hegel::hegel(|| {
        let min_size: usize = gen::integers().with_min(0).with_max(20).generate();
        let vec: Vec<i32> = gen::vecs(gen::integers::<i32>())
            .with_min_size(min_size)
            .unique()
            .generate();

        assert!(vec.len() >= min_size);

        let set: HashSet<_> = vec.iter().collect();
        assert_eq!(set.len(), vec.len());
    });
}

#[test]
fn test_vec_with_mapped_elements() {
    hegel::hegel(|| {
        let vec: Vec<i32> = gen::vecs(
            gen::integers::<i32>()
                .with_min(i32::MIN / 2)
                .with_max(i32::MAX / 2)
                .map(|x| x * 2),
        )
        .with_max_size(10)
        .generate();
        assert!(vec.iter().all(|&x| x % 2 == 0));
    });
}

// HashSet tests

#[test]
fn test_hashset_with_max_size() {
    hegel::hegel(|| {
        let max_size: usize = gen::integers().with_min(0).with_max(20).generate();
        let set: HashSet<i32> = gen::hashsets(gen::integers::<i32>())
            .with_max_size(max_size)
            .generate();
        assert!(set.len() <= max_size);
    });
}

#[test]
fn test_hashset_with_min_size() {
    hegel::hegel(|| {
        let min_size: usize = gen::integers().with_min(0).with_max(20).generate();
        let set: HashSet<i32> = gen::hashsets(gen::integers::<i32>())
            .with_min_size(min_size)
            .generate();
        assert!(set.len() >= min_size);
    });
}

#[test]
fn test_hashset_with_min_and_max_size() {
    hegel::hegel(|| {
        let min_size: usize = gen::integers().with_min(0).with_max(10).generate();
        let max_size = gen::integers().with_min(min_size).generate();
        let set: HashSet<i32> = gen::hashsets(gen::integers::<i32>())
            .with_min_size(min_size)
            .with_max_size(max_size)
            .generate();
        assert!(set.len() >= min_size && set.len() <= max_size);
    });
}

#[test]
fn test_hashset_with_mapped_elements() {
    hegel::hegel(|| {
        // Exclude i32::MIN to avoid overflow when taking abs
        let set: HashSet<i32> = gen::hashsets(
            gen::integers::<i32>()
                .with_min(i32::MIN + 1)
                .map(|x| x.abs()),
        )
        .with_max_size(10)
        .generate();
        assert!(set.iter().all(|&x| x >= 0));
    });
}

#[test]
fn test_vec_of_hashsets() {
    hegel::hegel(|| {
        let vec_of_sets: Vec<HashSet<i32>> = gen::vecs(
            gen::hashsets(gen::integers::<i32>().with_min(0).with_max(100)).with_max_size(5),
        )
        .with_max_size(3)
        .generate();
        for set in &vec_of_sets {
            assert!(set.len() <= 5);
            assert!(set.iter().all(|&x| x >= 0 && x <= 100));
        }
    });
}

// HashMap tests

#[test]
fn test_hashmap_with_max_size() {
    hegel::hegel(|| {
        let max_size: usize = gen::integers().with_min(0).with_max(20).generate();
        let map: HashMap<i32, i32> = gen::hashmaps(gen::integers::<i32>(), gen::integers::<i32>())
            .with_max_size(max_size)
            .generate();
        assert!(map.len() <= max_size);
    });
}

#[test]
fn test_hashmap_with_min_size() {
    hegel::hegel(|| {
        let min_size: usize = gen::integers().with_min(0).with_max(20).generate();
        let map: HashMap<i32, i32> = gen::hashmaps(gen::integers::<i32>(), gen::integers::<i32>())
            .with_min_size(min_size)
            .generate();
        assert!(map.len() >= min_size);
    });
}

#[test]
fn test_hashmap_with_min_and_max_size() {
    hegel::hegel(|| {
        let min_size: usize = gen::integers().with_min(0).with_max(10).generate();
        let max_size = gen::integers().with_min(min_size).generate();
        let map: HashMap<i32, i32> = gen::hashmaps(gen::integers::<i32>(), gen::integers::<i32>())
            .with_min_size(min_size)
            .with_max_size(max_size)
            .generate();
        assert!(map.len() >= min_size && map.len() <= max_size);
    });
}

#[test]
fn test_hashmap_with_mapped_keys() {
    hegel::hegel(|| {
        let map: HashMap<i32, i32> = gen::hashmaps(
            gen::integers::<i32>()
                .with_min(i32::MIN / 2)
                .with_max(i32::MAX / 2)
                .map(|x| x * 2),
            gen::integers(),
        )
        .with_max_size(10)
        .generate();
        assert!(map.keys().all(|&k| k % 2 == 0));
    });
}

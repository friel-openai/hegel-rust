// use hegel::generators::{self, Generate};
// use std::sync::atomic::{AtomicI32, Ordering};

// static GLOBAL_COUNTER: AtomicI32 = AtomicI32::new(0);

// #[test]
// fn test_flaky_global_state() {
//     hegel::hegel(|| {
//         let _x = generators::integers::<i32>()
//             .with_min(GLOBAL_COUNTER.load(Ordering::SeqCst))
//             .generate();
//         GLOBAL_COUNTER.fetch_add(1, Ordering::SeqCst);
//     });
// }

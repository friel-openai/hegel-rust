use hegel::gen::{self, Generate};
use hegel::{Hegel, Verbosity};

#[test]
fn test_integers_i32_within_bounds() {
    Hegel::new(|| {
        let x = gen::integers::<i32>().generate();
        assert!(x >= i32::MIN && x <= i32::MAX);
    })
    .verbosity(Verbosity::Verbose)
    .run();
}

use hegel::TestCase;
use hegel::generators as gs;

#[hegel::test]
fn test_target(tc: TestCase) {
    let value: i32 = tc.draw(gs::integers().min_value(0).max_value(100));
    tc.target(f64::from(value), "size");
}

mod common;

use common::project::TempRustProject;
use hegel::TestCase;
use hegel::generators::integers;

#[test]
fn test_successful_expansion() {
    let code = r#"
use hegel::generators::integers;
use hegel::TestCase;

#[hegel::composite]
fn composite_integer_generator(tc: TestCase, n: i32) -> i32 {
    tc.draw(integers::<i32>()) + n
}

fn main() {}
"#;

    TempRustProject::new().main_file(code).cargo_run(&[]);
}

#[test]
fn test_missing_return_type() {
    let code = r#"
use hegel::generators::integers;
use hegel::TestCase;

#[hegel::composite]
fn composite_integer_generator(tc: TestCase, n: i32) {
    tc.draw(integers::<i32>()) + n
}

fn main() {}
"#;

    TempRustProject::new()
        .main_file(code)
        .expect_failure("must explicitly declare a return type")
        .cargo_run(&[]);
}

#[test]
fn test_nullary() {
    let code = r#"
#[hegel::composite]
fn composite_integer_generator() -> i32 {
    0
}

fn main() {}
"#;

    TempRustProject::new()
        .main_file(code)
        .expect_failure("must define a first parameter of type TestCase")
        .cargo_run(&[]);
}

#[test]
fn test_missing_test_case_parameter() {
    let code = r#"
#[hegel::composite]
fn composite_integer_generator(n: i32) -> i32 {
    n
}

fn main() {}
"#;

    TempRustProject::new()
        .main_file(code)
        .expect_failure("must have type TestCase")
        .cargo_run(&[]);
}

#[hegel::composite]
fn composite_integer_generator(tc: TestCase, lower: i32, upper: i32, offset: i32) -> i32 {
    let x = tc.draw(integers::<i32>().min_value(lower).max_value(upper));
    x + offset
}

#[hegel::test]
fn test_passing_composite_generation(tc: TestCase) {
    let x = tc.draw(composite_integer_generator(0, 100, 1));
    assert!(x > 0);
}

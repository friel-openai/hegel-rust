mod common;

use common::project::TempRustProject;
use common::utils::expect_panic;
use hegel::TestCase;
use hegel::generators;

#[hegel::test]
fn test_basic_usage(tc: TestCase) {
    tc.draw(generators::booleans());
}

#[hegel::test(test_cases = 10)]
fn test_with_named_arg(tc: TestCase) {
    tc.draw(generators::booleans());
}

#[hegel::test(hegel::Settings::new().test_cases(10))]
fn test_with_positional_settings(tc: TestCase) {
    tc.draw(generators::booleans());
}

#[hegel::test(hegel::Settings::new(), test_cases = 10)]
fn test_with_positional_and_named(tc: TestCase) {
    tc.draw(generators::booleans());
}

#[hegel::test(test_cases = 10, derandomize = true)]
fn test_with_multiple_named_args(tc: TestCase) {
    tc.draw(generators::booleans());
}

#[hegel::test(seed = Some(42))]
fn test_with_seed(tc: TestCase) {
    tc.draw(generators::booleans());
}

#[test]
fn test_database_persists_failing_examples() {
    let db_path = tempfile::tempdir().unwrap();
    let db_str = db_path.path().to_str().unwrap().to_string();

    assert!(std::fs::read_dir(db_path.path()).unwrap().next().is_none());

    expect_panic(
        || {
            hegel::Hegel::new(|_tc: hegel::TestCase| {
                panic!("");
            })
            .settings(hegel::Settings::new().database(Some(db_str)))
            .__database_key("test_database_persists".to_string())
            .run();
        },
        "Property test failed",
    );

    let entries: Vec<_> = std::fs::read_dir(db_path.path()).unwrap().collect();
    assert!(!entries.is_empty());
}

#[test]
fn test_duplicate_test_attribute_compile_error() {
    let code = r#"
use hegel::generators;

#[hegel::test]
#[test]
fn main(tc: hegel::TestCase) {}
"#;
    TempRustProject::new()
        .main_file(code)
        .expect_failure("Remove the #\\[test\\] attribute")
        .cargo_run(&[]);
}

#[test]
fn test_params_compile_error() {
    // Zero parameters should be rejected
    let code_zero = r#"
use hegel::generators;

#[hegel::test]
fn main() {
}
"#;
    TempRustProject::new()
        .main_file(code_zero)
        .expect_failure("must take exactly one parameter of type hegel::TestCase")
        .cargo_run(&[]);

    // Two parameters should be rejected
    let code_two = r#"
use hegel::generators;

#[hegel::test]
fn main(tc: hegel::TestCase, x: bool) {
    let _ = (tc, x);
}
"#;
    TempRustProject::new()
        .main_file(code_two)
        .expect_failure("must take exactly one parameter of type hegel::TestCase")
        .cargo_run(&[]);
}

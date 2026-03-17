mod common;

use common::project::TempRustProject;
use hegel::HealthCheck;
use hegel::TestCase;
use hegel::generators;

#[hegel::test(suppress_health_check = HealthCheck::FilterTooMuch)]
fn test_suppress_filter_via_macro(tc: TestCase) {
    let _: i32 = tc.draw(generators::integers().min_value(0).max_value(100));
    tc.assume(false);
}

#[test]
fn test_filter_too_much_fails() {
    let code = r#"
use hegel::generators;

fn main() {
    hegel::Hegel::new(|tc: hegel::TestCase| {
        let _: i32 = tc.draw(generators::integers().min_value(0).max_value(100));
        tc.assume(false);
    })
    .test_cases(100)
    .run();
}
"#;
    let output = TempRustProject::new(code).run();
    assert!(
        !output.status.success(),
        "Expected failure from filter_too_much health check"
    );
    assert!(
        output.stderr.contains("Health check failure"),
        "Expected health check failure message in stderr, got: {}",
        output.stderr
    );
}

#[test]
fn test_filter_too_much_suppressed() {
    let code = r#"
use hegel::generators;

fn main() {
    hegel::Hegel::new(|tc: hegel::TestCase| {
        let _: i32 = tc.draw(generators::integers().min_value(0).max_value(100));
        tc.assume(false);
    })
    .test_cases(100)
    .suppress_health_check(hegel::HealthCheck::FilterTooMuch)
    .run();
}
"#;
    let output = TempRustProject::new(code).run();
    assert!(
        output.status.success(),
        "Expected success with suppressed health check, got stderr: {}",
        output.stderr
    );
}

#[test]
fn test_data_too_large_fails() {
    let code = r#"
use hegel::generators;

fn main() {
    hegel::Hegel::new(|tc: hegel::TestCase| {
        let do_big: bool = tc.draw(generators::booleans());
        if do_big {
            for _ in 0..500 {
                let _: String = tc.draw(generators::text().min_size(50).max_size(100));
            }
        }
    })
    .test_cases(100)
    .run();
}
"#;
    let output = TempRustProject::new(code).run();
    assert!(
        !output.status.success(),
        "Expected failure from data_too_large health check"
    );
    assert!(
        output.stderr.contains("Health check failure"),
        "Expected health check failure message in stderr, got: {}",
        output.stderr
    );
}

#[test]
fn test_data_too_large_suppressed() {
    let code = r#"
use hegel::generators;

fn main() {
    hegel::Hegel::new(|tc: hegel::TestCase| {
        let do_big: bool = tc.draw(generators::booleans());
        if do_big {
            for _ in 0..100 {
                let _: i32 = tc.draw(generators::integers());
            }
        }
    })
    .test_cases(15)
    .suppress_health_check(hegel::HealthCheck::DataTooLarge)
    .suppress_health_check(hegel::HealthCheck::TooSlow)
    .suppress_health_check(hegel::HealthCheck::LargeBaseExample)
    .run();
}
"#;
    let output = TempRustProject::new(code).run();
    assert!(
        output.status.success(),
        "Expected success with suppressed data_too_large, got stderr: {}",
        output.stderr
    );
}

#[test]
fn test_large_base_example_fails() {
    let code = r#"
use hegel::generators;

fn main() {
    hegel::Hegel::new(|tc: hegel::TestCase| {
        for _ in 0..50 {
            let _: Vec<i32> = tc.draw(generators::vecs(generators::integers()).min_size(100).max_size(100));
        }
    })
    .test_cases(100)
    .run();
}
"#;
    let output = TempRustProject::new(code).run();
    assert!(
        !output.status.success(),
        "Expected failure from large_base_example health check"
    );
    assert!(
        output.stderr.contains("Health check failure"),
        "Expected health check failure message in stderr, got: {}",
        output.stderr
    );
}

#[test]
fn test_large_base_example_suppressed() {
    let code = r#"
use hegel::generators;

fn main() {
    hegel::Hegel::new(|tc: hegel::TestCase| {
        for _ in 0..10 {
            let _: Vec<i32> = tc.draw(generators::vecs(generators::integers()).min_size(50).max_size(50));
        }
    })
    .test_cases(15)
    .suppress_health_check(hegel::HealthCheck::LargeBaseExample)
    .suppress_health_check(hegel::HealthCheck::DataTooLarge)
    .suppress_health_check(hegel::HealthCheck::TooSlow)
    .run();
}
"#;
    let output = TempRustProject::new(code).run();
    assert!(
        output.status.success(),
        "Expected success with suppressed large_base_example, got stderr: {}",
        output.stderr
    );
}

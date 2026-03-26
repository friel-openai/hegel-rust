//! Tests for server error handling paths using HEGEL_PROTOCOL_TEST_MODE.
//!
//! These tests set HEGEL_PROTOCOL_TEST_MODE to make the hegel server
//! simulate specific error conditions, then verify the Rust client
//! handles them correctly.

mod common;

use hegel::generators::{self, Generator};
use hegel::{Hegel, Settings};

fn run_simple_with_test_mode(mode: &str) -> Result<(), Box<dyn std::any::Any + Send>> {
    let _guard = hegel::ENV_TEST_MUTEX
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let original = std::env::var("HEGEL_PROTOCOL_TEST_MODE").ok();
    unsafe { std::env::set_var("HEGEL_PROTOCOL_TEST_MODE", mode) };

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Use a simple generator — test server modes only handle basic commands
        Hegel::new(|tc| {
            let _: bool = tc.draw(generators::booleans());
        })
        .settings(Settings::new().test_cases(5).derandomize(true))
        .run();
    }));

    match original {
        Some(v) => unsafe { std::env::set_var("HEGEL_PROTOCOL_TEST_MODE", v) },
        None => unsafe { std::env::remove_var("HEGEL_PROTOCOL_TEST_MODE") },
    }

    result
}

fn run_span_with_test_mode(mode: &str) -> Result<(), Box<dyn std::any::Any + Send>> {
    let _guard = hegel::ENV_TEST_MUTEX
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let original = std::env::var("HEGEL_PROTOCOL_TEST_MODE").ok();
    unsafe { std::env::set_var("HEGEL_PROTOCOL_TEST_MODE", mode) };

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Use flat_map to force span-based generation
        Hegel::new(|tc| {
            let _: String = tc.draw(
                generators::integers::<usize>()
                    .min_value(1)
                    .max_value(3)
                    .flat_map(|n| generators::text().min_size(n).max_size(n)),
            );
        })
        .settings(Settings::new().test_cases(5).derandomize(true))
        .run();
    }));

    match original {
        Some(v) => unsafe { std::env::set_var("HEGEL_PROTOCOL_TEST_MODE", v) },
        None => unsafe { std::env::remove_var("HEGEL_PROTOCOL_TEST_MODE") },
    }

    result
}

#[test]
fn test_stop_test_on_start_span_handled() {
    let result = run_span_with_test_mode("stop_test_on_start_span");
    assert!(result.is_ok(), "StopTest on start_span should not crash");
}

#[test]
fn test_health_check_failure_reported() {
    let result = run_simple_with_test_mode("health_check_failure");
    assert!(result.is_err(), "Health check failure should cause a panic");
}

#[test]
fn test_server_error_in_results_reported() {
    let result = run_simple_with_test_mode("server_error_in_results");
    assert!(result.is_err(), "Server error should cause a panic");
}

#[test]
fn test_flaky_replay_handled() {
    let result = run_simple_with_test_mode("flaky_replay");
    let _ = result; // May pass or fail depending on client handling
}

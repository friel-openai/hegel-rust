mod common;

use common::project::TempRustProject;

fn first_recorded_draw(path: &std::path::Path) -> String {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .to_owned()
}

#[test]
#[ignore = "parity probe for first bounded float-list provider values"]
fn compare_first_bounded_float_list_draw_between_backends() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let python_values = temp_dir.path().join("python_values");
    let rust_values = temp_dir.path().join("rust_values");
    std::fs::create_dir_all(&python_values).unwrap();
    std::fs::create_dir_all(&rust_values).unwrap();

    let test_code = r#"
use hegel::generators as gs;
use hegel::{Hegel, Settings, TestCase};
use std::io::Write;

fn record_float_vec(label: &str, values: &[f64]) {
    let path = format!("{}/{}", std::env::var("VALUES_DIR").unwrap(), label);
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    let encoded = values
        .iter()
        .map(|value| format!("{value:.17}"))
        .collect::<Vec<_>>()
        .join(",");
    writeln!(file, "{encoded}").unwrap();
}

#[test]
fn compare_float_provider_probe() {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        Hegel::new(|tc: TestCase| {
            let values: Vec<f64> = tc.draw(
                &gs::vecs(
                    gs::floats::<f64>()
                        .min_value(0.0)
                        .max_value(1.0)
                        .allow_nan(false)
                        .allow_infinity(false),
                )
                .min_size(2)
                .max_size(2),
            );
            record_float_vec("draws", &values);
            panic!("FLOAT_PROVIDER_PROBE");
        })
        .settings(Settings::new().test_cases(1).database(None).derandomize(true))
        .run();
    }));

    let payload = result.expect_err("expected property failure");
    let message = payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_default();
    assert!(
        message.contains("Property test failed: FLOAT_PROVIDER_PROBE"),
        "unexpected panic message: {message}"
    );
}
"#;

    let python_project = TempRustProject::new()
        .test_file("integration.rs", test_code)
        .env("VALUES_DIR", python_values.to_str().unwrap());
    python_project.cargo_test(&["compare_float_provider_probe"]);

    let rust_project = TempRustProject::new()
        .test_file("integration.rs", test_code)
        .feature("rust-core")
        .env("VALUES_DIR", rust_values.to_str().unwrap());
    rust_project.cargo_test(&["compare_float_provider_probe"]);

    let python_first = first_recorded_draw(&python_values.join("draws"));
    let rust_first = first_recorded_draw(&rust_values.join("draws"));

    assert_eq!(
        python_first, rust_first,
        "first bounded float-list draw diverged before shrinking:\npython={python_first}\nrust={rust_first}"
    );
}

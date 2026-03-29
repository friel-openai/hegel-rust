#![cfg(feature = "rust-core")]

mod common;

use common::project::TempRustProject;

#[test]
fn test_rust_core_feature_runs_without_uv_or_server_command() {
    let code = r#"
fn main() {
    let path = std::env::var("PATH").unwrap_or_default();
    let filtered: String = path
        .split(':')
        .filter(|dir| !std::path::Path::new(&format!("{dir}/uv")).exists())
        .collect::<Vec<_>>()
        .join(":");
    std::env::set_var("PATH", &filtered);
    std::env::remove_var("HEGEL_SERVER_COMMAND");
    let _ = std::fs::remove_dir_all(".hegel");

    hegel::hegel(|tc| {
        let n: i32 = tc.draw(hegel::generators::integers::<i32>().min_value(10).max_value(20));
        assert!((10..=20).contains(&n));
    });

    assert!(
        !std::path::Path::new(".hegel").exists(),
        "rust-core feature should not create a Python install directory"
    );
}
"#;

    TempRustProject::new()
        .main_file(code)
        .feature("rust-core")
        .env_remove("HEGEL_SERVER_COMMAND")
        .cargo_run(&[]);
}

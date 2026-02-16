use std::env;
use std::path::PathBuf;

// In order:
// * Prefer `hegel` on PATH
// * Use the constant "hegel"
//
// HEGEL_BINARY_PATH is exported for use by the code.

fn main() {
    let hegel_path = ensure_hegel();
    eprintln!("using hegel: {}", hegel_path.display());
    // export HEGEL_BINARY_PATH for use by our code
    println!("cargo:rustc-env=HEGEL_BINARY_PATH={}", hegel_path.display());
}

fn ensure_hegel() -> PathBuf {
    if let Some(path) = find_on_path("hegel") {
        eprintln!("found hegel on path: {}", path.display());
        path
    } else {
        eprintln!("Could not find hegel on path. Using default value of 'hegel'");
        "hegel".into()
    }
}

fn find_on_path(name: &str) -> Option<PathBuf> {
    env::var_os("PATH").and_then(|paths| {
        env::split_paths(&paths)
            .filter_map(|dir| {
                let full_path = dir.join(name);
                if full_path.is_file() {
                    Some(full_path)
                } else {
                    None
                }
            })
            .next()
    })
}

use hegel::gen::{self, Generate};
use hegel::Hegel;
use hegel_conformance::{get_test_cases, write};
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Deserialize)]
struct Params {
    min_length: usize,
    max_length: usize,
}

#[derive(Serialize)]
struct Metrics {
    length: usize,
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: test_text '<json_params>'");
        std::process::exit(1);
    }

    let params: Params = serde_json::from_str(&args[1]).unwrap_or_else(|e| {
        eprintln!("Failed to parse params: {}", e);
        std::process::exit(1);
    });

    Hegel::new(move || {
        let value = gen::text()
            .with_min_size(params.min_length)
            .with_max_size(params.max_length)
            .generate();
        // Report length in Unicode codepoints, not bytes
        let length = value.chars().count();
        write(&Metrics { length });
    })
    .test_cases(get_test_cases())
    .run();
}

use hegel::gen::{self, Generate};
use hegel::Hegel;
use hegel_conformance::{get_test_cases, write};
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Deserialize)]
struct Params {
    min_value: f64,
    max_value: f64,
    exclude_min: bool,
    exclude_max: bool,
}

#[derive(Serialize)]
struct Metrics {
    value: f64,
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: test_floats '<json_params>'");
        std::process::exit(1);
    }

    let params: Params = serde_json::from_str(&args[1]).unwrap_or_else(|e| {
        eprintln!("Failed to parse params: {}", e);
        std::process::exit(1);
    });

    Hegel::new(move || {
        let mut gen = gen::floats::<f64>()
            .with_min(params.min_value)
            .with_max(params.max_value);

        if params.exclude_min {
            gen = gen.exclude_min();
        }
        if params.exclude_max {
            gen = gen.exclude_max();
        }

        let value = gen.generate();
        write(&Metrics { value });
    })
    .test_cases(get_test_cases())
    .run();
}

use hegel::gen::{self, Generate};
use hegel::Hegel;
use hegel_conformance::{get_test_cases, write};
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Deserialize)]
struct Params {
    min_size: usize,
    max_size: usize,
    min_value: i32,
    max_value: i32,
}

#[derive(Serialize)]
struct Metrics {
    size: usize,
    min_element: Option<i32>,
    max_element: Option<i32>,
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: test_lists '<json_params>'");
        std::process::exit(1);
    }

    let params: Params = serde_json::from_str(&args[1]).unwrap_or_else(|e| {
        eprintln!("Failed to parse params: {}", e);
        std::process::exit(1);
    });

    Hegel::new(move || {
        let list: Vec<i32> = gen::vecs(
            gen::integers::<i32>()
                .with_min(params.min_value)
                .with_max(params.max_value),
        )
        .with_min_size(params.min_size)
        .with_max_size(params.max_size)
        .generate();

        let size = list.len();
        let (min_element, max_element) = if list.is_empty() {
            (None, None)
        } else {
            (list.iter().min().copied(), list.iter().max().copied())
        };

        write(&Metrics {
            size,
            min_element,
            max_element,
        });
    })
    .test_cases(get_test_cases())
    .run();
}

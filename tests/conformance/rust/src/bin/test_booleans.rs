use hegel::generators;
use hegel::{Hegel, Settings};
use hegel_conformance::{get_test_cases, write};
use serde::Serialize;

#[derive(Serialize)]
struct Metrics {
    value: bool,
}

fn main() {
    // booleans takes no params, so we ignore argv[1]

    Hegel::new(|tc| {
        let value = tc.draw(generators::booleans());
        write(&Metrics { value });
    })
    .settings(Settings::new().test_cases(get_test_cases()))
    .run();
}

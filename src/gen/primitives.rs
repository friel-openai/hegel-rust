use super::{generate_from_schema, BasicGenerator, Generate};
use serde_json::{json, Value};

/// Generator that always returns the same value.
///
/// Uses BasicGenerator with null schema and a value-returning transform.
pub type JustGenerator<T> = BasicGenerator<(), T, Box<dyn Fn(()) -> T + Send + Sync>>;

pub fn unit() -> JustGenerator<()> {
    just(())
}

/// Create a generator that always returns the same value.
pub fn just<T: Clone + Send + Sync + 'static>(value: T) -> JustGenerator<T> {
    BasicGenerator::new(json!({"const": null}), Box::new(move |_: ()| value.clone()))
}

pub struct JustAnyGenerator<T> {
    value: T,
}

impl<T: Clone + Send + Sync> Generate<T> for JustAnyGenerator<T> {
    fn generate(&self) -> T {
        self.value.clone()
    }

    fn schema(&self) -> Option<Value> {
        None
    }
}
pub fn just_any<T: Clone + Send + Sync>(value: T) -> JustAnyGenerator<T> {
    JustAnyGenerator { value }
}

pub struct BoolGenerator;

impl Generate<bool> for BoolGenerator {
    fn generate(&self) -> bool {
        generate_from_schema(&json!({"type": "boolean"}))
    }

    fn schema(&self) -> Option<Value> {
        Some(json!({"type": "boolean"}))
    }
}

pub fn booleans() -> BoolGenerator {
    BoolGenerator
}

use super::{generate_from_schema, Generate};
use serde_json::{json, Value};

pub fn unit() -> JustGenerator<()> {
    just(())
}

pub struct JustGenerator<T> {
    value: T,
}

impl<T: Clone + Send + Sync + serde::Serialize> Generate<T> for JustGenerator<T> {
    fn generate(&self) -> T {
        self.value.clone()
    }

    fn schema(&self) -> Option<Value> {
        Some(json!({"const": self.value}))
    }
}

pub fn just<T: Clone + Send + Sync + serde::Serialize>(value: T) -> JustGenerator<T> {
    JustGenerator { value }
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

use super::{generate_from_schema, BasicGenerator, Generate};
use crate::cbor_helpers::{cbor_map, cbor_serialize};
use ciborium::Value;

pub fn unit() -> JustGenerator<()> {
    just(())
}

pub struct JustGenerator<T> {
    value: T,
    schema: Option<Value>,
}

impl<T: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned> Generate<T>
    for JustGenerator<T>
{
    fn generate(&self) -> T {
        self.value.clone()
    }

    fn as_basic(&self) -> Option<BasicGenerator<'_, T>> {
        let schema = self.schema.as_ref()?.clone();
        Some(BasicGenerator::new(schema, |raw| {
            super::deserialize_value(raw)
        }))
    }
}

pub fn just<T: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned>(
    value: T,
) -> JustGenerator<T> {
    let schema = Some(cbor_map! {"const" => cbor_serialize(&value)});
    JustGenerator { value, schema }
}

pub struct JustAnyGenerator<T> {
    value: T,
}

impl<T: Clone + Send + Sync> Generate<T> for JustAnyGenerator<T> {
    fn generate(&self) -> T {
        self.value.clone()
    }
}
pub fn just_any<T: Clone + Send + Sync>(value: T) -> JustAnyGenerator<T> {
    JustAnyGenerator { value }
}

pub struct BoolGenerator;

impl Generate<bool> for BoolGenerator {
    fn generate(&self) -> bool {
        generate_from_schema(&cbor_map! {"type" => "boolean"})
    }

    fn as_basic(&self) -> Option<BasicGenerator<'_, bool>> {
        Some(BasicGenerator::new(
            cbor_map! {"type" => "boolean"},
            super::deserialize_value,
        ))
    }
}

pub fn booleans() -> BoolGenerator {
    BoolGenerator
}

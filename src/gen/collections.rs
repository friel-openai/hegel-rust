use super::{generate_from_schema, group, integers, labels, text, Generate};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

pub struct VecGenerator<G> {
    pub(crate) elements: G,
    pub(crate) min_size: usize,
    pub(crate) max_size: Option<usize>,
    pub(crate) unique: bool,
}

impl<G> VecGenerator<G> {
    pub fn with_min_size(mut self, min: usize) -> Self {
        self.min_size = min;
        self
    }

    pub fn with_max_size(mut self, max: usize) -> Self {
        self.max_size = Some(max);
        self
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }
}

impl<T, G> Generate<Vec<T>> for VecGenerator<G>
where
    G: Generate<T>,
    T: serde::de::DeserializeOwned,
{
    fn generate(&self) -> Vec<T> {
        if let Some(schema) = self.schema() {
            // Use composed schema for single round-trip
            generate_from_schema(&schema)
        } else {
            // Compositional fallback: generate length, then elements
            group(labels::LIST, || {
                let max = self.max_size.unwrap_or(100);
                let len = integers::<usize>()
                    .with_min(self.min_size)
                    .with_max(max)
                    .generate();

                (0..len)
                    .map(|_| group(labels::LIST_ELEMENT, || self.elements.generate()))
                    .collect()
            })
        }
    }

    fn schema(&self) -> Option<Value> {
        let element_schema = self.elements.schema()?;

        let schema_type = if self.unique { "set" } else { "list" };

        let mut schema = json!({
            "type": schema_type,
            "elements": element_schema,
            "min_size": self.min_size
        });

        if let Some(max) = self.max_size {
            schema["max_size"] = json!(max);
        }

        Some(schema)
    }
}

/// Generate vectors (lists).
pub fn vecs<T, G: Generate<T>>(elements: G) -> VecGenerator<G> {
    VecGenerator {
        elements,
        min_size: 0,
        max_size: None,
        unique: false,
    }
}

pub struct HashSetGenerator<G> {
    elements: G,
    min_size: usize,
    max_size: Option<usize>,
}

impl<G> HashSetGenerator<G> {
    pub fn with_min_size(mut self, min: usize) -> Self {
        self.min_size = min;
        self
    }

    pub fn with_max_size(mut self, max: usize) -> Self {
        self.max_size = Some(max);
        self
    }
}

impl<T, G> Generate<HashSet<T>> for HashSetGenerator<G>
where
    G: Generate<T>,
    T: serde::de::DeserializeOwned + Eq + Hash,
{
    fn generate(&self) -> HashSet<T> {
        // Generate as unique vec, convert to set
        let vec_gen = VecGenerator {
            elements: &self.elements,
            min_size: self.min_size,
            max_size: self.max_size,
            unique: true,
        };

        if let Some(schema) = vec_gen.schema() {
            let vec: Vec<T> = generate_from_schema(&schema);
            vec.into_iter().collect()
        } else {
            // Compositional fallback
            group(labels::SET, || {
                let max = self.max_size.unwrap_or(100);
                let target_len = integers::<usize>()
                    .with_min(self.min_size)
                    .with_max(max)
                    .generate();

                let mut set = HashSet::new();
                let mut attempts = 0;
                while set.len() < target_len && attempts < target_len * 10 {
                    set.insert(group(labels::SET_ELEMENT, || self.elements.generate()));
                    attempts += 1;
                }
                set
            })
        }
    }

    fn schema(&self) -> Option<Value> {
        let element_schema = self.elements.schema()?;

        let mut schema = json!({
            "type": "set",
            "elements": element_schema,
            "min_size": self.min_size
        });

        if let Some(max) = self.max_size {
            schema["max_size"] = json!(max);
        }

        Some(schema)
    }
}

pub fn hashsets<T, G: Generate<T>>(elements: G) -> HashSetGenerator<G> {
    HashSetGenerator {
        elements,
        min_size: 0,
        max_size: None,
    }
}

pub struct HashMapGenerator<V> {
    values: V,
    min_size: usize,
    max_size: Option<usize>,
}

impl<V> HashMapGenerator<V> {
    pub fn with_min_size(mut self, min: usize) -> Self {
        self.min_size = min;
        self
    }

    pub fn with_max_size(mut self, max: usize) -> Self {
        self.max_size = Some(max);
        self
    }
}

impl<T, V> Generate<HashMap<String, T>> for HashMapGenerator<V>
where
    V: Generate<T>,
    T: serde::de::DeserializeOwned,
{
    fn generate(&self) -> HashMap<String, T> {
        if let Some(schema) = self.schema() {
            generate_from_schema(&schema)
        } else {
            // Compositional fallback
            group(labels::MAP, || {
                let max = self.max_size.unwrap_or(100);
                let len = integers::<usize>()
                    .with_min(self.min_size)
                    .with_max(max)
                    .generate();

                let key_gen = text().with_min_size(1).with_max_size(20);

                let mut map = HashMap::new();
                let max_attempts = len * 10;
                let mut attempts = 0;
                while map.len() < len && attempts < max_attempts {
                    group(labels::MAP_ENTRY, || {
                        let key = key_gen.generate();
                        if !map.contains_key(&key) {
                            let value = self.values.generate();
                            map.insert(key, value);
                        }
                    });
                    attempts += 1;
                }
                crate::assume(map.len() >= self.min_size);
                map
            })
        }
    }

    fn schema(&self) -> Option<Value> {
        let value_schema = self.values.schema()?;

        let mut schema = json!({
            "type": "dict",
            "values": value_schema,
            "min_size": self.min_size
        });

        if let Some(max) = self.max_size {
            schema["max_size"] = json!(max);
        }

        Some(schema)
    }
}

/// Generate hash maps with string keys.
///
/// Keys are always strings due to JSON limitations.
pub fn hashmaps<T, V: Generate<T>>(values: V) -> HashMapGenerator<V> {
    HashMapGenerator {
        values,
        min_size: 0,
        max_size: None,
    }
}

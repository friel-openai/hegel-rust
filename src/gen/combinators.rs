use super::{
    discardable_group, generate_from_schema, group, integers, labels, request_from_schema,
    BasicGenerator, Generate,
};
use serde_json::{json, Value};
use std::marker::PhantomData;
use std::sync::Arc;

pub struct Mapped<T, U, F, G> {
    pub(crate) source: G,
    pub(crate) f: F,
    pub(crate) _phantom: PhantomData<(T, U)>,
}

impl<T, U, F, G> Generate<U> for Mapped<T, U, F, G>
where
    G: Generate<T>,
    F: Fn(T) -> U + Send + Sync,
{
    fn generate(&self) -> U {
        (self.f)(self.source.generate())
    }

    fn schema(&self) -> Option<Value> {
        None
    }
}

// Safety: Mapped is Send+Sync if its components are
unsafe impl<T, U, F, G> Send for Mapped<T, U, F, G>
where
    G: Send,
    F: Send,
{
}

unsafe impl<T, U, F, G> Sync for Mapped<T, U, F, G>
where
    G: Sync,
    F: Sync,
{
}

pub struct FlatMapped<T, U, G2, F, G1> {
    pub(crate) source: G1,
    pub(crate) f: F,
    pub(crate) _phantom: PhantomData<(T, U, G2)>,
}

impl<T, U, G2, F, G1> Generate<U> for FlatMapped<T, U, G2, F, G1>
where
    G1: Generate<T>,
    G2: Generate<U>,
    F: Fn(T) -> G2 + Send + Sync,
{
    fn generate(&self) -> U {
        group(labels::FLAT_MAP, || {
            let intermediate = self.source.generate();
            let next_gen = (self.f)(intermediate);
            next_gen.generate()
        })
    }

    fn schema(&self) -> Option<Value> {
        None
    }
}

unsafe impl<T, U, G2, F, G1> Send for FlatMapped<T, U, G2, F, G1>
where
    G1: Send,
    F: Send,
{
}

unsafe impl<T, U, G2, F, G1> Sync for FlatMapped<T, U, G2, F, G1>
where
    G1: Sync,
    F: Sync,
{
}

pub struct Filtered<T, F, G> {
    pub(crate) source: G,
    pub(crate) predicate: F,
    pub(crate) _phantom: PhantomData<T>,
}

impl<T, F, G> Generate<T> for Filtered<T, F, G>
where
    G: Generate<T>,
    F: Fn(&T) -> bool + Send + Sync,
{
    fn generate(&self) -> T {
        for _ in 0..3 {
            if let Some(value) = discardable_group(labels::FILTER, || {
                let value = self.source.generate();
                if (self.predicate)(&value) {
                    Some(value)
                } else {
                    None
                }
            }) {
                return value;
            }
        }
        crate::assume(false);
        unreachable!()
    }

    fn schema(&self) -> Option<Value> {
        None
    }
}

unsafe impl<T, F, G> Send for Filtered<T, F, G>
where
    G: Send,
    F: Send,
{
}

unsafe impl<T, F, G> Sync for Filtered<T, F, G>
where
    G: Sync,
    F: Sync,
{
}

// ============================================================================
// BoxedGenerator - Type-erased generator
// ============================================================================

/// A type-erased generator with a lifetime parameter.
///
/// This is useful for storing generators of different concrete types
/// in collections or struct fields.
///
/// Create a `BoxedGenerator` by calling `.boxed()` on any generator.
///
/// The lifetime `'a` represents the minimum lifetime of any borrowed data
/// in the generator. Use `'static` for generators that own all their data.
pub struct BoxedGenerator<'a, T> {
    pub(crate) inner: Arc<dyn Generate<T> + Send + Sync + 'a>,
}

impl<'a, T> Clone for BoxedGenerator<'a, T> {
    fn clone(&self) -> Self {
        BoxedGenerator {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<'a, T> Generate<T> for BoxedGenerator<'a, T> {
    fn generate(&self) -> T {
        self.inner.generate()
    }

    fn schema(&self) -> Option<Value> {
        self.inner.schema()
    }

    /// Returns self without re-wrapping.
    fn boxed<'b>(self) -> BoxedGenerator<'b, T>
    where
        Self: Sized + Send + Sync + 'b,
    {
        BoxedGenerator { inner: self.inner }
    }
}

/// Generator type for sampled_from - uses index-based BasicGenerator.
pub type SampledFromGenerator<T> =
    BasicGenerator<usize, T, Box<dyn Fn(usize) -> T + Send + Sync>>;

/// Sample uniformly from a list of values.
///
/// Uses index-based generation with BasicGenerator, which preserves
/// object identity and works with any type.
pub fn sampled_from<T: Clone + Send + Sync + 'static>(elements: Vec<T>) -> SampledFromGenerator<T> {
    assert!(!elements.is_empty(), "sampled_from requires at least one element");
    let max_idx = elements.len() - 1;
    let schema = json!({
        "type": "integer",
        "minimum": 0,
        "maximum": max_idx
    });
    BasicGenerator::new(schema, Box::new(move |idx: usize| elements[idx].clone()))
}

pub struct SampledFromSliceGenerator<'a, T> {
    elements: &'a [T],
}

impl<'a, T: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned> Generate<T>
    for SampledFromSliceGenerator<'a, T>
{
    fn generate(&self) -> T {
        crate::assume(!self.elements.is_empty());

        if let Some(schema) = self.schema() {
            generate_from_schema(&schema)
        } else {
            // Compositional fallback
            group(labels::SAMPLED_FROM, || {
                let idx = integers::<usize>()
                    .with_min(0)
                    .with_max(self.elements.len() - 1)
                    .generate();
                self.elements[idx].clone()
            })
        }
    }

    fn schema(&self) -> Option<Value> {
        Some(json!({
            "sampled_from": self.elements
        }))
    }
}

// Safety: SampledFromSliceGenerator is Send+Sync if T is Send+Sync
unsafe impl<'a, T: Send + Sync> Send for SampledFromSliceGenerator<'a, T> {}
unsafe impl<'a, T: Send + Sync> Sync for SampledFromSliceGenerator<'a, T> {}

/// Sample uniformly from a borrowed slice.
///
/// This allows creating generators that borrow from local data,
/// enabling non-`'static` lifetimes.
///
/// # Example
///
/// ```no_run
/// use hegel::gen::{self, Generate, BoxedGenerator};
///
/// let choices = vec!["apple".to_string(), "banana".to_string(), "cherry".to_string()];
/// let gen: BoxedGenerator<'_, String> = gen::sampled_from_slice(&choices).boxed();
/// let value = gen.generate();
/// ```
pub fn sampled_from_slice<
    T: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned,
>(
    elements: &[T],
) -> SampledFromSliceGenerator<'_, T> {
    SampledFromSliceGenerator { elements }
}

pub struct OneOfGenerator<'a, T> {
    generators: Vec<BoxedGenerator<'a, T>>,
}

impl<'a, T: serde::de::DeserializeOwned> Generate<T> for OneOfGenerator<'a, T> {
    fn generate(&self) -> T {
        crate::assume(!self.generators.is_empty());

        if let Some(schema) = self.schema() {
            generate_from_schema(&schema)
        } else {
            // Generate index and delegate
            group(labels::ONE_OF, || {
                let idx = integers::<usize>()
                    .with_min(0)
                    .with_max(self.generators.len() - 1)
                    .generate();
                self.generators[idx].generate()
            })
        }
    }

    fn schema(&self) -> Option<Value> {
        let schemas: Option<Vec<Value>> = self.generators.iter().map(|g| g.schema()).collect();

        schemas.map(|s| json!({"one_of": s}))
    }
}

/// Choose from multiple generators of the same type.
///
/// For a more convenient syntax, use the `one_of!` macro instead.
pub fn one_of<'a, T>(generators: Vec<BoxedGenerator<'a, T>>) -> OneOfGenerator<'a, T> {
    OneOfGenerator { generators }
}

/// Choose from multiple generators of the same type.
///
/// This macro automatically boxes each generator, providing a more ergonomic
/// syntax than calling [`one_of`] directly.
///
/// # Example
///
/// ```no_run
/// use hegel::gen::{self, Generate};
///
/// let gen = hegel::one_of!(
///     gen::integers::<i32>().with_min(0).with_max(10),
///     gen::integers::<i32>().with_min(100).with_max(110),
/// );
/// let value = gen.generate();
/// ```
#[macro_export]
macro_rules! one_of {
    ($($gen:expr),+ $(,)?) => {
        $crate::gen::one_of(vec![
            $($crate::gen::Generate::boxed($gen)),+
        ])
    };
}

pub struct OptionalGenerator<G> {
    inner: G,
}

impl<T, G> Generate<Option<T>> for OptionalGenerator<G>
where
    G: Generate<T>,
    T: serde::de::DeserializeOwned,
{
    fn generate(&self) -> Option<T> {
        if let Some(inner_schema) = self.inner.schema() {
            let schema = json!({
                "one_of": [
                    {"type": "null"},
                    inner_schema
                ]
            });
            generate_from_schema(&schema)
        } else {
            // Compositional fallback
            group(labels::OPTIONAL, || {
                let is_some: bool =
                    serde_json::from_value(request_from_schema(&json!({"type": "boolean"})))
                        .unwrap_or(false);
                if is_some {
                    Some(self.inner.generate())
                } else {
                    None
                }
            })
        }
    }

    fn schema(&self) -> Option<Value> {
        let inner_schema = self.inner.schema()?;
        Some(json!({
            "one_of": [
                {"type": "null"},
                inner_schema
            ]
        }))
    }
}

pub fn optional<T, G: Generate<T>>(inner: G) -> OptionalGenerator<G> {
    OptionalGenerator { inner }
}

//! Declarative macro for deriving generators for external types.

/// Derive a generator for a struct type defined externally.
///
/// This macro creates a generator struct with builder methods for each field,
/// allowing you to customize how each field is generated.
///
/// # Example
///
/// ```ignore
/// // In your production crate (no hegel dependency needed):
/// pub struct Person {
///     pub name: String,
///     pub age: u32,
/// }
///
/// // In your test crate:
/// use hegel::derive_generator;
/// use production_crate::Person;
///
/// derive_generator!(Person {
///     name: String,
///     age: u32,
/// });
///
/// // Now you can use PersonGenerator:
/// use hegel::gen::Generate;
///
/// let gen = PersonGenerator::new()
///     .with_name(hegel::gen::from_regex("[A-Z][a-z]+"))
///     .with_age(hegel::gen::integers::<u32>().with_min(0).with_max(120));
///
/// let person: Person = gen.generate();
/// ```
#[macro_export]
macro_rules! derive_generator {
    ($struct_name:ident { $($field_name:ident : $field_type:ty),* $(,)? }) => {
        $crate::paste::paste! {
            /// Generated generator for the struct.
            pub struct [<$struct_name Generator>]<'a> {
                $(
                    $field_name: $crate::gen::BoxedGenerator<'a, $field_type>,
                )*
            }

            impl<'a> [<$struct_name Generator>]<'a> {
                /// Create a new generator with default generators for all fields.
                pub fn new() -> Self
                where
                    $($field_type: $crate::gen::DefaultGenerator,)*
                    $(<$field_type as $crate::gen::DefaultGenerator>::Generator: Send + Sync + 'a,)*
                {
                    use $crate::gen::{DefaultGenerator, Generate};
                    Self {
                        $($field_name: <$field_type as DefaultGenerator>::default_generator().boxed(),)*
                    }
                }

                $(
                    /// Set a custom generator for this field.
                    pub fn [<with_ $field_name>]<G>(mut self, gen: G) -> Self
                    where
                        G: $crate::gen::Generate<$field_type> + Send + Sync + 'a,
                    {
                        use $crate::gen::Generate;
                        self.$field_name = gen.boxed();
                        self
                    }
                )*
            }

            impl<'a> Default for [<$struct_name Generator>]<'a>
            where
                $($field_type: $crate::gen::DefaultGenerator,)*
                $(<$field_type as $crate::gen::DefaultGenerator>::Generator: Send + Sync + 'a,)*
            {
                fn default() -> Self {
                    Self::new()
                }
            }

            impl<'a> $crate::gen::Generate<$struct_name> for [<$struct_name Generator>]<'a> {
                fn generate(&self) -> $struct_name {
                    use $crate::gen::Generate;
                    $struct_name {
                        $($field_name: self.$field_name.generate(),)*
                    }
                }
            }
        }
    };
}

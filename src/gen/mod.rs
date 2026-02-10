mod binary;
mod collections;
mod combinators;
mod default;
mod fixed_dict;
mod formats;
mod macros;
mod numeric;
mod primitives;
#[cfg(feature = "rand")]
mod random;
mod strings;
mod tuples;
mod value;

// public api
pub use binary::binary;
pub use collections::{hashmaps, hashsets, vecs, HashMapGenerator};
pub use combinators::{one_of, optional, sampled_from, sampled_from_slice, BoxedGenerator};
pub use default::DefaultGenerator;
pub use fixed_dict::fixed_dicts;
pub use formats::{dates, datetimes, domains, emails, ip_addresses, times, urls};
pub use numeric::{floats, integers};
pub use primitives::{booleans, just, just_any, unit};
#[cfg(feature = "rand")]
#[cfg_attr(docsrs, doc(cfg(feature = "rand")))]
pub use random::{randoms, HegelRandom, RandomsGenerator};
pub use strings::{from_regex, text};
pub use tuples::{tuples, tuples3};

pub(crate) use collections::VecGenerator;
pub(crate) use combinators::{Filtered, FlatMapped, Mapped, OptionalGenerator};
pub(crate) use numeric::{FloatGenerator, IntegerGenerator};
pub(crate) use primitives::BoolGenerator;
pub(crate) use strings::TextGenerator;

use serde_json::{json, Value};

pub(crate) mod exit_codes {
    #[allow(dead_code)] // Reserved for future use
    pub const TEST_FAILURE: i32 = 1;
    pub const SOCKET_ERROR: i32 = 134;
}
use std::cell::{Cell, RefCell};
use std::io::{BufRead, BufReader, Write};
use std::marker::PhantomData;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ============================================================================
// State Management (Thread-Local)
// ============================================================================

thread_local! {
    /// Whether this is the last run (for note() output)
    static IS_LAST_RUN: Cell<bool> = const { Cell::new(false) };
    /// Buffer for generated values during final replay
    static GENERATED_VALUES: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

/// Check if this is the last run.
pub(crate) fn is_last_run() -> bool {
    IS_LAST_RUN.with(|r| r.get())
}

/// Set the is_last_run flag (used by embedded module).
pub(crate) fn set_is_last_run(is_last: bool) {
    IS_LAST_RUN.with(|r| r.set(is_last));
}

/// Buffer a generated value for later output
fn buffer_generated_value(value: &str) {
    GENERATED_VALUES.with(|v| v.borrow_mut().push(value.to_string()));
}

/// Take all buffered generated values, clearing the buffer.
pub(crate) fn take_generated_values() -> Vec<String> {
    GENERATED_VALUES.with(|v| std::mem::take(&mut *v.borrow_mut()))
}

/// Print a note message.
///
/// Only prints on the last run (final replay for counterexample output).
pub fn note(message: &str) {
    if is_last_run() {
        eprintln!("{}", message);
    }
}

// ============================================================================
// Socket Communication with Thread-Local Connection
// ============================================================================

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Thread-local connection state.
/// Connection exists if and only if span_depth > 0.
pub(crate) struct ConnectionState {
    writer: UnixStream,
    reader: BufReader<UnixStream>,
    span_depth: usize,
}

thread_local! {
    static CONNECTION: RefCell<Option<ConnectionState>> = const { RefCell::new(None) };
}

fn is_debug() -> bool {
    std::env::var("HEGEL_DEBUG").is_ok()
}

/// Set the connection from an already-connected stream (used by embedded module).
/// This is used when the SDK creates a server and accepts connections from hegel.
pub(crate) fn set_embedded_connection(stream: UnixStream) {
    CONNECTION.with(|conn| {
        let mut conn = conn.borrow_mut();
        assert!(
            conn.is_none(),
            "set_embedded_connection called while already connected"
        );

        let writer = stream.try_clone().unwrap_or_else(|e| {
            panic!("Failed to clone socket: {}", e);
        });
        let reader = BufReader::new(stream);

        *conn = Some(ConnectionState {
            writer,
            reader,
            span_depth: 0,
        });
    });
}

/// Clear the embedded connection (used by embedded module).
pub(crate) fn clear_embedded_connection() {
    CONNECTION.with(|conn| {
        *conn.borrow_mut() = None;
    });
}

pub(crate) fn increment_span_depth() {
    CONNECTION.with(|conn| {
        let mut conn = conn.borrow_mut();
        let state = conn
            .as_mut()
            .expect("start_span called with no active connection");
        state.span_depth += 1;
    });
}

pub(crate) fn decrement_span_depth() {
    CONNECTION.with(|conn| {
        let mut conn = conn.borrow_mut();
        let state = conn
            .as_mut()
            .expect("stop_span called with no active connection");
        assert!(state.span_depth > 0, "stop_span called with no open spans");
        state.span_depth -= 1;
    });
}

/// Send a request and receive a response over the thread-local connection.
pub(crate) fn send_request(command: &str, payload: &Value) -> Value {
    let debug = is_debug();
    let request_id = REQUEST_COUNTER.fetch_add(1, Ordering::SeqCst) + 1;
    let request = json!({
        "id": request_id,
        "command": command,
        "payload": payload
    });
    let message = format!("{}\n", request);

    if debug {
        eprint!("REQUEST: {}", message);
    }

    CONNECTION.with(|conn| {
        let mut conn = conn.borrow_mut();
        let state = conn
            .as_mut()
            .expect("send_request called without active connection");

        if let Err(e) = state.writer.write_all(message.as_bytes()) {
            eprintln!("Failed to write to Hegel socket: {}", e);
            std::process::exit(exit_codes::SOCKET_ERROR);
        }

        let mut response = String::new();
        if let Err(e) = state.reader.read_line(&mut response) {
            eprintln!("Failed to read from Hegel socket: {}", e);
            std::process::exit(exit_codes::SOCKET_ERROR);
        }

        if debug {
            eprint!("RESPONSE: {}", response);
        }

        let parsed: Value = match serde_json::from_str(&response) {
            Ok(v) => v,
            Err(e) => {
                panic!(
                    "hegel: failed to parse server response as JSON: {}\nResponse: {}",
                    e, response
                );
            }
        };

        // Verify request ID matches
        let response_id = parsed.get("id").and_then(|v| v.as_u64());
        crate::assume(response_id == Some(request_id));
        crate::assume(parsed.get("error").is_none());

        parsed.get("result").cloned().unwrap_or(Value::Null)
    })
}

pub(crate) fn request_from_schema(schema: &Value) -> Value {
    send_request("generate", schema)
}

/// Generate a value from a schema.
pub fn generate_from_schema<T: serde::de::DeserializeOwned>(schema: &Value) -> T {
    let result = request_from_schema(schema);

    if is_last_run() {
        buffer_generated_value(&format!("Generated: {}", result));
    }

    // Convert to HegelValue to handle NaN/Infinity sentinel strings
    let hegel_value = value::HegelValue::from(result.clone());
    value::from_hegel_value(hegel_value).unwrap_or_else(|e| {
        panic!(
            "hegel: failed to deserialize server response: {}\nValue: {}",
            e, result
        );
    })
}

/// Start a span for grouping related generation.
///
/// Spans help Hypothesis understand the structure of generated data,
/// which improves shrinking. Call `stop_span()` when done.
pub fn start_span(label: u64) {
    increment_span_depth();
    send_request("start_span", &json!({"label": label}));
}

/// Stop the current span.
///
/// If `discard` is true, tells Hypothesis this span's data should be discarded
/// (e.g., because a filter rejected it).
pub fn stop_span(discard: bool) {
    decrement_span_depth();
    send_request("stop_span", &json!({"discard": discard}));
}

// ============================================================================
// Grouped Generation Helpers
// ============================================================================

/// Run a function within a labeled group.
///
/// Groups related generation calls together, which helps the testing engine
/// understand the structure of generated data and improve shrinking.
///
/// # Example
///
/// ```ignore
/// group(labels::LIST, || {
///     // generate list elements here
/// })
/// ```
pub fn group<T, F: FnOnce() -> T>(label: u64, f: F) -> T {
    start_span(label);
    let result = f();
    stop_span(false);
    result
}

/// Run a function within a labeled group, discarding if the function returns None.
///
/// Useful for filter-like operations where rejected values should be discarded.
pub fn discardable_group<T, F: FnOnce() -> Option<T>>(label: u64, f: F) -> Option<T> {
    start_span(label);
    let result = f();
    stop_span(result.is_none());
    result
}

/// Label constants for spans.
/// These help Hypothesis understand the structure of generated data.
pub mod labels {
    pub const LIST: u64 = 1;
    pub const LIST_ELEMENT: u64 = 2;
    pub const SET: u64 = 3;
    pub const SET_ELEMENT: u64 = 4;
    pub const MAP: u64 = 5;
    pub const MAP_ENTRY: u64 = 6;
    pub const TUPLE: u64 = 7;
    pub const ONE_OF: u64 = 8;
    pub const OPTIONAL: u64 = 9;
    pub const FIXED_DICT: u64 = 10;
    pub const FLAT_MAP: u64 = 11;
    pub const FILTER: u64 = 12;
    pub const ENUM_VARIANT: u64 = 13;
    pub const SAMPLED_FROM: u64 = 14;
}

// ============================================================================
// BasicGenerator - Schema + Transform
// ============================================================================

/// A basic generator with a schema and client-side transform.
///
/// Unlike the `Mapped` type where map() loses the schema,
/// BasicGenerator preserves the schema through transformations
/// by composing transform functions.
///
/// The transform defaults to identity, making this a drop-in
/// replacement for schema-backed generators.
pub struct BasicGenerator<Raw, T, F>
where
    Raw: serde::de::DeserializeOwned + Send + Sync,
    F: Fn(Raw) -> T + Send + Sync,
    T: Send + Sync,
{
    schema: Value,
    transform_fn: F,
    _phantom: PhantomData<(Raw, T)>,
}

impl<Raw, T, F> BasicGenerator<Raw, T, F>
where
    Raw: serde::de::DeserializeOwned + Send + Sync,
    F: Fn(Raw) -> T + Send + Sync,
    T: Send + Sync,
{
    /// Create a new BasicGenerator with a schema and transform function.
    pub fn new(schema: Value, transform_fn: F) -> Self {
        Self {
            schema,
            transform_fn,
            _phantom: PhantomData,
        }
    }

    /// Get the raw schema.
    pub fn raw_schema(&self) -> &Value {
        &self.schema
    }

    /// Transform generated values while preserving the schema.
    ///
    /// This shadows the trait's `map` method to preserve schema information.
    /// The resulting generator has the same schema but applies the additional
    /// transform after deserialization.
    pub fn map<U, G>(self, f: G) -> BasicGenerator<Raw, U, impl Fn(Raw) -> U + Send + Sync>
    where
        G: Fn(T) -> U + Send + Sync,
        U: Send + Sync,
    {
        let transform = self.transform_fn;
        BasicGenerator {
            schema: self.schema,
            transform_fn: move |raw: Raw| f(transform(raw)),
            _phantom: PhantomData,
        }
    }
}

impl<Raw, T, F> Generate<T> for BasicGenerator<Raw, T, F>
where
    Raw: serde::de::DeserializeOwned + Send + Sync,
    F: Fn(Raw) -> T + Send + Sync,
    T: Send + Sync,
{
    fn generate(&self) -> T {
        let raw: Raw = generate_from_schema(&self.schema);
        (self.transform_fn)(raw)
    }

    fn schema(&self) -> Option<Value> {
        Some(self.schema.clone())
    }
}

// ============================================================================
// Generate Trait
// ============================================================================

/// The core trait for all generators.
///
/// Generators produce values of type `T`. Schema support is provided
/// by the `BasicGenerator` type - generators that contain a BasicGenerator
/// can use schema-based generation for single-request composition.
pub trait Generate<T>: Send + Sync {
    /// Generate a value.
    fn generate(&self) -> T;

    /// Get the JSON Schema for this generator, if available.
    ///
    /// Returns `None` by default. Generators backed by a BasicGenerator
    /// override this to return the schema.
    ///
    /// Schemas enable composition optimizations where a single request to Hegel
    /// can generate complex nested structures.
    fn schema(&self) -> Option<Value> {
        None
    }

    /// Transform generated values using a function.
    ///
    /// The resulting generator has no schema since the transformation
    /// may invalidate the schema's semantics (unless the source is a
    /// BasicGenerator, which preserves schema through map).
    fn map<U, F>(self, f: F) -> Mapped<T, U, F, Self>
    where
        Self: Sized,
        F: Fn(T) -> U + Send + Sync,
    {
        Mapped {
            source: self,
            f,
            _phantom: PhantomData,
        }
    }

    /// Generate a value, then use it to create another generator.
    ///
    /// This is useful for dependent generation where the second value
    /// depends on the first.
    fn flat_map<U, G, F>(self, f: F) -> FlatMapped<T, U, G, F, Self>
    where
        Self: Sized,
        G: Generate<U>,
        F: Fn(T) -> G + Send + Sync,
    {
        FlatMapped {
            source: self,
            f,
            _phantom: PhantomData,
        }
    }

    /// Filter generated values using a predicate.
    fn filter<F>(self, predicate: F) -> Filtered<T, F, Self>
    where
        Self: Sized,
        F: Fn(&T) -> bool + Send + Sync,
    {
        Filtered {
            source: self,
            predicate,
            _phantom: PhantomData,
        }
    }

    /// Convert this generator into a type-erased boxed generator.
    ///
    /// This is useful when you need to store generators of different concrete
    /// types in a collection or struct field.
    ///
    /// The lifetime parameter is inferred from the generator being boxed.
    /// For generators that own all their data, this will be `'static`.
    /// For generators that borrow data, the lifetime will match the borrow.
    fn boxed<'a>(self) -> BoxedGenerator<'a, T>
    where
        Self: Sized + Send + Sync + 'a,
    {
        BoxedGenerator {
            inner: Arc::new(self),
        }
    }
}

// Implement Generate for references to generators
impl<T, G: Generate<T>> Generate<T> for &G {
    fn generate(&self) -> T {
        (*self).generate()
    }

    fn schema(&self) -> Option<Value> {
        (*self).schema()
    }
}

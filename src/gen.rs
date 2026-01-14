//! Generator types and combinators for property-based testing.
//!
//! This module provides a composable API for generating test data.
//! Generators can be combined using methods like `map`, `flat_map`, and `filter`,
//! and composed into complex data structures.

use crate::HegelMode;
use num::{Bounded, Float as NumFloat, Integer as NumInteger};
use serde_json::{json, Value};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::io::{BufRead, BufReader, Write};
use std::marker::PhantomData;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ============================================================================
// Mode and State Management (Thread-Local)
// ============================================================================

thread_local! {
    /// Current execution mode
    static MODE: Cell<HegelMode> = const { Cell::new(HegelMode::Standalone) };
    /// Whether this is the last run (for note() output in embedded mode)
    static IS_LAST_RUN: Cell<bool> = const { Cell::new(false) };
}

/// Get the current execution mode.
pub fn current_mode() -> HegelMode {
    MODE.with(|m| m.get())
}

/// Check if this is the last run.
pub fn is_last_run() -> bool {
    IS_LAST_RUN.with(|r| r.get())
}

/// Set the current execution mode (used by embedded module).
pub(crate) fn set_mode(mode: HegelMode) {
    MODE.with(|m| m.set(mode));
}

/// Set the is_last_run flag (used by embedded module).
pub(crate) fn set_is_last_run(is_last: bool) {
    IS_LAST_RUN.with(|r| r.set(is_last));
}

/// Print a note message.
///
/// In standalone mode, this always prints to stderr.
/// In embedded mode, this only prints on the last run.
pub fn note(message: &str) {
    match current_mode() {
        HegelMode::Standalone => eprintln!("{}", message),
        HegelMode::Embedded => {
            if is_last_run() {
                eprintln!("{}", message);
            }
        }
    }
}

// ============================================================================
// Socket Communication with Thread-Local Connection
// ============================================================================

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Thread-local connection state.
/// Connection exists if and only if span_depth > 0.
struct ConnectionState {
    writer: UnixStream,
    reader: BufReader<UnixStream>,
    span_depth: usize,
}

thread_local! {
    static CONNECTION: RefCell<Option<ConnectionState>> = const { RefCell::new(None) };
}

fn is_connected() -> bool {
    CONNECTION.with(|conn| conn.borrow().is_some())
}

fn get_span_depth() -> usize {
    CONNECTION.with(|conn| conn.borrow().as_ref().map(|s| s.span_depth).unwrap_or(0))
}

fn is_debug() -> bool {
    std::env::var("HEGEL_DEBUG").is_ok()
}

fn get_socket_path() -> String {
    std::env::var("HEGEL_SOCKET").expect("HEGEL_SOCKET environment variable not set")
}

/// Open a connection. Panics if already connected.
fn open_connection() {
    CONNECTION.with(|conn| {
        let mut conn = conn.borrow_mut();
        assert!(
            conn.is_none(),
            "open_connection called while already connected"
        );

        let socket_path = get_socket_path();
        let stream = match UnixStream::connect(&socket_path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!(
                    "Failed to connect to Hegel socket at {}: {}",
                    socket_path, e
                );
                std::process::exit(crate::exit_codes::SOCKET_ERROR);
            }
        };

        let writer = stream.try_clone().unwrap_or_else(|e| {
            eprintln!("Failed to clone socket: {}", e);
            std::process::exit(crate::exit_codes::SOCKET_ERROR);
        });
        let reader = BufReader::new(stream);

        *conn = Some(ConnectionState {
            writer,
            reader,
            span_depth: 0,
        });
    });
}

/// Close the connection. Panics if not connected or if spans are still open.
fn close_connection() {
    CONNECTION.with(|conn| {
        let mut conn = conn.borrow_mut();
        let state = conn
            .as_ref()
            .expect("close_connection called while not connected");
        assert_eq!(
            state.span_depth, 0,
            "close_connection called with {} unclosed span(s)",
            state.span_depth
        );
        *conn = None;
    });
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

fn increment_span_depth() {
    CONNECTION.with(|conn| {
        let mut conn = conn.borrow_mut();
        let state = conn
            .as_mut()
            .expect("start_span called with no active connection");
        state.span_depth += 1;
    });
}

fn decrement_span_depth() {
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
fn send_request(command: &str, payload: &Value) -> Value {
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
            std::process::exit(crate::exit_codes::SOCKET_ERROR);
        }

        let mut response = String::new();
        if let Err(e) = state.reader.read_line(&mut response) {
            eprintln!("Failed to read from Hegel socket: {}", e);
            std::process::exit(crate::exit_codes::SOCKET_ERROR);
        }

        if debug {
            eprint!("RESPONSE: {}", response);
        }

        let parsed: Value = match serde_json::from_str(&response) {
            Ok(v) => v,
            Err(e) => {
                crate::reject(&format!("Failed to parse Hegel response: {}", e));
            }
        };

        // Verify request ID matches
        let response_id = parsed.get("id").and_then(|v| v.as_u64());
        if response_id != Some(request_id) {
            crate::reject(&format!(
                "Response ID mismatch: expected {}, got {:?}",
                request_id, response_id
            ));
        }

        if let Some(error) = parsed.get("error") {
            crate::reject(&format!("Hegel error: {}", error));
        }

        parsed.get("result").cloned().unwrap_or(Value::Null)
    })
}

fn request_from_schema(schema: &Value) -> Value {
    send_request("generate", schema)
}

/// Generate a value from a schema.
/// If inside a span, uses the existing connection.
/// If not inside a span, opens a connection for this single request (standalone mode only).
pub fn generate_from_schema<T: serde::de::DeserializeOwned>(schema: &Value) -> T {
    // In embedded mode, connection is already set - don't try to open/close
    let need_connection = !is_connected() && current_mode() == HegelMode::Standalone;
    if need_connection {
        open_connection();
    }

    let result = request_from_schema(schema);

    if need_connection {
        close_connection();
    }

    // Auto-log generated value during final replay (counterexample)
    if is_last_run() {
        eprintln!("Generated: {}", result);
    }

    serde_json::from_value(result.clone()).unwrap_or_else(|e| {
        crate::reject(&format!("Failed to deserialize value {:?}: {}", result, e))
    })
}

/// Start a span for grouping related generation.
///
/// Opens a connection if this is the first span (standalone mode only).
/// Spans help Hypothesis understand the structure of generated data,
/// which improves shrinking. Call `stop_span()` when done.
pub fn start_span(label: u64) {
    // In embedded mode, connection is already set - don't try to open
    if !is_connected() && current_mode() == HegelMode::Standalone {
        open_connection();
    }
    increment_span_depth();
    send_request("start_span", &json!({"label": label}));
}

/// Stop the current span.
///
/// Closes the connection if this is the last span (in standalone mode only).
/// If `discard` is true, tells Hypothesis this span's data should be discarded
/// (e.g., because a filter rejected it).
pub fn stop_span(discard: bool) {
    decrement_span_depth();
    send_request("stop_span", &json!({"discard": discard}));
    // Only close connection in standalone mode - in embedded mode, the
    // connection is managed by the embedded module
    if get_span_depth() == 0 && current_mode() == HegelMode::Standalone {
        close_connection();
    }
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
// Generate Trait
// ============================================================================

/// The core trait for all generators.
///
/// Generators produce values of type `T` and optionally carry a JSON Schema
/// that describes the values they generate.
pub trait Generate<T>: Send + Sync {
    /// Generate a value.
    fn generate(&self) -> T;

    /// Get the JSON Schema for this generator, if available.
    ///
    /// Schemas enable composition optimizations where a single request to Hegel
    /// can generate complex nested structures.
    fn schema(&self) -> Option<Value>;

    /// Transform generated values using a function.
    ///
    /// The resulting generator has no schema since the transformation
    /// may invalidate the schema's semantics.
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
    ///
    /// If `max_attempts` consecutive values fail the predicate, calls `reject()`.
    fn filter<F>(self, predicate: F, max_attempts: usize) -> Filtered<T, F, Self>
    where
        Self: Sized,
        F: Fn(&T) -> bool + Send + Sync,
    {
        Filtered {
            source: self,
            predicate,
            max_attempts,
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
    inner: Arc<dyn Generate<T> + Send + Sync + 'a>,
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

// ============================================================================
// Combinator Types
// ============================================================================

/// Generator that transforms values using a function.
pub struct Mapped<T, U, F, G> {
    source: G,
    f: F,
    _phantom: PhantomData<(T, U)>,
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
        None // Transformation invalidates schema
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

/// Generator that uses a generated value to create another generator.
pub struct FlatMapped<T, U, G2, F, G1> {
    source: G1,
    f: F,
    _phantom: PhantomData<(T, U, G2)>,
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
        None // Dependent generation can't have a static schema
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

/// Generator that filters values using a predicate.
pub struct Filtered<T, F, G> {
    source: G,
    predicate: F,
    max_attempts: usize,
    _phantom: PhantomData<T>,
}

impl<T, F, G> Generate<T> for Filtered<T, F, G>
where
    G: Generate<T>,
    F: Fn(&T) -> bool + Send + Sync,
{
    fn generate(&self) -> T {
        for _ in 0..self.max_attempts {
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
        crate::reject(&format!(
            "Filter predicate failed {} consecutive times",
            self.max_attempts
        ))
    }

    fn schema(&self) -> Option<Value> {
        None // Filter invalidates schema semantics
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
// Primitive Generators
// ============================================================================

/// Generator that always produces unit `()`.
pub struct UnitGenerator;

impl Generate<()> for UnitGenerator {
    fn generate(&self) {
        // Unit type needs no generation - just return ()
    }

    fn schema(&self) -> Option<Value> {
        Some(json!({"type": "null"}))
    }
}

/// Generate unit values.
pub fn units() -> UnitGenerator {
    UnitGenerator
}

/// Generator that always produces the same value (with schema).
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

/// Generate a constant value with schema support.
///
/// Provides a `{"const": value}` schema for better shrinking.
/// For non-serializable types, use `just_any()`.
pub fn just<T: Clone + Send + Sync + serde::Serialize>(value: T) -> JustGenerator<T> {
    JustGenerator { value }
}

/// Generator that always produces the same value (no schema).
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

/// Generate a constant value without schema support.
///
/// Use for types that don't implement `Serialize`.
/// For serializable types, prefer `just()`.
pub fn just_any<T: Clone + Send + Sync>(value: T) -> JustAnyGenerator<T> {
    JustAnyGenerator { value }
}

/// Generator for boolean values.
pub struct BoolGenerator;

impl Generate<bool> for BoolGenerator {
    fn generate(&self) -> bool {
        generate_from_schema(&json!({"type": "boolean"}))
    }

    fn schema(&self) -> Option<Value> {
        Some(json!({"type": "boolean"}))
    }
}

/// Generate boolean values.
pub fn booleans() -> BoolGenerator {
    BoolGenerator
}

// ============================================================================
// Numeric Generators
// ============================================================================

/// Generator for integer values.
pub struct IntegerGenerator<T> {
    min: Option<T>,
    max: Option<T>,
    _phantom: PhantomData<T>,
}

impl<T> IntegerGenerator<T> {
    /// Set the minimum value (inclusive).
    pub fn with_min(mut self, min: T) -> Self {
        self.min = Some(min);
        self
    }

    /// Set the maximum value (inclusive).
    pub fn with_max(mut self, max: T) -> Self {
        self.max = Some(max);
        self
    }
}

impl<T> Generate<T> for IntegerGenerator<T>
where
    T: serde::de::DeserializeOwned + serde::Serialize + Bounded + NumInteger + Send + Sync + Copy,
{
    fn generate(&self) -> T {
        generate_from_schema(&self.schema().unwrap())
    }

    fn schema(&self) -> Option<Value> {
        // Always include bounds - use type's min/max as defaults since Hegel
        // generates arbitrary precision integers without bounds
        let min = self.min.unwrap_or_else(T::min_value);
        let max = self.max.unwrap_or_else(T::max_value);

        Some(json!({
            "type": "integer",
            "minimum": min,
            "maximum": max
        }))
    }
}

/// Generate integer values.
///
/// The type parameter determines the integer type. Bounds are automatically
/// derived from the type (e.g., `u8` uses 0-255). Use `with_min()` and
/// `with_max()` to constrain the range further.
///
/// # Example
///
/// ```no_run
/// use hegel::gen::{self, Generate};
///
/// // Generate any i32 (uses i32::MIN to i32::MAX)
/// let gen = gen::integers::<i32>();
///
/// // Generate u8 in range 0-100
/// let gen = gen::integers::<u8>().with_min(0).with_max(100);
/// ```
pub fn integers<T>() -> IntegerGenerator<T>
where
    T: serde::de::DeserializeOwned + serde::Serialize + Bounded + NumInteger + Send + Sync + Copy,
{
    IntegerGenerator {
        min: None,
        max: None,
        _phantom: PhantomData,
    }
}

/// Generator for floating-point values.
pub struct FloatGenerator<T> {
    min: Option<T>,
    max: Option<T>,
    exclude_min: bool,
    exclude_max: bool,
}

impl<T> FloatGenerator<T> {
    /// Set the minimum value.
    pub fn with_min(mut self, min: T) -> Self {
        self.min = Some(min);
        self
    }

    /// Set the maximum value.
    pub fn with_max(mut self, max: T) -> Self {
        self.max = Some(max);
        self
    }

    /// Exclude the minimum value from the range.
    pub fn exclude_min(mut self) -> Self {
        self.exclude_min = true;
        self
    }

    /// Exclude the maximum value from the range.
    pub fn exclude_max(mut self) -> Self {
        self.exclude_max = true;
        self
    }
}

impl<T> Generate<T> for FloatGenerator<T>
where
    T: serde::de::DeserializeOwned + serde::Serialize + NumFloat + Send + Sync,
{
    fn generate(&self) -> T {
        generate_from_schema(&self.schema().unwrap())
    }

    fn schema(&self) -> Option<Value> {
        let mut schema = json!({"type": "number"});

        if let Some(ref min) = self.min {
            if self.exclude_min {
                schema["exclusiveMinimum"] = json!(min);
            } else {
                schema["minimum"] = json!(min);
            }
        }

        if let Some(ref max) = self.max {
            if self.exclude_max {
                schema["exclusiveMaximum"] = json!(max);
            } else {
                schema["maximum"] = json!(max);
            }
        }

        Some(schema)
    }
}

/// Generate floating-point values.
pub fn floats<T>() -> FloatGenerator<T>
where
    T: NumFloat,
{
    FloatGenerator {
        min: None,
        max: None,
        exclude_min: false,
        exclude_max: false,
    }
}

// ============================================================================
// String Generators
// ============================================================================

/// Generator for text strings.
pub struct TextGenerator {
    min_size: usize,
    max_size: Option<usize>,
}

impl TextGenerator {
    /// Set the minimum size (in Unicode codepoints).
    pub fn with_min_size(mut self, min: usize) -> Self {
        self.min_size = min;
        self
    }

    /// Set the maximum size (in Unicode codepoints).
    pub fn with_max_size(mut self, max: usize) -> Self {
        self.max_size = Some(max);
        self
    }
}

impl Generate<String> for TextGenerator {
    fn generate(&self) -> String {
        generate_from_schema(&self.schema().unwrap())
    }

    fn schema(&self) -> Option<Value> {
        let mut schema = json!({"type": "string"});

        if self.min_size > 0 {
            schema["minLength"] = json!(self.min_size);
        }

        if let Some(max) = self.max_size {
            schema["maxLength"] = json!(max);
        }

        Some(schema)
    }
}

/// Generate text strings.
pub fn text() -> TextGenerator {
    TextGenerator {
        min_size: 0,
        max_size: None,
    }
}

/// Generator for strings matching a regex pattern.
pub struct RegexGenerator {
    pattern: String,
}

impl Generate<String> for RegexGenerator {
    fn generate(&self) -> String {
        generate_from_schema(&self.schema().unwrap())
    }

    fn schema(&self) -> Option<Value> {
        Some(json!({
            "type": "string",
            "pattern": self.pattern
        }))
    }
}

/// Generate strings matching a regular expression.
///
/// The pattern is automatically anchored with `^` and `$` if not already present.
pub fn from_regex(pattern: &str) -> RegexGenerator {
    let anchored = if pattern.starts_with('^') && pattern.ends_with('$') {
        pattern.to_string()
    } else if pattern.starts_with('^') {
        format!("{}$", pattern)
    } else if pattern.ends_with('$') {
        format!("^{}", pattern)
    } else {
        format!("^{}$", pattern)
    };

    RegexGenerator { pattern: anchored }
}

// ============================================================================
// Format String Generators
// ============================================================================

/// Generator for email addresses.
pub struct EmailGenerator;

impl Generate<String> for EmailGenerator {
    fn generate(&self) -> String {
        generate_from_schema(&self.schema().unwrap())
    }

    fn schema(&self) -> Option<Value> {
        Some(json!({"type": "string", "format": "email"}))
    }
}

/// Generate email addresses.
pub fn emails() -> EmailGenerator {
    EmailGenerator
}

/// Generator for URLs.
pub struct UrlGenerator;

impl Generate<String> for UrlGenerator {
    fn generate(&self) -> String {
        generate_from_schema(&self.schema().unwrap())
    }

    fn schema(&self) -> Option<Value> {
        Some(json!({"type": "string", "format": "uri"}))
    }
}

/// Generate URLs.
pub fn urls() -> UrlGenerator {
    UrlGenerator
}

/// Generator for domain names.
pub struct DomainGenerator {
    max_length: usize,
}

impl DomainGenerator {
    /// Set the maximum length.
    pub fn with_max_length(mut self, max: usize) -> Self {
        self.max_length = max;
        self
    }
}

impl Generate<String> for DomainGenerator {
    fn generate(&self) -> String {
        generate_from_schema(&self.schema().unwrap())
    }

    fn schema(&self) -> Option<Value> {
        Some(json!({
            "type": "string",
            "format": "hostname",
            "maxLength": self.max_length
        }))
    }
}

/// Generate domain names.
pub fn domains() -> DomainGenerator {
    DomainGenerator { max_length: 255 }
}

/// IP address version.
#[derive(Clone, Copy)]
pub enum IpVersion {
    V4,
    V6,
}

/// Generator for IP addresses.
pub struct IpAddressGenerator {
    version: Option<IpVersion>,
}

impl IpAddressGenerator {
    /// Generate only IPv4 addresses.
    pub fn v4(mut self) -> Self {
        self.version = Some(IpVersion::V4);
        self
    }

    /// Generate only IPv6 addresses.
    pub fn v6(mut self) -> Self {
        self.version = Some(IpVersion::V6);
        self
    }
}

impl Generate<String> for IpAddressGenerator {
    fn generate(&self) -> String {
        generate_from_schema(&self.schema().unwrap())
    }

    fn schema(&self) -> Option<Value> {
        match self.version {
            Some(IpVersion::V4) => Some(json!({"type": "string", "format": "ipv4"})),
            Some(IpVersion::V6) => Some(json!({"type": "string", "format": "ipv6"})),
            None => Some(json!({
                "anyOf": [
                    {"type": "string", "format": "ipv4"},
                    {"type": "string", "format": "ipv6"}
                ]
            })),
        }
    }
}

/// Generate IP addresses.
///
/// By default generates either IPv4 or IPv6. Use `.v4()` or `.v6()` to constrain.
pub fn ip_addresses() -> IpAddressGenerator {
    IpAddressGenerator { version: None }
}

/// Generator for ISO 8601 dates (YYYY-MM-DD).
pub struct DateGenerator;

impl Generate<String> for DateGenerator {
    fn generate(&self) -> String {
        generate_from_schema(&self.schema().unwrap())
    }

    fn schema(&self) -> Option<Value> {
        Some(json!({"type": "string", "format": "date"}))
    }
}

/// Generate ISO 8601 dates.
pub fn dates() -> DateGenerator {
    DateGenerator
}

/// Generator for ISO 8601 times (HH:MM:SS).
pub struct TimeGenerator;

impl Generate<String> for TimeGenerator {
    fn generate(&self) -> String {
        generate_from_schema(&self.schema().unwrap())
    }

    fn schema(&self) -> Option<Value> {
        Some(json!({"type": "string", "format": "time"}))
    }
}

/// Generate ISO 8601 times.
pub fn times() -> TimeGenerator {
    TimeGenerator
}

/// Generator for ISO 8601 datetimes.
pub struct DateTimeGenerator;

impl Generate<String> for DateTimeGenerator {
    fn generate(&self) -> String {
        generate_from_schema(&self.schema().unwrap())
    }

    fn schema(&self) -> Option<Value> {
        Some(json!({"type": "string", "format": "date-time"}))
    }
}

/// Generate ISO 8601 datetimes.
pub fn datetimes() -> DateTimeGenerator {
    DateTimeGenerator
}

// ============================================================================
// Collection Generators
// ============================================================================

/// Generator for Vec values.
pub struct VecGenerator<G> {
    elements: G,
    min_size: usize,
    max_size: Option<usize>,
    unique: bool,
}

impl<G> VecGenerator<G> {
    /// Set the minimum size.
    pub fn with_min_size(mut self, min: usize) -> Self {
        self.min_size = min;
        self
    }

    /// Set the maximum size.
    pub fn with_max_size(mut self, max: usize) -> Self {
        self.max_size = Some(max);
        self
    }

    /// Require all elements to be unique.
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

        let mut schema = json!({
            "type": "array",
            "items": element_schema,
            "minItems": self.min_size
        });

        if let Some(max) = self.max_size {
            schema["maxItems"] = json!(max);
        }

        if self.unique {
            schema["uniqueItems"] = json!(true);
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

/// Generator for HashSet values.
pub struct HashSetGenerator<G> {
    elements: G,
    min_size: usize,
    max_size: Option<usize>,
}

impl<G> HashSetGenerator<G> {
    /// Set the minimum size.
    pub fn with_min_size(mut self, min: usize) -> Self {
        self.min_size = min;
        self
    }

    /// Set the maximum size.
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
            "type": "array",
            "items": element_schema,
            "minItems": self.min_size,
            "uniqueItems": true
        });

        if let Some(max) = self.max_size {
            schema["maxItems"] = json!(max);
        }

        Some(schema)
    }
}

// Implement Generate for references to generators (needed for HashSetGenerator)
impl<T, G: Generate<T>> Generate<T> for &G {
    fn generate(&self) -> T {
        (*self).generate()
    }

    fn schema(&self) -> Option<Value> {
        (*self).schema()
    }
}

/// Generate hash sets.
pub fn hashsets<T, G: Generate<T>>(elements: G) -> HashSetGenerator<G> {
    HashSetGenerator {
        elements,
        min_size: 0,
        max_size: None,
    }
}

/// Generator for HashMap values with string keys.
pub struct HashMapGenerator<V> {
    values: V,
    min_size: usize,
    max_size: Option<usize>,
}

impl<V> HashMapGenerator<V> {
    /// Set the minimum size.
    pub fn with_min_size(mut self, min: usize) -> Self {
        self.min_size = min;
        self
    }

    /// Set the maximum size.
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
                if map.len() < self.min_size {
                    crate::reject(&format!(
                        "Failed to generate {} unique keys for hashmap after {} attempts",
                        self.min_size, max_attempts
                    ));
                }
                map
            })
        }
    }

    fn schema(&self) -> Option<Value> {
        let value_schema = self.values.schema()?;

        let mut schema = json!({
            "type": "object",
            "additionalProperties": value_schema,
            "minProperties": self.min_size
        });

        if let Some(max) = self.max_size {
            schema["maxProperties"] = json!(max);
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

// ============================================================================
// Tuple Generators
// ============================================================================

/// Generator for 2-tuples.
pub struct Tuple2Generator<G1, G2> {
    gen1: G1,
    gen2: G2,
}

impl<T1, T2, G1, G2> Generate<(T1, T2)> for Tuple2Generator<G1, G2>
where
    G1: Generate<T1>,
    G2: Generate<T2>,
    T1: serde::de::DeserializeOwned,
    T2: serde::de::DeserializeOwned,
{
    fn generate(&self) -> (T1, T2) {
        if let Some(schema) = self.schema() {
            generate_from_schema(&schema)
        } else {
            group(labels::TUPLE, || {
                let v1 = self.gen1.generate();
                let v2 = self.gen2.generate();
                (v1, v2)
            })
        }
    }

    fn schema(&self) -> Option<Value> {
        let s1 = self.gen1.schema()?;
        let s2 = self.gen2.schema()?;

        Some(json!({
            "type": "array",
            "prefixItems": [s1, s2],
            "items": false,
            "minItems": 2,
            "maxItems": 2
        }))
    }
}

/// Generator for 3-tuples.
pub struct Tuple3Generator<G1, G2, G3> {
    gen1: G1,
    gen2: G2,
    gen3: G3,
}

impl<T1, T2, T3, G1, G2, G3> Generate<(T1, T2, T3)> for Tuple3Generator<G1, G2, G3>
where
    G1: Generate<T1>,
    G2: Generate<T2>,
    G3: Generate<T3>,
    T1: serde::de::DeserializeOwned,
    T2: serde::de::DeserializeOwned,
    T3: serde::de::DeserializeOwned,
{
    fn generate(&self) -> (T1, T2, T3) {
        if let Some(schema) = self.schema() {
            generate_from_schema(&schema)
        } else {
            group(labels::TUPLE, || {
                let v1 = self.gen1.generate();
                let v2 = self.gen2.generate();
                let v3 = self.gen3.generate();
                (v1, v2, v3)
            })
        }
    }

    fn schema(&self) -> Option<Value> {
        let s1 = self.gen1.schema()?;
        let s2 = self.gen2.schema()?;
        let s3 = self.gen3.schema()?;

        Some(json!({
            "type": "array",
            "prefixItems": [s1, s2, s3],
            "items": false,
            "minItems": 3,
            "maxItems": 3
        }))
    }
}

/// Generate 2-tuples.
pub fn tuples<T1, T2, G1: Generate<T1>, G2: Generate<T2>>(
    gen1: G1,
    gen2: G2,
) -> Tuple2Generator<G1, G2> {
    Tuple2Generator { gen1, gen2 }
}

/// Generate 3-tuples.
pub fn tuples3<T1, T2, T3, G1: Generate<T1>, G2: Generate<T2>, G3: Generate<T3>>(
    gen1: G1,
    gen2: G2,
    gen3: G3,
) -> Tuple3Generator<G1, G2, G3> {
    Tuple3Generator { gen1, gen2, gen3 }
}

// ============================================================================
// Combinators
// ============================================================================

/// Generator that samples uniformly from a fixed collection.
pub struct SampledFromGenerator<T> {
    elements: Vec<T>,
}

impl<T: Clone + Send + Sync + serde::Serialize> Generate<T> for SampledFromGenerator<T> {
    fn generate(&self) -> T {
        if self.elements.is_empty() {
            crate::reject("sampled_from called with empty collection");
        }

        // Check if elements are primitive enough for enum schema
        if let Some(schema) = self.schema() {
            let value: Value = generate_from_schema(&schema);
            // Find matching element
            for elem in &self.elements {
                if json!(elem) == value {
                    return elem.clone();
                }
            }
            crate::reject("Generated value not in sampled_from collection");
        } else {
            // Generate index and pick
            let idx_gen = integers::<usize>()
                .with_min(0)
                .with_max(self.elements.len() - 1);
            let idx = idx_gen.generate();
            self.elements[idx].clone()
        }
    }

    fn schema(&self) -> Option<Value> {
        // Only use enum schema for JSON-primitive types
        let json_values: Vec<Value> = self.elements.iter().map(|e| json!(e)).collect();

        // Check if all values are primitives (not objects/arrays)
        let all_primitive = json_values.iter().all(|v| {
            matches!(
                v,
                Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)
            )
        });

        if all_primitive {
            Some(json!({"enum": json_values}))
        } else {
            None
        }
    }
}

/// Sample uniformly from a fixed collection (owned).
pub fn sampled_from<T: Clone + Send + Sync + serde::Serialize>(
    elements: Vec<T>,
) -> SampledFromGenerator<T> {
    SampledFromGenerator { elements }
}

/// Generator that samples from a borrowed slice.
pub struct SampledFromSliceGenerator<'a, T> {
    elements: &'a [T],
}

impl<'a, T: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned> Generate<T>
    for SampledFromSliceGenerator<'a, T>
{
    fn generate(&self) -> T {
        if self.elements.is_empty() {
            crate::reject("sampled_from_slice called with empty slice");
        }

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
            "enum": self.elements
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

/// Generator that chooses from multiple generators.
pub struct OneOfGenerator<'a, T> {
    generators: Vec<BoxedGenerator<'a, T>>,
}

impl<'a, T: serde::de::DeserializeOwned> Generate<T> for OneOfGenerator<'a, T> {
    fn generate(&self) -> T {
        if self.generators.is_empty() {
            crate::reject("one_of called with no generators");
        }

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

        schemas.map(|s| json!({"anyOf": s}))
    }
}

/// Choose from multiple generators of the same type.
///
/// For a more convenient syntax, use the [`one_of!`] macro instead.
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

/// Generator for optional values.
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
                "anyOf": [
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
            "anyOf": [
                {"type": "null"},
                inner_schema
            ]
        }))
    }
}

/// Generate optional values (either None or Some(value)).
pub fn optional<T, G: Generate<T>>(inner: G) -> OptionalGenerator<G> {
    OptionalGenerator { inner }
}

// ============================================================================
// Fixed Dictionaries (for struct-like data)
// ============================================================================

/// Builder for fixed-key dictionary generators.
pub struct FixedDictBuilder<'a> {
    fields: Vec<(String, BoxedGenerator<'a, Value>)>,
}

impl<'a> FixedDictBuilder<'a> {
    /// Add a field with a generator.
    pub fn field<T, G>(mut self, name: &str, gen: G) -> Self
    where
        G: Generate<T> + Send + Sync + 'a,
        T: serde::Serialize + 'a,
    {
        let boxed = BoxedGenerator {
            inner: Arc::new(MappedToValue {
                inner: gen,
                _phantom: PhantomData::<T>,
            }),
        };
        self.fields.push((name.to_string(), boxed));
        self
    }

    /// Build the generator.
    pub fn build(self) -> FixedDictGenerator<'a> {
        FixedDictGenerator {
            fields: self.fields,
        }
    }
}

struct MappedToValue<T, G> {
    inner: G,
    _phantom: PhantomData<T>,
}

impl<T: serde::Serialize, G: Generate<T>> Generate<Value> for MappedToValue<T, G> {
    fn generate(&self) -> Value {
        json!(self.inner.generate())
    }

    fn schema(&self) -> Option<Value> {
        self.inner.schema()
    }
}

unsafe impl<T, G: Send> Send for MappedToValue<T, G> {}
unsafe impl<T, G: Sync> Sync for MappedToValue<T, G> {}

/// Generator for dictionaries with fixed keys.
pub struct FixedDictGenerator<'a> {
    fields: Vec<(String, BoxedGenerator<'a, Value>)>,
}

impl<'a> Generate<Value> for FixedDictGenerator<'a> {
    fn generate(&self) -> Value {
        if let Some(schema) = self.schema() {
            generate_from_schema(&schema)
        } else {
            // Compositional fallback
            group(labels::FIXED_DICT, || {
                let mut map = serde_json::Map::new();
                for (name, gen) in &self.fields {
                    map.insert(name.clone(), gen.generate());
                }
                Value::Object(map)
            })
        }
    }

    fn schema(&self) -> Option<Value> {
        let mut properties = serde_json::Map::new();
        let mut required = Vec::new();

        for (name, gen) in &self.fields {
            let field_schema = gen.schema()?;
            properties.insert(name.clone(), field_schema);
            required.push(json!(name));
        }

        Some(json!({
            "type": "object",
            "properties": properties,
            "required": required
        }))
    }
}

/// Create a generator for dictionaries with fixed keys.
///
/// # Example
///
/// ```no_run
/// use hegel::gen::{self, Generate};
///
/// let gen = gen::fixed_dicts()
///     .field("name", gen::text())
///     .field("age", gen::integers::<u32>())
///     .build();
/// ```
pub fn fixed_dicts<'a>() -> FixedDictBuilder<'a> {
    FixedDictBuilder { fields: Vec::new() }
}

// ============================================================================
// DefaultGenerator Trait
// ============================================================================

/// Trait for types that have a default generator.
///
/// This is used by derive macros to automatically generate values for fields.
pub trait DefaultGenerator: Sized {
    /// The generator type for this type.
    type Generator: Generate<Self>;

    /// Get the default generator for this type.
    fn default_generator() -> Self::Generator;
}

impl DefaultGenerator for bool {
    type Generator = BoolGenerator;
    fn default_generator() -> Self::Generator {
        booleans()
    }
}

impl DefaultGenerator for String {
    type Generator = TextGenerator;
    fn default_generator() -> Self::Generator {
        text()
    }
}

impl DefaultGenerator for i8 {
    type Generator = IntegerGenerator<i8>;
    fn default_generator() -> Self::Generator {
        integers()
    }
}

impl DefaultGenerator for i16 {
    type Generator = IntegerGenerator<i16>;
    fn default_generator() -> Self::Generator {
        integers()
    }
}

impl DefaultGenerator for i32 {
    type Generator = IntegerGenerator<i32>;
    fn default_generator() -> Self::Generator {
        integers()
    }
}

impl DefaultGenerator for i64 {
    type Generator = IntegerGenerator<i64>;
    fn default_generator() -> Self::Generator {
        integers()
    }
}

impl DefaultGenerator for u8 {
    type Generator = IntegerGenerator<u8>;
    fn default_generator() -> Self::Generator {
        integers()
    }
}

impl DefaultGenerator for u16 {
    type Generator = IntegerGenerator<u16>;
    fn default_generator() -> Self::Generator {
        integers()
    }
}

impl DefaultGenerator for u32 {
    type Generator = IntegerGenerator<u32>;
    fn default_generator() -> Self::Generator {
        integers()
    }
}

impl DefaultGenerator for u64 {
    type Generator = IntegerGenerator<u64>;
    fn default_generator() -> Self::Generator {
        integers()
    }
}

impl DefaultGenerator for f32 {
    type Generator = FloatGenerator<f32>;
    fn default_generator() -> Self::Generator {
        floats()
    }
}

impl DefaultGenerator for f64 {
    type Generator = FloatGenerator<f64>;
    fn default_generator() -> Self::Generator {
        floats()
    }
}

impl<T: DefaultGenerator> DefaultGenerator for Option<T>
where
    T: serde::de::DeserializeOwned,
{
    type Generator = OptionalGenerator<T::Generator>;
    fn default_generator() -> Self::Generator {
        optional(T::default_generator())
    }
}

impl<T: DefaultGenerator> DefaultGenerator for Vec<T>
where
    T: serde::de::DeserializeOwned,
{
    type Generator = VecGenerator<T::Generator>;
    fn default_generator() -> Self::Generator {
        vecs(T::default_generator())
    }
}

// ============================================================================
// Declarative Macro for External Types
// ============================================================================

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

                fn schema(&self) -> Option<serde_json::Value> {
                    use $crate::gen::Generate;

                    let mut properties = serde_json::Map::new();
                    let mut required = Vec::new();

                    $(
                        let field_schema = self.$field_name.schema()?;
                        properties.insert(stringify!($field_name).to_string(), field_schema);
                        required.push(serde_json::json!(stringify!($field_name)));
                    )*

                    Some(serde_json::json!({
                        "type": "object",
                        "properties": properties,
                        "required": required
                    }))
                }
            }
        }
    };
}

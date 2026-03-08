use ciborium::Value;

use crate::cbor_utils::{cbor_map, map_insert};

use std::cell::{Cell, RefCell};
use std::sync::{Arc, LazyLock};

use crate::protocol::{Channel, Connection};
use crate::runner::Verbosity;

use super::value;

static PROTOCOL_DEBUG: LazyLock<bool> = LazyLock::new(|| {
    matches!(
        std::env::var("HEGEL_PROTOCOL_DEBUG")
            .unwrap_or_default()
            .to_lowercase()
            .as_str(),
        "1" | "true"
    )
});

// shared per-test-case data
#[doc(hidden)]
pub struct TestCaseData {
    #[allow(dead_code)]
    connection: Arc<Connection>,
    pub(crate) channel: Channel,
    pub(crate) span_depth: Cell<usize>,
    verbosity: Verbosity,
    pub(crate) is_last_run: bool,
    pub(crate) output: RefCell<Vec<String>>,
    draw_count: Cell<usize>,
    pub(crate) test_aborted: Cell<bool>,
    // only public for our compose! macro. Ideally would be pub(crate).
    #[doc(hidden)]
    pub in_composite: Cell<bool>,
}

impl TestCaseData {
    pub(crate) fn new(
        connection: Arc<Connection>,
        channel: Channel,
        verbosity: Verbosity,
        is_last_run: bool,
    ) -> Self {
        TestCaseData {
            connection,
            channel,
            span_depth: Cell::new(0),
            verbosity,
            is_last_run,
            output: RefCell::new(Vec::new()),
            draw_count: Cell::new(0),
            test_aborted: Cell::new(false),
            in_composite: Cell::new(false),
        }
    }

    pub(crate) fn record_draw<T: std::fmt::Debug>(&self, value: &T) {
        if !self.is_last_run {
            return;
        }
        let count = self.draw_count.get() + 1;
        self.draw_count.set(count);
        self.output
            .borrow_mut()
            .push(format!("Draw {}: {:?}", count, value));
    }

    pub fn start_span(&self, label: u64) {
        self.span_depth.set(self.span_depth.get() + 1);
        if let Err(StopTestError) = self.send_request("start_span", &cbor_map! {"label" => label}) {
            let depth = self.span_depth.get();
            assert!(depth > 0);
            self.span_depth.set(depth - 1);
            crate::assume(false);
        }
    }

    pub fn stop_span(&self, discard: bool) {
        let depth = self.span_depth.get();
        assert!(depth > 0);
        self.span_depth.set(depth - 1);
        let _ = self.send_request("stop_span", &cbor_map! {"discard" => discard});
    }

    /// Returns Err(StopTestError) if the server sends an overflow error.
    pub(super) fn send_request(
        &self,
        command: &str,
        payload: &Value,
    ) -> Result<Value, StopTestError> {
        let debug = *PROTOCOL_DEBUG || self.verbosity == Verbosity::Debug;

        let mut entries = vec![(
            Value::Text("command".to_string()),
            Value::Text(command.to_string()),
        )];

        if let Value::Map(map) = payload {
            for (k, v) in map {
                entries.push((k.clone(), v.clone()));
            }
        }

        let request = Value::Map(entries);

        if debug {
            eprintln!("REQUEST: {:?}", request);
        }

        let result = self.channel.request_cbor(&request);

        match result {
            Ok(response) => {
                if debug {
                    eprintln!("RESPONSE: {:?}", response);
                }
                Ok(response)
            }
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("overflow") || error_msg.contains("StopTest") {
                    if debug {
                        eprintln!("RESPONSE: StopTest/overflow");
                    }
                    // Mark test as aborted so the runner skips sending mark_complete
                    // (the server has already moved on from this test case)
                    self.test_aborted.set(true);
                    Err(StopTestError)
                } else {
                    panic!("Failed to communicate with Hegel: {}", e);
                }
            }
        }
    }

    /// Send a schema to the server and return the raw CBOR response.
    ///
    /// This is the core generation primitive. It handles StopTest errors
    /// by calling `assume(false)` to mark the test case as invalid.
    pub fn generate_raw(&self, schema: &Value) -> Value {
        match self.send_request("generate", &cbor_map! {"schema" => schema.clone()}) {
            Ok(v) => v,
            Err(StopTestError) => {
                crate::assume(false);
                unreachable!()
            }
        }
    }

    pub fn generate_from_schema<T: serde::de::DeserializeOwned>(&self, schema: &Value) -> T {
        deserialize_value(self.generate_raw(schema))
    }

}

/// Custom error for StopTest (overflow) condition.
#[derive(Debug)]
pub struct StopTestError;

impl std::fmt::Display for StopTestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Server ran out of data (StopTest)")
    }
}

impl std::error::Error for StopTestError {}

/// Deserialize a raw CBOR value into a Rust type.
///
/// This is a public helper for use by derived generators (proc macros)
/// that need to deserialize individual field values from CBOR.
pub fn deserialize_value<T: serde::de::DeserializeOwned>(raw: Value) -> T {
    let hv = value::HegelValue::from(raw.clone());
    value::from_hegel_value(hv).unwrap_or_else(|e| {
        panic!("Failed to deserialize value: {}\nValue: {:?}", e, raw);
    })
}

#[doc(hidden)]
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
    pub const MAPPED: u64 = 13;
    pub const SAMPLED_FROM: u64 = 14;
    pub const ENUM_VARIANT: u64 = 15;
}

/// Uses the hegel server to determine collection sizing.
///
///  The server-side `many` object is created lazily on the first call to
/// [`more()`](Collection::more).
///
/// # Example
///
/// ```ignore
/// use hegel::generators::Collection;
///
/// let data = hegel::generators::test_case_data();
/// let mut coll = Collection::new(data, "my_list", 0, None);
/// let mut result = Vec::new();
/// while coll.more() {
///     result.push(generators::integers::<i32>().do_draw(data));
/// }
/// ```
pub struct Collection<'a> {
    data: &'a TestCaseData,
    base_name: String,
    min_size: usize,
    max_size: Option<usize>,
    server_name: Option<String>,
    finished: bool,
}

impl<'a> Collection<'a> {
    pub fn new(
        data: &'a TestCaseData,
        name: &str,
        min_size: usize,
        max_size: Option<usize>,
    ) -> Self {
        Collection {
            data,
            base_name: name.to_string(),
            min_size,
            max_size,
            server_name: None,
            finished: false,
        }
    }

    fn ensure_initialized(&mut self) -> &str {
        if self.server_name.is_none() {
            let mut payload = cbor_map! {
                "name" => self.base_name.as_str(),
                "min_size" => self.min_size as u64
            };
            if let Some(max) = self.max_size {
                map_insert(&mut payload, "max_size", Value::from(max as u64));
            }
            let response = match self.data.send_request("new_collection", &payload) {
                Ok(v) => v,
                Err(StopTestError) => {
                    crate::assume(false);
                    unreachable!()
                }
            };
            let name = match response {
                Value::Text(s) => s,
                _ => panic!(
                    "Expected text response from new_collection, got {:?}",
                    response
                ),
            };
            self.server_name = Some(name);
        }
        self.server_name.as_ref().unwrap()
    }

    pub fn more(&mut self) -> bool {
        if self.finished {
            return false;
        }
        let server_name = self.ensure_initialized().to_string();
        let response = match self.data.send_request(
            "collection_more",
            &cbor_map! { "collection" => server_name.as_str() },
        ) {
            Ok(v) => v,
            Err(StopTestError) => {
                self.finished = true;
                crate::assume(false);
                unreachable!()
            }
        };
        let result = match response {
            Value::Bool(b) => b,
            _ => panic!("Expected bool from collection_more, got {:?}", response),
        };
        if !result {
            self.finished = true;
        }
        result
    }

    /// Reject the last element (don't count it towards the size budget).
    ///
    /// This is useful for unique collections where a generated element
    /// turned out to be a duplicate.
    pub fn reject(&mut self, why: Option<&str>) {
        if self.finished {
            return;
        }
        let server_name = self.ensure_initialized().to_string();
        let mut payload = cbor_map! {
            "collection" => server_name.as_str()
        };
        if let Some(reason) = why {
            map_insert(&mut payload, "why", Value::Text(reason.to_string()));
        }
        let _ = self.data.send_request("collection_reject", &payload);
    }
}

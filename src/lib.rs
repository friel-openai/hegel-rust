//! Hegel SDK - Property-based testing with JSON Schema
//!
//! This crate provides a Hypothesis-like API for property-based testing,
//! communicating with the Hegel backend via Unix sockets.
//!
//! # Standalone Mode Example
//!
//! ```no_run
//! use hegel::gen::{self, Generate};
//!
//! let gen = gen::vecs(gen::integers::<i32>())
//!     .with_min_size(1)
//!     .with_max_size(10);
//!
//! let values: Vec<i32> = gen.generate();
//! assert!(!values.is_empty());
//! assert!(values.len() <= 10);
//! ```
//!
//! # Embedded Mode Example
//!
//! ```no_run
//! use hegel::{gen::{self, Generate}, note};
//!
//! fn main() {
//!     hegel::hegel(|| {
//!         let x = gen::integers::<i32>().generate();
//!         let y = gen::integers::<i32>().generate();
//!         note(&format!("Testing {} + {} = {}", x, y, x + y));
//!         assert_eq!(x + y, y + x);
//!     });
//! }
//! ```

pub mod embedded;
pub mod gen;

pub use gen::Generate;

// Re-export for macro use
#[doc(hidden)]
pub use paste;

// Re-export derive macro
pub use hegel_derive::Generate;

// Re-export embedded mode API
pub use embedded::{hegel, hegel_with_options, HegelOptions};

/// The execution mode for the Hegel SDK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HegelMode {
    /// Standalone mode: test binary runs, hegel is external.
    /// This is the default mode when HEGEL_SOCKET is set.
    #[default]
    Standalone,
    /// Embedded mode: test binary runs hegel as subprocess.
    /// Used when calling `hegel::embedded::hegel()`.
    Embedded,
}

/// Get the current execution mode.
pub fn current_mode() -> HegelMode {
    gen::current_mode()
}

/// Check if this is the last run (during shrinking).
/// In embedded mode, this indicates when `note()` output should be printed.
pub fn is_last_run() -> bool {
    gen::is_last_run()
}

/// Print a note message.
///
/// In standalone mode, this always prints to stderr.
/// In embedded mode, this only prints on the last run (during shrinking).
pub fn note(message: &str) {
    gen::note(message)
}

/// Reject the current test input and signal Hegel to try different data.
///
/// This should be called when generated data doesn't meet preconditions
/// that can't be expressed in the schema (e.g., complex filters).
///
/// # Behavior
///
/// - In standalone mode: exits the process with `HEGEL_REJECT_CODE`
/// - In embedded mode: panics with a special marker that the SDK catches
pub fn reject(message: &str) -> ! {
    match current_mode() {
        HegelMode::Standalone => {
            let code: i32 = std::env::var("HEGEL_REJECT_CODE")
                .expect("HEGEL_REJECT_CODE environment variable not set")
                .parse()
                .expect("HEGEL_REJECT_CODE must be a valid integer");

            eprintln!("REJECT: {}", message);
            std::process::exit(code);
        }
        HegelMode::Embedded => {
            panic!("HEGEL_REJECT: {}", message);
        }
    }
}

/// Exit codes used by Hegel
pub mod exit_codes {
    /// Test assertion failed
    pub const TEST_FAILURE: i32 = 1;
    /// Socket connection error
    pub const SOCKET_ERROR: i32 = 134;
}

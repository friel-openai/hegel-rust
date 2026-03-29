use crate::antithesis::TestLocation;
use crate::control::{currently_in_test_context, with_test_context};
use crate::protocol::{Channel, Connection, HANDSHAKE_STRING};
use crate::test_case::{ASSUME_FAIL_STRING, STOP_TEST_STRING, TestCase};
use ciborium::Value;

#[cfg(not(feature = "rust-core"))]
use crate::antithesis::is_running_in_antithesis;
use crate::cbor_utils::cbor_map;
#[cfg(not(feature = "rust-core"))]
use crate::cbor_utils::{as_bool, as_text, as_u64, map_get};
#[cfg(not(feature = "rust-core"))]
use crate::protocol::SERVER_CRASHED_MESSAGE;
use std::backtrace::{Backtrace, BacktraceStatus};
use std::cell::RefCell;
use std::fs::{File, OpenOptions};
use std::panic::{self, AssertUnwindSafe, catch_unwind};
use std::process::{Command, Stdio};
#[cfg(feature = "rust-core")]
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Once};

#[cfg(feature = "rust-core")]
use crate::local_backend::LocalBackend;
#[cfg(feature = "rust-core")]
use hegel_core::choices::{Choice, choices_from_bytes, choices_to_bytes, shortlex_cmp};
#[cfg(feature = "rust-core")]
use hegel_core::database::ExampleDatabase;
#[cfg(feature = "rust-core")]
use hegel_core::engine::generate_simplest_value;
#[cfg(feature = "rust-core")]
use hegel_core::runtime::{save_corpus_replacement, save_interesting_origin_replacement};
#[cfg(feature = "rust-core")]
use hegel_core::schema::{DataValue, Schema};
#[cfg(feature = "rust-core")]
use hegel_core::shrink::{
    ExampleSortKey, IntegerShrinkObservation, float_choice_index, preferred_float_candidates,
    shrink_boolean_dict_observation as shrink_core_boolean_dict_observation,
    shrink_boolean_list_list_observation as shrink_core_boolean_list_list_observation,
    shrink_boolean_list_observation as shrink_core_boolean_list_observation,
    shrink_float_list_observation as shrink_core_float_list_observation,
    shrink_integer_dict_observation as shrink_core_integer_dict_observation,
    shrink_integer_list_list_observation as shrink_core_integer_list_list_observation,
    shrink_integer_list_observation as shrink_core_integer_list_observation,
    shrink_integer_observation as shrink_core_integer_observation,
    shrink_integer_pair_observation as shrink_core_integer_pair_observation,
    shrink_integer_string_dict_observation as shrink_core_integer_string_dict_observation,
    shrink_integer_tuple_list_observation as shrink_core_integer_tuple_list_observation,
};
#[cfg(feature = "rust-core")]
use std::cmp::Ordering as CmpOrdering;
#[cfg(feature = "rust-core")]
use std::io::Write;

const SUPPORTED_PROTOCOL_VERSIONS: (f64, f64) = (0.6, 0.7);
const HEGEL_SERVER_VERSION: &str = "0.2.3";
const HEGEL_SERVER_COMMAND_ENV: &str = "HEGEL_SERVER_COMMAND";
const HEGEL_SERVER_DIR: &str = ".hegel";
#[cfg(feature = "rust-core")]
const INVALID_THRESHOLD_BASE: u64 = 458;
#[cfg(feature = "rust-core")]
const INVALID_PER_VALID: u64 = 100;
const UV_NOT_FOUND_MESSAGE: &str = "\
You are seeing this error message because hegel-rust tried to use `uv` to install \
hegel-core, but could not find uv on the PATH.

Hegel uses a Python server component called `hegel-core` to share core property-based \
testing functionality across languages. There are two ways for Hegel to get hegel-core:

* By default, Hegel looks for uv (https://docs.astral.sh/uv/) on the PATH, and \
  uses uv to install hegel-core to a local `.hegel/venv` directory. We recommend this \
  option. To continue, install uv: https://docs.astral.sh/uv/getting-started/installation/.
* Alternatively, you can manage the installation of hegel-core yourself. After installing, \
  setting the HEGEL_SERVER_COMMAND environment variable to your hegel-core binary path tells \
  hegel-rust to use that hegel-core instead.

See https://hegel.dev/reference/installation for more details.";
static HEGEL_SERVER_COMMAND: std::sync::OnceLock<String> = std::sync::OnceLock::new();
static SERVER_LOG_FILE: std::sync::OnceLock<Mutex<File>> = std::sync::OnceLock::new();
static SESSION: std::sync::OnceLock<HegelSession> = std::sync::OnceLock::new();

static PANIC_HOOK_INIT: Once = Once::new();

/// A persistent connection to the hegel server subprocess.
///
/// Created once per process on first use. The subprocess and connection
/// are reused across all `Hegel::run()` calls. The Python server supports
/// multiple sequential `run_test` commands over a single connection.
struct HegelSession {
    connection: Arc<Connection>,
    /// The control channel is shared across threads, so it's behind a Mutex
    /// because Channel is not thread-safe. The lock is only held for the
    /// brief run_test send/receive; test execution runs concurrently on
    /// per-test channels.
    control: Mutex<Channel>,
}

impl HegelSession {
    fn get() -> &'static HegelSession {
        SESSION.get_or_init(|| {
            init_panic_hook();
            HegelSession::init()
        })
    }

    fn init() -> HegelSession {
        let hegel_binary_path = find_hegel();
        let mut cmd = Command::new(&hegel_binary_path);
        cmd.arg("--stdio").arg("--verbosity").arg("normal");

        cmd.env("PYTHONUNBUFFERED", "1");
        let log_file = server_log_file();
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::from(log_file));

        #[allow(clippy::expect_fun_call)]
        let mut child = cmd
            .spawn()
            .expect(format!("Failed to spawn hegel at path {}", hegel_binary_path).as_str());

        let child_stdin = child.stdin.take().expect("Failed to take child stdin");
        let child_stdout = child.stdout.take().expect("Failed to take child stdout");

        let connection = Connection::new(Box::new(child_stdout), Box::new(child_stdin));
        let mut control = connection.control_channel();

        // Handshake
        let req_id = control
            .send_request(HANDSHAKE_STRING.to_vec())
            .expect("Failed to send version negotiation");
        let response = control
            .receive_reply(req_id)
            .expect("Failed to receive version response");

        let decoded = String::from_utf8_lossy(&response);
        let server_version = match decoded.strip_prefix("Hegel/") {
            Some(v) => v,
            None => {
                let _ = child.kill();
                panic!("Bad handshake response: {decoded:?}");
            }
        };
        let version: f64 = server_version.parse().unwrap_or_else(|_| {
            let _ = child.kill();
            panic!("Bad version number: {server_version}");
        });

        let (lo, hi) = SUPPORTED_PROTOCOL_VERSIONS;
        if !(lo <= version && version <= hi) {
            let _ = child.kill();
            panic!(
                "hegel-rust supports protocol versions {lo} through {hi}, but \
                 the connected server is using protocol version {version}. Upgrading \
                 hegel-rust or downgrading hegel-core might help."
            );
        }

        // Monitor thread: detects server crash. The pipe close from
        // the child exiting will unblock any pending reads.
        let conn_for_monitor = Arc::clone(&connection);
        std::thread::spawn(move || {
            let _ = child.wait();
            conn_for_monitor.mark_server_exited();
        });

        HegelSession {
            connection,
            control: Mutex::new(control),
        }
    }
}

thread_local! {
    /// (thread_name, thread_id, location, backtrace)
    static LAST_PANIC_INFO: RefCell<Option<(String, String, String, Backtrace)>> = const { RefCell::new(None) };
}

/// (thread_name, thread_id, location, backtrace).
fn take_panic_info() -> Option<(String, String, String, Backtrace)> {
    LAST_PANIC_INFO.with(|info| info.borrow_mut().take())
}

/// Format a backtrace, optionally filtering to "short" format.
///
/// Short format shows only frames between `__rust_end_short_backtrace` and
/// `__rust_begin_short_backtrace` markers, matching the default Rust panic handler.
/// Frame numbers are renumbered to start at 0.
fn format_backtrace(bt: &Backtrace, full: bool) -> String {
    let backtrace_str = format!("{}", bt);

    if full {
        return backtrace_str;
    }

    // Filter to short backtrace: keep lines between the markers
    // Frame groups look like:
    //    N: function::name
    //              at /path/to/file.rs:123:45
    let lines: Vec<&str> = backtrace_str.lines().collect();
    let mut start_idx = 0;
    let mut end_idx = lines.len();

    for (i, line) in lines.iter().enumerate() {
        if line.contains("__rust_end_short_backtrace") {
            // Skip past this frame (find the next frame number)
            for (j, next_line) in lines.iter().enumerate().skip(i + 1) {
                if next_line
                    .trim_start()
                    .chars()
                    .next()
                    .map(|c| c.is_ascii_digit())
                    .unwrap_or(false)
                {
                    start_idx = j;
                    break;
                }
            }
        }
        if line.contains("__rust_begin_short_backtrace") {
            // Find the start of this frame (the line with the frame number)
            for (j, prev_line) in lines
                .iter()
                .enumerate()
                .take(i + 1)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
            {
                if prev_line
                    .trim_start()
                    .chars()
                    .next()
                    .map(|c| c.is_ascii_digit())
                    .unwrap_or(false)
                {
                    end_idx = j;
                    break;
                }
            }
            break;
        }
    }

    // Renumber frames starting at 0
    let filtered: Vec<&str> = lines[start_idx..end_idx].to_vec();
    let mut new_frame_num = 0usize;
    let mut result = Vec::new();

    for line in filtered {
        let trimmed = line.trim_start();
        if trimmed
            .chars()
            .next()
            .map(|c| c.is_ascii_digit())
            .unwrap_or(false)
        {
            // This is a frame number line like "   8: function_name"
            // Find where the number ends (at the colon)
            if let Some(colon_pos) = trimmed.find(':') {
                let rest = &trimmed[colon_pos..];
                // Preserve original indentation style (right-aligned numbers)
                result.push(format!("{:>4}{}", new_frame_num, rest));
                new_frame_num += 1;
            } else {
                result.push(line.to_string());
            }
        } else {
            result.push(line.to_string());
        }
    }

    result.join("\n")
}

// Panic unconditionally prints to stderr, even if it's caught later. This results in
// messy output during shrinking. To avoid this, we replace the panic hook with our
// own that suppresses the printing except for the final replay.
//
// This is called once per process, the first time any hegel test runs.
fn init_panic_hook() {
    PANIC_HOOK_INIT.call_once(|| {
        let prev_hook = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            if !currently_in_test_context() {
                // use actual panic hook outside of tests
                prev_hook(info);
                return;
            }

            let thread = std::thread::current();
            let thread_name = thread.name().unwrap_or("<unnamed>").to_string();
            // ThreadId's debug output is ThreadId(N)
            let thread_id = format!("{:?}", thread.id())
                .trim_start_matches("ThreadId(")
                .trim_end_matches(')')
                .to_string();
            let location = info
                .location()
                .map(|loc| format!("{}:{}:{}", loc.file(), loc.line(), loc.column()))
                .unwrap_or_else(|| "<unknown>".to_string());

            let backtrace = Backtrace::capture();

            LAST_PANIC_INFO
                .with(|l| *l.borrow_mut() = Some((thread_name, thread_id, location, backtrace)));
        }));
    });
}

fn ensure_hegel_installed() -> Result<String, String> {
    let venv_dir = format!("{HEGEL_SERVER_DIR}/venv");
    let version_file = format!("{venv_dir}/hegel-version");
    let hegel_bin = format!("{venv_dir}/bin/hegel");
    let install_log = format!("{HEGEL_SERVER_DIR}/install.log");

    // Check cached version
    if let Ok(cached) = std::fs::read_to_string(&version_file) {
        if cached.trim() == HEGEL_SERVER_VERSION && std::path::Path::new(&hegel_bin).is_file() {
            return Ok(hegel_bin);
        }
    }

    std::fs::create_dir_all(HEGEL_SERVER_DIR)
        .map_err(|e| format!("Failed to create {HEGEL_SERVER_DIR}: {e}"))?;

    let log_file = std::fs::File::create(&install_log)
        .map_err(|e| format!("Failed to create install log: {e}"))?;

    let status = std::process::Command::new("uv")
        .args(["venv", "--clear", &venv_dir])
        .stderr(log_file.try_clone().unwrap())
        .stdout(log_file.try_clone().unwrap())
        .status();
    match &status {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(UV_NOT_FOUND_MESSAGE.to_string());
        }
        Err(e) => {
            return Err(format!("Failed to run `uv venv`: {e}"));
        }
        Ok(s) if !s.success() => {
            let log = std::fs::read_to_string(&install_log).unwrap_or_default();
            return Err(format!("uv venv failed. Install log:\n{log}"));
        }
        Ok(_) => {}
    }

    let python_path = format!("{venv_dir}/bin/python");
    let status = std::process::Command::new("uv")
        .args([
            "pip",
            "install",
            "--python",
            &python_path,
            &format!("hegel-core=={HEGEL_SERVER_VERSION}"),
        ])
        .stderr(log_file.try_clone().unwrap())
        .stdout(log_file)
        .status()
        .map_err(|e| format!("Failed to run `uv pip install`: {e}"))?;
    if !status.success() {
        let log = std::fs::read_to_string(&install_log).unwrap_or_default();
        return Err(format!(
            "Failed to install hegel-core (version: {HEGEL_SERVER_VERSION}). \
             Set {HEGEL_SERVER_COMMAND_ENV} to a hegel binary path to skip installation.\n\
             Install log:\n{log}"
        ));
    }

    if !std::path::Path::new(&hegel_bin).is_file() {
        return Err(format!("hegel not found at {hegel_bin} after installation"));
    }

    std::fs::write(&version_file, HEGEL_SERVER_VERSION)
        .map_err(|e| format!("Failed to write version file: {e}"))?;

    Ok(hegel_bin)
}

fn server_log_file() -> File {
    let file = SERVER_LOG_FILE.get_or_init(|| {
        std::fs::create_dir_all(HEGEL_SERVER_DIR).ok();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!("{HEGEL_SERVER_DIR}/server.log"))
            .expect("Failed to open server log file");
        Mutex::new(file)
    });
    file.lock()
        .unwrap()
        .try_clone()
        .expect("Failed to clone server log file handle")
}

fn find_hegel() -> String {
    if let Ok(override_path) = std::env::var(HEGEL_SERVER_COMMAND_ENV) {
        return override_path;
    }
    HEGEL_SERVER_COMMAND
        .get_or_init(|| ensure_hegel_installed().unwrap_or_else(|e| panic!("{e}")))
        .clone()
}

/// Health checks that can be suppressed during test execution.
///
/// Health checks detect common issues with test configuration that would
/// otherwise cause tests to run inefficiently or not at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HealthCheck {
    /// Too many test cases are being filtered out via `assume()`.
    FilterTooMuch,
    /// Test execution is too slow.
    TooSlow,
    /// Generated test cases are too large.
    TestCasesTooLarge,
    /// The smallest natural input is very large.
    LargeInitialTestCase,
}

impl HealthCheck {
    /// Returns all health check variants.
    ///
    /// Useful for suppressing all health checks at once:
    ///
    /// ```no_run
    /// use hegel::HealthCheck;
    ///
    /// #[hegel::test(suppress_health_check = HealthCheck::all())]
    /// fn my_test(tc: hegel::TestCase) {
    ///     // ...
    /// }
    /// ```
    pub const fn all() -> [HealthCheck; 4] {
        [
            HealthCheck::FilterTooMuch,
            HealthCheck::TooSlow,
            HealthCheck::TestCasesTooLarge,
            HealthCheck::LargeInitialTestCase,
        ]
    }

    fn as_str(&self) -> &'static str {
        match self {
            HealthCheck::FilterTooMuch => "filter_too_much",
            HealthCheck::TooSlow => "too_slow",
            HealthCheck::TestCasesTooLarge => "test_cases_too_large",
            HealthCheck::LargeInitialTestCase => "large_initial_test_case",
        }
    }
}

/// Controls how much output Hegel produces during test runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verbosity {
    /// Suppress all output.
    Quiet,
    /// Default output level.
    Normal,
    /// Show more detail about the test run.
    Verbose,
    /// Show protocol-level debug information.
    Debug,
}

impl Verbosity {}

// internal use only
#[doc(hidden)]
pub fn hegel<F>(test_fn: F)
where
    F: FnMut(TestCase),
{
    Hegel::new(test_fn).run();
}

fn is_in_ci() -> bool {
    const CI_VARS: &[(&str, Option<&str>)] = &[
        ("CI", None),
        ("TF_BUILD", Some("true")),
        ("BUILDKITE", Some("true")),
        ("CIRCLECI", Some("true")),
        ("CIRRUS_CI", Some("true")),
        ("CODEBUILD_BUILD_ID", None),
        ("GITHUB_ACTIONS", Some("true")),
        ("GITLAB_CI", None),
        ("HEROKU_TEST_RUN_ID", None),
        ("TEAMCITY_VERSION", None),
        ("bamboo.buildKey", None),
    ];

    CI_VARS.iter().any(|(key, value)| match value {
        None => std::env::var_os(key).is_some(),
        Some(expected) => std::env::var(key).ok().as_deref() == Some(expected),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Database {
    Unset,
    Disabled,
    Path(String),
}

#[cfg(feature = "rust-core")]
struct LocalExampleDatabase {
    inner: ExampleDatabase,
}

#[cfg(feature = "rust-core")]
impl LocalExampleDatabase {
    fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            inner: ExampleDatabase::new(path),
        }
    }

    fn fetch(&self, key: &[u8]) -> Vec<Vec<u8>> {
        self.inner.fetch(key).unwrap_or_default()
    }

    fn delete(&self, key: &[u8], value: &[u8]) {
        let _ = self.inner.delete(key, value);
    }

    fn save_corpus_replacement(
        &self,
        database_key: &[u8],
        primary_bytes: &[u8],
        demoted_primary_bytes: &[Vec<u8>],
    ) {
        let _ = save_corpus_replacement(
            &self.inner,
            database_key,
            primary_bytes,
            demoted_primary_bytes,
        );
    }
}

#[cfg(feature = "rust-core")]
fn shortlex_sort(values: &mut [Vec<u8>]) {
    values.sort_by(|left, right| shortlex_cmp(left, right));
}

#[cfg(feature = "rust-core")]
fn append_local_history_trace(event: &str, bytes: &[u8]) {
    let Some(path) = std::env::var_os("HEGEL_TRACE_LOCAL_HISTORY") else {
        return;
    };
    let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) else {
        return;
    };
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    let _ = writeln!(file, "{event} {hex}");
}

#[cfg(feature = "rust-core")]
fn append_local_float_list_trace(event: &str, values: &[f64], bytes: &[u8]) {
    let Some(path) = std::env::var_os("HEGEL_TRACE_LOCAL_HISTORY") else {
        return;
    };
    let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) else {
        return;
    };
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    let values = values
        .iter()
        .map(|value| format!("{value:.17}"))
        .collect::<Vec<_>>()
        .join(",");
    let _ = writeln!(file, "{event} [{values}] {hex}");
}

/// Configuration for a Hegel test run.
///
/// Use builder methods to customize, then pass to [`Hegel::settings`] or
/// the `settings` parameter of `#[hegel::test]`.
///
/// In CI environments (detected automatically), the database is disabled
/// and tests are derandomized by default.
#[derive(Debug, Clone)]
pub struct Settings {
    test_cases: u64,
    verbosity: Verbosity,
    seed: Option<u64>,
    derandomize: bool,
    database: Database,
    suppress_health_check: Vec<HealthCheck>,
}

impl Settings {
    /// Create settings with defaults. Detects CI environments automatically.
    pub fn new() -> Self {
        let in_ci = is_in_ci();
        Self {
            test_cases: 100,
            verbosity: Verbosity::Normal,
            seed: None,
            derandomize: in_ci,
            database: if in_ci {
                Database::Disabled
            } else {
                Database::Unset
            },
            suppress_health_check: Vec::new(),
        }
    }

    /// Set the number of test cases to run (default: 100).
    pub fn test_cases(mut self, n: u64) -> Self {
        self.test_cases = n;
        self
    }

    /// Set the verbosity level.
    pub fn verbosity(mut self, verbosity: Verbosity) -> Self {
        self.verbosity = verbosity;
        self
    }

    /// Set a fixed seed for reproducibility, or `None` for random.
    pub fn seed(mut self, seed: Option<u64>) -> Self {
        self.seed = seed;
        self
    }

    /// When true, use a fixed seed derived from the test name. Enabled by default in CI.
    pub fn derandomize(mut self, derandomize: bool) -> Self {
        self.derandomize = derandomize;
        self
    }

    /// Set the database path for storing failing examples, or `None` to disable.
    pub fn database(mut self, database: Option<String>) -> Self {
        self.database = match database {
            None => Database::Disabled,
            Some(path) => Database::Path(path),
        };
        self
    }

    /// Suppress one or more health checks so they do not cause test failure.
    ///
    /// Health checks detect common issues like excessive filtering or slow
    /// tests. Use this to suppress specific checks when they are expected.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use hegel::{HealthCheck, Verbosity};
    /// use hegel::generators as gs;
    ///
    /// #[hegel::test(suppress_health_check = [HealthCheck::FilterTooMuch, HealthCheck::TooSlow])]
    /// fn my_test(tc: hegel::TestCase) {
    ///     let n: i32 = tc.draw(gs::integers());
    ///     tc.assume(n > 0);
    /// }
    /// ```
    pub fn suppress_health_check(mut self, checks: impl IntoIterator<Item = HealthCheck>) -> Self {
        self.suppress_health_check.extend(checks);
        self
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self::new()
    }
}

// internal use only
#[doc(hidden)]
pub struct Hegel<F> {
    test_fn: F,
    database_key: Option<String>,
    test_location: Option<TestLocation>,
    settings: Settings,
}

impl<F> Hegel<F>
where
    F: FnMut(TestCase),
{
    /// Create a new test builder with default settings.
    pub fn new(test_fn: F) -> Self {
        Self {
            test_fn,
            database_key: None,
            settings: Settings::new(),
            test_location: None,
        }
    }

    /// Override the default settings.
    pub fn settings(mut self, settings: Settings) -> Self {
        self.settings = settings;
        self
    }

    #[doc(hidden)]
    pub fn __database_key(mut self, key: String) -> Self {
        self.database_key = Some(key);
        self
    }

    #[doc(hidden)]
    pub fn test_location(mut self, location: TestLocation) -> Self {
        self.test_location = Some(location);
        self
    }

    /// Run the property-based tests.
    pub fn run(self) {
        #[cfg(feature = "rust-core")]
        return self.run_local();

        #[cfg(not(feature = "rust-core"))]
        {
            let session = HegelSession::get();
            let connection = &session.connection;

            let mut test_fn = self.test_fn;
            let verbosity = self.settings.verbosity;
            let got_interesting = Arc::new(AtomicBool::new(false));
            let mut test_channel = connection.new_channel();

            let suppress_names: Vec<Value> = self
                .settings
                .suppress_health_check
                .iter()
                .map(|c| Value::Text(c.as_str().to_string()))
                .collect();

            let database_key_bytes = self
                .database_key
                .map_or(Value::Null, |k| Value::Bytes(k.into_bytes()));

            let mut run_test_msg = cbor_map! {
                "command" => "run_test",
                "test_cases" => self.settings.test_cases,
                "seed" => self.settings.seed.map_or(Value::Null, Value::from),
                "channel_id" => test_channel.channel_id,
                "database_key" => database_key_bytes,
                "derandomize" => self.settings.derandomize
            };
            let db_value = match &self.settings.database {
                Database::Unset => Option::None,
                Database::Disabled => Some(Value::Null),
                Database::Path(s) => Some(Value::Text(s.clone())),
            };
            if let Some(db) = db_value {
                if let Value::Map(ref mut map) = run_test_msg {
                    map.push((Value::Text("database".to_string()), db));
                }
            }
            if !suppress_names.is_empty() {
                if let Value::Map(ref mut map) = run_test_msg {
                    map.push((
                        Value::Text("suppress_health_check".to_string()),
                        Value::Array(suppress_names),
                    ));
                }
            }

            // The control channel is behind a Mutex because Channel requires &mut self.
            // This only serializes the brief run_test send/receive — actual test
            // execution happens on per-test channels without holding this lock.
            {
                let mut control = session.control.lock().unwrap();
                let run_test_id = control
                    .send_request(cbor_encode(&run_test_msg))
                    .expect("Failed to send run_test");

                let run_test_response = control
                    .receive_reply(run_test_id)
                    .expect("Failed to receive run_test response");
                let _run_test_result: Value = cbor_decode(&run_test_response);
            }

            if verbosity == Verbosity::Debug {
                eprintln!("run_test response received");
            }

            let result_data: Value;
            let ack_null = cbor_map! {"result" => Value::Null};
            loop {
                // Handle the server dying between events: receive_request will
                // fail with RecvError once the background reader clears the senders.
                let (event_id, event_payload) = match test_channel.receive_request() {
                    Ok(event) => event,
                    Err(_) if connection.server_has_exited() => {
                        panic!("{}", SERVER_CRASHED_MESSAGE);
                    }
                    Err(e) => unreachable!("Failed to receive event (server still running): {}", e),
                };

                let event: Value = cbor_decode(&event_payload);
                let event_type = map_get(&event, "event")
                    .and_then(as_text)
                    .expect("Expected event in payload");

                if verbosity == Verbosity::Debug {
                    eprintln!("Received event: {:?}", event);
                }

                match event_type {
                    "test_case" => {
                        let channel_id = map_get(&event, "channel_id")
                            .and_then(as_u64)
                            .expect("Missing channel id")
                            as u32;

                        let test_case_channel = connection.connect_channel(channel_id);

                        // Ack the test_case event BEFORE running the test (prevents deadlock)
                        test_channel
                            .write_reply(event_id, cbor_encode(&ack_null))
                            .expect("Failed to ack test_case");

                        run_test_case(
                            TestBackend::Remote {
                                connection,
                                test_channel: test_case_channel,
                            },
                            &mut test_fn,
                            false,
                            verbosity,
                            &got_interesting,
                        );
                    }
                    "test_done" => {
                        let ack_true = cbor_map! {"result" => true};
                        test_channel
                            .write_reply(event_id, cbor_encode(&ack_true))
                            .expect("Failed to ack test_done");
                        result_data = map_get(&event, "results").cloned().unwrap_or(Value::Null);
                        break;
                    }
                    _ => {
                        panic!("unknown event: {}", event_type);
                    }
                }
            }

            // Check for server-side errors before processing results
            if let Some(error_msg) = map_get(&result_data, "error").and_then(as_text) {
                panic!("Server error: {}", error_msg);
            }

            // Check for health check failure before processing results
            if let Some(failure_msg) =
                map_get(&result_data, "health_check_failure").and_then(as_text)
            {
                panic!("Health check failure:\n{}", failure_msg);
            }

            // Check for flaky test detection
            if let Some(flaky_msg) = map_get(&result_data, "flaky").and_then(as_text) {
                panic!("Flaky test detected: {}", flaky_msg);
            }

            let n_interesting = map_get(&result_data, "interesting_test_cases")
                .and_then(as_u64)
                .unwrap_or(0);

            if verbosity == Verbosity::Debug {
                eprintln!("Test done. interesting_test_cases={}", n_interesting);
            }

            // Process final replay test cases (one per interesting example)
            let mut final_result: Option<TestCaseResult> = None;
            for _ in 0..n_interesting {
                let (event_id, event_payload) = test_channel
                    .receive_request()
                    .expect("Failed to receive final test_case");

                let event: Value = cbor_decode(&event_payload);
                let event_type = map_get(&event, "event").and_then(as_text);
                assert_eq!(event_type, Some("test_case"));

                let channel_id = map_get(&event, "channel_id")
                    .and_then(as_u64)
                    .expect("Missing channel id") as u32;

                let test_case_channel = connection.connect_channel(channel_id);

                test_channel
                    .write_reply(event_id, cbor_encode(&ack_null))
                    .expect("Failed to ack final test_case");

                let tc_result = run_test_case(
                    TestBackend::Remote {
                        connection,
                        test_channel: test_case_channel,
                    },
                    &mut test_fn,
                    true,
                    verbosity,
                    &got_interesting,
                );

                if matches!(&tc_result, TestCaseResult::Interesting { .. }) {
                    final_result = Some(tc_result);
                }

                if connection.server_has_exited() {
                    panic!("{}", SERVER_CRASHED_MESSAGE);
                }
            }

            let passed = map_get(&result_data, "passed")
                .and_then(as_bool)
                .unwrap_or(true);

            let test_failed = !passed || got_interesting.load(Ordering::SeqCst);

            if is_running_in_antithesis() {
                #[cfg(not(feature = "antithesis"))]
                panic!(
                    "When Hegel is run inside of Antithesis, it requires the `antithesis` feature. \
                You can add it with {{ features = [\"antithesis\"] }}."
                );

                #[cfg(feature = "antithesis")]
                if let Some(ref loc) = self.test_location {
                    crate::antithesis::emit_assertion(loc, !test_failed);
                }
            }

            if test_failed {
                let msg = match &final_result {
                    Some(TestCaseResult::Interesting { panic_message, .. }) => {
                        panic_message.as_str()
                    }
                    _ => "unknown",
                };
                panic!("Property test failed: {}", msg);
            }
        }
    }
}

#[cfg(feature = "rust-core")]
impl<F> Hegel<F>
where
    F: FnMut(TestCase),
{
    fn run_local(self) {
        let mut test_fn = self.test_fn;
        let verbosity = self.settings.verbosity;
        let got_interesting = Arc::new(AtomicBool::new(false));
        let mut replay_plans = Vec::new();
        let mut exact_replay_origins = std::collections::HashSet::new();
        let database = match &self.settings.database {
            Database::Path(path) => Some(LocalExampleDatabase::new(path)),
            Database::Unset | Database::Disabled => None,
        };
        let database_key = self.database_key.as_deref().map(str::as_bytes);
        let base_seed = self.settings.seed.unwrap_or(0);
        let mut valid_examples = 0u64;
        let mut invalid_examples = 0u64;
        let suppress_filter_too_much = self
            .settings
            .suppress_health_check
            .contains(&HealthCheck::FilterTooMuch);

        if let (Some(database), Some(database_key)) = (&database, database_key) {
            let mut corpus = database.fetch(database_key);
            shortlex_sort(&mut corpus);
            for existing in corpus {
                let Some(replay_choices) = choices_from_bytes(&existing) else {
                    database.delete(database_key, &existing);
                    continue;
                };
                let backend = Rc::new(RefCell::new(LocalBackend::from_choices(
                    replay_choices.clone(),
                )));
                let tc_result = run_test_case(
                    TestBackend::Local {
                        backend: Rc::clone(&backend),
                    },
                    &mut test_fn,
                    false,
                    verbosity,
                    &got_interesting,
                );
                match tc_result {
                    TestCaseResult::Interesting { origin, .. } => {
                        valid_examples += 1;
                        exact_replay_origins.insert(origin.clone());
                        replay_plans.push(LocalReplayPlan {
                            origin,
                            seed: None,
                            replay_choices: Some(replay_choices),
                            forced_prefix_values: Vec::new(),
                            forced_value: None,
                            downgraded_primary_bytes: Vec::new(),
                        });
                    }
                    TestCaseResult::Valid => {
                        valid_examples += 1;
                        database.delete(database_key, &existing);
                    }
                    TestCaseResult::Invalid => {
                        invalid_examples += 1;
                        database.delete(database_key, &existing);
                    }
                }
            }
        }

        if self.settings.test_cases > 0 {
            let simplest_backend = Rc::new(RefCell::new(LocalBackend::simplest()));
            let simplest_result = run_test_case(
                TestBackend::Local {
                    backend: Rc::clone(&simplest_backend),
                },
                &mut test_fn,
                false,
                verbosity,
                &got_interesting,
            );
            match simplest_result {
                TestCaseResult::Valid => {
                    valid_examples += 1;
                }
                TestCaseResult::Invalid => {
                    invalid_examples += 1;
                    if !suppress_filter_too_much
                        && invalid_examples
                            > INVALID_THRESHOLD_BASE + INVALID_PER_VALID * valid_examples
                    {
                        panic!(
                            "Health check failure:\n{}",
                            local_filter_too_much_message(valid_examples, invalid_examples)
                        );
                    }
                }
                TestCaseResult::Interesting { origin, .. } => {
                    valid_examples += 1;
                    let recorded_choices = simplest_backend.borrow().recorded_choices().to_vec();
                    exact_replay_origins.insert(origin.clone());
                    replay_plans.push(LocalReplayPlan {
                        origin,
                        seed: None,
                        replay_choices: Some(recorded_choices),
                        forced_prefix_values: Vec::new(),
                        forced_value: None,
                        downgraded_primary_bytes: Vec::new(),
                    });
                }
            }
        }

        for case_index in 0..self.settings.test_cases.saturating_sub(1) {
            let seed = base_seed.wrapping_add(case_index);
            let backend = Rc::new(RefCell::new(LocalBackend::from_seed(seed)));
            let tc_result = run_test_case(
                TestBackend::Local {
                    backend: Rc::clone(&backend),
                },
                &mut test_fn,
                false,
                verbosity,
                &got_interesting,
            );
            match tc_result {
                TestCaseResult::Valid => {
                    valid_examples += 1;
                    if replay_plans.is_empty() {
                        let observed_values = backend.borrow().observed_values().to_vec();
                        let recorded_choices = backend.borrow().recorded_choices().to_vec();
                        for forced_prefix_values in
                            local_integer_containment_mutations(&observed_values)
                        {
                            let mutation_backend = Rc::new(RefCell::new(
                                LocalBackend::from_choices(recorded_choices.clone()),
                            ));
                            mutation_backend
                                .borrow_mut()
                                .force_values(forced_prefix_values.clone());
                            let mutation_result = run_test_case(
                                TestBackend::Local {
                                    backend: Rc::clone(&mutation_backend),
                                },
                                &mut test_fn,
                                false,
                                verbosity,
                                &got_interesting,
                            );
                            if let TestCaseResult::Interesting { origin, .. } = mutation_result {
                                let recorded_choices =
                                    mutation_backend.borrow().recorded_choices().to_vec();
                                replay_plans.push(LocalReplayPlan {
                                    origin,
                                    seed: Some(seed),
                                    replay_choices: Some(recorded_choices),
                                    forced_prefix_values,
                                    forced_value: None,
                                    downgraded_primary_bytes: Vec::new(),
                                });
                                break;
                            }
                        }
                    }
                }
                TestCaseResult::Invalid => {
                    invalid_examples += 1;
                    if !suppress_filter_too_much
                        && invalid_examples
                            > INVALID_THRESHOLD_BASE + INVALID_PER_VALID * valid_examples
                    {
                        panic!(
                            "Health check failure:\n{}",
                            local_filter_too_much_message(valid_examples, invalid_examples)
                        );
                    }
                }
                TestCaseResult::Interesting { origin, .. } => {
                    valid_examples += 1;
                    let recorded_choices = backend.borrow().recorded_choices().to_vec();
                    replay_plans.push(LocalReplayPlan {
                        origin,
                        seed: Some(seed),
                        replay_choices: Some(recorded_choices),
                        forced_prefix_values: Vec::new(),
                        forced_value: None,
                        downgraded_primary_bytes: Vec::new(),
                    });
                    if database.is_none() {
                        break;
                    }
                }
            }
        }

        let mut final_result: Option<TestCaseResult> = None;
        let final_plans = if self.settings.derandomize {
            let mut best_by_origin: std::collections::HashMap<String, LocalReplayPlan> =
                std::collections::HashMap::new();
            for plan in replay_plans
                .iter()
                .filter(|plan| plan.sort_key().is_some())
                .cloned()
            {
                match best_by_origin.get(&plan.origin) {
                    Some(existing) if existing.sort_key().as_ref() <= plan.sort_key().as_ref() => {}
                    _ => {
                        best_by_origin.insert(plan.origin.clone(), plan);
                    }
                }
            }
            if best_by_origin.is_empty() {
                replay_plans
            } else {
                best_by_origin.into_values().collect()
            }
        } else {
            replay_plans
        };
        let mut best_final_choices: Option<Vec<Choice>> = None;
        let mut best_final_bytes: Option<Vec<u8>> = None;
        let mut best_final_display_plan: Option<LocalReplayPlan> = None;
        let mut best_final_display_sort_key: Option<ExampleSortKey> = None;
        let mut downgraded_primary_bytes = Vec::new();
        let mut saved_primary_by_origin: std::collections::HashMap<String, Vec<u8>> =
            std::collections::HashMap::new();
        for plan in final_plans {
            let mut plan = plan;
            if self.settings.derandomize && plan.forced_value.is_none() {
                let backend = Rc::new(RefCell::new(plan.backend()));
                if !plan.forced_prefix_values.is_empty() {
                    backend
                        .borrow_mut()
                        .force_values(plan.forced_prefix_values.clone());
                }
                let tc_result = run_test_case(
                    TestBackend::Local {
                        backend: Rc::clone(&backend),
                    },
                    &mut test_fn,
                    false,
                    verbosity,
                    &got_interesting,
                );
                if matches!(tc_result, TestCaseResult::Interesting { .. }) {
                    let recorded_choices = backend.borrow().recorded_choices().to_vec();
                    let observed_values = backend.borrow().observed_values().to_vec();
                    if let Some((forced_value, forced_second_value)) =
                        shrink_local_integer_containment_observation(
                            plan.seed.unwrap_or(0),
                            &observed_values,
                            &mut test_fn,
                            verbosity,
                            &got_interesting,
                        )
                    {
                        plan.forced_value = Some(forced_value);
                        if plan.forced_prefix_values.len() >= 2 {
                            plan.forced_prefix_values[1] = forced_second_value;
                        }
                    } else if let Some(forced_prefix_values) =
                        shrink_local_integer_pair_observation(
                            plan.seed.unwrap_or(0),
                            &observed_values,
                            &mut test_fn,
                            verbosity,
                            &got_interesting,
                        )
                    {
                        plan.forced_prefix_values = forced_prefix_values;
                        plan.forced_value = None;
                    } else if let Some(forced_prefix_values) =
                        shrink_local_separated_integer_pair_observation(
                            plan.seed.unwrap_or(0),
                            &observed_values,
                            &mut test_fn,
                            verbosity,
                            &got_interesting,
                        )
                    {
                        plan.forced_prefix_values = forced_prefix_values;
                        plan.forced_value = None;
                    } else if let Some(replay_choices) =
                        observed_values.first().and_then(|(schema, value)| {
                            shrink_local_one_of_observation(
                                plan.seed.unwrap_or(0),
                                schema,
                                value,
                                &mut test_fn,
                                verbosity,
                                &got_interesting,
                            )
                        })
                    {
                        plan.replay_choices = Some(replay_choices);
                        plan.forced_prefix_values = Vec::new();
                        plan.forced_value = None;
                    } else if let Some(result) =
                        backend
                            .borrow()
                            .observed_first_value()
                            .and_then(|(schema, value)| {
                                shrink_local_observation(
                                    plan.seed.unwrap_or(0),
                                    &schema,
                                    &value,
                                    &choices_to_bytes(&recorded_choices),
                                    &mut test_fn,
                                    verbosity,
                                    &got_interesting,
                                )
                            })
                    {
                        plan.forced_value = Some(result.forced_value);
                        plan.downgraded_primary_bytes = result.downgraded_primary_bytes;
                    }
                }
            }
            let backend = Rc::new(RefCell::new(plan.backend()));
            if let Some(value) = &plan.forced_value {
                let mut forced_values = vec![value.clone().into_data_value()];
                forced_values.extend(plan.forced_prefix_values.iter().skip(1).cloned());
                backend.borrow_mut().force_values(forced_values);
            } else if !plan.forced_prefix_values.is_empty() {
                backend
                    .borrow_mut()
                    .force_values(plan.forced_prefix_values.clone());
            }
            let tc_result = run_test_case(
                TestBackend::Local {
                    backend: Rc::clone(&backend),
                },
                &mut test_fn,
                true,
                verbosity,
                &got_interesting,
            );
            if let TestCaseResult::Interesting { origin, .. } = &tc_result {
                let recorded_choices = backend.borrow().recorded_choices().to_vec();
                let recorded_bytes = choices_to_bytes(&recorded_choices);
                if let (Some(database), Some(database_key)) = (&database, database_key) {
                    let existing = saved_primary_by_origin.get(origin).map(Vec::as_slice);
                    if save_interesting_origin_replacement(
                        &database.inner,
                        database_key,
                        existing,
                        &recorded_bytes,
                    )
                    .unwrap_or(false)
                    {
                        if let Some(existing) = existing {
                            append_local_history_trace("saved-secondary", existing);
                        }
                        saved_primary_by_origin.insert(origin.clone(), recorded_bytes.clone());
                    }
                }
                if best_final_bytes.as_ref().is_none_or(|existing| {
                    shortlex_cmp(&recorded_bytes, existing) == CmpOrdering::Less
                }) {
                    append_local_history_trace("accepted-primary", &recorded_bytes);
                    downgraded_primary_bytes.extend(plan.downgraded_primary_bytes.clone());
                    if let Some(existing) = &best_final_bytes {
                        append_local_history_trace("demoted-primary", existing);
                        downgraded_primary_bytes.push(existing.clone());
                    }
                    best_final_bytes = Some(recorded_bytes.clone());
                    best_final_choices = Some(recorded_choices);
                }
                let display_sort_key = {
                    let recorded_bytes = recorded_bytes.clone();
                    ExampleSortKey::from_parts(
                        recorded_bytes.len(),
                        recorded_bytes.into_iter().map(u128::from).collect(),
                    )
                };
                if best_final_display_sort_key
                    .as_ref()
                    .is_none_or(|existing| &display_sort_key < existing)
                {
                    best_final_display_sort_key = Some(display_sort_key);
                    best_final_display_plan = Some(plan.clone());
                }
                final_result = Some(tc_result);
            }
        }

        if let Some(best_plan) = best_final_display_plan {
            let backend = Rc::new(RefCell::new(best_plan.backend()));
            if let Some(value) = &best_plan.forced_value {
                let mut forced_values = vec![value.clone().into_data_value()];
                forced_values.extend(best_plan.forced_prefix_values.iter().skip(1).cloned());
                backend.borrow_mut().force_values(forced_values);
            } else if !best_plan.forced_prefix_values.is_empty() {
                backend
                    .borrow_mut()
                    .force_values(best_plan.forced_prefix_values.clone());
            }
            let tc_result = run_test_case(
                TestBackend::Local {
                    backend: Rc::clone(&backend),
                },
                &mut test_fn,
                true,
                verbosity,
                &got_interesting,
            );
            if matches!(&tc_result, TestCaseResult::Interesting { .. }) {
                final_result = Some(tc_result);
            }
        }

        if let (Some(database), Some(database_key), Some(choices)) =
            (&database, database_key, best_final_choices.as_ref())
        {
            let bytes = choices_to_bytes(choices);
            for previous_primary in &downgraded_primary_bytes {
                if previous_primary == &bytes {
                    continue;
                }
                append_local_history_trace("saved-secondary", &previous_primary);
            }
            database.save_corpus_replacement(database_key, &bytes, &downgraded_primary_bytes);
        }

        if got_interesting.load(Ordering::SeqCst) {
            let msg = match &final_result {
                Some(TestCaseResult::Interesting { panic_message, .. }) => panic_message.as_str(),
                _ => "unknown",
            };
            panic!("Property test failed: {}", msg);
        }
    }
}

#[cfg(feature = "rust-core")]
fn local_filter_too_much_message(valid_examples: u64, invalid_examples: u64) -> String {
    format!(
        "It looks like this test is filtering out a lot of inputs. \
{valid_examples} inputs were generated successfully, while {invalid_examples} inputs were filtered out. \
\n\n\
An input might be filtered out by calls to assume(), strategy.filter(...), or occasionally by Hypothesis internals.\
\n\n\
Applying this much filtering makes input generation slow, since Hypothesis must discard inputs which are filtered out and try generating it again. It is also possible that applying this much filtering will distort the domain and/or distribution of the test, leaving your testing less rigorous than expected.\
\n\n\
If you expect this many inputs to be filtered out during generation, you can disable this health check with @settings(suppress_health_check=[HealthCheck.filter_too_much]). See https://hypothesis.readthedocs.io/en/latest/reference/api.html#hypothesis.HealthCheck for details."
    )
}

#[cfg(feature = "rust-core")]
#[derive(Clone, Debug)]
struct LocalShrinkResult {
    forced_value: ForcedLocalValue,
    downgraded_primary_bytes: Vec<Vec<u8>>,
}

#[cfg(feature = "rust-core")]
#[derive(Clone, Debug)]
struct LocalReplayPlan {
    origin: String,
    seed: Option<u64>,
    replay_choices: Option<Vec<Choice>>,
    forced_prefix_values: Vec<DataValue>,
    forced_value: Option<ForcedLocalValue>,
    downgraded_primary_bytes: Vec<Vec<u8>>,
}

#[cfg(feature = "rust-core")]
impl LocalReplayPlan {
    fn sort_key(&self) -> Option<ExampleSortKey> {
        self.forced_value
            .as_ref()
            .map(ForcedLocalValue::sort_key)
            .or_else(|| {
                self.replay_choices
                    .as_ref()
                    .map(|choices| ExampleSortKey::from_choices(choices))
            })
    }

    fn backend(&self) -> LocalBackend {
        match (&self.replay_choices, self.seed) {
            (Some(choices), _) => LocalBackend::from_choices(choices.clone()),
            (None, Some(seed)) => LocalBackend::from_seed(seed),
            (None, None) => unreachable!("local replay plan requires a seed or exact choices"),
        }
    }
}

#[cfg(feature = "rust-core")]
fn local_integer_containment_mutations(
    observed_values: &[(Schema, DataValue)],
) -> Vec<Vec<DataValue>> {
    let [
        (
            Schema::List {
                elements,
                unique: false,
                ..
            },
            DataValue::List(values),
        ),
        (Schema::Integer { .. }, DataValue::Integer(_)),
    ] = observed_values
    else {
        return Vec::new();
    };
    if !matches!(elements.as_ref(), Schema::Integer { .. }) {
        return Vec::new();
    }

    let mut mutations = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for value in values {
        let DataValue::Integer(value) = value else {
            return Vec::new();
        };
        if !seen.insert(*value) {
            continue;
        }
        mutations.push(vec![
            DataValue::List(values.clone()),
            DataValue::Integer(*value),
        ]);
        if mutations.len() >= 4 {
            break;
        }
    }
    mutations
}

#[cfg(feature = "rust-core")]
fn shrink_local_integer_containment_observation<F: FnMut(TestCase)>(
    seed: u64,
    observed_values: &[(Schema, DataValue)],
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> Option<(ForcedLocalValue, DataValue)> {
    let [
        (
            Schema::List {
                elements,
                min_size,
                unique,
                ..
            },
            DataValue::List(values),
        ),
        (
            Schema::Integer {
                min_value: scalar_min_value,
                max_value: scalar_max_value,
            },
            DataValue::Integer(scalar),
        ),
    ] = observed_values
    else {
        return None;
    };
    let Schema::Integer {
        min_value,
        max_value,
    } = elements.as_ref()
    else {
        return None;
    };
    let mut current_scalar = *scalar;
    let mut current_values = values
        .iter()
        .map(|value| match value {
            DataValue::Integer(value) => Some(*value),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;

    current_scalar = shrink_core_integer_observation(
        IntegerShrinkObservation {
            min_value: scalar_min_value.unwrap_or(i64::MIN),
            max_value: scalar_max_value.unwrap_or(i64::MAX),
            value: current_scalar,
        },
        |candidate| {
            let candidate_values = current_values
                .iter()
                .map(|value| {
                    if *value == current_scalar {
                        candidate
                    } else {
                        *value
                    }
                })
                .collect::<Vec<_>>();
            local_forced_values_are_interesting(
                seed,
                vec![
                    DataValue::List(
                        candidate_values
                            .into_iter()
                            .map(DataValue::Integer)
                            .collect(),
                    ),
                    DataValue::Integer(candidate),
                ],
                test_fn,
                verbosity,
                got_interesting,
            )
        },
    );
    for value in &mut current_values {
        if *value == *scalar {
            *value = current_scalar;
        }
    }

    let current_values = shrink_core_integer_list_observation(
        current_values,
        *min_size,
        min_value.unwrap_or(i64::MIN),
        max_value.unwrap_or(i64::MAX),
        *unique,
        |candidate| {
            candidate.contains(&current_scalar)
                && local_forced_values_are_interesting(
                    seed,
                    vec![
                        DataValue::List(
                            candidate.iter().copied().map(DataValue::Integer).collect(),
                        ),
                        DataValue::Integer(current_scalar),
                    ],
                    test_fn,
                    verbosity,
                    got_interesting,
                )
        },
    );

    Some((
        ForcedLocalValue::IntegerList {
            values: current_values,
            min_size: *min_size,
            element_min_value: *min_value,
            element_max_value: *max_value,
        },
        DataValue::Integer(current_scalar),
    ))
}

#[cfg(feature = "rust-core")]
fn shrink_local_integer_pair_observation<F: FnMut(TestCase)>(
    seed: u64,
    observed_values: &[(Schema, DataValue)],
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> Option<Vec<DataValue>> {
    let [
        (
            Schema::Integer {
                min_value: first_min_value,
                max_value: first_max_value,
            },
            DataValue::Integer(first),
        ),
        (
            Schema::Integer {
                min_value: second_min_value,
                max_value: second_max_value,
            },
            DataValue::Integer(second),
        ),
    ] = observed_values
    else {
        return None;
    };

    let shrunk = shrink_core_integer_pair_observation(
        [*first, *second],
        [
            first_min_value.unwrap_or(i64::MIN),
            second_min_value.unwrap_or(i64::MIN),
        ],
        [
            first_max_value.unwrap_or(i64::MAX),
            second_max_value.unwrap_or(i64::MAX),
        ],
        |candidate| {
            local_forced_values_are_interesting(
                seed,
                vec![
                    DataValue::Integer(candidate[0]),
                    DataValue::Integer(candidate[1]),
                ],
                test_fn,
                verbosity,
                got_interesting,
            )
        },
    );

    Some(vec![
        DataValue::Integer(shrunk[0]),
        DataValue::Integer(shrunk[1]),
    ])
}

#[cfg(feature = "rust-core")]
fn shrink_local_separated_integer_pair_observation<F: FnMut(TestCase)>(
    seed: u64,
    observed_values: &[(Schema, DataValue)],
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> Option<Vec<DataValue>> {
    let [
        (
            Schema::Integer {
                min_value: first_min_value,
                max_value: first_max_value,
            },
            DataValue::Integer(first),
        ),
        (middle_text_schema @ Schema::String { .. }, _),
        (middle_bool_schema @ Schema::Boolean { .. }, _),
        (middle_integer_schema @ Schema::Integer { .. }, _),
        (
            Schema::Integer {
                min_value: second_min_value,
                max_value: second_max_value,
            },
            DataValue::Integer(second),
        ),
    ] = observed_values
    else {
        return None;
    };

    let middle_text = generate_simplest_value(middle_text_schema).ok()?;
    let middle_bool = generate_simplest_value(middle_bool_schema).ok()?;
    let middle_integer = generate_simplest_value(middle_integer_schema).ok()?;
    let shrunk = shrink_core_integer_pair_observation(
        [*first, *second],
        [
            first_min_value.unwrap_or(i64::MIN),
            second_min_value.unwrap_or(i64::MIN),
        ],
        [
            first_max_value.unwrap_or(i64::MAX),
            second_max_value.unwrap_or(i64::MAX),
        ],
        |candidate| {
            local_forced_values_are_interesting(
                seed,
                vec![
                    DataValue::Integer(candidate[0]),
                    middle_text.clone(),
                    middle_bool.clone(),
                    middle_integer.clone(),
                    DataValue::Integer(candidate[1]),
                ],
                test_fn,
                verbosity,
                got_interesting,
            )
        },
    );

    Some(vec![
        DataValue::Integer(shrunk[0]),
        middle_text,
        middle_bool,
        middle_integer,
        DataValue::Integer(shrunk[1]),
    ])
}

#[cfg(feature = "rust-core")]
fn shrink_local_one_of_observation<F: FnMut(TestCase)>(
    seed: u64,
    schema: &Schema,
    value: &DataValue,
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> Option<Vec<Choice>> {
    let Schema::OneOf { options } = schema else {
        return None;
    };
    let current_index = options
        .iter()
        .position(|option| hegel_core::engine::value_conforms_to_schema(value, option))?;

    for option in &options[..current_index] {
        let candidate = generate_simplest_value(option).ok()?;
        let backend = Rc::new(RefCell::new(LocalBackend::from_seed(seed)));
        backend.borrow_mut().force_first_value(candidate.clone());
        let is_interesting = matches!(
            run_test_case(
                TestBackend::Local {
                    backend: Rc::clone(&backend),
                },
                test_fn,
                false,
                verbosity,
                got_interesting,
            ),
            TestCaseResult::Interesting { .. }
        );
        if is_interesting
            && backend
                .borrow()
                .observed_first_value()
                .is_some_and(|(_, observed_value)| observed_value.same_observed_value(&candidate))
        {
            return Some(backend.borrow().recorded_choices().to_vec());
        }
    }

    None
}

#[cfg(feature = "rust-core")]
#[derive(Clone, Debug, PartialEq)]
enum ForcedLocalValue {
    Float {
        value: f64,
        min_value: Option<f64>,
        max_value: Option<f64>,
        allow_nan: bool,
        allow_infinity: bool,
    },
    Integer {
        value: i64,
        min_value: Option<i64>,
        max_value: Option<i64>,
    },
    IntegerList {
        values: Vec<i64>,
        min_size: usize,
        element_min_value: Option<i64>,
        element_max_value: Option<i64>,
    },
    IntegerListList {
        values: Vec<Vec<i64>>,
        min_size: usize,
        inner_min_size: usize,
        inner_element_min_value: Option<i64>,
        inner_element_max_value: Option<i64>,
        inner_unique: bool,
    },
    IntegerTupleList {
        values: Vec<Vec<i64>>,
        min_size: usize,
        tuple_min_values: Vec<i64>,
        tuple_max_values: Vec<i64>,
        unique: bool,
    },
    IntegerDict {
        values: Vec<(i64, i64)>,
        min_size: usize,
        key_min_value: i64,
        key_max_value: i64,
        value_min_value: i64,
        value_max_value: i64,
    },
    IntegerStringDict {
        values: Vec<(i64, String)>,
        min_size: usize,
        key_min_value: i64,
        key_max_value: i64,
        value_min_size: usize,
    },
    BooleanDict {
        values: Vec<(bool, bool)>,
        min_size: usize,
    },
    BooleanList {
        values: Vec<bool>,
        min_size: usize,
        max_size: Option<usize>,
    },
    BooleanListList {
        values: Vec<Vec<bool>>,
        min_size: usize,
        inner_min_size: usize,
        inner_max_size: Option<usize>,
    },
    FloatList {
        values: Vec<f64>,
        min_size: usize,
        element_min_value: Option<f64>,
        element_max_value: Option<f64>,
        allow_nan: bool,
        allow_infinity: bool,
    },
    Binary {
        value: Vec<u8>,
        min_size: usize,
        max_size: Option<usize>,
    },
    String {
        value: String,
        min_size: usize,
        max_size: Option<usize>,
    },
    StringList {
        values: Vec<String>,
        min_size: usize,
        element_min_size: usize,
        element_max_size: Option<usize>,
    },
}

#[cfg(feature = "rust-core")]
impl ForcedLocalValue {
    fn into_data_value(self) -> DataValue {
        match self {
            Self::Float { value, .. } => DataValue::Float(value),
            Self::Integer { value, .. } => DataValue::Integer(value),
            Self::IntegerList { values, .. } => {
                DataValue::List(values.into_iter().map(DataValue::Integer).collect())
            }
            Self::IntegerListList { values, .. } => DataValue::List(
                values
                    .into_iter()
                    .map(|values| {
                        DataValue::List(values.into_iter().map(DataValue::Integer).collect())
                    })
                    .collect(),
            ),
            Self::IntegerTupleList { values, .. } => DataValue::List(
                values
                    .into_iter()
                    .map(|values| {
                        DataValue::Tuple(values.into_iter().map(DataValue::Integer).collect())
                    })
                    .collect(),
            ),
            Self::IntegerDict { values, .. } => DataValue::Dict(
                values
                    .into_iter()
                    .map(|(key, value)| (DataValue::Integer(key), DataValue::Integer(value)))
                    .collect(),
            ),
            Self::IntegerStringDict { values, .. } => DataValue::Dict(
                values
                    .into_iter()
                    .map(|(key, value)| (DataValue::Integer(key), DataValue::String(value)))
                    .collect(),
            ),
            Self::BooleanDict { values, .. } => DataValue::Dict(
                values
                    .into_iter()
                    .map(|(key, value)| (DataValue::Boolean(key), DataValue::Boolean(value)))
                    .collect(),
            ),
            Self::BooleanList { values, .. } => {
                DataValue::List(values.into_iter().map(DataValue::Boolean).collect())
            }
            Self::BooleanListList { values, .. } => DataValue::List(
                values
                    .into_iter()
                    .map(|values| {
                        DataValue::List(values.into_iter().map(DataValue::Boolean).collect())
                    })
                    .collect(),
            ),
            Self::FloatList { values, .. } => {
                DataValue::List(values.into_iter().map(DataValue::Float).collect())
            }
            Self::Binary { value, .. } => DataValue::Binary(value),
            Self::String { value, .. } => DataValue::String(value),
            Self::StringList { values, .. } => {
                DataValue::List(values.into_iter().map(DataValue::String).collect())
            }
        }
    }

    fn sort_key(&self) -> ExampleSortKey {
        let (length, indices) = match self {
            Self::Float {
                value,
                min_value: _,
                max_value: _,
                allow_nan: _,
                allow_infinity: _,
            } => (1, vec![float_choice_index(*value)]),
            Self::Integer {
                value,
                min_value,
                max_value,
            } => (
                1,
                vec![integer_choice_index(*value, *min_value, *max_value) as u128],
            ),
            Self::IntegerList {
                values,
                min_size,
                element_min_value,
                element_max_value,
            } => {
                let mut indices =
                    Vec::with_capacity(values.len().saturating_mul(2).saturating_add(1));
                for (index, value) in values.iter().enumerate().rev() {
                    indices.push(if index < *min_size { 0 } else { 1 });
                    indices.push(integer_choice_index(
                        *value,
                        *element_min_value,
                        *element_max_value,
                    ) as u128);
                }
                indices.push(0);
                (indices.len(), indices)
            }
            Self::IntegerListList {
                values,
                min_size,
                inner_min_size,
                inner_element_min_value,
                inner_element_max_value,
                inner_unique: _,
            } => {
                let mut indices = Vec::new();
                for (index, values) in values.iter().enumerate().rev() {
                    indices.push(if index < *min_size { 0 } else { 1 });
                    for (inner_index, value) in values.iter().enumerate().rev() {
                        indices.push(if inner_index < *inner_min_size { 0 } else { 1 });
                        indices.push(integer_choice_index(
                            *value,
                            *inner_element_min_value,
                            *inner_element_max_value,
                        ) as u128);
                    }
                    indices.push(0);
                }
                indices.push(0);
                (indices.len(), indices)
            }
            Self::IntegerTupleList {
                values,
                min_size,
                tuple_min_values,
                tuple_max_values,
                unique: _,
            } => {
                let mut indices = Vec::new();
                for (index, values) in values.iter().enumerate().rev() {
                    indices.push(if index < *min_size { 0 } else { 1 });
                    for (value, (min_value, max_value)) in values
                        .iter()
                        .zip(tuple_min_values.iter().zip(tuple_max_values))
                        .rev()
                    {
                        indices.push(integer_choice_index(
                            *value,
                            Some(*min_value),
                            Some(*max_value),
                        ) as u128);
                    }
                }
                indices.push(0);
                (indices.len(), indices)
            }
            Self::IntegerDict {
                values,
                min_size,
                key_min_value,
                key_max_value,
                value_min_value,
                value_max_value,
            } => {
                let mut indices = Vec::new();
                for (index, (key, value)) in values.iter().enumerate().rev() {
                    indices.push(if index < *min_size { 0 } else { 1 });
                    indices.push(integer_choice_index(
                        *key,
                        Some(*key_min_value),
                        Some(*key_max_value),
                    ) as u128);
                    indices.push(integer_choice_index(
                        *value,
                        Some(*value_min_value),
                        Some(*value_max_value),
                    ) as u128);
                }
                indices.push(0);
                (indices.len(), indices)
            }
            Self::IntegerStringDict {
                values,
                min_size,
                key_min_value,
                key_max_value,
                value_min_size,
            } => {
                let mut indices = Vec::new();
                for (index, (key, value)) in values.iter().enumerate().rev() {
                    indices.push(if index < *min_size { 0 } else { 1 });
                    indices.push(integer_choice_index(
                        *key,
                        Some(*key_min_value),
                        Some(*key_max_value),
                    ) as u128);
                    indices.extend(string_sort_key(value, *value_min_size, None).1.into_iter());
                }
                indices.push(0);
                (indices.len(), indices)
            }
            Self::BooleanDict { values, min_size } => {
                let mut indices = Vec::new();
                for (index, (key, value)) in values.iter().enumerate().rev() {
                    indices.push(if index < *min_size { 0 } else { 1 });
                    indices.push(u128::from(*key));
                    indices.push(u128::from(*value));
                }
                indices.push(0);
                (indices.len(), indices)
            }
            Self::BooleanList {
                values,
                min_size,
                max_size,
            } => {
                let mut indices =
                    Vec::with_capacity(values.len().saturating_mul(2).saturating_add(1));
                for (index, value) in values.iter().enumerate().rev() {
                    indices.push(if index < *min_size { 0 } else { 1 });
                    indices.push(u128::from(*value));
                }
                if max_size.is_none_or(|max_size| values.len() < max_size) {
                    indices.push(0);
                }
                (indices.len(), indices)
            }
            Self::BooleanListList {
                values,
                min_size,
                inner_min_size,
                inner_max_size,
            } => {
                let mut indices = Vec::new();
                for (index, values) in values.iter().enumerate().rev() {
                    indices.push(if index < *min_size { 0 } else { 1 });
                    for (inner_index, value) in values.iter().enumerate().rev() {
                        indices.push(if inner_index < *inner_min_size { 0 } else { 1 });
                        indices.push(u128::from(*value));
                    }
                    if inner_max_size.is_none_or(|max_size| values.len() < max_size) {
                        indices.push(0);
                    }
                }
                indices.push(0);
                (indices.len(), indices)
            }
            Self::FloatList {
                values,
                min_size,
                element_min_value: _,
                element_max_value: _,
                allow_nan: _,
                allow_infinity: _,
            } => {
                let mut indices =
                    Vec::with_capacity(values.len().saturating_mul(2).saturating_add(1));
                for (index, value) in values.iter().enumerate().rev() {
                    indices.push(if index < *min_size { 0 } else { 1 });
                    indices.push(float_choice_index(*value));
                }
                indices.push(0);
                (indices.len(), indices)
            }
            Self::Binary {
                value,
                min_size,
                max_size,
            } => {
                let mut indices = Vec::with_capacity(value.len().saturating_add(1));
                let effective_min = *min_size;
                for index in 0..value.len() {
                    indices.push(if index < effective_min { 0 } else { 1 });
                    indices.push(value[index] as u128);
                }
                if max_size.is_none_or(|max_size| value.len() < max_size) {
                    indices.push(0);
                }
                (indices.len(), indices)
            }
            Self::String {
                value,
                min_size,
                max_size,
            } => string_sort_key(value, *min_size, *max_size),
            Self::StringList {
                values,
                min_size,
                element_min_size,
                element_max_size,
            } => {
                let mut indices = Vec::new();
                for (index, value) in values.iter().enumerate().rev() {
                    indices.push(if index < *min_size { 0 } else { 1 });
                    indices.extend(
                        string_sort_key(value, *element_min_size, *element_max_size)
                            .1
                            .into_iter(),
                    );
                }
                indices.push(0);
                (indices.len(), indices)
            }
        };
        ExampleSortKey::from_parts(length, indices)
    }
}

#[cfg(feature = "rust-core")]
fn shrink_local_observation<F: FnMut(TestCase)>(
    seed: u64,
    schema: &Schema,
    value: &DataValue,
    initial_primary_bytes: &[u8],
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> Option<LocalShrinkResult> {
    match (schema, value) {
        (
            Schema::Float {
                min_value,
                max_value,
                allow_nan,
                allow_infinity,
                ..
            },
            DataValue::Float(value),
        ) => shrink_float_observation(
            seed,
            *value,
            *min_value,
            *max_value,
            *allow_nan,
            *allow_infinity,
            test_fn,
            verbosity,
            got_interesting,
        )
        .map(|value| LocalShrinkResult {
            forced_value: ForcedLocalValue::Float {
                value,
                min_value: *min_value,
                max_value: *max_value,
                allow_nan: *allow_nan,
                allow_infinity: *allow_infinity,
            },
            downgraded_primary_bytes: Vec::new(),
        }),
        (
            Schema::Integer {
                min_value,
                max_value,
            },
            DataValue::Integer(value),
        ) => Some(LocalShrinkResult {
            forced_value: ForcedLocalValue::Integer {
                value: shrink_core_integer_observation(
                    IntegerShrinkObservation {
                        min_value: min_value.unwrap_or(i64::MIN),
                        max_value: max_value.unwrap_or(i64::MAX),
                        value: *value,
                    },
                    |candidate| {
                        local_integer_candidate_is_interesting(
                            seed,
                            candidate,
                            test_fn,
                            verbosity,
                            got_interesting,
                        )
                    },
                ),
                min_value: *min_value,
                max_value: *max_value,
            },
            downgraded_primary_bytes: Vec::new(),
        }),
        (
            Schema::List {
                elements,
                min_size,
                max_size,
                unique,
            },
            DataValue::List(values),
        ) if matches!(
            elements.as_ref(),
            Schema::Tuple { elements } if elements.iter().all(|element| matches!(element, Schema::Integer { .. }))
        ) =>
        {
            let Schema::Tuple { elements } = elements.as_ref() else {
                unreachable!("guard already ensured tuple schema");
            };
            let tuple_min_values = elements
                .iter()
                .map(|element| match element {
                    Schema::Integer { min_value, .. } => min_value.unwrap_or(i64::MIN),
                    _ => unreachable!("guard already ensured integer tuple schema"),
                })
                .collect::<Vec<_>>();
            let tuple_max_values = elements
                .iter()
                .map(|element| match element {
                    Schema::Integer { max_value, .. } => max_value.unwrap_or(i64::MAX),
                    _ => unreachable!("guard already ensured integer tuple schema"),
                })
                .collect::<Vec<_>>();
            let tuples = values
                .iter()
                .map(|value| match value {
                    DataValue::Tuple(values) => values
                        .iter()
                        .map(|value| match value {
                            DataValue::Integer(value) => Some(*value),
                            _ => None,
                        })
                        .collect::<Option<Vec<_>>>(),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            shrink_integer_tuple_list_observation(
                seed,
                tuples,
                *min_size,
                tuple_min_values.clone(),
                tuple_max_values.clone(),
                *unique,
                test_fn,
                verbosity,
                got_interesting,
            )
            .map(|values| LocalShrinkResult {
                forced_value: ForcedLocalValue::IntegerTupleList {
                    values,
                    min_size: *min_size,
                    tuple_min_values,
                    tuple_max_values,
                    unique: *unique,
                },
                downgraded_primary_bytes: Vec::new(),
            })
        }
        (
            Schema::List {
                elements,
                min_size,
                max_size,
                unique: _,
            },
            DataValue::List(values),
        ) if matches!(
            elements.as_ref(),
            Schema::List {
                elements,
                ..
            } if matches!(elements.as_ref(), Schema::Integer { .. })
        ) =>
        {
            let Schema::List {
                elements: inner_elements,
                min_size: inner_min_size,
                max_size: _,
                unique: inner_unique,
            } = elements.as_ref()
            else {
                unreachable!("guard already ensured nested list schema");
            };
            let Schema::Integer {
                min_value,
                max_value,
            } = inner_elements.as_ref()
            else {
                unreachable!("guard already ensured integer element schema");
            };
            let integers = values
                .iter()
                .map(|value| match value {
                    DataValue::List(values) => values
                        .iter()
                        .map(|value| match value {
                            DataValue::Integer(value) => Some(*value),
                            _ => None,
                        })
                        .collect::<Option<Vec<_>>>(),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            shrink_integer_list_list_observation(
                seed,
                integers,
                *min_size,
                *inner_min_size,
                min_value.unwrap_or(i64::MIN),
                max_value.unwrap_or(i64::MAX),
                *inner_unique,
                test_fn,
                verbosity,
                got_interesting,
            )
            .map(|values| LocalShrinkResult {
                forced_value: ForcedLocalValue::IntegerListList {
                    values,
                    min_size: *min_size,
                    inner_min_size: *inner_min_size,
                    inner_element_min_value: *min_value,
                    inner_element_max_value: *max_value,
                    inner_unique: *inner_unique,
                },
                downgraded_primary_bytes: Vec::new(),
            })
        }
        (
            Schema::Dict {
                keys,
                values: dict_values,
                min_size,
                ..
            },
            DataValue::Dict(values),
        ) if matches!(keys.as_ref(), Schema::Integer { .. })
            && matches!(dict_values.as_ref(), Schema::Integer { .. }) =>
        {
            let Schema::Integer {
                min_value: key_min_value,
                max_value: key_max_value,
            } = keys.as_ref()
            else {
                unreachable!("guard already ensured integer key schema");
            };
            let Schema::Integer {
                min_value: value_min_value,
                max_value: value_max_value,
            } = dict_values.as_ref()
            else {
                unreachable!("guard already ensured integer value schema");
            };
            let entries = values
                .iter()
                .map(|(key, value)| match (key, value) {
                    (DataValue::Integer(key), DataValue::Integer(value)) => Some((*key, *value)),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            shrink_integer_dict_observation(
                seed,
                entries,
                *min_size,
                key_min_value.unwrap_or(i64::MIN),
                key_max_value.unwrap_or(i64::MAX),
                value_min_value.unwrap_or(i64::MIN),
                value_max_value.unwrap_or(i64::MAX),
                test_fn,
                verbosity,
                got_interesting,
            )
            .map(|values| LocalShrinkResult {
                forced_value: ForcedLocalValue::IntegerDict {
                    values,
                    min_size: *min_size,
                    key_min_value: key_min_value.unwrap_or(i64::MIN),
                    key_max_value: key_max_value.unwrap_or(i64::MAX),
                    value_min_value: value_min_value.unwrap_or(i64::MIN),
                    value_max_value: value_max_value.unwrap_or(i64::MAX),
                },
                downgraded_primary_bytes: Vec::new(),
            })
        }
        (
            Schema::Dict {
                keys,
                values: dict_values,
                min_size,
                ..
            },
            DataValue::Dict(values),
        ) if matches!(keys.as_ref(), Schema::Integer { .. })
            && matches!(dict_values.as_ref(), Schema::String { .. }) =>
        {
            let Schema::Integer {
                min_value: key_min_value,
                max_value: key_max_value,
            } = keys.as_ref()
            else {
                unreachable!("guard already ensured integer key schema");
            };
            let Schema::String {
                min_size: value_min_size,
                ..
            } = dict_values.as_ref()
            else {
                unreachable!("guard already ensured string value schema");
            };
            let entries = values
                .iter()
                .map(|(key, value)| match (key, value) {
                    (DataValue::Integer(key), DataValue::String(value)) => {
                        Some((*key, value.clone()))
                    }
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            shrink_integer_string_dict_observation(
                seed,
                entries,
                *min_size,
                key_min_value.unwrap_or(i64::MIN),
                key_max_value.unwrap_or(i64::MAX),
                *value_min_size,
                test_fn,
                verbosity,
                got_interesting,
            )
            .map(|values| LocalShrinkResult {
                forced_value: ForcedLocalValue::IntegerStringDict {
                    values,
                    min_size: *min_size,
                    key_min_value: key_min_value.unwrap_or(i64::MIN),
                    key_max_value: key_max_value.unwrap_or(i64::MAX),
                    value_min_size: *value_min_size,
                },
                downgraded_primary_bytes: Vec::new(),
            })
        }
        (
            Schema::Dict {
                keys,
                values: dict_values,
                min_size,
                ..
            },
            DataValue::Dict(values),
        ) if matches!(keys.as_ref(), Schema::Boolean { .. })
            && matches!(dict_values.as_ref(), Schema::Boolean { .. }) =>
        {
            let entries = values
                .iter()
                .map(|(key, value)| match (key, value) {
                    (DataValue::Boolean(key), DataValue::Boolean(value)) => Some((*key, *value)),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            shrink_boolean_dict_observation(
                seed,
                entries,
                *min_size,
                test_fn,
                verbosity,
                got_interesting,
            )
            .map(|values| LocalShrinkResult {
                forced_value: ForcedLocalValue::BooleanDict {
                    values,
                    min_size: *min_size,
                },
                downgraded_primary_bytes: Vec::new(),
            })
        }
        (
            Schema::List {
                elements,
                min_size,
                max_size,
                unique: _,
            },
            DataValue::List(values),
        ) if matches!(
            elements.as_ref(),
            Schema::List {
                elements,
                ..
            } if matches!(elements.as_ref(), Schema::Boolean { .. })
        ) =>
        {
            let Schema::List {
                min_size: inner_min_size,
                max_size: inner_max_size,
                ..
            } = elements.as_ref()
            else {
                unreachable!("guard already ensured nested list schema");
            };
            let booleans = values
                .iter()
                .map(|value| match value {
                    DataValue::List(values) => values
                        .iter()
                        .map(|value| match value {
                            DataValue::Boolean(value) => Some(*value),
                            _ => None,
                        })
                        .collect::<Option<Vec<_>>>(),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            shrink_boolean_list_list_observation(
                seed,
                booleans,
                *min_size,
                *inner_min_size,
                *inner_max_size,
                test_fn,
                verbosity,
                got_interesting,
            )
            .map(|values| LocalShrinkResult {
                forced_value: ForcedLocalValue::BooleanListList {
                    values,
                    min_size: *min_size,
                    inner_min_size: *inner_min_size,
                    inner_max_size: *inner_max_size,
                },
                downgraded_primary_bytes: Vec::new(),
            })
        }
        (
            Schema::List {
                elements,
                min_size,
                max_size,
                unique: _,
            },
            DataValue::List(values),
        ) if matches!(elements.as_ref(), Schema::Boolean { .. }) => {
            let booleans = values
                .iter()
                .map(|value| match value {
                    DataValue::Boolean(value) => Some(*value),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            shrink_boolean_list_observation(
                seed,
                booleans,
                *min_size,
                *max_size,
                test_fn,
                verbosity,
                got_interesting,
            )
            .map(|values| LocalShrinkResult {
                forced_value: ForcedLocalValue::BooleanList {
                    values,
                    min_size: *min_size,
                    max_size: *max_size,
                },
                downgraded_primary_bytes: Vec::new(),
            })
        }
        (
            Schema::List {
                elements,
                min_size,
                max_size: _,
                unique: _,
            },
            DataValue::List(values),
        ) if matches!(elements.as_ref(), Schema::Float { .. }) => {
            let Schema::Float {
                min_value,
                max_value,
                allow_nan,
                allow_infinity,
                ..
            } = elements.as_ref()
            else {
                unreachable!("guard already ensured float element schema");
            };
            let floats = values
                .iter()
                .map(|value| match value {
                    DataValue::Float(value) => Some(*value),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            shrink_float_list_observation(
                seed,
                floats,
                initial_primary_bytes,
                *min_size,
                *min_value,
                *max_value,
                *allow_nan,
                *allow_infinity,
                test_fn,
                verbosity,
                got_interesting,
            )
            .map(|(values, downgraded_primary_bytes)| LocalShrinkResult {
                forced_value: ForcedLocalValue::FloatList {
                    values,
                    min_size: *min_size,
                    element_min_value: *min_value,
                    element_max_value: *max_value,
                    allow_nan: *allow_nan,
                    allow_infinity: *allow_infinity,
                },
                downgraded_primary_bytes,
            })
        }
        (
            Schema::List {
                elements,
                min_size,
                max_size: _,
                unique,
            },
            DataValue::List(values),
        ) if matches!(elements.as_ref(), Schema::Integer { .. }) => {
            let Schema::Integer {
                min_value,
                max_value,
            } = elements.as_ref()
            else {
                unreachable!("guard already ensured integer element schema");
            };
            let integers = values
                .iter()
                .map(|value| match value {
                    DataValue::Integer(value) => Some(*value),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            shrink_integer_list_observation(
                seed,
                integers,
                *min_size,
                min_value.unwrap_or(i64::MIN),
                max_value.unwrap_or(i64::MAX),
                *unique,
                test_fn,
                verbosity,
                got_interesting,
            )
            .map(|values| LocalShrinkResult {
                forced_value: ForcedLocalValue::IntegerList {
                    values,
                    min_size: *min_size,
                    element_min_value: *min_value,
                    element_max_value: *max_value,
                },
                downgraded_primary_bytes: Vec::new(),
            })
        }
        (Schema::Binary { min_size, max_size }, DataValue::Binary(value)) => {
            shrink_binary_observation(
                seed,
                value.clone(),
                *min_size,
                *max_size,
                test_fn,
                verbosity,
                got_interesting,
            )
            .map(|value| LocalShrinkResult {
                forced_value: ForcedLocalValue::Binary {
                    value,
                    min_size: *min_size,
                    max_size: *max_size,
                },
                downgraded_primary_bytes: Vec::new(),
            })
        }
        (Schema::String { min_size, max_size }, DataValue::String(value)) => {
            shrink_string_observation(
                seed,
                value.clone(),
                *min_size,
                *max_size,
                test_fn,
                verbosity,
                got_interesting,
            )
            .map(|value| LocalShrinkResult {
                forced_value: ForcedLocalValue::String {
                    value,
                    min_size: *min_size,
                    max_size: *max_size,
                },
                downgraded_primary_bytes: Vec::new(),
            })
        }
        (
            Schema::List {
                elements,
                min_size,
                max_size: _,
                unique: _,
            },
            DataValue::List(values),
        ) if matches!(elements.as_ref(), Schema::String { .. }) => {
            let Schema::String {
                min_size: element_min_size,
                max_size: element_max_size,
            } = elements.as_ref()
            else {
                unreachable!("guard already ensured string element schema");
            };
            let strings = values
                .iter()
                .map(|value| match value {
                    DataValue::String(value) => Some(value.clone()),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            shrink_string_list_observation(
                seed,
                strings,
                *min_size,
                *element_min_size,
                *element_max_size,
                test_fn,
                verbosity,
                got_interesting,
            )
            .map(|values| LocalShrinkResult {
                forced_value: ForcedLocalValue::StringList {
                    values,
                    min_size: *min_size,
                    element_min_size: *element_min_size,
                    element_max_size: *element_max_size,
                },
                downgraded_primary_bytes: Vec::new(),
            })
        }
        _ => None,
    }
}

#[cfg(feature = "rust-core")]
fn shrink_float_observation<F: FnMut(TestCase)>(
    seed: u64,
    current: f64,
    min_value: Option<f64>,
    max_value: Option<f64>,
    allow_nan: bool,
    allow_infinity: bool,
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> Option<f64> {
    if current.is_nan() {
        return Some(f64::NAN);
    }
    for candidate in preferred_float_candidates(min_value, max_value, allow_nan, allow_infinity) {
        if local_float_candidate_is_interesting(
            seed,
            candidate,
            min_value,
            max_value,
            allow_nan,
            allow_infinity,
            test_fn,
            verbosity,
            got_interesting,
        ) {
            return Some(candidate);
        }
    }
    Some(current)
}

#[cfg(feature = "rust-core")]
fn shrink_boolean_list_observation<F: FnMut(TestCase)>(
    seed: u64,
    current: Vec<bool>,
    min_size: usize,
    max_size: Option<usize>,
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> Option<Vec<bool>> {
    Some(shrink_core_boolean_list_observation(
        current,
        min_size,
        |candidate| {
            local_value_candidate_is_interesting(
                seed,
                &ForcedLocalValue::BooleanList {
                    values: candidate.to_vec(),
                    min_size,
                    max_size,
                },
                test_fn,
                verbosity,
                got_interesting,
            )
        },
    ))
}

#[cfg(feature = "rust-core")]
fn shrink_boolean_list_list_observation<F: FnMut(TestCase)>(
    seed: u64,
    current: Vec<Vec<bool>>,
    min_size: usize,
    inner_min_size: usize,
    inner_max_size: Option<usize>,
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> Option<Vec<Vec<bool>>> {
    Some(shrink_core_boolean_list_list_observation(
        current,
        min_size,
        inner_min_size,
        |candidate: &[Vec<bool>]| {
            local_value_candidate_is_interesting(
                seed,
                &ForcedLocalValue::BooleanListList {
                    values: candidate.to_vec(),
                    min_size,
                    inner_min_size,
                    inner_max_size,
                },
                test_fn,
                verbosity,
                got_interesting,
            )
        },
    ))
}

#[cfg(feature = "rust-core")]
fn shrink_integer_list_list_observation<F: FnMut(TestCase)>(
    seed: u64,
    current: Vec<Vec<i64>>,
    min_size: usize,
    inner_min_size: usize,
    min_value: i64,
    max_value: i64,
    inner_unique: bool,
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> Option<Vec<Vec<i64>>> {
    Some(shrink_core_integer_list_list_observation(
        current,
        min_size,
        inner_min_size,
        min_value,
        max_value,
        inner_unique,
        |candidate: &[Vec<i64>]| {
            local_value_candidate_is_interesting(
                seed,
                &ForcedLocalValue::IntegerListList {
                    values: candidate.to_vec(),
                    min_size,
                    inner_min_size,
                    inner_element_min_value: Some(min_value),
                    inner_element_max_value: Some(max_value),
                    inner_unique,
                },
                test_fn,
                verbosity,
                got_interesting,
            )
        },
    ))
}

#[cfg(feature = "rust-core")]
fn shrink_integer_tuple_list_observation<F: FnMut(TestCase)>(
    seed: u64,
    current: Vec<Vec<i64>>,
    min_size: usize,
    tuple_min_values: Vec<i64>,
    tuple_max_values: Vec<i64>,
    unique: bool,
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> Option<Vec<Vec<i64>>> {
    Some(shrink_core_integer_tuple_list_observation(
        current,
        min_size,
        &tuple_min_values,
        &tuple_max_values,
        unique,
        |candidate: &[Vec<i64>]| {
            local_value_candidate_is_interesting(
                seed,
                &ForcedLocalValue::IntegerTupleList {
                    values: candidate.to_vec(),
                    min_size,
                    tuple_min_values: tuple_min_values.clone(),
                    tuple_max_values: tuple_max_values.clone(),
                    unique,
                },
                test_fn,
                verbosity,
                got_interesting,
            )
        },
    ))
}

#[cfg(feature = "rust-core")]
fn shrink_integer_dict_observation<F: FnMut(TestCase)>(
    seed: u64,
    current: Vec<(i64, i64)>,
    min_size: usize,
    key_min_value: i64,
    key_max_value: i64,
    value_min_value: i64,
    value_max_value: i64,
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> Option<Vec<(i64, i64)>> {
    Some(shrink_core_integer_dict_observation(
        current,
        min_size,
        key_min_value,
        key_max_value,
        value_min_value,
        value_max_value,
        |candidate: &[(i64, i64)]| {
            local_value_candidate_is_interesting(
                seed,
                &ForcedLocalValue::IntegerDict {
                    values: candidate.to_vec(),
                    min_size,
                    key_min_value,
                    key_max_value,
                    value_min_value,
                    value_max_value,
                },
                test_fn,
                verbosity,
                got_interesting,
            )
        },
    ))
}

#[cfg(feature = "rust-core")]
fn shrink_integer_string_dict_observation<F: FnMut(TestCase)>(
    seed: u64,
    current: Vec<(i64, String)>,
    min_size: usize,
    key_min_value: i64,
    key_max_value: i64,
    value_min_size: usize,
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> Option<Vec<(i64, String)>> {
    Some(shrink_core_integer_string_dict_observation(
        current,
        min_size,
        key_min_value,
        key_max_value,
        value_min_size,
        |candidate: &[(i64, String)]| {
            local_value_candidate_is_interesting(
                seed,
                &ForcedLocalValue::IntegerStringDict {
                    values: candidate.to_vec(),
                    min_size,
                    key_min_value,
                    key_max_value,
                    value_min_size,
                },
                test_fn,
                verbosity,
                got_interesting,
            )
        },
    ))
}

#[cfg(feature = "rust-core")]
fn shrink_boolean_dict_observation<F: FnMut(TestCase)>(
    seed: u64,
    current: Vec<(bool, bool)>,
    min_size: usize,
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> Option<Vec<(bool, bool)>> {
    Some(shrink_core_boolean_dict_observation(
        current,
        min_size,
        |candidate: &[(bool, bool)]| {
            local_value_candidate_is_interesting(
                seed,
                &ForcedLocalValue::BooleanDict {
                    values: candidate.to_vec(),
                    min_size,
                },
                test_fn,
                verbosity,
                got_interesting,
            )
        },
    ))
}

#[cfg(feature = "rust-core")]
fn shrink_float_list_observation<F: FnMut(TestCase)>(
    seed: u64,
    current: Vec<f64>,
    initial_primary_bytes: &[u8],
    min_size: usize,
    min_value: Option<f64>,
    max_value: Option<f64>,
    allow_nan: bool,
    allow_infinity: bool,
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> Option<(Vec<f64>, Vec<Vec<u8>>)> {
    let (current, _current_primary_bytes) = shrink_core_float_list_observation(
        current,
        initial_primary_bytes.to_vec(),
        min_size,
        min_value,
        max_value,
        allow_nan,
        allow_infinity,
        |candidate| {
            local_value_candidate_bytes_if_interesting(
                seed,
                &ForcedLocalValue::FloatList {
                    values: candidate.to_vec(),
                    min_size,
                    element_min_value: min_value,
                    element_max_value: max_value,
                    allow_nan,
                    allow_infinity,
                },
                test_fn,
                verbosity,
                got_interesting,
            )
        },
        |event, values, bytes| append_local_float_list_trace(event, values, bytes),
    );

    Some((current, Vec::new()))
}

#[cfg(feature = "rust-core")]
fn shrink_string_list_observation<F: FnMut(TestCase)>(
    seed: u64,
    mut current: Vec<String>,
    min_size: usize,
    element_min_size: usize,
    element_max_size: Option<usize>,
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> Option<Vec<String>> {
    while current.len() > min_size {
        let candidate = current[..current.len() - 1].to_vec();
        if local_value_candidate_is_interesting(
            seed,
            &ForcedLocalValue::StringList {
                values: candidate.clone(),
                min_size,
                element_min_size,
                element_max_size,
            },
            test_fn,
            verbosity,
            got_interesting,
        ) {
            current = candidate;
        } else {
            break;
        }
    }

    for index in 0..current.len() {
        current[index] = shrink_string_at_list_index(
            seed,
            &current,
            min_size,
            element_min_size,
            element_max_size,
            index,
            test_fn,
            verbosity,
            got_interesting,
        )?;
    }

    Some(current)
}

#[cfg(feature = "rust-core")]
fn shrink_string_at_list_index<F: FnMut(TestCase)>(
    seed: u64,
    current: &[String],
    min_size: usize,
    element_min_size: usize,
    element_max_size: Option<usize>,
    index: usize,
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> Option<String> {
    let mut best = current[index].clone();
    let mut best_chars: Vec<char> = best.chars().collect();

    while best_chars.len() > element_min_size {
        let candidate_chars = &best_chars[..best_chars.len() - 1];
        let candidate: String = candidate_chars.iter().collect();
        let mut probe = current.to_vec();
        probe[index] = candidate.clone();
        if local_value_candidate_is_interesting(
            seed,
            &ForcedLocalValue::StringList {
                values: probe,
                min_size,
                element_min_size,
                element_max_size,
            },
            test_fn,
            verbosity,
            got_interesting,
        ) {
            best = candidate;
            best_chars = candidate_chars.to_vec();
        } else {
            break;
        }
    }

    for char_index in 0..best_chars.len() {
        if best_chars[char_index] == '0' {
            continue;
        }
        let mut candidate_chars = best_chars.clone();
        candidate_chars[char_index] = '0';
        let candidate: String = candidate_chars.iter().collect();
        let mut probe = current.to_vec();
        probe[index] = candidate.clone();
        if local_value_candidate_is_interesting(
            seed,
            &ForcedLocalValue::StringList {
                values: probe,
                min_size,
                element_min_size,
                element_max_size,
            },
            test_fn,
            verbosity,
            got_interesting,
        ) {
            best = candidate;
            best_chars = candidate_chars;
        }
    }

    Some(best)
}

#[cfg(feature = "rust-core")]
fn shrink_string_observation<F: FnMut(TestCase)>(
    seed: u64,
    current: String,
    min_size: usize,
    max_size: Option<usize>,
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> Option<String> {
    let mut current_chars: Vec<char> = current.chars().collect();

    while current_chars.len() > min_size {
        let candidate: String = current_chars[..current_chars.len() - 1].iter().collect();
        if local_value_candidate_is_interesting(
            seed,
            &ForcedLocalValue::String {
                value: candidate.clone(),
                min_size,
                max_size,
            },
            test_fn,
            verbosity,
            got_interesting,
        ) {
            current_chars.pop();
        } else {
            break;
        }
    }

    for index in 0..current_chars.len() {
        if current_chars[index] == '0' {
            continue;
        }
        let mut candidate_chars = current_chars.clone();
        candidate_chars[index] = '0';
        let candidate: String = candidate_chars.iter().collect();
        if local_value_candidate_is_interesting(
            seed,
            &ForcedLocalValue::String {
                value: candidate,
                min_size,
                max_size,
            },
            test_fn,
            verbosity,
            got_interesting,
        ) {
            current_chars[index] = '0';
        }
    }

    Some(current_chars.into_iter().collect())
}

#[cfg(feature = "rust-core")]
fn shrink_binary_observation<F: FnMut(TestCase)>(
    seed: u64,
    mut current: Vec<u8>,
    min_size: usize,
    max_size: Option<usize>,
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> Option<Vec<u8>> {
    while current.len() > min_size {
        let candidate = current[..current.len() - 1].to_vec();
        if local_value_candidate_is_interesting(
            seed,
            &ForcedLocalValue::Binary {
                value: candidate.clone(),
                min_size,
                max_size,
            },
            test_fn,
            verbosity,
            got_interesting,
        ) {
            current = candidate;
        } else {
            break;
        }
    }

    for index in 0..current.len() {
        if current[index] == 0 {
            continue;
        }
        let mut candidate = current.clone();
        candidate[index] = 0;
        if local_value_candidate_is_interesting(
            seed,
            &ForcedLocalValue::Binary {
                value: candidate.clone(),
                min_size,
                max_size,
            },
            test_fn,
            verbosity,
            got_interesting,
        ) {
            current = candidate;
        }
    }

    Some(current)
}

#[cfg(feature = "rust-core")]
fn shrink_integer_list_observation<F: FnMut(TestCase)>(
    seed: u64,
    current: Vec<i64>,
    min_size: usize,
    min_value: i64,
    max_value: i64,
    unique: bool,
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> Option<Vec<i64>> {
    Some(shrink_core_integer_list_observation(
        current,
        min_size,
        min_value,
        max_value,
        unique,
        |candidate: &[i64]| {
            local_value_candidate_is_interesting(
                seed,
                &ForcedLocalValue::IntegerList {
                    values: candidate.to_vec(),
                    min_size,
                    element_min_value: Some(min_value),
                    element_max_value: Some(max_value),
                },
                test_fn,
                verbosity,
                got_interesting,
            )
        },
    ))
}

#[cfg(feature = "rust-core")]
fn local_integer_candidate_is_interesting<F: FnMut(TestCase)>(
    seed: u64,
    candidate: i64,
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> bool {
    local_value_candidate_is_interesting(
        seed,
        &ForcedLocalValue::Integer {
            value: candidate,
            min_value: None,
            max_value: None,
        },
        test_fn,
        verbosity,
        got_interesting,
    )
}

#[cfg(feature = "rust-core")]
fn local_float_candidate_is_interesting<F: FnMut(TestCase)>(
    seed: u64,
    candidate: f64,
    min_value: Option<f64>,
    max_value: Option<f64>,
    allow_nan: bool,
    allow_infinity: bool,
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> bool {
    local_value_candidate_is_interesting(
        seed,
        &ForcedLocalValue::Float {
            value: candidate,
            min_value,
            max_value,
            allow_nan,
            allow_infinity,
        },
        test_fn,
        verbosity,
        got_interesting,
    )
}

#[cfg(feature = "rust-core")]
fn local_value_candidate_is_interesting<F: FnMut(TestCase)>(
    seed: u64,
    candidate: &ForcedLocalValue,
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> bool {
    local_value_candidate_bytes_if_interesting(seed, candidate, test_fn, verbosity, got_interesting)
        .is_some()
}

#[cfg(feature = "rust-core")]
fn local_forced_values_are_interesting<F: FnMut(TestCase)>(
    seed: u64,
    forced_values: Vec<DataValue>,
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> bool {
    let backend = Rc::new(RefCell::new(LocalBackend::from_seed(seed)));
    backend.borrow_mut().force_values(forced_values);
    matches!(
        run_test_case(
            TestBackend::Local {
                backend: Rc::clone(&backend),
            },
            test_fn,
            false,
            verbosity,
            got_interesting,
        ),
        TestCaseResult::Interesting { .. }
    )
}

#[cfg(feature = "rust-core")]
fn local_value_candidate_bytes_if_interesting<F: FnMut(TestCase)>(
    seed: u64,
    candidate: &ForcedLocalValue,
    test_fn: &mut F,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> Option<Vec<u8>> {
    let backend = Rc::new(RefCell::new(LocalBackend::from_seed(seed)));
    backend
        .borrow_mut()
        .force_first_value(candidate.clone().into_data_value());
    let is_interesting = matches!(
        run_test_case(
            TestBackend::Local {
                backend: Rc::clone(&backend),
            },
            test_fn,
            false,
            verbosity,
            got_interesting,
        ),
        TestCaseResult::Interesting { .. }
    );
    if !is_interesting {
        return None;
    }

    backend
        .borrow()
        .observed_first_value()
        .and_then(|(_, value)| {
            if value.same_observed_value(&candidate.clone().into_data_value()) {
                Some(choices_to_bytes(backend.borrow().recorded_choices()))
            } else {
                None
            }
        })
}

#[cfg(feature = "rust-core")]
fn integer_choice_index(value: i64, min_value: Option<i64>, max_value: Option<i64>) -> u64 {
    let mut shrink_towards = 0i64;
    if let Some(min_value) = min_value {
        shrink_towards = shrink_towards.max(min_value);
    }
    if let Some(max_value) = max_value {
        shrink_towards = shrink_towards.min(max_value);
    }
    let distance_from_shrink_towards = value.abs_diff(shrink_towards);

    match (min_value, max_value) {
        (None, None) => zigzag_index(value, shrink_towards),
        (Some(min_value), None) => {
            if distance_from_shrink_towards <= shrink_towards.abs_diff(min_value) {
                zigzag_index(value, shrink_towards)
            } else {
                value.abs_diff(min_value)
            }
        }
        (None, Some(max_value)) => {
            if distance_from_shrink_towards <= max_value.abs_diff(shrink_towards) {
                zigzag_index(value, shrink_towards)
            } else {
                max_value.abs_diff(value)
            }
        }
        (Some(min_value), Some(max_value)) => {
            let below_distance = shrink_towards.abs_diff(min_value);
            let above_distance = max_value.abs_diff(shrink_towards);
            if below_distance < above_distance {
                if distance_from_shrink_towards <= below_distance {
                    zigzag_index(value, shrink_towards)
                } else {
                    value.abs_diff(min_value)
                }
            } else if distance_from_shrink_towards <= above_distance {
                zigzag_index(value, shrink_towards)
            } else {
                max_value.abs_diff(value)
            }
        }
    }
}

#[cfg(feature = "rust-core")]
fn string_char_choice_index(value: char) -> u128 {
    let codepoint = value as u32;
    let shrink_towards = '0' as u32;
    let distance = codepoint.abs_diff(shrink_towards) as u128;
    let tie_break = if codepoint >= shrink_towards { 0 } else { 1 };
    distance.saturating_mul(2) + tie_break
}

#[cfg(feature = "rust-core")]
fn string_sort_key(value: &str, min_size: usize, max_size: Option<usize>) -> (usize, Vec<u128>) {
    let chars: Vec<char> = value.chars().collect();
    let mut indices = Vec::with_capacity(chars.len().saturating_mul(2).saturating_add(1));
    for (index, ch) in chars.iter().enumerate() {
        indices.push(if index < min_size { 0 } else { 1 });
        indices.push(string_char_choice_index(*ch));
    }
    if max_size.is_none_or(|max_size| chars.len() < max_size) {
        indices.push(0);
    }
    (indices.len(), indices)
}

#[cfg(feature = "rust-core")]
fn zigzag_index(value: i64, shrink_towards: i64) -> u64 {
    let distance = value.abs_diff(shrink_towards);
    let mut index = distance.saturating_mul(2);
    if value > shrink_towards {
        index = index.saturating_sub(1);
    }
    index
}

enum TestCaseResult {
    Valid,
    Invalid,
    Interesting {
        panic_message: String,
        origin: String,
    },
}

enum TestBackend<'a> {
    Remote {
        connection: &'a Arc<Connection>,
        test_channel: Channel,
    },
    #[cfg(feature = "rust-core")]
    Local { backend: Rc<RefCell<LocalBackend>> },
}

fn run_test_case<F: FnMut(TestCase)>(
    backend: TestBackend<'_>,
    test_fn: &mut F,
    is_final: bool,
    verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> TestCaseResult {
    // Create TestCase. The test function gets a clone (cheap Rc bump),
    // so we retain access to the same underlying TestCaseData after the test runs.
    let tc = match backend {
        TestBackend::Remote {
            connection,
            test_channel,
        } => TestCase::new_remote(Arc::clone(connection), test_channel, verbosity, is_final),
        #[cfg(feature = "rust-core")]
        TestBackend::Local { backend } => TestCase::new_local(backend, verbosity, is_final),
    };

    let result = with_test_context(|| catch_unwind(AssertUnwindSafe(|| test_fn(tc.clone()))));

    let (tc_result, origin) = match &result {
        Ok(()) => (TestCaseResult::Valid, None),
        Err(e) => {
            let msg = panic_message(e);
            if msg == ASSUME_FAIL_STRING || msg == STOP_TEST_STRING {
                (TestCaseResult::Invalid, None)
            } else {
                got_interesting.store(true, Ordering::SeqCst);

                // Take panic info - we need location for origin, and print details on final
                let (thread_name, thread_id, location, backtrace) = take_panic_info()
                    .unwrap_or_else(|| {
                        (
                            "<unknown>".to_string(),
                            "?".to_string(),
                            "<unknown>".to_string(),
                            Backtrace::disabled(),
                        )
                    });

                if is_final {
                    eprintln!(
                        "thread '{}' ({}) panicked at {}:",
                        thread_name, thread_id, location
                    );
                    eprintln!("{}", msg);

                    if backtrace.status() == BacktraceStatus::Captured {
                        let is_full = std::env::var("RUST_BACKTRACE")
                            .map(|v| v == "full")
                            .unwrap_or(false);
                        let formatted = format_backtrace(&backtrace, is_full);
                        eprintln!("stack backtrace:\n{}", formatted);
                        if !is_full {
                            eprintln!(
                                "note: Some details are omitted, run with `RUST_BACKTRACE=full` for a verbose backtrace."
                            );
                        }
                    }
                }

                let origin = format!("Panic at {}", location);
                (
                    TestCaseResult::Interesting {
                        panic_message: msg,
                        origin: origin.clone(),
                    },
                    Some(origin),
                )
            }
        }
    };

    // Send mark_complete using the same channel that generators used.
    // Skip if test was aborted (StopTest) - server already closed the channel.
    if !tc.test_aborted() {
        let status = match &tc_result {
            TestCaseResult::Valid => "VALID",
            TestCaseResult::Invalid => "INVALID",
            TestCaseResult::Interesting { .. } => "INTERESTING",
        };
        let origin_value = match &origin {
            Some(s) => Value::Text(s.clone()),
            None => Value::Null,
        };
        let mark_complete = cbor_map! {
            "command" => "mark_complete",
            "status" => status,
            "origin" => origin_value
        };
        tc.send_mark_complete(&mark_complete);
    }

    tc_result
}

/// Extract a message from a panic payload.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "Unknown panic".to_string()
    }
}

/// Encode a ciborium::Value to CBOR bytes.
fn cbor_encode(value: &Value) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes).expect("CBOR encoding failed");
    bytes
}

/// Decode CBOR bytes to a ciborium::Value.
fn cbor_decode(bytes: &[u8]) -> Value {
    ciborium::from_reader(bytes).expect("CBOR decoding failed")
}

#[cfg(all(test, feature = "rust-core"))]
mod tests {
    use super::*;
    use crate::generators as gs;
    use crate::generators::Generator;

    fn bounded_positive_replay_plans(test_cases: u64) -> Vec<LocalReplayPlan> {
        let mut test_fn = |tc: TestCase| {
            let _value = tc.draw(
                &gs::integers::<i64>()
                    .min_value(-10)
                    .max_value(10)
                    .filter(|&x| x != 0),
            );
            panic!("HEGEL_MINIMAL_FOUND");
        };
        let verbosity = Verbosity::Quiet;
        let got_interesting = Arc::new(AtomicBool::new(false));
        let mut replay_plans = Vec::new();

        for seed in 0..test_cases {
            let backend = Rc::new(RefCell::new(LocalBackend::from_seed(seed)));
            let tc_result = run_test_case(
                TestBackend::Local {
                    backend: Rc::clone(&backend),
                },
                &mut test_fn,
                false,
                verbosity,
                &got_interesting,
            );
            if matches!(tc_result, TestCaseResult::Interesting { .. }) {
                let forced_value = backend
                    .borrow()
                    .observed_first_value()
                    .and_then(|(schema, value)| {
                        shrink_local_observation(
                            seed,
                            &schema,
                            &value,
                            &choices_to_bytes(backend.borrow().recorded_choices()),
                            &mut test_fn,
                            verbosity,
                            &got_interesting,
                        )
                    })
                    .map(|result| result.forced_value);
                replay_plans.push(LocalReplayPlan {
                    origin: "Panic at tests::bounded_positive".to_owned(),
                    seed: Some(seed),
                    replay_choices: None,
                    forced_prefix_values: Vec::new(),
                    forced_value,
                    downgraded_primary_bytes: Vec::new(),
                });
            }
        }

        replay_plans
    }

    #[test]
    fn bounded_positive_has_a_seed_with_forced_one() {
        let replay_plans = bounded_positive_replay_plans(500);
        assert!(
            replay_plans.iter().any(|plan| {
                plan.forced_value
                    == Some(ForcedLocalValue::Integer {
                        value: 1,
                        min_value: Some(-10),
                        max_value: Some(10),
                    })
            }),
            "expected at least one seed to shrink to 1, got {replay_plans:?}"
        );
    }

    #[test]
    fn bounded_integer_sort_key_orders_positive_before_negative() {
        let positive = ForcedLocalValue::Integer {
            value: 1,
            min_value: Some(-10),
            max_value: Some(10),
        };
        let negative = ForcedLocalValue::Integer {
            value: -1,
            min_value: Some(-10),
            max_value: Some(10),
        };

        assert!(positive.sort_key() < negative.sort_key());
    }

    #[test]
    fn bounded_string_sort_key_orders_zeroes_before_letters() {
        let zeroes = ForcedLocalValue::String {
            value: "00".to_owned(),
            min_size: 2,
            max_size: Some(2),
        };
        let letters = ForcedLocalValue::String {
            value: "ac".to_owned(),
            min_size: 2,
            max_size: Some(2),
        };

        assert!(zeroes.sort_key() < letters.sort_key());
    }

    fn float_list_minimal_test_fn(tc: TestCase) {
        let values = tc.draw(&gs::vecs(gs::floats::<f64>()).min_size(2));
        if values.iter().any(|&value| value != 0.0) {
            panic!("HEGEL_MINIMAL_FOUND");
        }
    }

    fn bounded_float_list_minimal_test_fn(tc: TestCase) {
        let values = tc.draw(
            &gs::vecs(
                gs::floats::<f64>()
                    .min_value(0.0)
                    .max_value(1.0)
                    .allow_nan(false)
                    .allow_infinity(false),
            )
            .min_size(2)
            .max_size(2),
        );
        if values.iter().any(|&value| value != 0.0) {
            panic!("HEGEL_MINIMAL_FOUND");
        }
    }

    fn decode_hex_bytes(input: &str) -> Vec<u8> {
        assert_eq!(input.len() % 2, 0, "expected even-length hex input");
        input
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let text = std::str::from_utf8(pair).expect("expected valid utf-8 hex pair");
                u8::from_str_radix(text, 16).expect("expected valid hex byte")
            })
            .collect()
    }

    #[test]
    fn float_list_candidate_one_zero_is_interesting_and_replayable() {
        let mut test_fn = float_list_minimal_test_fn;
        let got_interesting = Arc::new(AtomicBool::new(false));
        let candidate = ForcedLocalValue::FloatList {
            values: vec![1.0, 0.0],
            min_size: 2,
            element_min_value: None,
            element_max_value: None,
            allow_nan: true,
            allow_infinity: true,
        };

        let bytes = local_value_candidate_bytes_if_interesting(
            0,
            &candidate,
            &mut test_fn,
            Verbosity::Quiet,
            &got_interesting,
        );

        assert!(
            bytes.is_some(),
            "expected [1.0, 0.0] to be an interesting replayable float-list witness"
        );
    }

    #[test]
    fn float_list_shrinker_can_reduce_large_witness_to_one() {
        let mut test_fn = float_list_minimal_test_fn;
        let got_interesting = Arc::new(AtomicBool::new(false));
        let current = ForcedLocalValue::FloatList {
            values: vec![210_798.0, 0.0],
            min_size: 2,
            element_min_value: None,
            element_max_value: None,
            allow_nan: true,
            allow_infinity: true,
        };
        let initial_primary_bytes = local_value_candidate_bytes_if_interesting(
            0,
            &current,
            &mut test_fn,
            Verbosity::Quiet,
            &got_interesting,
        )
        .expect("expected initial float-list witness to be replayable");

        let result = shrink_float_list_observation(
            0,
            vec![210_798.0, 0.0],
            &initial_primary_bytes,
            2,
            None,
            None,
            true,
            true,
            &mut test_fn,
            Verbosity::Quiet,
            &got_interesting,
        )
        .expect("expected shrink result");

        assert_eq!(result.0.len(), 2);
        assert_eq!(result.0.iter().filter(|&&value| value == 0.0).count(), 1);
        assert!(
            result.0.contains(&1.0),
            "expected shrink result to contain 1.0, got {:?}",
            result.0
        );
    }

    #[test]
    fn bounded_float_list_shrinker_reduces_python_secondary_witness_to_primary_example() {
        let mut test_fn = bounded_float_list_minimal_test_fn;
        let got_interesting = Arc::new(AtomicBool::new(false));
        let current = ForcedLocalValue::FloatList {
            values: vec![0.797_419_994_097_881_7, 0.574_610_631_420_536_7],
            min_size: 2,
            element_min_value: Some(0.0),
            element_max_value: Some(1.0),
            allow_nan: false,
            allow_infinity: false,
        };
        let initial_primary_bytes = local_value_candidate_bytes_if_interesting(
            0,
            &current,
            &mut test_fn,
            Verbosity::Quiet,
            &got_interesting,
        )
        .expect("expected bounded float-list witness to be replayable");

        let result = shrink_float_list_observation(
            0,
            vec![0.797_419_994_097_881_7, 0.574_610_631_420_536_7],
            &initial_primary_bytes,
            2,
            Some(0.0),
            Some(1.0),
            false,
            false,
            &mut test_fn,
            Verbosity::Quiet,
            &got_interesting,
        )
        .expect("expected shrink result");

        assert_eq!(result.0, vec![0.0, 1.0]);
        assert!(
            result.1.is_empty(),
            "expected bounded float-list shrinker to leave demotion timing to the engine-level save step"
        );
    }

    #[test]
    fn bounded_float_list_python_secondary_bytes_replay_to_expected_observation() {
        let bytes = decode_hex_bytes("283fe98476ef7a7616283fe26335d5bc52dd");
        let choices = choices_from_bytes(&bytes).expect("expected decodable choice bytes");
        let backend = Rc::new(RefCell::new(LocalBackend::from_choices(choices)));
        let mut test_fn = bounded_float_list_minimal_test_fn;
        let got_interesting = Arc::new(AtomicBool::new(false));

        let result = run_test_case(
            TestBackend::Local {
                backend: Rc::clone(&backend),
            },
            &mut test_fn,
            false,
            Verbosity::Quiet,
            &got_interesting,
        );

        assert!(
            matches!(result, TestCaseResult::Interesting { .. }),
            "expected python secondary replay to stay interesting"
        );
        let (_, value) = backend
            .borrow()
            .observed_first_value()
            .expect("expected one observed value");
        assert_eq!(
            value,
            DataValue::List(vec![
                DataValue::Float(0.797_419_994_097_881_7),
                DataValue::Float(0.574_610_631_420_536_7),
            ])
        );
    }
}

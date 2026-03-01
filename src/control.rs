use crate::generators::TestCaseData;
use std::cell::Cell;

/// The sentinel string used to identify assume-rejection panics.
pub(crate) const ASSUME_FAIL_STRING: &str = "__HEGEL_ASSUME_FAIL";

thread_local! {
    static TEST_CASE_DATA: Cell<*const TestCaseData> = const { Cell::new(std::ptr::null()) };
}

/// A reference to the current TestCaseData, if any.
#[doc(hidden)]
pub fn test_case_data() -> Option<&'static TestCaseData> {
    TEST_CASE_DATA.with(|c| {
        let ptr = c.get();
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { &*ptr })
        }
    })
}

/// Set the thread-local test case data pointer.
///
/// # Safety
/// The caller must ensure that the referenced `TestCaseData` outlives the
/// test case execution.  In practice, `run_test_case` creates the data on
/// the stack and calls `clear_test_case_data` before the data is dropped.
pub(crate) fn set_test_case_data(data: &TestCaseData) {
    TEST_CASE_DATA.with(|c| c.set(data as *const TestCaseData));
}

/// Clear the thread-local test case data pointer.
pub(crate) fn clear_test_case_data() {
    TEST_CASE_DATA.with(|c| c.set(std::ptr::null()));
}

/// Returns `true` if we are currently inside a Hegel test context.
///
/// This can be used to conditionally execute code that depends on a
/// live test case (e.g., generating values, recording notes).
///
/// # Example
///
/// ```no_run
/// if hegel::currently_in_test_context() {
///     hegel::note("inside a test");
/// }
/// ```
pub fn currently_in_test_context() -> bool {
    test_case_data().is_some()
}

/// Assume a condition is true. If false, reject the current test input.
///
/// # Example
///
/// ```no_run
/// use hegel::generators;
///
/// # hegel::hegel(|| {
/// let age: u32 = hegel::draw(&generators::integers());
/// hegel::assume(age >= 18);
/// // Test logic for adults only...
/// # });
/// ```
pub fn assume(condition: bool) {
    assert!(
        currently_in_test_context(),
        "assume() cannot be called outside of a Hegel test"
    );
    if !condition {
        panic!("{}", ASSUME_FAIL_STRING);
    }
}

/// Note a message which will be displayed with the reported failing test case.
///
/// Only prints during the final replay of a failing test case.
///
/// # Example
///
/// ```no_run
/// use hegel::generators;
///
/// # hegel::hegel(|| {
/// let x: i32 = hegel::draw(&generators::integers());
/// hegel::note(&format!("Generated x = {}", x));
/// # });
/// ```
pub fn note(message: &str) {
    let data = test_case_data().expect("note() cannot be called outside of a Hegel test.");
    if data.is_last_run() {
        eprintln!("{}", message);
    }
}

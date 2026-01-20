"""Conformance tests for hegel-rust SDK."""

from pathlib import Path

from hegel.conformance import run_conformance_tests

# Path to the built conformance binaries
BUILD_DIR = Path(__file__).parent / "rust" / "target" / "release"


def test_conformance():
    """Run all conformance tests against the hegel-rust SDK."""
    binaries = {
        "booleans": BUILD_DIR / "test_booleans",
        "integers": BUILD_DIR / "test_integers",
        "floats": BUILD_DIR / "test_floats",
        # "text": BUILD_DIR / "test_text",  # Disabled due to hypothesis-jsonschema bug
        "lists": BUILD_DIR / "test_lists",
        "sampled_from": BUILD_DIR / "test_sampled_from",
        "floats_nan": BUILD_DIR / "test_floats_nan",
        "floats_infinity": BUILD_DIR / "test_floats_infinity",
    }

    # Check all binaries exist
    missing = [name for name, path in binaries.items() if not path.exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing conformance binaries: {missing}. "
            f"Run 'cargo build --release' in tests/conformance/rust first."
        )

    run_conformance_tests(binaries)

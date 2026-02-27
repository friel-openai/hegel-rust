# Hegel SDK for Rust
# This justfile provides the standard build recipes.

# Install dependencies and the hegel binary.
# If HEGEL_BINARY is set, symlinks it into .venv/bin instead of installing from git.
setup:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -d .venv ]; then
        uv venv .venv
    fi
    if [ -n "${HEGEL_BINARY:-}" ]; then
        ln -sf "$HEGEL_BINARY" .venv/bin/hegel
    else
        uv pip install --python .venv/bin/python "hegel @ git+ssh://git@github.com/antithesishq/hegel.git"
    fi

check: lint docs test test-all-features

docs:
    cargo clean --doc && cargo doc --open --all-features --no-deps

test:
    #!/usr/bin/env bash
    set -euo pipefail
    export PATH="$(pwd)/.venv/bin:$PATH"
    RUST_BACKTRACE=1 cargo test

test-all-features:
    #!/usr/bin/env bash
    set -euo pipefail
    export PATH="$(pwd)/.venv/bin:$PATH"
    RUST_BACKTRACE=1 cargo test --all-features

format:
    cargo fmt
    # also run format-nix if we have nix installed
    which nix && just format-nix || true

check-format:
    cargo fmt --check

format-nix:
    nix run nixpkgs#nixfmt -- flake.nix

check-format-nix:
    nix run nixpkgs#nixfmt -- --check flake.nix

lint:
    cargo fmt --check
    cargo clippy --all-features --tests -- -D warnings
    RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps

coverage:
    #!/usr/bin/env bash
    set -euo pipefail
    # requires:
    # * cargo install cargo-llvm-cov
    # * rustup component add llvm-tools-preview
    export PATH="$(pwd)/.venv/bin:$PATH"
    RUST_BACKTRACE=1 cargo llvm-cov --all-features --fail-under-lines 30 --show-missing-lines

build-conformance:
    cargo build --release --manifest-path tests/conformance/rust/Cargo.toml

conformance: build-conformance
    #!/usr/bin/env bash
    set -euo pipefail
    export PATH="$(pwd)/.venv/bin:$PATH"
    uv pip install --python .venv/bin/python pytest pytest-subtests hypothesis > /dev/null 2>&1 || true
    .venv/bin/python -m pytest tests/conformance/test_conformance.py --durations=20 --durations-min=1.0

# don't print bash comments as output during `just` invocation
set ignore-comments := true

# Install dependencies and the hegel binary.
# If HEGEL_BINARY is set, symlinks it into ~/.local/bin instead of installing from git.
setup:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -n "${HEGEL_BINARY:-}" ]; then
        mkdir -p "$HOME/.local/bin"
        ln -sf "$HEGEL_BINARY" "$HOME/.local/bin/hegel"
    else
        uv tool install "hegel @ git+ssh://git@github.com/antithesishq/hegel-core.git"
    fi

check: lint docs test test-all-features

docs:
    cargo clean --doc && cargo doc --open --all-features --no-deps

test:
    RUST_BACKTRACE=1 cargo test

test-all-features:
    RUST_BACKTRACE=1 cargo test --all-features

format:
    cargo fmt
    # also run format-nix if we have nix installed
    @which nix && just format-nix || true

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
    # requires cargo-llvm-cov and llvm-tools-preview
    RUST_BACKTRACE=1 cargo llvm-cov --all-features --fail-under-lines 30 --show-missing-lines

build-conformance:
    cargo build --release --manifest-path tests/conformance/rust/Cargo.toml

conformance: build-conformance
    uv run --with "hegel @ git+ssh://git@github.com/antithesishq/hegel-core.git" \
        --with pytest --with hypothesis pytest tests/conformance/test_conformance.py

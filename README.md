# Hegel Rust SDK

Hegel rust SDK.

## Installation

Add to your `Cargo.toml`:

```toml
[dev-dependencies]
hegel = { git = "ssh://git@github.com/antithesishq/hegel-rust" }
```

During build, `hegel-rust`:

* Looks for `hegel` on PATH
* Otherwise, installs hegel with uv
   * Looks for `uv` on PATH
   * Otherwise, installs uv from installer

`hegel-rust` build artifacts are stored in cargo's `OUT_DIR / hegel`.

### Nix

To use hegel-rust in Nix:

```nix
{
  description = "sandbox";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    hegel-rust.url = "git+ssh://git@github.com/antithesishq/hegel-rust";
  };

  outputs = { nixpkgs, hegel-rust, ... }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};
    in
    {
      packages.${system}.default = pkgs.rustPlatform.buildRustPackage {
        pname = "rust-hegel-sandbox";
        version = "0.1.0";
        src = ./.;
        cargoLock.lockFile = ./Cargo.lock;

        # hegel binary on PATH so build.rs finds it
        nativeBuildInputs = [
          hegel-rust.packages.${system}.default
        ];
      };

      devShells.${system}.default = pkgs.mkShell {
        buildInputs = [
          pkgs.cargo
          pkgs.rustc
          hegel-rust.packages.${system}.default
        ];
      };
    };
}
```

## Quick Start

```rust
use hegel::gen::{self, Generate};

#[test]
fn test_addition_commutative() {
    hegel::hegel(|| {
        let x = gen::integers::<i32>().generate();
        let y = gen::integers::<i32>().generate();
        assert_eq!(x + y, y + x);
    });
}
```

Run with `cargo test`.

## Documentation

`just docs` to build and open the docs locally.

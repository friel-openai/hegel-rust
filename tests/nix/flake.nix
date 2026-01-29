{
  description = "Nix integration test for hegel-rust";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    hegel.url = "git+ssh://git@github.com/antithesishq/hegel";
  };

  outputs = { nixpkgs, hegel, ... }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};

      # Assemble source: tests/nix files + hegel-rust repo at "hegel-rust" subdir
      src = pkgs.runCommand "nix-test-src" {} ''
        mkdir $out
        cp -r ${./.}/* $out/
        cp -r ${./../..} $out/hegel-rust
      '';
    in
    {
      packages.${system}.default = pkgs.rustPlatform.buildRustPackage {
        pname = "nix-test";
        version = "0.1.0";
        src = src;
        cargoLock.lockFile = ./Cargo.lock;
        nativeBuildInputs = [ hegel.packages.${system}.default ];
        doCheck = true;
      };

      devShells.${system}.default = pkgs.mkShell {
        buildInputs = [
          pkgs.cargo
          pkgs.rustc
          hegel.packages.${system}.default
        ];
      };
    };
}

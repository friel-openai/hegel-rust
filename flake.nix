{
  description = "Hegel Rust SDK";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    hegel.url = "git+ssh://git@github.com/antithesishq/hegel";
    flake-compat.url = "https://flakehub.com/f/edolstra/flake-compat/1.tar.gz";
  };

  outputs =
    {
      nixpkgs,
      hegel,
      ...
    }:
    let
      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;

      mkHegelRustProject =
        {
          pkgs,
          system ? pkgs.system,
        }@args:
        let
        in
        pkgs.rustPlatform.buildRustPackage {
          pname = "hegel";
          version = "0.1.0";
          src = ./.;
          cargoLock.lockFile = ./Cargo.lock;

          # hegel binary on PATH so build.rs finds it
          nativeBuildInputs = [ hegel.packages.${system}.default ];
        };
    in
    {

      # Export the builder for users
      lib = {
        inherit mkHegelRustProject;
      };

      packages = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          lib = pkgs.lib;
        in
        {
          default = mkHegelRustProject { inherit pkgs; };
        }
      );

      devShells = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          default = pkgs.mkShell {
            buildInputs = [
              pkgs.cargo
              pkgs.rustc
              hegel.packages.${system}.default
            ];
          };
        }
      );
    };
}

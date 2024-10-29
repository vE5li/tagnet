{
  inputs = {
    flake-utils = {
      url = "github:numtide/flake-utils";
    };

    nixpkgs = {
      url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    };

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
    };
  };

  outputs = {
    self,
    flake-utils,
    nixpkgs,
    rust-overlay,
  }:
    {
      overlays.default = final: prev: {
        tagnet = final.callPackage ./tagnet.nix {};
      };

      nixosModules.default = import ./module.nix self;
    }
    // flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = (import nixpkgs) {
          inherit system;
          overlays = [self.overlays.default (import rust-overlay)];
        };
      in {
        formatter = pkgs.alejandra;

        packages.default = pkgs.tagnet;

        devShell =
          pkgs.mkShell
          {
            nativeBuildInputs = with pkgs; [
              (rust-bin.fromRustupToolchainFile ./rust-toolchain.toml)
              pkg-config
            ];
            buildInputs = with pkgs; [
              openssl
            ];

            RUST_SRC_PATH = pkgs.rustPlatform.rustLibSrc;
          };
      }
    );
}

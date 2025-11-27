{
  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = {
    self,
    flake-utils,
    nixpkgs,
    rust-overlay,
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        overlays = [(import rust-overlay)];
        pkgs = (import nixpkgs) {inherit system overlays;};
        # frontend = pkgs.writeShellScriptBin "frontend" "(cd tagnet && deno task tauri dev)";
      in {
        formatter = pkgs.alejandra;

        devShell =
          pkgs.mkShell
          {
            nativeBuildInputs = with pkgs; [
              (rust-bin.fromRustupToolchainFile ./rust-toolchain.toml)
              pkg-config
            ];
            buildInputs = with pkgs; [
              openssl
              # frontend
            ];

            RUST_SRC_PATH = pkgs.rustPlatform.rustLibSrc;
            PKG_CONFIG_PATH = "${pkgs.glib.dev}/lib/pkgconfig:${pkgs.libsoup_3.dev}/lib/pkgconfig:${pkgs.webkitgtk_4_1.dev}/lib/pkgconfig:${pkgs.at-spi2-atk.dev}/lib/pkgconfig:${pkgs.gtk3.dev}/lib/pkgconfig:${pkgs.gdk-pixbuf.dev}/lib/pkgconfig:${pkgs.cairo.dev}/lib/pkgconfig:${pkgs.pango.dev}/lib/pkgconfig:${pkgs.harfbuzz.dev}/lib/pkgconfig";
          };
      }
    );
}

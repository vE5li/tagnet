{
  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = { self, flake-utils, rust-overlay, nixpkgs }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = (import nixpkgs) { inherit system overlays; };
      in
      {
        devShell = pkgs.mkShell
          {
            nativeBuildInputs = with pkgs; [
              (rust-bin.fromRustupToolchainFile ./rust-toolchain.toml)
              pkg-config
            ];
            buildInputs = with pkgs;
              [
                at-spi2-atk
                atkmm
                cairo
                gdk-pixbuf
                glib
                gobject-introspection
                gobject-introspection.dev
                gtk3
                harfbuzz
                librsvg
                libsoup_3
                pango
                webkitgtk_4_1
                webkitgtk_4_1.dev

                openssl
                deno
              ];

            RUST_SRC_PATH = pkgs.rustPlatform.rustLibSrc;
            PKG_CONFIG_PATH = "${pkgs.glib.dev}/lib/pkgconfig:${pkgs.libsoup_3.dev}/lib/pkgconfig:${pkgs.webkitgtk_4_1.dev}/lib/pkgconfig:${pkgs.at-spi2-atk.dev}/lib/pkgconfig:${pkgs.gtk3.dev}/lib/pkgconfig:${pkgs.gdk-pixbuf.dev}/lib/pkgconfig:${pkgs.cairo.dev}/lib/pkgconfig:${pkgs.pango.dev}/lib/pkgconfig:${pkgs.harfbuzz.dev}/lib/pkgconfig";

            # shellHook =
            #   ''
            #     export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${
            #       pkgs.lib.strings.makeLibraryPath
            #       [
            #         pkgs.wayland
            #         pkgs.vulkan-loader
            #         pkgs.libxkbcommon
            #       ]};
            #   '';
          };
      }
    );
}

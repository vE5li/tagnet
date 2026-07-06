{
  lib,
  rustPlatform,
}:
rustPlatform.buildRustPackage {
  pname = "tagnetd";
  version = "0.1.0";

  src = lib.cleanSource ./.;

  cargoLock = {
    lockFile = ./Cargo.lock;
  };

  # Only build and install the `tagnetd` daemon binary from the workspace.
  cargoBuildFlags = ["--package" "tagnetd"];
  cargoTestFlags = ["--package" "tagnetd"];

  meta = {
    description = "Tagnet file synchronization daemon";
    mainProgram = "tagnetd";
  };
}

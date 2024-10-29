{
  lib,
  rustPlatform,
}:
rustPlatform.buildRustPackage {
  pname = "tagnet";
  version = "0.1.0";

  src = lib.cleanSource ./.;

  cargoLock = {
    lockFile = ./Cargo.lock;
  };

  # Only build and install the `tagnet` daemon binary from the workspace.
  cargoBuildFlags = ["--package" "tagnet"];
  cargoTestFlags = ["--package" "tagnet"];

  meta = {
    description = "Tagnet file synchronization daemon";
    mainProgram = "tagnet";
  };
}

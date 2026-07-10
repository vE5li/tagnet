{
  lib,
  rustPlatform,
}:
rustPlatform.buildRustPackage {
  pname = "tagnet";
  version = "0.1.0";

  src = lib.cleanSource ../.;

  cargoLock = {
    lockFile = ../Cargo.lock;
  };

  cargoBuildFlags = ["--package" "tagnet"];
  cargoTestFlags = ["--package" "tagnet"];

  meta = {
    description = "Tagnet CLI client";
    mainProgram = "tagnet";
  };
}

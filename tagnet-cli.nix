{
  lib,
  rustPlatform,
}:
rustPlatform.buildRustPackage {
  pname = "tagnet-cli";
  version = "0.1.0";

  src = lib.cleanSource ./.;

  cargoLock = {
    lockFile = ./Cargo.lock;
  };

  cargoBuildFlags = ["--package" "tagnet-cli"];
  cargoTestFlags = ["--package" "tagnet-cli"];

  meta = {
    description = "Tagnet CLI";
    mainProgram = "tagnet-cli";
  };
}

//! Thin desktop daemon wrapper around the `tagnetd` library.
//!
//! All runtime logic lives in the library (`tagnetd::run`); this
//! binary only parses arguments, resolves on-disk paths from the environment,
//! and wires up a Ctrl-C handler to the library's cooperative shutdown.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tagnetd::{
    RunPaths, ShutdownSignal,
    configuration::Configuration,
    control::serve_control,
    identity::Identity,
    paths::{Paths, control_socket_path},
};

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Arguments {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Create this machine's long-lived identity key in `~/.tagnet`.
    Keygen,
    /// Write an example configuration file, filling in this machine's public key.
    Generate {
        file_name: PathBuf,
    },
    Run {
        configuration_file: PathBuf,
    },
}

/// Resolve on-disk paths from the environment.
///
/// The library no longer reads the environment itself; the desktop binary is
/// responsible for turning `TAGNET_DATA_DIR` / `TAGNET_PRIVATE_KEY_FILE` into a
/// [`Paths`]. Panicking here (rather than deep in the library) keeps the
/// failure mode obvious for a shell-launched daemon.
fn paths_from_env() -> Paths {
    let data_dir =
        std::env::var("TAGNET_DATA_DIR").expect("TAGNET_DATA_DIR environment variable not set");
    let identity_file = std::env::var("TAGNET_PRIVATE_KEY_FILE")
        .expect("TAGNET_PRIVATE_KEY_FILE environment variable not set");
    Paths::new(data_dir, identity_file)
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    env_logger::init();

    let arguments = Arguments::parse();

    match arguments.command {
        // FIX: Refactor, just output to stdout instead of writing to a file.
        Commands::Keygen => {
            let paths = paths_from_env();
            let path = paths.identity_path();
            if path.exists() {
                panic!(
                    "An identity key already exists at {}. Refusing to overwrite it; \
                     delete it manually if you really want to rotate this machine's identity.",
                    path.display()
                );
            }
            std::fs::create_dir_all(paths.data_dir()).unwrap();

            let identity = Identity::generate();
            identity.save(path).unwrap_or_else(|error| {
                panic!(
                    "Failed to write identity key to {}: {error}",
                    path.display()
                )
            });

            log::info!("Generated identity key at {}", path.display());
            log::info!("Public key: {}", identity.public_key());
        }
        // FIX: Remove, for development only.
        Commands::Generate { file_name } => {
            let paths = paths_from_env();
            let path = paths.identity_path();
            let _identity = Identity::load(path).unwrap_or_else(|error| {
                panic!(
                    "No usable identity key at {} ({error}). Run 'tagnet keygen' first.",
                    path.display()
                )
            });

            let configuration = Configuration::new_example();
            configuration.write_to_file(file_name);
        }
        Commands::Run { configuration_file } => {
            let paths = paths_from_env();
            let configuration = Configuration::new(configuration_file);

            let run_paths = RunPaths {
                data_dir: paths.data_dir().to_path_buf(),
                identity_file: paths.identity_path().to_path_buf(),
            };

            // Wire Ctrl-C to the library's cooperative shutdown so the daemon
            // (and systemd stop) exits cleanly instead of being killed.
            let shutdown = ShutdownSignal::new();

            {
                let shutdown = shutdown.clone();
                tokio::spawn(async move {
                    if let Err(error) = tokio::signal::ctrl_c().await {
                        log::warn!("Failed to listen for Ctrl-C: {error}");
                        return;
                    }
                    log::info!("Received Ctrl-C; shutting down");
                    shutdown.shutdown();
                });
            }

            // Start the runtime, keeping the UI-facing `Api` so we can also
            // serve the local control socket (portability plan section 7):
            // the desktop daemon owns the DB, and a separate UI process attaches
            // over this socket. It shares the runtime's shutdown signal so a
            // Ctrl-C / systemd stop tears both down together.
            let (api, driver) = match tagnetd::run(configuration, run_paths, shutdown.clone()).await
            {
                Ok(pair) => pair,
                Err(error) => {
                    log::error!("tagnet runtime failed to start: {error}");
                    return Err(std::io::Error::other(error.to_string()));
                }
            };

            let control_socket = control_socket_path();
            let control = tokio::spawn(serve_control(
                api,
                control_socket,
                shutdown.token().child_token(),
            ));

            let run_result = driver.await;

            // The runtime driver returned (shutdown observed). Make sure the
            // control task also winds down and log any late error.
            shutdown.shutdown();
            match control.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => log::warn!("Control socket error: {error}"),
                Err(error) => log::warn!("Control task panicked: {error}"),
            }

            if let Err(error) = run_result {
                log::error!("tagnet runtime failed: {error}");
                return Err(std::io::Error::other(error.to_string()));
            }
        }
    }

    Ok(())
}

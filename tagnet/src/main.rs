use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use futures_util::{SinkExt, StreamExt, stream::SplitSink, stream::SplitStream};

use clap::{Parser, Subcommand};
use tagnet_core::{
    FileId, TagId,
    state::{Change, ChangeOrigin, Frame, ManifestEntry, Sync as SyncMessage},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{
        RwLock,
        mpsc::{UnboundedReceiver, UnboundedSender},
    },
};
use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Message};

use crate::{
    configuration::{Configuration, Peer, RuntimeConfiguration, SyncType},
    database::{FileDatabase, SubtagRule},
    directory_manager::{SyncDirectoryCommand, SyncDirectoryManager},
    identity::{HandshakeMessage, Identity},
};

mod configuration;
mod database;
mod directory_manager;
mod identity;
mod paths;
mod watcher;

use crate::paths::{identity_path, main_db_path, tagnet_data_dir};

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Arguments {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    // FIX: Remove, for development only.
    Reset {
        configuration_file: PathBuf,
    },
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

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    env_logger::init();

    let arguments = Arguments::parse();

    match arguments.command {
        // FIX: Remove, for development only.
        Commands::Reset { configuration_file } => {
            let data_dir = tagnet_data_dir();
            log::info!("Re-creating {}", data_dir.to_string_lossy());
            std::fs::remove_dir_all(&data_dir).unwrap();
            std::fs::create_dir(&data_dir).unwrap();

            let configuration = Configuration::new(configuration_file);
            for sync_directory in configuration.sync_directories {
                if let SyncType::Universal = sync_directory.sync_type {
                    log::info!("Re-creating {}", sync_directory.path.to_string_lossy());
                    std::fs::remove_dir_all(&sync_directory.path).unwrap();
                    std::fs::create_dir(&sync_directory.path).unwrap();
                }
            }

            let database =
                FileDatabase::initialize(main_db_path()).expect("Failed to open database file");

            database
                .add_tag(
                    TagId::from_string("e1de1ee0-3dec-47b2-8e95-842c0acc0dfd").unwrap(),
                    "screenshots",
                    "red",
                )
                .unwrap();
            database
                .add_tag(
                    TagId::from_string("ca39bd61-1b06-4907-b36f-e7a968793e48").unwrap(),
                    "computer",
                    "red",
                )
                .unwrap();
            database
                .add_tag(
                    TagId::from_string("5a0e2939-f881-4c55-a349-cbb91c082057").unwrap(),
                    "image",
                    "red",
                )
                .unwrap();

            database.show_content(false).unwrap();
        }
        // FIX: Refactor, just output to stdout instead of writing to a file.
        Commands::Keygen => {
            let path = identity_path();
            if path.exists() {
                panic!(
                    "An identity key already exists at {}. Refusing to overwrite it; \
                     delete it manually if you really want to rotate this machine's identity.",
                    path.display()
                );
            }
            std::fs::create_dir_all(tagnet_data_dir()).unwrap();

            let identity = Identity::generate();
            identity.save(&path).unwrap_or_else(|error| {
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
            let path = identity_path();
            let identity = Identity::load(&path).unwrap_or_else(|error| {
                panic!(
                    "No usable identity key at {} ({error}). Run 'tagnet keygen' first.",
                    path.display()
                )
            });

            let configuration = Configuration::new_example();
            configuration.write_to_file(file_name);
        }
        Commands::Run { configuration_file } => {
            let configuration = Configuration::new(configuration_file);
            let runtime_configuration =
                Arc::new(RwLock::new(RuntimeConfiguration::new(&configuration)));

            // Load this machine's identity keypair.
            let path = identity_path();
            let identity = Identity::load(&path).unwrap_or_else(|error| {
                panic!(
                    "No usable identity key at {} ({error}). Run 'tagnet keygen' first.",
                    path.display()
                )
            });
            let our_public_key = identity.public_key();

            let identity = Arc::new(identity);

            // Open the main DB. It will be owned by `handle_changes` (the only
            // task that mutates it). Before handing it off, snapshot the
            // latest content hash per file so `SyncDirectoryManager` can
            // detect files that changed on disk while we were offline without
            // ever touching the main DB itself.
            let database =
                FileDatabase::initialize(main_db_path()).expect("Failed to open database file");
            let last_known_hashes = database
                .latest_content_hashes()
                .expect("Failed to load last-known content hashes");

            let (change_sender, change_receiver) = tokio::sync::mpsc::unbounded_channel();
            let (command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();

            tokio::spawn(handle_sync_directories(
                configuration.clone(),
                last_known_hashes,
                change_sender.clone(),
                command_receiver,
            ));

            tokio::spawn(handle_changes(
                configuration.clone(),
                runtime_configuration.clone(),
                database,
                change_receiver,
                command_sender.clone(),
            ));

            // Spawn one outbound connection task per peer that has an address configured.
            for peer in &configuration.peers {
                if peer.address.is_some() {
                    tokio::spawn(connect_to_peer(
                        identity.clone(),
                        peer.clone(),
                        change_sender.clone(),
                        command_sender.clone(),
                        runtime_configuration.clone(),
                    ));
                }
            }

            if let Some(listen_port) = configuration.listen_port {
                let bind_address = format!("0.0.0.0:{listen_port}");
                let listener = TcpListener::bind(&bind_address)
                    .await
                    .unwrap_or_else(|e| panic!("Failed to bind to {bind_address}: {e}"));
                log::info!("Listening for peer connections on {bind_address}");

                while let Ok((stream, address)) = listener.accept().await {
                    tokio::spawn(handle_connection(
                        configuration.clone(),
                        identity.clone(),
                        runtime_configuration.clone(),
                        change_sender.clone(),
                        command_sender.clone(),
                        stream,
                        address,
                    ));
                }
            } else {
                log::info!("No listen_port configured; not accepting inbound peer connections");
                // Keep the process alive so the spawned tasks can run.
                std::future::pending::<()>().await;
            }
        }
    }

    Ok(())
}

async fn handle_connection(
    configuration: Configuration,
    identity: Arc<Identity>,
    runtime_configuration: Arc<RwLock<RuntimeConfiguration>>,
    change_sender: UnboundedSender<(Change, ChangeOrigin)>,
    command_sender: UnboundedSender<SyncDirectoryCommand>,
    raw_stream: TcpStream,
    address: SocketAddr,
) {
    log::debug!("Incoming TCP connection from: {:?}", address);

    let Ok(ws_stream) = tokio_tungstenite::accept_async(raw_stream).await else {
        log::error!("Error during the websocket handshake occurred");
        return;
    };

    log::debug!("WebSocket connection established: {:?}", address);

    let (mut outgoing, mut incoming) = ws_stream.split();

    // Read the peer's handshake first (they initiated the TCP connection).
    let peer_public_key = match read_handshake(&mut incoming, &configuration, &identity).await {
        HandshakeResult::Accepted(public_key) => public_key,
        HandshakeResult::Rejected => return,
    };

    // Respond: sign the peer's public key to prove we own our private key.
    let response = match identity.sign_handshake(&peer_public_key) {
        Ok(response) => response,
        Err(error) => {
            log::warn!("Failed to build handshake response for {address}: {error}");
            return;
        }
    };
    if let Err(error) = outgoing
        .send(Message::text(serde_json::to_string(&response).unwrap()))
        .await
    {
        log::warn!("Failed to send handshake to {address}: {error}");
        return;
    }

    let peer_name = configuration.peer_name(&peer_public_key).to_owned();

    log::info!("Inbound peer at {address} identified as {peer_name} ({peer_public_key})");

    run_peer_session(
        &peer_public_key,
        &peer_name,
        outgoing,
        incoming,
        runtime_configuration,
        change_sender,
        command_sender,
    )
    .await;

    log::info!("Inbound connection from {peer_name} closed");
}

/// Maintain an outbound WebSocket connection to a single peer.
///
/// On each successful connection, a fresh `(peer_tx, peer_rx)` channel is created.
/// `peer_tx` is stored in `RuntimeConfiguration.peers[public_key].outbound` so that
/// `forward_to_peers` can send `Change`s to this peer. When the connection drops,
/// `outbound` is reset to `None` and the task sleeps before retrying.
async fn connect_to_peer(
    identity: Arc<Identity>,
    peer: Peer,
    change_sender: UnboundedSender<(Change, ChangeOrigin)>,
    command_sender: UnboundedSender<SyncDirectoryCommand>,
    runtime_configuration: Arc<RwLock<RuntimeConfiguration>>,
) {
    // TODO: Make this configurable.
    const RETRY_INTERVAL: Duration = Duration::from_secs(5);

    let Some((ip, port)) = peer.address else {
        // Caller should have filtered these out, but be defensive.
        return;
    };
    let url = format!("ws://{ip}:{port}");

    loop {
        log::debug!("Attempting outbound connection to {} ({url})", peer.name);
        match tokio_tungstenite::connect_async(&url).await {
            Ok((ws_stream, _response)) => {
                log::info!("Outbound connection established to {} ({url})", peer.name);

                let (mut outgoing, mut incoming) = ws_stream.split();

                // Build our handshake: sign the peer's public key to prove our identity.
                let handshake = match identity.sign_handshake(&peer.public_key) {
                    Ok(handshake) => handshake,
                    Err(error) => {
                        log::error!("Cannot build handshake for peer {}: {error}", peer.name);
                        tokio::time::sleep(RETRY_INTERVAL).await;
                        continue;
                    }
                };

                // Send our handshake first.
                if let Err(error) = outgoing
                    .send(Message::text(serde_json::to_string(&handshake).unwrap()))
                    .await
                {
                    log::warn!("Failed to send handshake to {}: {error}", peer.name);
                    tokio::time::sleep(RETRY_INTERVAL).await;
                    continue;
                }

                // Read their response.
                let received = match incoming.next().await {
                    Some(Ok(message)) => message.to_string(),
                    Some(Err(error)) => {
                        log::warn!("Handshake read error from {}: {error}", peer.name);
                        tokio::time::sleep(RETRY_INTERVAL).await;
                        continue;
                    }
                    None => {
                        log::warn!("Peer {} closed before sending handshake", peer.name);
                        tokio::time::sleep(RETRY_INTERVAL).await;
                        continue;
                    }
                };
                let response: HandshakeMessage = match serde_json::from_str(&received) {
                    Ok(response) => response,
                    Err(error) => {
                        log::warn!("Invalid handshake JSON from {}: {error}", peer.name);
                        tokio::time::sleep(RETRY_INTERVAL).await;
                        continue;
                    }
                };

                // Verify their public key matches what we expect.
                if response.public_key != peer.public_key {
                    log::warn!(
                        "Peer {} announced public_key {:?}, expected {:?}; \
                         dropping connection",
                        peer.name,
                        response.public_key,
                        peer.public_key
                    );
                    tokio::time::sleep(RETRY_INTERVAL).await;
                    continue;
                }

                // Verify their signature proves ownership of that public key.
                if let Err(error) = identity.verify_handshake(&response) {
                    log::warn!(
                        "Peer {} handshake verification failed ({error}); dropping connection",
                        peer.name
                    );
                    tokio::time::sleep(RETRY_INTERVAL).await;
                    continue;
                }

                run_peer_session(
                    &peer.public_key,
                    &peer.name,
                    outgoing,
                    incoming,
                    runtime_configuration.clone(),
                    change_sender.clone(),
                    command_sender.clone(),
                )
                .await;

                log::info!("Outbound connection to {} dropped", peer.name);
            }
            Err(error) => {
                log::debug!("Outbound connection to {url} failed: {error}");
            }
        }

        tokio::time::sleep(RETRY_INTERVAL).await;
    }
}

enum HandshakeResult {
    Accepted(String),
    Rejected,
}

async fn read_handshake(
    incoming: &mut SplitStream<WebSocketStream<TcpStream>>,
    configuration: &Configuration,
    identity: &Identity,
) -> HandshakeResult {
    let Some(first) = incoming.next().await else {
        log::warn!("Peer closed before sending handshake");
        return HandshakeResult::Rejected;
    };
    let first = match first {
        Ok(message) => message.to_string(),
        Err(error) => {
            log::warn!("Handshake read error: {error}");
            return HandshakeResult::Rejected;
        }
    };
    let message: HandshakeMessage = match serde_json::from_str(&first) {
        Ok(message) => message,
        Err(error) => {
            log::warn!("Invalid handshake JSON: {error}");
            return HandshakeResult::Rejected;
        }
    };

    // Reject unknown public keys.
    if !configuration
        .peers
        .iter()
        .any(|peer| peer.public_key == message.public_key)
    {
        log::warn!(
            "Rejecting connection: unknown public_key {:?}",
            message.public_key
        );
        return HandshakeResult::Rejected;
    }

    // Verify the peer's signature proves ownership of that public key.
    match identity.verify_handshake(&message) {
        Ok(peer_public_key) => HandshakeResult::Accepted(peer_public_key),
        Err(error) => {
            log::warn!("Peer handshake verification failed ({error}); rejecting connection");
            HandshakeResult::Rejected
        }
    }
}

/// Drive a fully-handshaken WebSocket connection until it closes.
///
/// Shared between inbound (`handle_connection`) and outbound (`connect_to_peer`)
/// paths because the post-handshake behaviour is identical: build and send our
/// manifest, register an outbound channel, then loop over outbound `Frame`s
/// and inbound WS frames.
///
/// Opens its own read-only handle on the main DB. The DB is shared with
/// `handle_changes` and with other connection tasks; SQLite serialises these
/// accesses at the file level. Writes still only happen from `handle_changes`.
async fn run_peer_session<S>(
    peer_public_key: &str,
    peer_name: &str,
    mut outgoing: SplitSink<WebSocketStream<S>, Message>,
    mut incoming: SplitStream<WebSocketStream<S>>,
    runtime_configuration: Arc<RwLock<RuntimeConfiguration>>,
    change_sender: UnboundedSender<(Change, ChangeOrigin)>,
    command_sender: UnboundedSender<SyncDirectoryCommand>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    // FileDatabase wraps a rusqlite Connection which is Send but not Sync.
    // We must never hold `&FileDatabase` across an `.await` in this task,
    // otherwise tokio::spawn rejects the future as non-Send. All sync helpers
    // below take `&FileDatabase` synchronously and return owned data; this
    // function does the awaits separately.
    let database = match FileDatabase::initialize(main_db_path()) {
        Ok(database) => database,
        Err(error) => {
            log::error!("Peer {peer_name}: failed to open main DB for session: {error:?}");
            return;
        }
    };

    let (peer_tx, mut peer_rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();
    // Sentinel clone retained for the lifetime of the session: we use
    // `same_channel` against the slot in `RuntimePeer.outbound` to know whether
    // the sender currently parked there is still ours (vs. one a sibling
    // session installed after we registered).
    let our_sender = peer_tx.clone();

    // Register our outbound sender so `forward_to_peers` can route live
    // changes through this connection.
    //
    // The slot can hold one of three things:
    // - `None`: free, install our sender, we own it.
    // - `Some(dead)`: a previous session's sender whose receiver has been
    //   dropped (this happens because the cleanup at the end of a session
    //   cannot detect "I am the dropped receiver"; `is_closed` returns false
    //   while we still hold our own receiver). We replace it transparently.
    // - `Some(live)`: a sibling session is actively running for this peer
    //   (e.g. both sides dialed each other at the same time). Fall back to
    //   inbound-only so we don't double-send.
    let owns_outbound = {
        let mut runtime = runtime_configuration.write().await;
        match runtime.peers.get_mut(peer_public_key) {
            Some(runtime_peer) => {
                let slot_is_dead = runtime_peer
                    .outbound
                    .as_ref()
                    .map(|sender| sender.is_closed())
                    .unwrap_or(true);
                if slot_is_dead {
                    runtime_peer.outbound = Some(peer_tx);
                    true
                } else {
                    log::debug!(
                        "Peer {peer_name} already has an outbound sender; \
                         inbound-only mode for this connection"
                    );
                    false
                }
            }
            None => {
                log::error!(
                    "Peer {peer_name} missing from RuntimeConfiguration; \
                     dropping connection"
                );
                return;
            }
        }
    };

    // Announce our manifest first thing post-handshake. The peer will compare
    // it against their own history and request anything they need.
    match build_local_manifest(&database) {
        Ok(manifest) => {
            let frame = Frame::Sync(SyncMessage::Manifest { entries: manifest });
            if let Err(error) = send_frame(&mut outgoing, &frame).await {
                log::warn!("Failed to send initial manifest to {peer_name}: {error}");
                clear_outbound_if_owned(
                    &runtime_configuration,
                    peer_public_key,
                    owns_outbound,
                    &our_sender,
                )
                .await;
                return;
            }
            log::debug!("Sent initial manifest to {peer_name}");
        }
        Err(error) => {
            log::error!("Peer {peer_name}: failed to build initial manifest: {error:?}");
            // Continue without manifest; the peer's manifest still drives
            // anything they need to receive from us.
        }
    }

    loop {
        tokio::select! {
            outbound = peer_rx.recv() => {
                let Some(frame) = outbound else {
                    // Sender dropped (cleared during teardown or replaced).
                    break;
                };
                if let Err(error) = send_frame(&mut outgoing, &frame).await {
                    log::warn!("Outbound send to {peer_name} failed: {error}");
                    break;
                }
            }
            inbound = incoming.next() => {
                let Some(message) = inbound else {
                    log::info!("Peer {peer_name} closed the connection");
                    break;
                };
                let message = match message {
                    Ok(message) => message,
                    Err(error) => {
                        log::warn!("Read error from {peer_name}: {error}");
                        break;
                    }
                };
                let text = message.to_string();
                let frame: Frame = match serde_json::from_str(&text) {
                    Ok(frame) => frame,
                    Err(error) => {
                        log::error!(
                            "Failed to deserialize inbound Frame from {peer_name}: {error}"
                        );
                        continue;
                    }
                };
                match frame {
                    Frame::Change(change) => {
                        if let Err(error) = change_sender.send((
                            change,
                            ChangeOrigin::Peer {
                                public_key: peer_public_key.to_owned(),
                            },
                        )) {
                            log::error!(
                                "change_sender closed; cannot dispatch inbound Change: {error}"
                            );
                            break;
                        }
                    }
                    Frame::Sync(SyncMessage::Manifest { entries }) => {
                        // Resolve our outbound sender once, then run the
                        // synchronous reconciliation. Doing the DB work
                        // outside of any held `RwLockReadGuard` keeps this
                        // future `Send` (FileDatabase isn't Sync).
                        let outbound = {
                            let runtime = runtime_configuration.read().await;
                            runtime
                                .peers
                                .get(peer_public_key)
                                .and_then(|runtime_peer| runtime_peer.outbound.clone())
                        };
                        let Some(outbound) = outbound else {
                            log::warn!(
                                "No outbound channel registered for {peer_name}; \
                                 cannot answer manifest"
                            );
                            continue;
                        };
                        reconcile_peer_manifest(
                            peer_name,
                            entries,
                            &database,
                            &outbound,
                        );
                    }
                    Frame::Sync(SyncMessage::Request { file_id }) => {
                        // Ask the directory manager to read the bytes off
                        // disk. We await the oneshot here, not inside a
                        // synchronous helper, so the only thing held across
                        // the await is the `command_sender` reference.
                        let (respond_to, response) = tokio::sync::oneshot::channel();
                        if let Err(error) = command_sender.send(
                            SyncDirectoryCommand::ReadFile { file_id, respond_to },
                        ) {
                            log::error!(
                                "command_sender closed; cannot fulfil Sync::Request \
                                 from {peer_name}: {error}"
                            );
                            break;
                        }
                        let read_result = match response.await {
                            Ok(result) => result,
                            Err(error) => {
                                log::error!(
                                    "Directory manager dropped ReadFile responder for {}: \
                                     {error}",
                                    file_id.to_string()
                                );
                                continue;
                            }
                        };
                        let outbound = {
                            let runtime = runtime_configuration.read().await;
                            runtime
                                .peers
                                .get(peer_public_key)
                                .and_then(|runtime_peer| runtime_peer.outbound.clone())
                        };
                        let Some(outbound) = outbound else {
                            log::warn!(
                                "No outbound channel for {peer_name}; \
                                 dropping response to Sync::Request"
                            );
                            continue;
                        };
                        let frame = build_request_response(
                            peer_name,
                            file_id,
                            read_result,
                            &database,
                        );
                        if let Err(error) = outbound.send(frame) {
                            log::warn!(
                                "Failed to enqueue Sync response for {peer_name}: \
                                 {error}"
                            );
                        }
                    }
                    Frame::Sync(SyncMessage::NotFound { file_id }) => {
                        // The peer told us they no longer have content for a
                        // file we asked about. Nothing we can do
                        // automatically; flag it so a human can chase it up
                        // (e.g. it might still exist on a third peer).
                        log::warn!(
                            "Peer {peer_name} reported NotFound for file {}",
                            file_id.to_string()
                        );
                    }
                }
            }
        }
    }

    clear_outbound_if_owned(
        &runtime_configuration,
        peer_public_key,
        owns_outbound,
        &our_sender,
    )
    .await;
}

async fn send_frame<S>(
    outgoing: &mut SplitSink<WebSocketStream<S>, Message>,
    frame: &Frame,
) -> Result<(), String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let text = serde_json::to_string(frame).map_err(|e| format!("serialize: {e}"))?;
    outgoing
        .send(Message::text(text))
        .await
        .map_err(|e| format!("send: {e}"))
}

async fn clear_outbound_if_owned(
    runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
    peer_public_key: &str,
    owns_outbound: bool,
    our_sender: &UnboundedSender<Frame>,
) {
    if !owns_outbound {
        return;
    }
    let mut runtime = runtime_configuration.write().await;
    if let Some(runtime_peer) = runtime.peers.get_mut(peer_public_key)
        && let Some(current) = runtime_peer.outbound.as_ref()
        && current.same_channel(our_sender)
    {
        // The slot still holds the sender we installed (no sibling session
        // replaced it). Drop it so the next session sees a free slot.
        //
        // We deliberately do not check `is_closed()` here: that check is
        // unreliable while we (the receiver's owner) are still alive, and
        // pointless once we're not. Identity via `same_channel` is the only
        // reliable test.
        runtime_peer.outbound = None;
    }
}

/// Read every file's full version history from the main DB and pack into a
/// `Vec<ManifestEntry>` suitable for `Sync::Manifest`. Files without any
/// recorded version are skipped (with a warning).
fn build_local_manifest(database: &FileDatabase) -> Result<Vec<ManifestEntry>, String> {
    let rows = database
        .manifest_entries()
        .map_err(|e| format!("manifest_entries: {e:?}"))?;
    Ok(rows
        .into_iter()
        .map(|(file_id, history, latest_observed_at)| ManifestEntry {
            file_id,
            history,
            latest_observed_at,
        })
        .collect())
}

/// Compare the peer's manifest against our local `file_versions` table and
/// decide which files we need them to send us. Sends back `Sync::Request` for
/// each one over `outbound`.
///
/// Pure synchronous function: no `.await`, no `RwLock`. Lets callers hold
/// `&FileDatabase` (which is `!Sync`) without making their future non-`Send`.
///
/// Categories per entry:
/// - **Unknown file_id**: we've never seen this file — request it.
/// - **Equal latest**: identical state — nothing to do.
/// - **Sender's latest hash appears in our history**: they are behind. Their
///   side will request from us when they process our manifest; we do nothing.
/// - **Our latest hash appears in their history**: we are behind — request the
///   newer bytes.
/// - **Divergent**: neither side's latest appears in the other's history.
///   Newer `latest_observed_at` wins. If theirs wins, request. If ours wins,
///   do nothing (their side will accept ours via the symmetric path).
///   Divergence is logged at `error!` level with a TODO for a future
///   deadletter store.
fn reconcile_peer_manifest(
    peer_name: &str,
    entries: Vec<ManifestEntry>,
    database: &FileDatabase,
    outbound: &UnboundedSender<Frame>,
) {
    log::info!(
        "Reconciling {} manifest entries from {peer_name}",
        entries.len()
    );

    for entry in entries {
        let decision = match decide_request(database, &entry) {
            Ok(decision) => decision,
            Err(error) => {
                log::error!(
                    "Reconciliation lookup failed for {}: {error:?}",
                    entry.file_id.to_string()
                );
                continue;
            }
        };
        match decision {
            ReconcileDecision::Nothing => {}
            ReconcileDecision::Request(reason) => {
                log::debug!(
                    "Requesting {} from {peer_name}: {reason}",
                    entry.file_id.to_string()
                );
                let frame = Frame::Sync(SyncMessage::Request {
                    file_id: entry.file_id,
                });
                if let Err(error) = outbound.send(frame) {
                    log::warn!("Failed to enqueue Sync::Request for {peer_name}: {error}");
                    return;
                }
            }
            ReconcileDecision::Divergent {
                ours_observed_at,
                request,
            } => {
                // TODO: When a deadletter / conflict store exists, preserve
                // the losing version there instead of just logging.
                log::error!(
                    "Divergent history for {} between us and {peer_name} \
                     (our latest observed_at={ours_observed_at}, theirs={}). \
                     {}.",
                    entry.file_id.to_string(),
                    entry.latest_observed_at,
                    if request {
                        "Their version wins; requesting"
                    } else {
                        "Our version wins; keeping"
                    },
                );
                if request {
                    let frame = Frame::Sync(SyncMessage::Request {
                        file_id: entry.file_id,
                    });
                    if let Err(error) = outbound.send(frame) {
                        log::warn!("Failed to enqueue Sync::Request for {peer_name}: {error}");
                        return;
                    }
                }
            }
        }
    }
}

enum ReconcileDecision {
    Nothing,
    Request(&'static str),
    Divergent {
        ours_observed_at: i64,
        request: bool,
    },
}

/// Pure decision function: given our local DB and the peer's entry, what
/// should we do? Separated from `handle_peer_manifest` so it can be reasoned
/// about (and later tested) without touching channels.
fn decide_request(
    database: &FileDatabase,
    entry: &ManifestEntry,
) -> Result<ReconcileDecision, database::DatabaseError> {
    let our_history = database.version_history(entry.file_id)?;

    if our_history.is_empty() {
        return Ok(ReconcileDecision::Request("unknown file"));
    }

    let their_latest = match entry.history.last() {
        Some((_, hash)) => hash.as_str(),
        None => return Ok(ReconcileDecision::Nothing),
    };
    let our_latest = our_history
        .last()
        .expect("checked non-empty above")
        .1
        .as_str();

    if our_latest == their_latest {
        return Ok(ReconcileDecision::Nothing);
    }

    let our_hashes: HashSet<&str> = our_history.iter().map(|(_, hash)| hash.as_str()).collect();
    let their_hashes: HashSet<&str> = entry
        .history
        .iter()
        .map(|(_, hash)| hash.as_str())
        .collect();

    let they_have_our_latest = their_hashes.contains(our_latest);
    let we_have_their_latest = our_hashes.contains(their_latest);

    match (they_have_our_latest, we_have_their_latest) {
        // Their latest is somewhere in our history → they are strictly behind.
        // They'll request from us when they process our manifest.
        (_, true) => Ok(ReconcileDecision::Nothing),
        // Our latest is somewhere in their history → we are strictly behind.
        (true, false) => Ok(ReconcileDecision::Request("we are behind")),
        // Neither side knows the other's latest hash → divergent.
        (false, false) => {
            let ours_observed_at = database
                .latest_version(entry.file_id)?
                .map(|version| version.observed_at)
                .unwrap_or(0);
            let request = entry.latest_observed_at > ours_observed_at;
            Ok(ReconcileDecision::Divergent {
                ours_observed_at,
                request,
            })
        }
    }
}

/// Build the response `Frame` for a peer's `Sync::Request` once the bytes
/// have been retrieved by the directory manager. Synchronous so callers can
/// hold `&FileDatabase` (which is `!Sync`) without losing `Send`-ness.
fn build_request_response(
    peer_name: &str,
    file_id: FileId,
    read_result: Option<(PathBuf, Vec<u8>, String)>,
    database: &FileDatabase,
) -> Frame {
    match read_result {
        Some((path, content, content_hash)) => {
            // Look up tags so the receiver can apply them. Reconciliation
            // currently only sends files, not tag definitions; if the peer
            // doesn't already know a tag they will receive an unknown TagId
            // here. Documented limitation, see roadmap step 8.
            let tags: Vec<TagId> = match database.tag_ids_for_file(file_id, SubtagRule::Exclude) {
                Ok(iter) => iter.into_iter().collect(),
                Err(error) => {
                    log::warn!(
                        "Failed to read tags for {}: {error:?}; sending with empty tag list",
                        file_id.to_string()
                    );
                    Vec::new()
                }
            };
            Frame::Change(Change::FileAdded {
                file_id,
                path: path.to_string_lossy().to_string(),
                content,
                content_hash,
                tags,
            })
        }
        None => {
            log::warn!(
                "Sync::Request from {peer_name} for {} but no sync directory has it",
                file_id.to_string()
            );
            Frame::Sync(SyncMessage::NotFound { file_id })
        }
    }
}

async fn handle_sync_directories(
    configuration: Configuration,
    last_known_hashes: HashMap<FileId, String>,
    change_sender: UnboundedSender<(Change, ChangeOrigin)>,
    command_receiver: UnboundedReceiver<SyncDirectoryCommand>,
) {
    SyncDirectoryManager::new(configuration, change_sender, command_receiver)
        .await
        .run(last_known_hashes)
        .await;
}

fn contains_all_tags(sync_directory_tags: &[TagId], file_tags: &[TagId]) -> bool {
    sync_directory_tags
        .iter()
        .all(|tag_id| file_tags.contains(tag_id))
}

async fn handle_changes(
    configuration: Configuration,
    runtime_configuration: Arc<RwLock<RuntimeConfiguration>>,
    mut database: FileDatabase,
    mut change_receiver: tokio::sync::mpsc::UnboundedReceiver<(Change, ChangeOrigin)>,
    command_sender: UnboundedSender<SyncDirectoryCommand>,
) {
    /// Origin tag stored in `file_versions.origin` for locally-observed
    /// versions. Peer-originated versions will use the originating peer's
    /// public key here instead.
    const LOCAL_ORIGIN: &str = "local";

    /// Resolve the `origin` string to store in `file_versions.origin` for a
    /// `Change` we just received.
    fn version_origin(change_origin: &ChangeOrigin) -> &str {
        match change_origin {
            ChangeOrigin::Local { .. } => LOCAL_ORIGIN,
            ChangeOrigin::Peer { public_key } => public_key.as_str(),
        }
    }

    async fn forward_to_peers(
        configuration: &Configuration,
        runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
        change: &Change,
        change_origin: &ChangeOrigin,
    ) {
        // TODO: Apply per-peer SyncType filtering once it's tracked (step 8).
        let runtime = runtime_configuration.read().await;
        for peer in &configuration.peers {
            if let ChangeOrigin::Peer { public_key } = &change_origin
                && public_key == &peer.public_key
            {
                // Nothing to do, the change originates from this peer.
                continue;
            }

            let Some(runtime_peer) = runtime.peers.get(&peer.public_key) else {
                log::warn!(
                    "Peer {} ({}) missing from RuntimeConfiguration",
                    peer.name,
                    peer.public_key
                );
                continue;
            };

            let Some(outbound) = runtime_peer.outbound.as_ref() else {
                // TODO: Buffer or rely on reconciliation (step 6) when peer reconnects.
                log::debug!("Peer {} not connected; dropping outbound Change", peer.name);
                continue;
            };

            if let Err(error) = outbound.send(Frame::Change(change.clone())) {
                log::warn!("Failed to enqueue Change for peer {}: {error}", peer.name);
            }
        }
    }

    while let Some((change, change_origin)) = change_receiver.recv().await {
        match &change {
            // Change::Copy {
            //   path,
            //   content,
            // }
            Change::FileAdded {
                file_id,
                path,
                content,
                content_hash,
                tags,
            } => {
                // Reconciliation (step 6) and live edits can both deliver a
                // `FileAdded` for a `file_id` we already know about. Branch
                // on existence so we stay idempotent:
                //
                // - **Unknown file_id**: original behaviour — insert into
                //   `files`, attach tags, record initial version, dispatch
                //   `CreateFile` to matching sync directories.
                // - **Known file_id, hash already in our history**: nothing
                //   to do; the bytes already exist locally. Log and skip.
                // - **Known file_id, new hash**: treat as a `FileChanged` —
                //   record the new version and dispatch `ChangeFile`
                //   commands. Tags from the inbound message are ignored in
                //   this branch (tag reconciliation is out of scope; the
                //   local tag set on this file is the source of truth).
                let already_exists = database.file_exists(*file_id).unwrap_or_else(|error| {
                    log::error!(
                        "file_exists check failed for {}: {:?}; assuming new",
                        file_id.to_string(),
                        error
                    );
                    false
                });

                if !already_exists {
                    database
                        .add_file(*file_id, path.clone())
                        .expect("Failed to add file to database");

                    tags.iter().for_each(|tag_id| {
                        database
                            .tag_file(*tag_id, *file_id)
                            .expect("failed to tag added file");
                    });

                    if let Err(error) = database.record_version(
                        *file_id,
                        content_hash,
                        version_origin(&change_origin),
                    ) {
                        log::error!(
                            "Failed to record initial version for {}: {:?}",
                            file_id.to_string(),
                            error
                        );
                    }

                    for sync_directory in &configuration.sync_directories {
                        if let ChangeOrigin::Local { directory_path } = &change_origin
                            && directory_path == &sync_directory.path
                            && let SyncType::TagBased { .. } = &sync_directory.sync_type
                        {
                            // If the file came from a tag based sync directory, we don't
                            // need to take any action.
                            continue;
                        };

                        if let SyncType::TagBased {
                            tags: sync_directory_tags,
                        } = &sync_directory.sync_type
                            && !contains_all_tags(sync_directory_tags, tags)
                        {
                            // If the directory is tag based and the file *does not* have
                            // all the tags the sync directory does, skip this sync
                            // directory.
                            continue;
                        }

                        // This means the event didn't originate from this sync directory
                        // itself and the tags match, thus we may want to apply the
                        // change.
                        // TODO: Handle result.
                        let _ = command_sender.send(SyncDirectoryCommand::CreateFile {
                            file_id: *file_id,
                            file_name: path.clone().into(),
                            content: content.clone(),
                            sync_directory_path: sync_directory.path.clone(),
                        });
                    }
                } else {
                    // Known file. Decide based on whether we've ever recorded
                    // this content_hash for this file.
                    let history = database.version_history(*file_id).unwrap_or_else(|error| {
                        log::error!(
                            "version_history failed for known file {}: {:?}; \
                             treating as duplicate",
                            file_id.to_string(),
                            error
                        );
                        Vec::new()
                    });
                    let hash_known = history
                        .iter()
                        .any(|(_, hash)| hash.as_str() == content_hash.as_str());

                    if hash_known {
                        log::debug!(
                            "Ignoring duplicate FileAdded for {} (hash already in history)",
                            file_id.to_string()
                        );
                    } else {
                        log::debug!(
                            "Promoting FileAdded for known file {} to FileChanged \
                             (new content_hash)",
                            file_id.to_string()
                        );
                        if let Err(error) = database.record_version(
                            *file_id,
                            content_hash,
                            version_origin(&change_origin),
                        ) {
                            log::error!(
                                "Failed to record version for {}: {:?}",
                                file_id.to_string(),
                                error
                            );
                        }

                        // For dispatch we mirror the FileChanged path:
                        // ignore the inbound `tags` (local tag set is
                        // authoritative) and use what we already have on file.
                        let local_file_tags = database
                            .tag_ids_for_file(*file_id, database::SubtagRule::Exclude)
                            .map(|iter| iter.into_iter().collect::<Vec<TagId>>())
                            .unwrap_or_else(|error| {
                                log::error!(
                                    "Failed to read local tags for {}: {:?}",
                                    file_id.to_string(),
                                    error
                                );
                                Vec::new()
                            });

                        for sync_directory in &configuration.sync_directories {
                            if let ChangeOrigin::Local { directory_path } = &change_origin
                                && directory_path == &sync_directory.path
                            {
                                continue;
                            };

                            if let SyncType::TagBased {
                                tags: sync_directory_tags,
                            } = &sync_directory.sync_type
                                && !contains_all_tags(sync_directory_tags, &local_file_tags)
                            {
                                continue;
                            }

                            let _ = command_sender.send(SyncDirectoryCommand::ChangeFile {
                                file_id: *file_id,
                                content: content.clone(),
                                sync_directory_path: sync_directory.path.clone(),
                            });
                        }
                    }
                }

                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            Change::FileMoved { file_id, path } => {
                // TODO: Don't unwrap.
                // TODO: Should this be include? Currently this WILL NOT WORK since add file
                // doesn't consider subtags. We would need to get a list of *all* tags (incuding
                // subdags) when adding the file to make it work.
                // -> Maybe make it configurable in the config, per-sync directory.
                let file_tags = database
                    .tag_ids_for_file(*file_id, database::SubtagRule::Exclude)
                    .expect("failed to get file tags")
                    .into_iter()
                    .collect::<Vec<TagId>>();

                database
                    .update_file_path(*file_id, path.clone())
                    .expect("Failed to update file path");

                for sync_directory in &configuration.sync_directories {
                    if let ChangeOrigin::Local { directory_path } = &change_origin
                        && directory_path == &sync_directory.path
                    {
                        // If the file is already modified in the origin, we don't need to take
                        // any action.
                        continue;
                    };

                    if let SyncType::TagBased {
                        tags: sync_directory_tags,
                    } = &sync_directory.sync_type
                        && !contains_all_tags(sync_directory_tags, &file_tags)
                    {
                        // If the directory is tag based and the file *does not* have all the
                        // tags the sync directory does, skip this sync directory.
                        continue;
                    }

                    // This means the event didn't originate from this sync directory itself and
                    // the tags match, thus we may want to apply the change.
                    // TODO: Handle result.
                    let _ = command_sender.send(SyncDirectoryCommand::MoveFile {
                        file_id: *file_id,
                        path: PathBuf::from(&path),
                        sync_directory_path: sync_directory.path.clone(),
                    });
                }

                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            Change::FileChanged {
                file_id,
                content,
                content_hash,
            } => {
                // TODO: Don't unwrap.
                // TODO: Should this be include? Currently this WILL NOT WORK since add file
                // doesn't consider subtags. We would need to get a list of *all* tags (incuding
                // subdags) when adding the file to make it work.
                // -> Maybe make it configurable in the config, per-sync directory.
                let file_tags = database
                    .tag_ids_for_file(*file_id, database::SubtagRule::Exclude)
                    .expect("failed to get file tags")
                    .into_iter()
                    .collect::<Vec<TagId>>();

                if let Err(error) =
                    database.record_version(*file_id, content_hash, version_origin(&change_origin))
                {
                    log::error!(
                        "Failed to record version for {}: {:?}",
                        file_id.to_string(),
                        error
                    );
                }

                for sync_directory in &configuration.sync_directories {
                    if let ChangeOrigin::Local { directory_path } = &change_origin
                        && directory_path == &sync_directory.path
                    {
                        // If the file is already modified in the origin, we don't need to take
                        // any action.
                        continue;
                    };

                    if let SyncType::TagBased {
                        tags: sync_directory_tags,
                    } = &sync_directory.sync_type
                        && !contains_all_tags(sync_directory_tags, &file_tags)
                    {
                        // If the directory is tag based and the file *does not* have all the
                        // tags the sync directory does, skip this sync directory.
                        continue;
                    }

                    // This means the event didn't originate from this sync directory itself and
                    // the tags match, thus we may want to apply the change.
                    // TODO: Handle result.
                    let _ = command_sender.send(SyncDirectoryCommand::ChangeFile {
                        file_id: *file_id,
                        content: content.clone(),
                        sync_directory_path: sync_directory.path.clone(),
                    });
                }

                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            Change::FileDeleted { file_id } => {
                // NOTE: No row is appended to `file_versions` here. A
                // "tombstone" version (e.g. a row with a sentinel hash, or a
                // separate `is_deleted` flag) will be needed for cross-peer
                // delete-vs-edit conflict resolution: a peer that was offline
                // during the delete will reconnect and announce a regular
                // version for this `file_id`, and we need a way to recognise
                // that our delete supersedes it. The shape of that row
                // (nullable hash? new column? separate table?) depends on the
                // conflict-resolution scheme, so it is intentionally left to
                // the session implementing peer sync. `Change::FileDeleted`
                // also has no version metadata on the wire yet for the same
                // reason.
                //
                // TODO: Don't unwrap.
                // TODO: Should this be include? Currently this WILL NOT WORK since add file
                // doesn't consider subtags. We would need to get a list of *all* tags (incuding
                // subdags) when adding the file to make it work.
                // -> Maybe make it configurable in the config, per-sync directory.
                let file_tags = database
                    .tag_ids_for_file(*file_id, database::SubtagRule::Exclude)
                    .expect("failed to get file tags")
                    .into_iter()
                    .collect::<Vec<TagId>>();

                database
                    .remove_file(*file_id)
                    .expect("Failed to remove file from database");

                for sync_directory in &configuration.sync_directories {
                    if let ChangeOrigin::Local { directory_path } = &change_origin
                        && directory_path == &sync_directory.path
                    {
                        // If the file came from this directory, it is already removed. We
                        // can just skip this directory.
                        continue;
                    };

                    if let SyncType::TagBased {
                        tags: sync_directory_tags,
                    } = &sync_directory.sync_type
                        && !contains_all_tags(sync_directory_tags, &file_tags)
                    {
                        // If the directory is tag based and the file *does not* have all the
                        // tags the sync directory does, skip this sync directory.
                        continue;
                    }

                    // This means the event didn't originate from this sync directory itself, thus
                    // we may want to apply it.
                    // TODO: Handle result.
                    let _ = command_sender.send(SyncDirectoryCommand::RemoveFile {
                        file_id: *file_id,
                        sync_directory_path: sync_directory.path.clone(),
                    });
                }

                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            Change::TagAdded {
                tag_id,
                tag_name,
                metadata: _,
            } => {
                // TODO: Don't unwrap.
                database.add_tag(*tag_id, tag_name, "red").unwrap();
                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            Change::TagRenamed { tag_id, tag_name } => {
                // TODO: Don't unwrap.
                database.update_tag_name(*tag_id, tag_name).unwrap();
                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            Change::TagChanged {
                tag_id: _,
                metadata: _,
            } => { // TODO
            }
            Change::TagRemoved { tag_id } => {
                // TODO: Don't unwrap.
                database.remove_tag(*tag_id).unwrap();
                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            Change::FileTagged {
                file_id,
                tag_id,
                metadata: _,
            } => {
                // TODO: Don't unwrap.
                database.tag_file(*tag_id, *file_id).unwrap();

                // FIX: Update the files in the sync directories.

                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            Change::FileTagChanged {
                file_id: _,
                tag_id: _,
                metadata: _,
            } => { // TODO
            }
            Change::FileUntagged { file_id, tag_id } => {
                // TODO: Don't unwrap.
                database.untag_file(*tag_id, *file_id).unwrap();

                // FIX: Update the files in the sync directories.

                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            Change::TagTagged {
                taggee_id,
                tag_id,
                metadata: _,
            } => {
                // TODO: Don't unwrap.
                database.tag_tag(*tag_id, *taggee_id).unwrap();

                // NOTE: Currently this is correct, but if we change the subtag rules on the sync
                // directories we will have to update the sync directories here too.

                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            Change::TagTagChanged {
                taggee_id: _,
                tag_id: _,
                metadata: _,
            } => { // TODO
            }
            Change::TagUntagged { taggee_id, tag_id } => {
                // TODO: Don't unwrap.
                database.untag_tag(*tag_id, *taggee_id).unwrap();

                // NOTE: Currently this is correct, but if we change the subtag rules on the sync
                // directories we will have to update the sync directories here too.

                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
        }

        database.show_content(false).unwrap();
    }
}

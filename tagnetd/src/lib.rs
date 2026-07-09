//! Core tagnet runtime as a library.
//!
//! This crate used to be a pure binary whose entire runtime lived in
//! `main.rs`. It has been split so that the runtime is callable as a library
//! function ([`run`]): the desktop binary (`main.rs`) is a thin CLI wrapper,
//! and other frontends (e.g. an Android native library) can link this crate
//! and call [`run`] directly without a `main()`.
//!
//! All business logic (peer sync, the DB pipeline, change handling) lives
//! here behind [`run`]. Frontends supply:
//!
//! - a [`Configuration`](configuration::Configuration),
//! - a [`RunPaths`] describing where the data directory and identity key live,
//! - a [`ShutdownSignal`] used to stop the runtime cleanly.

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use futures_util::{SinkExt, StreamExt, stream::SplitSink, stream::SplitStream};

use tagnet_core::{
    FileId, PhysicalPath, TagId,
    state::{
        Change, ChangeOrigin, Frame, ManifestEntry, RelationshipManifestEntry, Sync as SyncMessage,
        TagManifestEntry,
    },
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{
        RwLock,
        mpsc::{UnboundedReceiver, UnboundedSender},
    },
};
use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Message};
use tokio_util::sync::CancellationToken;

use crate::{
    bus::DaemonMessage,
    configuration::{Configuration, Peer, RuntimeConfiguration, SyncType},
    database::{FileDatabase, SubtagRule},
    directory_manager::{SyncDirectoryCommand, SyncDirectoryManager},
    fetch::PendingFetches,
    identity::{HandshakeMessage, Identity},
    paths::Paths,
};

pub mod api;
pub mod bus;
pub mod configuration;
pub mod control;
pub mod database;
pub mod directory_manager;
pub mod fetch;
pub mod identity;
pub mod paths;
pub mod transport;
pub mod watcher;

/// On-disk locations a caller must supply to [`run`].
///
/// `data_dir` holds the databases; `identity_file` is this machine's
/// long-lived identity key. Kept as a small owned struct (rather than reading
/// the environment inside the library) so every frontend can decide where
/// these live: the desktop binary reads env vars, Android passes
/// `getFilesDir()`.
#[derive(Debug, Clone)]
pub struct RunPaths {
    pub data_dir: PathBuf,
    pub identity_file: PathBuf,
}

impl From<RunPaths> for Paths {
    fn from(run_paths: RunPaths) -> Self {
        Paths::new(run_paths.data_dir, run_paths.identity_file)
    }
}

/// The shared routing handles every peer-connection task needs: the runtime
/// peer table, the pending on-demand fetches, and the two senders into the
/// change bus and sync-directory manager.
///
/// Bundled into one `Clone` struct so `handle_connection`, `connect_to_peer`,
/// and `run_peer_session` can pass a single context around instead of the same
/// four arguments each (which also keeps them under clippy's argument-count
/// lint). All four fields are cheap to clone (`Arc`s / channel senders).
#[derive(Clone)]
struct PeerContext {
    runtime_configuration: Arc<RwLock<RuntimeConfiguration>>,
    pending_fetches: PendingFetches,
    change_sender: UnboundedSender<DaemonMessage>,
    command_sender: UnboundedSender<SyncDirectoryCommand>,
}

/// Cooperative shutdown handle for [`run`].
///
/// A thin wrapper around a [`CancellationToken`]. The caller holds the
/// [`ShutdownSignal`] and calls [`ShutdownSignal::shutdown`] (e.g. from a
/// Ctrl-C handler, a systemd stop, or the Android service `onDestroy`); the
/// running [`run`] future observes the cancellation, stops accepting new work,
/// drains its tasks, and returns cleanly.
#[derive(Debug, Clone, Default)]
pub struct ShutdownSignal {
    token: CancellationToken,
}

impl ShutdownSignal {
    /// Create a fresh, un-triggered shutdown signal.
    pub fn new() -> Self {
        Self::default()
    }

    /// Request shutdown. Idempotent; safe to call from any task/thread.
    pub fn shutdown(&self) {
        self.token.cancel();
    }

    /// Has shutdown been requested yet?
    pub fn is_shutdown(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Access the underlying token (e.g. to derive child tokens for tasks).
    pub fn token(&self) -> &CancellationToken {
        &self.token
    }
}

/// Errors that can abort startup of [`run`].
#[derive(Debug)]
pub enum RunError {
    /// The identity key could not be loaded from `identity_file`.
    Identity {
        path: PathBuf,
        source: std::io::Error,
    },
    /// Opening the main database failed.
    Database(database::DatabaseError),
    /// Binding the peer-sync listener failed.
    Bind {
        address: String,
        source: std::io::Error,
    },
}

impl std::fmt::Display for RunError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::Identity { path, source } => write!(
                formatter,
                "failed to load identity key at {}: {source}",
                path.display()
            ),
            RunError::Database(error) => {
                write!(formatter, "failed to open main database: {error:?}")
            }
            RunError::Bind { address, source } => {
                write!(
                    formatter,
                    "failed to bind peer listener to {address}: {source}"
                )
            }
        }
    }
}

impl std::error::Error for RunError {}

/// Start the tagnet sync engine, returning a UI-facing [`Api`](api::Api)
/// handle alongside the runtime driver future.
///
/// This is the former body of the `Run` CLI subcommand, lifted into a library
/// function so it can be driven by any frontend. It performs all fallible
/// startup (loading the identity, opening the main DB, binding the peer
/// listener) up front and returns:
///
/// - an [`Api`](api::Api) the caller can use immediately to serve the UI
///   (reads, writes, event subscription), and
/// - a driver future that runs the accept loop / idle-until-shutdown and then
///   drains the spawned tasks. The caller must poll it to completion (e.g.
///   `tokio::spawn` it, or `.await` it) for the runtime to make progress; it
///   returns once `shutdown` is triggered.
///
/// Every frontend (desktop binary, Android in-process backend, host harness)
/// uses this: the ones that do not need the [`Api`](api::Api) simply await the
/// driver and drop the handle.
pub async fn run(
    configuration: Configuration,
    paths: RunPaths,
    shutdown: ShutdownSignal,
) -> Result<
    (
        api::Api,
        impl std::future::Future<Output = Result<(), RunError>>,
    ),
    RunError,
> {
    let paths: Paths = paths.into();

    let runtime_configuration = Arc::new(RwLock::new(RuntimeConfiguration::new(&configuration)));

    // Shared table of in-flight on-demand fetches (`tagnet edit`). Seeded by
    // `handle_changes` for local-origin requests and by peer sessions for
    // relayed ones; replies are routed by `request_id`. Owns the peer runtime so
    // its engine operations are plain methods. Cheap to clone (Arcs).
    let pending_fetches = crate::fetch::PendingFetches::new(runtime_configuration.clone());

    // Load this machine's identity keypair.
    let identity = Identity::load(paths.identity_path()).map_err(|source| RunError::Identity {
        path: paths.identity_path().to_path_buf(),
        source,
    })?;

    let identity = Arc::new(identity);

    // The path to the main DB. Peer sessions each open their own read-only
    // handle on it (SQLite serialises file-level access), so we keep the path
    // around to hand to every spawned session.
    let main_db_path = paths.main_db_path();

    // Open the main DB. It will be owned by `handle_changes` (the only task
    // that mutates it). Before handing it off, snapshot the latest content
    // hash per file so `SyncDirectoryManager` can detect files that changed on
    // disk while we were offline without ever touching the main DB itself.
    let database = FileDatabase::initialize(&main_db_path).map_err(RunError::Database)?;
    let last_known_hashes = database
        .latest_content_hashes()
        .map_err(RunError::Database)?;

    let (change_sender, change_receiver) = tokio::sync::mpsc::unbounded_channel();
    let (command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();

    // Broadcast of applied changes for the UI-facing API event stream (plan
    // section 5.5). `handle_changes` publishes every change it applies here;
    // API subscribers receive them best-effort. Capacity bounds how far a slow
    // subscriber may lag before it observes `Lagged` (mapped to `Resynced` by
    // the transport). Sized generously; the UI is expected to keep up.
    let (event_sender, _event_receiver) = tokio::sync::broadcast::channel(1024);

    // The UI-facing API handle. Reads open their own read-only DB handle on
    // `main_db_path`; writes go onto `change_sender`; events come from
    // `event_sender`.
    let api = api::Api::new(
        main_db_path.clone(),
        change_sender.clone(),
        command_sender.clone(),
        event_sender.clone(),
    );

    // Spawned tasks each get a child of the shutdown token so we can cancel
    // and drain them together with the accept loop.
    let sync_directories_handle = tokio::spawn(handle_sync_directories(
        configuration.clone(),
        paths.clone(),
        last_known_hashes,
        change_sender.clone(),
        command_receiver,
        shutdown.token().child_token(),
    ));

    let changes_handle = tokio::spawn(handle_changes(
        configuration.clone(),
        runtime_configuration.clone(),
        pending_fetches.clone(),
        database,
        change_receiver,
        command_sender.clone(),
        event_sender,
        shutdown.token().child_token(),
    ));

    // The routing handles every peer-connection task needs. Built once and
    // cloned per spawned task (below, and in the accept loop inside `driver`).
    let peer_context = PeerContext {
        runtime_configuration: runtime_configuration.clone(),
        pending_fetches: pending_fetches.clone(),
        change_sender: change_sender.clone(),
        command_sender: command_sender.clone(),
    };

    // Spawn one outbound connection task per peer that has an address configured.
    let mut peer_handles = Vec::new();
    for peer in &configuration.peers {
        if peer.address.is_some() {
            peer_handles.push(tokio::spawn(connect_to_peer(
                identity.clone(),
                peer.clone(),
                main_db_path.clone(),
                peer_context.clone(),
                shutdown.token().child_token(),
            )));
        }
    }

    // Bind the peer-sync listener up front (if configured) so bind failures
    // surface to the caller before we hand back the `Api`, rather than inside
    // the driver future.
    let listener = if let Some(listen_port) = configuration.listen_port {
        let bind_address = format!("0.0.0.0:{listen_port}");
        let listener = TcpListener::bind(&bind_address)
            .await
            .map_err(|source| RunError::Bind {
                address: bind_address.clone(),
                source,
            })?;
        log::info!("Listening for peer connections on {bind_address}");
        Some(listener)
    } else {
        log::info!("No listen_port configured; not accepting inbound peer connections");
        None
    };

    // The driver future: runs the accept loop (or idles until shutdown), then
    // cancels and drains all spawned tasks. The caller polls it to completion.
    let driver = async move {
        if let Some(listener) = listener {
            loop {
                tokio::select! {
                    _ = shutdown.token().cancelled() => {
                        log::info!("Shutdown requested; stopping peer listener");
                        break;
                    }
                    accepted = listener.accept() => {
                        match accepted {
                            Ok((stream, address)) => {
                                tokio::spawn(handle_connection(
                                    configuration.clone(),
                                    identity.clone(),
                                    main_db_path.clone(),
                                    peer_context.clone(),
                                    stream,
                                    address,
                                    shutdown.token().child_token(),
                                ));
                            }
                            Err(error) => {
                                log::warn!("Peer listener accept error: {error}");
                                break;
                            }
                        }
                    }
                }
            }
        } else {
            // Keep the runtime alive so the spawned tasks can run, until shutdown.
            shutdown.token().cancelled().await;
            log::info!("Shutdown requested; stopping runtime");
        }

        // Ensure the long-lived tasks observe cancellation, then drain them.
        shutdown.shutdown();

        // Dropping the senders lets the receiving tasks fall out of their loops
        // once their channels are empty.
        drop(change_sender);
        drop(command_sender);

        let _ = sync_directories_handle.await;
        let _ = changes_handle.await;
        for handle in peer_handles {
            let _ = handle.await;
        }

        log::info!("tagnet runtime stopped cleanly");
        Ok(())
    };

    Ok((api, driver))
}

async fn handle_connection(
    configuration: Configuration,
    identity: Arc<Identity>,
    main_db_path: PathBuf,
    context: PeerContext,
    raw_stream: TcpStream,
    address: SocketAddr,
    shutdown: CancellationToken,
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
        &main_db_path,
        outgoing,
        incoming,
        context,
        &shutdown,
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
    main_db_path: PathBuf,
    context: PeerContext,
    shutdown: CancellationToken,
) {
    // TODO: Make this configurable.
    const RETRY_INTERVAL: Duration = Duration::from_secs(5);

    let Some((ip, port)) = peer.address else {
        // Caller should have filtered these out, but be defensive.
        return;
    };
    let url = format!("ws://{ip}:{port}");

    loop {
        if shutdown.is_cancelled() {
            return;
        }

        log::debug!("Attempting outbound connection to {} ({url})", peer.name);
        let connect = tokio::select! {
            _ = shutdown.cancelled() => return,
            connect = tokio_tungstenite::connect_async(&url) => connect,
        };
        match connect {
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
                    &main_db_path,
                    outgoing,
                    incoming,
                    context.clone(),
                    &shutdown,
                )
                .await;

                log::info!("Outbound connection to {} dropped", peer.name);
            }
            Err(error) => {
                log::debug!("Outbound connection to {url} failed: {error}");
            }
        }

        if shutdown.is_cancelled() {
            return;
        }
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(RETRY_INTERVAL) => {}
        }
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
    main_db_path: &std::path::Path,
    mut outgoing: SplitSink<WebSocketStream<S>, Message>,
    mut incoming: SplitStream<WebSocketStream<S>>,
    context: PeerContext,
    shutdown: &CancellationToken,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let PeerContext {
        runtime_configuration,
        pending_fetches,
        change_sender,
        command_sender,
    } = context;
    // FileDatabase wraps a rusqlite Connection which is Send but not Sync.
    // We must never hold `&FileDatabase` across an `.await` in this task,
    // otherwise tokio::spawn rejects the future as non-Send. All sync helpers
    // below take `&FileDatabase` synchronously and return owned data; this
    // function does the awaits separately.
    //
    // The database path is supplied by the caller (originally derived from
    // `Paths::main_db_path`).
    let database = match FileDatabase::initialize(main_db_path) {
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

    // Send our tag manifest right after the file manifest. Same anti-entropy
    // idea, driven by last-writer-wins timestamps instead of version chains.
    match build_local_tag_manifest(&database) {
        Ok((definitions, relationships)) => {
            let frame = Frame::Sync(SyncMessage::TagManifest {
                definitions,
                relationships,
            });
            if let Err(error) = send_frame(&mut outgoing, &frame).await {
                log::warn!("Failed to send initial tag manifest to {peer_name}: {error}");
                clear_outbound_if_owned(
                    &runtime_configuration,
                    peer_public_key,
                    owns_outbound,
                    &our_sender,
                )
                .await;
                return;
            }
            log::debug!("Sent initial tag manifest to {peer_name}");
        }
        Err(error) => {
            log::error!("Peer {peer_name}: failed to build initial tag manifest: {error:?}");
        }
    }

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                log::info!("Shutdown requested; closing session with {peer_name}");
                break;
            }
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
                // Ignore WebSocket control frames (ping/pong/close); only
                // data frames carry a `Frame`. Peer `Frame`s are MessagePack
                // (see `send_frame`).
                let payload = match &message {
                    Message::Binary(bytes) => bytes.as_ref(),
                    Message::Text(text) => text.as_bytes(),
                    _ => continue,
                };
                let frame: Frame = match rmp_serde::from_slice(payload) {
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
                        if let Err(error) = change_sender.send(DaemonMessage::Change(
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
                        let outbound = runtime_configuration
                            .read()
                            .await
                            .peers
                            .get(peer_public_key)
                            .and_then(|runtime_peer| runtime_peer.outbound.clone());

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
                    Frame::Sync(SyncMessage::FetchRequest {
                        request_id,
                        file_id,
                        expected_hash,
                    }) => {
                        // A peer is asking us (recursively) for a file's bytes.
                        // Answer locally if we hold matching content; otherwise
                        // relay to our other peers.
                        let have_local = read_local_if_hash_matches(
                            &command_sender,
                            file_id,
                            &expected_hash,
                        )
                        .await;
                        pending_fetches
                            .handle_incoming_request(
                                peer_public_key,
                                request_id,
                                file_id,
                                expected_hash,
                                have_local,
                            )
                            .await;
                    }
                    Frame::Sync(SyncMessage::FetchFound {
                        request_id,
                        file_id,
                        content,
                        content_hash,
                    }) => {
                        pending_fetches
                            .handle_incoming_found(request_id, file_id, content, content_hash)
                            .await;
                    }
                    Frame::Sync(SyncMessage::FetchMissing { request_id }) => {
                        pending_fetches
                            .handle_incoming_missing(peer_public_key, request_id)
                            .await;
                    }
                    Frame::Sync(SyncMessage::TagManifest {
                        definitions,
                        relationships,
                    }) => {
                        // Relationships carry their whole state (including the
                        // soft-delete flag), so apply them directly via the bus
                        // — last-writer-wins is enforced in the DB layer. For
                        // definitions, request the full payload of any the peer
                        // has newer than (or that are unknown to) us.
                        let outbound = runtime_configuration
                            .read()
                            .await
                            .peers
                            .get(peer_public_key)
                            .and_then(|runtime_peer| runtime_peer.outbound.clone());
                        let Some(outbound) = outbound else {
                            log::warn!(
                                "No outbound channel registered for {peer_name}; \
                                 cannot answer tag manifest"
                            );
                            continue;
                        };
                        reconcile_peer_tag_manifest(
                            peer_name,
                            peer_public_key,
                            definitions,
                            relationships,
                            &database,
                            &outbound,
                            &change_sender,
                        );
                    }
                    Frame::Sync(SyncMessage::TagRequest { tag_id }) => {
                        // Answer with the full tag definition as a
                        // `Change::TagAdded`, mirroring how `Sync::Request` is
                        // answered with `Change::FileAdded`. `TagNotFound` if we
                        // no longer hold the tag.
                        let outbound = runtime_configuration
                            .read()
                            .await
                            .peers
                            .get(peer_public_key)
                            .and_then(|runtime_peer| runtime_peer.outbound.clone());
                        let Some(outbound) = outbound else {
                            log::warn!(
                                "No outbound channel for {peer_name}; \
                                 dropping response to TagRequest"
                            );
                            continue;
                        };
                        let frame = build_tag_request_response(peer_name, tag_id, &database);
                        if let Err(error) = outbound.send(frame) {
                            log::warn!(
                                "Failed to enqueue tag Sync response for {peer_name}: {error}"
                            );
                        }
                    }
                    Frame::Sync(SyncMessage::TagNotFound { tag_id }) => {
                        log::warn!(
                            "Peer {peer_name} reported TagNotFound for tag {}",
                            tag_id.to_string()
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
    // Peer `Frame`s are encoded as MessagePack and sent as binary WebSocket
    // frames. This avoids serde_json's `Vec<u8>` -> array-of-integers blowup
    // (~4x on the wire), which dominated the payload for file transfers.
    let bytes = rmp_serde::to_vec_named(frame).map_err(|e| format!("serialize: {e}"))?;
    outgoing
        .send(Message::binary(bytes))
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

/// Read `file_id`'s bytes from local sync directories, but only return them if
/// they hash to `expected_hash`. Used by the on-demand fetch engine to decide
/// whether this node can satisfy a `Sync::FetchRequest` locally.
///
/// Returns `Some(bytes)` on a hash match, `None` if the file is absent locally
/// or its local content does not match the requested hash (in which case the
/// request should be forwarded to peers).
async fn read_local_if_hash_matches(
    command_sender: &UnboundedSender<SyncDirectoryCommand>,
    file_id: FileId,
    expected_hash: &str,
) -> Option<Vec<u8>> {
    let (respond_to, response) = tokio::sync::oneshot::channel();
    if command_sender
        .send(SyncDirectoryCommand::ReadFile {
            file_id,
            respond_to,
        })
        .is_err()
    {
        log::error!("command_sender closed; cannot read local bytes for fetch");
        return None;
    }
    match response.await {
        Ok(Some((_physical_path, content, content_hash))) if content_hash == expected_hash => {
            Some(content)
        }
        Ok(_) => None,
        Err(error) => {
            log::error!(
                "Directory manager dropped ReadFile responder for {}: {error}",
                file_id.to_string()
            );
            None
        }
    }
}

/// Build the response `Frame` for a peer's `Sync::Request` once the bytes
/// have been retrieved by the directory manager. Synchronous so callers can
/// hold `&FileDatabase` (which is `!Sync`) without losing `Send`-ness.
fn build_request_response(
    peer_name: &str,
    file_id: FileId,
    read_result: Option<(PhysicalPath, Vec<u8>, String)>,
    database: &FileDatabase,
) -> Frame {
    match read_result {
        Some((physical_path, content, content_hash)) => {
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
            // `ReadFile` returns the *physical* on-disk path in the serving sync
            // directory. For a Universal directory that is the `file_id`, which
            // would strip the human-readable name for the peer. The main database
            // holds the *logical* path, so prefer it and only fall back — via the
            // blessed ingestion conversion — to the physical path if the lookup
            // fails (a "shouldn't happen" case: we served bytes for a file with
            // no main-DB row).
            let logical_path = match database.logical_path_for_file_id(file_id) {
                Ok(logical_path) => logical_path,
                Err(error) => {
                    log::warn!(
                        "Failed to read logical path for {} from main database: \
                         {error:?}; falling back to physical path {}",
                        file_id.to_string(),
                        physical_path
                    );
                    physical_path.into_logical()
                }
            };
            Frame::Change(Change::FileAdded {
                file_id,
                logical_path,
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

/// Build our local tag manifest: lightweight definition entries (id +
/// `modified_at`) plus every relationship (with its soft-delete state). The
/// tag counterpart of [`build_local_manifest`].
fn build_local_tag_manifest(
    database: &FileDatabase,
) -> Result<(Vec<TagManifestEntry>, Vec<RelationshipManifestEntry>), String> {
    let definitions = database
        .tag_manifest_entries()
        .map_err(|e| format!("tag_manifest_entries: {e:?}"))?;
    let relationships = database
        .relationship_manifest_entries()
        .map_err(|e| format!("relationship_manifest_entries: {e:?}"))?;
    Ok((definitions, relationships))
}

/// Reconcile a peer's tag manifest against ours.
///
/// - **Definitions**: for each tag whose `modified_at` is newer than ours (or
///   that we don't know), enqueue a `TagRequest`; the peer answers with a
///   `Change::TagAdded` carrying the full definition. Older/equal definitions
///   are skipped — the peer will request ours via the symmetric path.
/// - **Relationships**: applied directly. Each carries its whole state, so we
///   translate it into the matching relationship `Change` (tag/untag) stamped
///   with the peer's `modified_at` and hand it to the single DB-writer, which
///   enforces last-writer-wins. This routes through the same code path as a
///   live relationship change, keeping behaviour uniform.
///
/// Pure of `.await` and holds no lock: takes `&FileDatabase` synchronously so
/// the caller's future stays `Send`.
fn reconcile_peer_tag_manifest(
    peer_name: &str,
    peer_public_key: &str,
    definitions: Vec<TagManifestEntry>,
    relationships: Vec<RelationshipManifestEntry>,
    database: &FileDatabase,
    outbound: &UnboundedSender<Frame>,
    change_sender: &UnboundedSender<DaemonMessage>,
) {
    log::info!(
        "Reconciling {} tag definitions and {} relationships from {peer_name}",
        definitions.len(),
        relationships.len()
    );

    for definition in definitions {
        let ours = match database.tag_modified_at(definition.tag_id) {
            Ok(value) => value,
            Err(error) => {
                log::error!(
                    "tag_modified_at failed for {}: {error:?}",
                    definition.tag_id.to_string()
                );
                continue;
            }
        };
        // Request when we don't know the tag, or the peer's is strictly newer.
        let need = match ours {
            None => true,
            Some(ours) => definition.modified_at > ours,
        };
        if need {
            let frame = Frame::Sync(SyncMessage::TagRequest {
                tag_id: definition.tag_id,
            });
            if let Err(error) = outbound.send(frame) {
                log::warn!("Failed to enqueue TagRequest for {peer_name}: {error}");
                return;
            }
        }
    }

    for relationship in relationships {
        // LWW pre-check: skip anything not newer than what we hold. The DB layer
        // also enforces this, but skipping here avoids bus traffic for no-ops.
        let ours = match database.relationship_modified_at(
            relationship.tag_id,
            &relationship.target_id,
            relationship.kind.into(),
        ) {
            Ok(value) => value,
            Err(error) => {
                log::error!("relationship_modified_at failed: {error:?}");
                continue;
            }
        };
        if let Some(ours) = ours
            && relationship.modified_at <= ours
        {
            continue;
        }

        let Some(change) = relationship_to_change(&relationship) else {
            log::warn!(
                "Skipping relationship with unparseable target_id {}",
                relationship.target_id
            );
            continue;
        };
        if let Err(error) = change_sender.send(DaemonMessage::Change(
            change,
            ChangeOrigin::Peer {
                public_key: peer_public_key.to_owned(),
            },
        )) {
            log::error!("change_sender closed; cannot apply reconciled relationship: {error}");
            return;
        }
    }
}

/// Translate a reconciled relationship manifest entry into the equivalent
/// relationship `Change`, carrying the entry's `modified_at` so last-writer-wins
/// is preserved. Returns `None` if `target_id` doesn't parse as the id kind.
fn relationship_to_change(entry: &RelationshipManifestEntry) -> Option<Change> {
    use tagnet_core::state::RelationshipKind;
    let change = match (entry.kind, entry.deleted) {
        (RelationshipKind::File, false) => Change::FileTagged {
            file_id: FileId::from_string(&entry.target_id)?,
            tag_id: entry.tag_id,
            metadata: None,
            modified_at: entry.modified_at,
        },
        (RelationshipKind::File, true) => Change::FileUntagged {
            file_id: FileId::from_string(&entry.target_id)?,
            tag_id: entry.tag_id,
            modified_at: entry.modified_at,
        },
        (RelationshipKind::Tag, false) => Change::TagTagged {
            taggee_id: TagId::from_string(&entry.target_id)?,
            tag_id: entry.tag_id,
            metadata: None,
            modified_at: entry.modified_at,
        },
        (RelationshipKind::Tag, true) => Change::TagUntagged {
            taggee_id: TagId::from_string(&entry.target_id)?,
            tag_id: entry.tag_id,
            modified_at: entry.modified_at,
        },
    };
    Some(change)
}

/// Answer a peer's `TagRequest` with the full tag definition as a
/// `Change::TagAdded`, or `TagNotFound` if we no longer hold the tag. The tag
/// counterpart of [`build_request_response`].
fn build_tag_request_response(peer_name: &str, tag_id: TagId, database: &FileDatabase) -> Frame {
    match database.tag_definition(tag_id) {
        Ok(Some((name, color, modified_at))) => Frame::Change(Change::TagAdded {
            tag_id,
            tag_name: name,
            color,
            metadata: None,
            modified_at,
        }),
        Ok(None) => {
            log::warn!(
                "TagRequest from {peer_name} for {} but we no longer hold it",
                tag_id.to_string()
            );
            Frame::Sync(SyncMessage::TagNotFound { tag_id })
        }
        Err(error) => {
            log::error!(
                "tag_definition failed for {} requested by {peer_name}: {error:?}",
                tag_id.to_string()
            );
            Frame::Sync(SyncMessage::TagNotFound { tag_id })
        }
    }
}

async fn handle_sync_directories(
    configuration: Configuration,
    paths: Paths,
    last_known_hashes: HashMap<FileId, String>,
    change_sender: UnboundedSender<DaemonMessage>,
    command_receiver: UnboundedReceiver<SyncDirectoryCommand>,
    shutdown: CancellationToken,
) {
    let mut manager =
        SyncDirectoryManager::new(configuration, &paths, change_sender, command_receiver).await;
    tokio::select! {
        _ = shutdown.cancelled() => {
            log::info!("Shutdown requested; stopping sync directory manager");
        }
        _ = manager.run(last_known_hashes) => {}
    }
}

fn contains_all_tags(sync_directory_tags: &[TagId], file_tags: &[TagId]) -> bool {
    sync_directory_tags
        .iter()
        .all(|tag_id| file_tags.contains(tag_id))
}

// This is the single change-handling pipeline task; its parameters are the
// distinct long-lived handles it owns (a change receiver, database, event
// sender) plus the routing handles it shares. They don't form a reusable
// cluster the way `PeerContext` does, so they're kept as plain arguments
// rather than bundled into a single-use struct.
#[allow(clippy::too_many_arguments)]
async fn handle_changes(
    configuration: Configuration,
    runtime_configuration: Arc<RwLock<RuntimeConfiguration>>,
    pending_fetches: PendingFetches,
    mut database: FileDatabase,
    mut change_receiver: tokio::sync::mpsc::UnboundedReceiver<DaemonMessage>,
    command_sender: UnboundedSender<SyncDirectoryCommand>,
    event_sender: tokio::sync::broadcast::Sender<Change>,
    shutdown: CancellationToken,
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

    log::info!("handle_changes task started; awaiting changes");

    loop {
        let message = tokio::select! {
            _ = shutdown.cancelled() => {
                log::info!("Shutdown requested; stopping change handler");
                break;
            }
            received = change_receiver.recv() => {
                match received {
                    Some(item) => item,
                    None => {
                        log::warn!(
                            "handle_changes: change_receiver returned None \
                             (all senders dropped); exiting"
                        );
                        break;
                    }
                }
            }
        };

        // Route the two bus message kinds. A `Fetch` is an on-demand request
        // for a file's bytes (from `tagnet edit`): satisfy it locally if we
        // hold matching content, otherwise flood a `FetchRequest` to peers. A
        // `Change` falls through to the DB-writer pipeline below.
        let (change, change_origin) = match message {
            DaemonMessage::Change(change, change_origin) => (change, change_origin),
            DaemonMessage::Fetch {
                file_id,
                expected_hash,
                respond_to,
            } => {
                let have_local =
                    read_local_if_hash_matches(&command_sender, file_id, &expected_hash).await;
                pending_fetches
                    .start_local_fetch(file_id, expected_hash, have_local, respond_to)
                    .await;
                continue;
            }
        };

        match &change {
            // Change::Copy {
            //   path,
            //   content,
            // }
            Change::FileAdded {
                file_id,
                logical_path,
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
                    if let Err(error) = database.add_file(*file_id, logical_path) {
                        // Do not panic: a single bad inbound change must not
                        // take down `handle_changes` (the sole DB writer),
                        // which would close `change_sender` and break every
                        // peer connection into a reconnect loop. Log and skip.
                        log::error!(
                            "Failed to add file {} ({}): {:?}; skipping change",
                            file_id.to_string(),
                            logical_path,
                            error
                        );
                        continue;
                    }

                    // NOTE: We intentionally do NOT apply `tags` here anymore.
                    // File-tag relationships now reconcile through their own tag
                    // manifest (with per-relationship `modified_at` for
                    // last-writer-wins), decoupled from file content. Applying
                    // them here would be both redundant and wrong: a bare
                    // `FileAdded.tags` list carries no timestamp, so it could
                    // resurrect a relationship a peer just untagged. The tag
                    // manifest is the authoritative path. `tags` is retained on
                    // the wire for now for backwards compatibility / local
                    // dispatch below.
                    let _ = tags;

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
                        // change. Resolve where this directory should physically
                        // store the file from its logical path.
                        let physical_path = sync_directory
                            .sync_type
                            .physical_for(logical_path, *file_id);
                        // TODO: Handle result.
                        let _ = command_sender.send(SyncDirectoryCommand::CreateFile {
                            file_id: *file_id,
                            physical_path,
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
            Change::FileMoved {
                file_id,
                logical_path,
            } => {
                // TODO: Don't unwrap.
                // TODO: Should this be include? Currently this WILL NOT WORK since add file
                // doesn't consider subtags. We would need to get a list of *all* tags (incuding
                // subdags) when adding the file to make it work.
                // -> Maybe make it configurable in the config, per-sync directory.
                let file_tags =
                    match database.tag_ids_for_file(*file_id, database::SubtagRule::Exclude) {
                        Ok(tags) => tags.into_iter().collect::<Vec<TagId>>(),
                        Err(error) => {
                            log::error!(
                                "FileMoved: failed to get tags for {}: {:?}; skipping",
                                file_id.to_string(),
                                error
                            );
                            continue;
                        }
                    };

                if let Err(error) = database.update_file_logical_path(*file_id, logical_path) {
                    log::error!(
                        "Failed to update logical path for file {}: {:?}; skipping",
                        file_id.to_string(),
                        error
                    );
                    continue;
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
                    // the tags match, thus we may want to apply the change. Resolve where this
                    // directory should physically place the file from its new logical path.
                    let physical_path = sync_directory
                        .sync_type
                        .physical_for(logical_path, *file_id);
                    // TODO: Handle result.
                    let _ = command_sender.send(SyncDirectoryCommand::MoveFile {
                        file_id: *file_id,
                        physical_path,
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
                let file_tags =
                    match database.tag_ids_for_file(*file_id, database::SubtagRule::Exclude) {
                        Ok(tags) => tags.into_iter().collect::<Vec<TagId>>(),
                        Err(error) => {
                            log::error!(
                                "FileChanged: failed to get tags for {}: {:?}; skipping",
                                file_id.to_string(),
                                error
                            );
                            continue;
                        }
                    };

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
                let file_tags =
                    match database.tag_ids_for_file(*file_id, database::SubtagRule::Exclude) {
                        Ok(tags) => tags.into_iter().collect::<Vec<TagId>>(),
                        Err(error) => {
                            log::error!(
                                "FileDeleted: failed to get tags for {}: {:?}; skipping",
                                file_id.to_string(),
                                error
                            );
                            continue;
                        }
                    };

                if let Err(error) = database.remove_file(*file_id) {
                    log::error!(
                        "Failed to remove file {}: {:?}; skipping",
                        file_id.to_string(),
                        error
                    );
                    continue;
                }

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
            // Every tag mutation below carries `modified_at`, stamped on the
            // originating device and preserved across the wire. It is passed
            // straight to the DB layer, which applies last-writer-wins: an
            // older change is a no-op. This makes both live application and
            // reconciliation replay idempotent and convergent.
            Change::TagAdded {
                tag_id,
                tag_name,
                color,
                metadata: _,
                modified_at,
            } => {
                if let Err(error) = database.add_tag(*tag_id, tag_name, color, *modified_at) {
                    log::error!(
                        "Failed to add tag {} ({}): {:?}",
                        tag_id.to_string(),
                        tag_name,
                        error
                    );
                }
                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            Change::TagRenamed {
                tag_id,
                tag_name,
                modified_at,
            } => {
                if let Err(error) = database.update_tag_name(*tag_id, tag_name, *modified_at) {
                    log::error!("Failed to rename tag {}: {:?}", tag_id.to_string(), error);
                }
                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            Change::TagRecolored {
                tag_id,
                color,
                modified_at,
            } => {
                // Carries the full new color; applied with the same `modified_at`
                // LWW guard as the other tag mutations, then forwarded so peers
                // converge. Mirrors `TagRenamed`.
                if let Err(error) = database.update_tag_color(*tag_id, color, *modified_at) {
                    log::error!("Failed to recolor tag {}: {:?}", tag_id.to_string(), error);
                }
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
                modified_at: _,
            } => {
                // Tag metadata is not yet stored (the whole `MetadataFormat`
                // API is `todo!()` in tagnet-core). When metadata lands, apply
                // it here with the same `modified_at` LWW guard as the other
                // tag mutations and forward. Deliberately not forwarded until
                // then, so we never propagate state we can't apply.
            }
            Change::TagRemoved { tag_id } => {
                // Tag removal is a hard delete for now; the tombstone-based
                // deletion design (which will make removal reconcile
                // offline-safely, like untag) is deferred. See roadmap.
                if let Err(error) = database.remove_tag(*tag_id) {
                    log::error!("Failed to remove tag {}: {:?}", tag_id.to_string(), error);
                }
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
                modified_at,
            } => {
                if let Err(error) = database.tag_file(*tag_id, *file_id, *modified_at) {
                    log::error!(
                        "Failed to tag file {} with {}: {:?}",
                        file_id.to_string(),
                        tag_id.to_string(),
                        error
                    );
                }

                // FIX: Update the files in the tag-based sync directories: a
                // file that just gained a directory's tags should be
                // materialized there (and dropped when it loses them on
                // untag). Tracked separately from tag sync.

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
                modified_at: _,
            } => {
                // Relationship metadata: deferred with the rest of the metadata
                // API. See `TagChanged`.
            }
            Change::FileUntagged {
                file_id,
                tag_id,
                modified_at,
            } => {
                if let Err(error) = database.untag_file(*tag_id, *file_id, *modified_at) {
                    log::error!(
                        "Failed to untag file {} from {}: {:?}",
                        file_id.to_string(),
                        tag_id.to_string(),
                        error
                    );
                }

                // FIX: Update the files in the tag-based sync directories (see
                // `FileTagged`).

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
                modified_at,
            } => {
                if let Err(error) = database.tag_tag(*tag_id, *taggee_id, *modified_at) {
                    log::error!(
                        "Failed to tag tag {} with {}: {:?}",
                        taggee_id.to_string(),
                        tag_id.to_string(),
                        error
                    );
                }

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
                modified_at: _,
            } => {
                // Relationship metadata: deferred with the rest of the metadata
                // API. See `TagChanged`.
            }
            Change::TagUntagged {
                taggee_id,
                tag_id,
                modified_at,
            } => {
                if let Err(error) = database.untag_tag(*tag_id, *taggee_id, *modified_at) {
                    log::error!(
                        "Failed to untag tag {} from {}: {:?}",
                        taggee_id.to_string(),
                        tag_id.to_string(),
                        error
                    );
                }

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

        // Publish the applied change to UI-facing API subscribers (plan 5.5).
        // Best-effort: if there are no subscribers, or the channel is full and
        // a subscriber lags, the send/receive machinery handles it (the
        // subscriber observes `Lagged`, mapped to `Resynced` by the transport).
        let _ = event_sender.send(change);
    }

    log::info!("handle_changes task exited");
}

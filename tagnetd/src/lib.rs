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
    FileId, LogicalPath, PhysicalPath, TagId, TransferId,
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
    bus::{ContentChange, DaemonMessage, Ingest},
    configuration::{Configuration, Peer, RuntimeConfiguration, SyncType},
    database::FileDatabase,
    directory_manager::{SyncDirectoryCommand, SyncDirectoryManager},
    fetch::PendingFetches,
    file_bytes::FileBytes,
    identity::{HandshakeMessage, Identity},
    paths::Paths,
    transfer::{ReceiveOutcome, TransferMessage, spawn_receiver, spawn_sender},
};

/// A resolved sync-directory destination for a content-bearing change, plus how
/// that directory should be told to place the bytes. Produced by
/// `handle_changes` after the origin-skip and tag-match filtering, then turned
/// into a [`SyncDirectoryCommand`] with the actual [`FileBytes`] once the
/// move-vs-copy policy has been applied.
enum ContentTarget {
    Create {
        file_id: FileId,
        physical_path: PhysicalPath,
        sync_directory_path: std::path::PathBuf,
    },
    Change {
        file_id: FileId,
        sync_directory_path: std::path::PathBuf,
    },
}

/// Why the peer session started a receiver transfer, and what to do with the
/// received bytes on completion. Carried alongside a transfer's outcome so the
/// session's completion handler can dispatch correctly.
enum ReceiverPurpose {
    /// Reconciliation / live-change pull: materialize the received bytes into
    /// our sync directories and record the version, placing per `placement`.
    Materialize {
        file_id: FileId,
        content_hash: String,
        origin: ChangeOrigin,
        placement: bus::MaterializePlacement,
    },
    /// On-demand fetch (`tagnet edit`): follow-up action from the fetch engine
    /// (deliver to the local waiter, or relay upward).
    Fetch { then: fetch::FoundThen },
}

impl ContentTarget {
    fn into_command(self, content: FileBytes) -> SyncDirectoryCommand {
        match self {
            ContentTarget::Create {
                file_id,
                physical_path,
                sync_directory_path,
            } => SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content,
                sync_directory_path,
            },
            ContentTarget::Change {
                file_id,
                sync_directory_path,
            } => SyncDirectoryCommand::ChangeFile {
                file_id,
                content,
                sync_directory_path,
            },
        }
    }
}

pub mod api;
pub mod bus;
pub mod configuration;
pub mod control;
pub mod database;
pub mod directory_manager;
pub mod fetch;
pub mod file_bytes;
pub mod identity;
pub mod paths;
pub mod transfer;
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
/// `modified_at` stamped on config-declared tag definitions. Deliberately the
/// lowest possible value so a declaration acts as a last-writer-wins *floor*:
/// `add_tag`'s guard (`excluded.modified_at > tags.modified_at`) means any real
/// edit — always stamped with a positive wall-clock `now_millis()` — wins, and
/// a re-declared tag on the next boot never clobbers a rename/recolor made in
/// between. See [`TagDeclaration`](configuration::TagDeclaration).
const DECLARED_TAG_MODIFIED_AT: i64 = i64::MIN;

/// Enqueue a `Change::TagAdded` for every tag declared in the configuration, so
/// their definitions are guaranteed to exist before any tagging/reconciliation
/// runs. Called from [`run`] before `handle_changes` starts draining the bus.
/// Best-effort per tag: an empty name is skipped (the DB rejects it anyway) and
/// a closed channel is logged.
fn enqueue_declared_tags(
    change_sender: &UnboundedSender<DaemonMessage>,
    configuration: &Configuration,
) {
    for tag in &configuration.tags {
        if tag.name.trim().is_empty() {
            log::warn!(
                "Skipping config tag declaration {} with empty name",
                tag.id.to_string()
            );
            continue;
        }
        // Normalize an empty color to the same default the API uses, so a
        // declared tag renders consistently with a UI-created one.
        let color = if tag.color.trim().is_empty() {
            "#F44336".to_owned()
        } else {
            tag.color.clone()
        };
        let change = Change::TagAdded {
            tag_id: tag.id,
            tag_name: tag.name.clone(),
            color,
            metadata: None,
            modified_at: DECLARED_TAG_MODIFIED_AT,
        };
        if let Err(error) = change_sender.send(DaemonMessage::change(
            change,
            ChangeOrigin::Local {
                directory_path: std::path::PathBuf::new(),
            },
        )) {
            log::error!(
                "Failed to enqueue declared tag {} ({}): {error}",
                tag.name,
                tag.id.to_string()
            );
        }
    }
}

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

    // Guarantee the config-declared tag definitions exist before anything else.
    // These are enqueued now, while `handle_changes` has not yet started
    // draining the bus, so they are the *first* changes it applies — before any
    // peer connects and before any `FileTagged`/reconciliation runs. That way a
    // `SyncType::TagBased` directory referencing a declared id always resolves.
    //
    // Each declaration is a last-writer-wins *floor* (see `TagDeclaration`): it
    // is stamped with a very low `modified_at`, so `add_tag`'s LWW guard creates
    // the tag when absent but never clobbers a newer UI/peer edit.
    enqueue_declared_tags(&change_sender, &configuration);

    // Broadcast of applied changes for the UI-facing API event stream (plan
    // section 5.5). `handle_changes` publishes every change it applies here;
    // API subscribers receive them best-effort. Capacity bounds how far a slow
    // subscriber may lag before it observes `Lagged` (mapped to `Resynced` by
    // the transport). Sized generously; the UI is expected to keep up.
    let (event_sender, _event_receiver) = tokio::sync::broadcast::channel(1024);

    // The UI-facing API handle. Reads open their own read-only DB handle on
    // `main_db_path`; writes go onto `change_sender`; events come from
    // `event_sender`.
    // Daemon-owned temp dir for on-demand fetch results. Clear any orphans a
    // crashed caller left behind, then hand the location to the API so
    // `fetch_file` can stage each result there.
    let fetch_temp_dir = paths.fetch_temp_dir();
    if let Err(error) = paths.clean_fetch_temp_dir().await {
        log::warn!(
            "Failed to prepare fetch temp dir {}: {error}",
            fetch_temp_dir.display()
        );
    }

    let api = api::Api::new(
        main_db_path.clone(),
        change_sender.clone(),
        command_sender.clone(),
        event_sender.clone(),
        pending_fetches.clone(),
        fetch_temp_dir,
    );

    // The sync-directory manager is inherently single-threaded: it holds
    // `RefCell`s (the debounce skip-queue and the rusqlite connections) that are
    // `!Send`, and it now `.await`s file I/O (streaming materialization) while
    // borrowing them. Rather than force `Send` on all of that, run it on a
    // dedicated OS thread with a current-thread runtime + `LocalSet`. A oneshot
    // lets the shutdown path below join it like the other tasks.
    let (sync_directories_done_tx, sync_directories_handle) = tokio::sync::oneshot::channel();
    let sync_directories_thread = {
        let configuration = configuration.clone();
        let paths = paths.clone();
        let change_sender = change_sender.clone();
        let shutdown_child = shutdown.token().child_token();
        std::thread::Builder::new()
            .name("tagnet-sync-directories".to_owned())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build sync-directory runtime");
                let local = tokio::task::LocalSet::new();
                local.block_on(
                    &runtime,
                    handle_sync_directories(
                        configuration,
                        paths,
                        last_known_hashes,
                        change_sender,
                        command_receiver,
                        shutdown_child,
                    ),
                );
                let _ = sync_directories_done_tx.send(());
            })
            .expect("failed to spawn sync-directory thread")
    };

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
        // Join the dedicated OS thread now that its runtime has finished.
        let _ = sync_directories_thread.join();
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

    // Per-link file transfers. `transfers` demuxes inbound `Sync::Transfer*`
    // frames (by `TransferId`) to the endpoint task serving them — a sender we
    // started (answering a peer's `TransferStart`) or a receiver we started
    // (pulling a file we reconciled as wanted). When a receiver finishes it
    // reports on `receiver_done`, which the select loop drains to materialize
    // the bytes and drop the demux entry.
    let mut transfers: HashMap<TransferId, UnboundedSender<TransferMessage>> = HashMap::new();
    let (receiver_done_tx, mut receiver_done_rx) =
        tokio::sync::mpsc::unbounded_channel::<(TransferId, ReceiverPurpose, ReceiveOutcome)>();
    // Temp directory for in-flight received files. Kept per-session under the
    // system temp dir; a completed transfer's temp file is then materialized
    // (moved) into the sync directories.
    let transfer_temp_dir = std::env::temp_dir().join(format!(
        "tagnet-transfer-{}-{}",
        std::process::id(),
        peer_public_key
    ));
    // Start a receiver pull for `file_id`/`content_hash` on this link, tagged
    // with `purpose` (what to do with the bytes once received). Spawns the
    // receiver endpoint and a bridge that forwards its outcome onto
    // `receiver_done_rx`. Returns the `(transfer_id, inbound_sender)` for the
    // caller to register in the demux table.
    let start_pull = {
        let our_sender = our_sender.clone();
        let receiver_done_tx = receiver_done_tx.clone();
        let transfer_temp_dir = transfer_temp_dir.clone();
        move |file_id: FileId, content_hash: String, purpose: ReceiverPurpose| {
            let transfer_id = TransferId::new();
            let temp_path = transfer_temp_dir.join(transfer_id.to_string());
            let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
            let inbound = spawn_receiver(
                transfer_id,
                file_id,
                content_hash,
                temp_path,
                our_sender.clone(),
                Frame::Sync,
                outcome_tx,
            );
            let done_tx = receiver_done_tx.clone();
            tokio::spawn(async move {
                if let Ok(outcome) = outcome_rx.await {
                    let _ = done_tx.send((transfer_id, purpose, outcome));
                }
            });
            (transfer_id, inbound)
        }
    };

    if let Err(error) = tokio::fs::create_dir_all(&transfer_temp_dir).await {
        log::warn!(
            "Failed to create transfer temp dir for {peer_name}: {error}; \
             transfers to this peer will fail"
        );
    }

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
    // Command channel for `handle_changes` to trigger byte pulls on this link.
    let (command_tx, mut command_rx) = tokio::sync::mpsc::unbounded_channel::<bus::PeerCommand>();

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
                    runtime_peer.commands = Some(command_tx);
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

    // Announce our *tag* manifest first thing post-handshake, before the file
    // manifest. Ordering is deliberate and matters for placement efficiency:
    //
    // - Frames travel over one ordered link, so the peer handles our
    //   `TagManifest` before our `Manifest`.
    // - Handling `TagManifest` enqueues the `FileTagged`/`FileUntagged`
    //   relationships onto the change bus; handling `Manifest` starts file pull
    //   *transfers* whose `Materialize` is only enqueued once the bytes finish
    //   arriving (many round-trips later).
    // - `handle_changes` is a single FIFO consumer, so relationships enqueued
    //   first are applied before any later `Materialize`.
    //
    // Net effect: when a peer brings both new tags and new files, the tags are
    // in place by the time files materialize, so each file lands in its
    // matching TagBased directories on the *first* placement — avoiding the
    // re-placement copy that `ReconcileTagPlacement` would otherwise perform
    // (STREAMING_FOLLOWUPS §1.3). That fix still guarantees *correctness*
    // regardless of order; this ordering is purely the efficiency win.
    //
    // Relationship rows carry no FK on the tag definition (`entries` table), so
    // applying `FileTagged` before the corresponding `TagAdded` definition
    // (which may still be in flight via `TagRequest`) is safe.
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

    // Send our file manifest right after the tag manifest (see the ordering
    // rationale above). The peer compares it against their own history and
    // requests anything they need.
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
            command = command_rx.recv() => {
                let Some(command) = command else {
                    // All command senders dropped (peer removed from runtime).
                    continue;
                };
                match command {
                    bus::PeerCommand::StartReceive {
                        file_id,
                        content_hash,
                        placement,
                    } => {
                        // `handle_changes` recorded a live change this peer
                        // announced and wants its bytes. Pull them over this
                        // link; materialize (and record the version) on
                        // completion.
                        let purpose = ReceiverPurpose::Materialize {
                            file_id,
                            content_hash: content_hash.clone(),
                            origin: ChangeOrigin::Peer {
                                public_key: peer_public_key.to_owned(),
                            },
                            placement,
                        };
                        let (transfer_id, inbound) = start_pull(file_id, content_hash, purpose);
                        transfers.insert(transfer_id, inbound);
                    }
                }
            }
            completed = receiver_done_rx.recv() => {
                let Some((transfer_id, purpose, outcome)) = completed else {
                    // The done channel is never fully dropped while the session
                    // lives (we hold a sender clone), so `None` only at teardown.
                    continue;
                };
                transfers.remove(&transfer_id);
                let content = match outcome {
                    ReceiveOutcome::Complete(content) => content,
                    ReceiveOutcome::Failed(error) => {
                        log::warn!(
                            "Transfer {transfer_id} from {peer_name} failed: {error}"
                        );
                        // A fetch that fails to receive its bytes must still
                        // report failure to its waiter/parent so the fetch does
                        // not hang.
                        if let ReceiverPurpose::Fetch { then } = purpose {
                            match then {
                                fetch::FoundThen::DeliverLocal(sender) => {
                                    let _ = sender.send(Err(bus::FetchError::NotAvailable));
                                }
                                fetch::FoundThen::RelayUp {
                                    parent_peer,
                                    request_id,
                                    ..
                                } => {
                                    if let Some(sender) =
                                        peer_outbound(&runtime_configuration, &parent_peer).await
                                    {
                                        let _ = sender.send(Frame::Sync(
                                            SyncMessage::FetchMissing { request_id },
                                        ));
                                    }
                                }
                            }
                        }
                        continue;
                    }
                };
                match purpose {
                    ReceiverPurpose::Materialize {
                        file_id,
                        content_hash,
                        origin,
                        placement,
                    } => {
                        log::debug!(
                            "Transfer {transfer_id} from {peer_name} completed for {}; materializing",
                            file_id.to_string()
                        );
                        if let Err(error) = change_sender.send(DaemonMessage::Materialize {
                            file_id,
                            content,
                            content_hash,
                            origin,
                            placement,
                        }) {
                            log::error!(
                                "change_sender closed; cannot materialize transfer for {}: {error}",
                                file_id.to_string()
                            );
                            break;
                        }
                    }
                    ReceiverPurpose::Fetch { then } => match then {
                        fetch::FoundThen::DeliverLocal(sender) => {
                            let _ = sender.send(Ok(content));
                        }
                        fetch::FoundThen::RelayUp {
                            parent_peer,
                            request_id,
                            file_id,
                            content_hash,
                        } => {
                            // Relay: hold the received bytes so our parent can
                            // pull them, then announce (content-less) upward.
                            pending_fetches
                                .cache_relay_file(file_id, content_hash.clone(), content)
                                .await;
                            if let Some(sender) =
                                peer_outbound(&runtime_configuration, &parent_peer).await
                            {
                                let _ = sender.send(Frame::Sync(SyncMessage::FetchFound {
                                    request_id,
                                    file_id,
                                    content_hash,
                                }));
                            }
                        }
                    },
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
                            Ingest::from_change(change),
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
                        // Confirm the peer is registered (so the pull transfers
                        // below have a live link to drive) before running the
                        // synchronous reconciliation. Doing the DB work outside
                        // of any held `RwLockReadGuard` keeps this future `Send`
                        // (FileDatabase isn't Sync).
                        let peer_registered = runtime_configuration
                            .read()
                            .await
                            .peers
                            .get(peer_public_key)
                            .and_then(|runtime_peer| runtime_peer.outbound.clone())
                            .is_some();

                        if !peer_registered {
                            log::warn!(
                                "No outbound channel registered for {peer_name}; \
                                 cannot reconcile manifest"
                            );
                            continue;
                        }

                        let wanted = reconcile_peer_manifest(peer_name, entries, &database);
                        // Start a pull transfer for each wanted file: we are the
                        // receiver/driver, the peer serves via its own sender
                        // when it sees our `TransferStart`. `placement` is
                        // `Create` for files we've never seen (using the
                        // manifest's `logical_path`) and `Change` for files we
                        // already know — see `reconcile_peer_manifest`.
                        for WantedFile {
                            file_id,
                            content_hash,
                            placement,
                        } in wanted
                        {
                            // For a `Create` placement the `files` row does
                            // not yet exist locally; insert it *before* the
                            // pull so that when `Materialize` records the
                            // version, the FK from `file_versions` -> `files`
                            // resolves. Mirrors the live `FileMetadataAdded`
                            // handler which does the same synchronous
                            // `add_file` before requesting the pull.
                            if let bus::MaterializePlacement::Create {
                                logical_path, ..
                            } = &placement
                                && let Err(error) = database.add_file(file_id, logical_path)
                            {
                                log::error!(
                                    "Reconciliation: failed to add file {} ({}) \
                                     announced by {peer_name}: {error:?}; \
                                     skipping pull",
                                    file_id.to_string(),
                                    logical_path
                                );
                                continue;
                            }
                            let purpose = ReceiverPurpose::Materialize {
                                file_id,
                                content_hash: content_hash.clone(),
                                origin: ChangeOrigin::Peer {
                                    public_key: peer_public_key.to_owned(),
                                },
                                placement,
                            };
                            let (transfer_id, inbound) =
                                start_pull(file_id, content_hash, purpose);
                            transfers.insert(transfer_id, inbound);
                        }
                    }
                    Frame::Sync(SyncMessage::FetchRequest {
                        request_id,
                        file_id,
                        expected_hash,
                    }) => {
                        // A peer is asking us (recursively) for a file's bytes.
                        // Answer locally if we hold matching content; otherwise
                        // relay to our other peers. We only need a boolean here:
                        // the bytes themselves are pulled over a transfer later.
                        let have_local =
                            local_hash_matches(&command_sender, file_id, &expected_hash).await;
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
                        content_hash,
                    }) => {
                        // Content-less: a child announced it has the file. Pull
                        // the bytes from that child over a transfer, then either
                        // deliver locally (origin) or relay upward.
                        let action = pending_fetches
                            .handle_incoming_found(
                                peer_public_key,
                                request_id,
                                file_id,
                                content_hash,
                            )
                            .await;
                        if let Some(fetch::FoundAction::Receive {
                            from_peer,
                            file_id,
                            expected_hash,
                            then,
                        }) = action
                        {
                            // The child that answered is `from_peer`, which is
                            // this very connection (`peer_public_key`); pull over
                            // our own link.
                            let _ = from_peer; // == peer_public_key; kept for clarity
                            let (transfer_id, inbound) = start_pull(
                                file_id,
                                expected_hash,
                                ReceiverPurpose::Fetch { then },
                            );
                            transfers.insert(transfer_id, inbound);
                        }
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
                        // `Change::TagAdded`. `TagNotFound` if we no longer hold
                        // the tag.
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
                    // A peer opened a transfer: we are the holder/sender. Read
                    // the file's bytes off disk (as a `FileToCopy`, so nothing
                    // is buffered) and spawn a sender endpoint to serve them.
                    Frame::Sync(SyncMessage::TransferStart {
                        transfer_id,
                        file_id,
                        content_hash,
                    }) => {
                        let (respond_to, response) = tokio::sync::oneshot::channel();
                        if command_sender
                            .send(SyncDirectoryCommand::ReadFile { file_id, respond_to })
                            .is_err()
                        {
                            log::error!(
                                "command_sender closed; cannot serve transfer {transfer_id} \
                                 to {peer_name}"
                            );
                            break;
                        }
                        let read_result = response.await.ok().flatten();
                        // Resolve who serves the bytes, in priority order:
                        //   1. a matching file in our sync directories
                        //   2. a temporary provider (a local client serving on
                        //      demand, e.g. the CLI uploading)
                        //   3. a file we are holding as a fetch relay (evicted).
                        // Each yields an endpoint we register in the demux table.
                        let sync_content = match read_result {
                            Some((_physical_path, content, local_hash))
                                if local_hash == content_hash =>
                            {
                                Some(content)
                            }
                            _ => None,
                        };
                        let inbound = if let Some(content) = sync_content {
                            Some(spawn_sender(transfer_id, content, our_sender.clone(), Frame::Sync))
                        } else if let Some(provider) =
                            pending_fetches.provider_for(file_id, &content_hash).await
                        {
                            Some(spawn_sender(transfer_id, provider, our_sender.clone(), Frame::Sync))
                        } else {
                            pending_fetches
                                .take_fetch_cached(file_id, &content_hash)
                                .await
                                .map(|content| {
                                    spawn_sender(transfer_id, content, our_sender.clone(), Frame::Sync)
                                })
                        };
                        match inbound {
                            Some(inbound) => {
                                // Deliver the opening `Start` so the sender knows
                                // the transfer began (it only acts on requests).
                                let _ = inbound.send(TransferMessage::Start {
                                    file_id,
                                    content_hash,
                                });
                                transfers.insert(transfer_id, inbound);
                            }
                            None => {
                                log::warn!(
                                    "Transfer {transfer_id} from {peer_name}: {} not available \
                                     (no sync dir, provider, or fetch cache); aborting",
                                    file_id.to_string()
                                );
                                let _ = our_sender.send(Frame::Sync(SyncMessage::TransferAbort {
                                    transfer_id,
                                    reason: "file not available".to_owned(),
                                }));
                            }
                        }
                    }
                    // Chunk requests / chunks / aborts belong to an existing
                    // transfer: demux to its endpoint by `transfer_id`.
                    Frame::Sync(sync @ SyncMessage::TransferChunkRequest { .. })
                    | Frame::Sync(sync @ SyncMessage::TransferChunk { .. })
                    | Frame::Sync(sync @ SyncMessage::TransferAbort { .. }) => {
                        if let Some((transfer_id, message)) = TransferMessage::from_sync(sync) {
                            match transfers.get(&transfer_id) {
                                Some(endpoint) => {
                                    let is_abort =
                                        matches!(message, TransferMessage::Abort { .. });
                                    if endpoint.send(message).is_err() || is_abort {
                                        // Endpoint gone, or the transfer aborted:
                                        // drop the demux entry.
                                        transfers.remove(&transfer_id);
                                    }
                                }
                                None => {
                                    // Benign and expected: a completed receiver
                                    // removes its demux entry while the sender's
                                    // windowed in-flight chunks (offsets past
                                    // EOF) are still arriving. Trace, not debug.
                                    log::trace!(
                                        "Peer {peer_name} sent frame for unknown transfer \
                                         {transfer_id}; ignoring (late/orphaned)"
                                    );
                                }
                            }
                        }
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
        // The command channel is installed and cleared in lockstep with
        // `outbound` (same owner), so clear it here too.
        runtime_peer.commands = None;
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
        .map(
            |(file_id, history, latest_observed_at, logical_path)| ManifestEntry {
                file_id,
                history,
                latest_observed_at,
                logical_path,
            },
        )
        .collect())
}

/// One reconciliation outcome: pull `content_hash` for `file_id` from the peer
/// and materialize it with `placement`.
///
/// Placement is `Create` when we've never seen this file locally (the
/// manifest's `logical_path` gives us where to put it) and `Change` when we
/// already know the file and are only fetching newer bytes.
#[derive(Debug, Clone)]
pub struct WantedFile {
    pub file_id: FileId,
    pub content_hash: String,
    pub placement: bus::MaterializePlacement,
}

/// Compare the peer's manifest against our local `file_versions` table and
/// decide which files we should pull from them.
///
/// Returns each wanted file paired with the peer's latest content hash and a
/// placement describing how the eventual `Materialize` should route the bytes.
/// The caller starts a pull transfer per returned entry.
///
/// Pure synchronous function: no `.await`, no `RwLock`, no channels/transfers.
/// Lets callers hold `&FileDatabase` (which is `!Sync`) without making their
/// future non-`Send`, and keeps the decision testable. **Note:** for unknown
/// files this function does *not* insert the `files` row itself; the caller
/// must `add_file` before starting the pull (mirroring the live
/// `FileMetadataAdded` handler).
///
/// Categories per entry:
/// - **Unknown file_id**: we've never seen this file — request it and place
///   with `Create { logical_path, tags: <empty> }`. Tags arrive independently
///   via `Sync::TagManifest`; `reconcile_tag_placement` will re-place the file
///   into any TagBased sync directory that later matches.
/// - **Equal latest**: identical state — nothing to do.
/// - **Sender's latest hash appears in our history**: they are behind. Their
///   side will request from us when they process our manifest; we do nothing.
/// - **Our latest hash appears in their history**: we are behind — request the
///   newer bytes (`Change` placement, we already hold the file row).
/// - **Divergent**: neither side's latest appears in the other's history.
///   Newer `latest_observed_at` wins. If theirs wins, request. If ours wins,
///   do nothing (their side will accept ours via the symmetric path).
///   Divergence is logged at `error!` level with a TODO for a future
///   deadletter store.
fn reconcile_peer_manifest(
    peer_name: &str,
    entries: Vec<ManifestEntry>,
    database: &FileDatabase,
) -> Vec<WantedFile> {
    log::info!(
        "Reconciling {} manifest entries from {peer_name}",
        entries.len()
    );

    let mut wanted = Vec::new();
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
        // The hash we want is the peer's latest for this file.
        let their_latest = entry.history.last().map(|(_, hash)| hash.clone());
        // Placement depends on whether we already know the file locally: an
        // unknown file must be materialized as `Create` (using the manifest's
        // `logical_path`) so the sync-directory dispatch can place it.
        let known = database.file_exists(entry.file_id).unwrap_or(false);
        let placement_for_request = |entry: &ManifestEntry| -> bus::MaterializePlacement {
            if known {
                bus::MaterializePlacement::Change
            } else {
                // Tags are deliberately empty here; they are reconciled via
                // `Sync::TagManifest` and, when they land, the incoming
                // `FileTagged` handler runs `reconcile_tag_placement` to
                // re-place the file into any newly-matching TagBased sync
                // directories using the already-materialized bytes as a
                // source. This gives order-independence between the file and
                // tag manifests without enforcing a global ordering.
                bus::MaterializePlacement::Create {
                    logical_path: entry.logical_path.clone(),
                    tags: Vec::new(),
                }
            }
        };
        match decision {
            ReconcileDecision::Nothing => {}
            ReconcileDecision::Request(reason) => {
                log::debug!(
                    "Requesting {} from {peer_name}: {reason}",
                    entry.file_id.to_string()
                );
                if let Some(hash) = their_latest {
                    let placement = placement_for_request(&entry);
                    wanted.push(WantedFile {
                        file_id: entry.file_id,
                        content_hash: hash,
                        placement,
                    });
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
                if request && let Some(hash) = their_latest {
                    let placement = placement_for_request(&entry);
                    wanted.push(WantedFile {
                        file_id: entry.file_id,
                        content_hash: hash,
                        placement,
                    });
                }
            }
        }
    }

    wanted
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
) -> Option<FileBytes> {
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
        Ok(Some((_physical_path, file_bytes, content_hash))) if content_hash == expected_hash => {
            Some(file_bytes)
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

/// Resolve a peer's outbound `Frame` sender by public key, if connected.
async fn peer_outbound(
    runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
    public_key: &str,
) -> Option<UnboundedSender<Frame>> {
    runtime_configuration
        .read()
        .await
        .peers
        .get(public_key)
        .and_then(|runtime_peer| runtime_peer.outbound.clone())
}

/// Ask the peer that announced a change (`change_origin`) to serve us its
/// bytes: send a `StartReceive` command to that peer's live session, which owns
/// the transfer machinery. No-op if the change is local-origin or the peer has
/// no live session (reconciliation will pick it up on the next connect).
async fn request_pull_from_origin(
    runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
    change_origin: &ChangeOrigin,
    file_id: FileId,
    content_hash: String,
    placement: bus::MaterializePlacement,
) {
    let ChangeOrigin::Peer { public_key } = change_origin else {
        // Local-origin content already has its bytes; nothing to pull.
        return;
    };
    let commands = runtime_configuration
        .read()
        .await
        .peers
        .get(public_key)
        .and_then(|runtime_peer| runtime_peer.commands.clone());
    match commands {
        Some(commands) => {
            if commands
                .send(bus::PeerCommand::StartReceive {
                    file_id,
                    content_hash,
                    placement,
                })
                .is_err()
            {
                log::warn!(
                    "Peer {public_key} command channel closed; cannot pull {}; \
                     reconciliation will retry on reconnect",
                    file_id.to_string()
                );
            }
        }
        None => {
            log::debug!(
                "Announcing peer {public_key} has no live session; deferring pull of {} \
                 to reconciliation",
                file_id.to_string()
            );
        }
    }
}

/// Whether a local sync directory holds `file_id` with content hashing to
/// `expected_hash`, without buffering the bytes. Used to decide whether we can
/// answer a relayed `Sync::FetchRequest` (as a content-less `FetchFound`) or
/// must forward it onward.
async fn local_hash_matches(
    command_sender: &UnboundedSender<SyncDirectoryCommand>,
    file_id: FileId,
    expected_hash: &str,
) -> bool {
    let (respond_to, response) = tokio::sync::oneshot::channel();
    if command_sender
        .send(SyncDirectoryCommand::ReadFile {
            file_id,
            respond_to,
        })
        .is_err()
    {
        return false;
    }
    matches!(
        response.await,
        Ok(Some((_physical_path, _content, content_hash))) if content_hash == expected_hash
    )
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
            Ingest::from_change(change),
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

/// Ask the sync-directory manager to re-evaluate `file_id`'s TagBased placement
/// against its current tag set. Called whenever the file's tags change
/// (`FileTagged` / `FileUntagged`), so a file that gained a directory's tags is
/// placed there and one that lost them is dropped. Also the recovery path for
/// the tag-vs-content reconciliation race (see
/// `SyncDirectoryCommand::ReconcileTagPlacement`).
///
/// Reads the file's current tags and logical path from the main database and
/// hands both to the manager. Best-effort: a file with no logical path (never
/// added locally) or a closed command channel is logged and skipped.
fn reconcile_tag_placement(
    command_sender: &UnboundedSender<SyncDirectoryCommand>,
    database: &FileDatabase,
    file_id: FileId,
) {
    let logical_path = match database.logical_path_for_file_id(file_id) {
        Ok(logical_path) => logical_path,
        Err(error) => {
            log::debug!(
                "reconcile_tag_placement: no logical path for {} ({:?}); skipping",
                file_id.to_string(),
                error
            );
            return;
        }
    };

    let file_tags = match database.tag_ids_for_file(file_id, database::SubtagRule::Exclude) {
        Ok(tags) => tags.into_iter().collect::<Vec<TagId>>(),
        Err(error) => {
            log::error!(
                "reconcile_tag_placement: failed to read tags for {}: {:?}",
                file_id.to_string(),
                error
            );
            return;
        }
    };

    if let Err(error) = command_sender.send(SyncDirectoryCommand::ReconcileTagPlacement {
        file_id,
        logical_path,
        file_tags,
    }) {
        log::error!(
            "reconcile_tag_placement: command channel closed for {}: {error}",
            file_id.to_string()
        );
    }
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

    /// Dispatch a content-bearing change to matching sync directories, applying
    /// the move-vs-copy policy.
    ///
    /// `content` is a [`FileBytes`] that may still live on disk. This function
    /// decides, per the number of matching sync directories (`N`), how each one
    /// obtains the bytes:
    ///
    /// - `N == 0`: nothing is dispatched. A `FileToMove` source is left in
    ///   place (no auto-cleanup this pass; see `file_bytes` docs).
    /// - `N == 1`: the single directory receives the producer's original
    ///   variant. A `FileToMove` stays a move — zero extra copies, the common
    ///   single-directory win.
    /// - `N > 1`: a `FileToMove` can be honored only once, so every directory
    ///   instead receives a `FileToCopy` of the source and, after dispatch, the
    ///   source is removed here (preserving the "move" intent: the source does
    ///   not survive ingestion). `InMemory` is cloned per directory as before.
    async fn dispatch_content_to_sync_directories(
        command_sender: &UnboundedSender<SyncDirectoryCommand>,
        targets: Vec<ContentTarget>,
        content: FileBytes,
    ) {
        let source_path = content.path().map(|path| path.to_path_buf());
        let move_intent = matches!(content, FileBytes::FileToMove(_));

        match targets.len() {
            0 => {
                // No matching sync directory. Drop the content; a move source
                // is intentionally left in place (documented no-cleanup).
            }
            1 => {
                let target = targets.into_iter().next().expect("len checked == 1");
                let _ = command_sender.send(target.into_command(content));
            }
            _ => {
                // Multiple destinations: a destructive move can be honored only
                // once, so hand every directory a copy instead. If the intent
                // was to move (source should not survive), remove the source
                // after dispatching all copies.
                for target in targets {
                    let per_dir = match &source_path {
                        Some(path) => FileBytes::FileToCopy(path.clone()),
                        // No backing path => in-memory: clone the buffer.
                        None => match &content {
                            FileBytes::InMemory(bytes) => FileBytes::InMemory(bytes.clone()),
                            // Unreachable: source_path is None only for InMemory.
                            _ => unreachable!("no source path implies InMemory content"),
                        },
                    };
                    let _ = command_sender.send(target.into_command(per_dir));
                }

                if move_intent
                    && let Some(path) = &source_path
                    && let Err(error) = tokio::fs::remove_file(path).await
                {
                    log::warn!(
                        "Failed to remove move source {} after fan-out: {error}",
                        path.display()
                    );
                }
            }
        }
    }

    /// Handle a [`ContentChange`] (`FileAdded`/`FileChanged` carrying
    /// [`FileBytes`]): persist the version, dispatch bytes to matching sync
    /// directories, and forward a wire `Change` to peers.
    async fn handle_content_change(
        configuration: &Configuration,
        runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
        database: &mut FileDatabase,
        command_sender: &UnboundedSender<SyncDirectoryCommand>,
        content_change: ContentChange,
        change_origin: ChangeOrigin,
    ) {
        match content_change {
            ContentChange::FileAdded {
                file_id,
                logical_path,
                content,
                content_hash,
                tags,
            } => {
                // Reconciliation and live edits can both deliver a `FileAdded`
                // for a `file_id` we already know. Branch on existence to stay
                // idempotent (see the historical notes preserved below).
                let already_exists = database.file_exists(file_id).unwrap_or_else(|error| {
                    log::error!(
                        "file_exists check failed for {}: {:?}; assuming new",
                        file_id.to_string(),
                        error
                    );
                    false
                });

                if !already_exists {
                    if let Err(error) = database.add_file(file_id, &logical_path) {
                        // Do not panic: a single bad inbound change must not
                        // take down the sole DB writer.
                        log::error!(
                            "Failed to add file {} ({}): {:?}; skipping change",
                            file_id.to_string(),
                            logical_path,
                            error
                        );
                        return;
                    }

                    // NOTE: We intentionally do NOT apply `tags` here anymore.
                    // File-tag relationships reconcile through their own tag
                    // manifest. `tags` is retained for local dispatch filtering
                    // below only.
                    if let Err(error) = database.record_version(
                        file_id,
                        &content_hash,
                        version_origin(&change_origin),
                    ) {
                        log::error!(
                            "Failed to record initial version for {}: {:?}",
                            file_id.to_string(),
                            error
                        );
                    }

                    let mut targets = Vec::new();
                    for sync_directory in &configuration.sync_directories {
                        if let ChangeOrigin::Local { directory_path } = &change_origin
                            && directory_path == &sync_directory.path
                            && let SyncType::TagBased { .. } = &sync_directory.sync_type
                        {
                            continue;
                        };

                        if let SyncType::TagBased {
                            tags: sync_directory_tags,
                        } = &sync_directory.sync_type
                            && !contains_all_tags(sync_directory_tags, &tags)
                        {
                            continue;
                        }

                        let physical_path = sync_directory
                            .sync_type
                            .physical_for(&logical_path, file_id);
                        targets.push(ContentTarget::Create {
                            file_id,
                            physical_path,
                            sync_directory_path: sync_directory.path.clone(),
                        });
                    }

                    dispatch_and_forward(
                        configuration,
                        runtime_configuration,
                        command_sender,
                        targets,
                        content,
                        &change_origin,
                        WireKind::Added {
                            file_id,
                            logical_path,
                            content_hash,
                            tags,
                        },
                    )
                    .await;
                } else {
                    // Known file: decide by whether this is already the version
                    // we currently hold (latest). Matching an *older* historical
                    // hash is a legitimate revert and must be promoted to a new
                    // version — not ignored. (Materialization echoes are already
                    // suppressed upstream by the directory manager's
                    // already-tracked / skip-queue guards, so this need only
                    // guard against a true no-op re-announcement of the current
                    // content.)
                    let current_hash = database
                        .latest_version(file_id)
                        .unwrap_or_else(|error| {
                            log::error!(
                                "latest_version failed for known file {}: {:?}; \
                                 treating as no-op",
                                file_id.to_string(),
                                error
                            );
                            None
                        })
                        .map(|version| version.content_hash);

                    if current_hash.as_deref() == Some(content_hash.as_str()) {
                        log::debug!(
                            "Ignoring no-op FileAdded for {} (already the current version)",
                            file_id.to_string()
                        );
                        return;
                    }

                    log::debug!(
                        "Promoting FileAdded for known file {} to FileChanged (new content_hash)",
                        file_id.to_string()
                    );
                    if let Err(error) = database.record_version(
                        file_id,
                        &content_hash,
                        version_origin(&change_origin),
                    ) {
                        log::error!(
                            "Failed to record version for {}: {:?}",
                            file_id.to_string(),
                            error
                        );
                    }

                    let local_file_tags = database
                        .tag_ids_for_file(file_id, database::SubtagRule::Exclude)
                        .map(|iter| iter.into_iter().collect::<Vec<TagId>>())
                        .unwrap_or_else(|error| {
                            log::error!(
                                "Failed to read local tags for {}: {:?}",
                                file_id.to_string(),
                                error
                            );
                            Vec::new()
                        });

                    let targets =
                        change_targets(configuration, &change_origin, file_id, &local_file_tags);
                    dispatch_and_forward(
                        configuration,
                        runtime_configuration,
                        command_sender,
                        targets,
                        content,
                        &change_origin,
                        WireKind::Changed {
                            file_id,
                            content_hash,
                        },
                    )
                    .await;
                }
            }
            ContentChange::FileChanged {
                file_id,
                content,
                content_hash,
            } => {
                let file_tags =
                    match database.tag_ids_for_file(file_id, database::SubtagRule::Exclude) {
                        Ok(tags) => tags.into_iter().collect::<Vec<TagId>>(),
                        Err(error) => {
                            log::error!(
                                "FileChanged: failed to get tags for {}: {:?}; skipping",
                                file_id.to_string(),
                                error
                            );
                            return;
                        }
                    };

                if let Err(error) =
                    database.record_version(file_id, &content_hash, version_origin(&change_origin))
                {
                    log::error!(
                        "Failed to record version for {}: {:?}",
                        file_id.to_string(),
                        error
                    );
                }

                let targets = change_targets(configuration, &change_origin, file_id, &file_tags);
                dispatch_and_forward(
                    configuration,
                    runtime_configuration,
                    command_sender,
                    targets,
                    content,
                    &change_origin,
                    WireKind::Changed {
                        file_id,
                        content_hash,
                    },
                )
                .await;
            }
        }
    }

    /// Build the list of sync directories that should receive a `ChangeFile`
    /// for `file_id`, applying the origin-skip and tag-match filters.
    fn change_targets(
        configuration: &Configuration,
        change_origin: &ChangeOrigin,
        file_id: FileId,
        file_tags: &[TagId],
    ) -> Vec<ContentTarget> {
        let mut targets = Vec::new();
        for sync_directory in &configuration.sync_directories {
            if let ChangeOrigin::Local { directory_path } = change_origin
                && directory_path == &sync_directory.path
            {
                continue;
            };

            if let SyncType::TagBased {
                tags: sync_directory_tags,
            } = &sync_directory.sync_type
                && !contains_all_tags(sync_directory_tags, file_tags)
            {
                continue;
            }

            targets.push(ContentTarget::Change {
                file_id,
                sync_directory_path: sync_directory.path.clone(),
            });
        }
        targets
    }

    /// The metadata-only wire `Change` to announce to peers for a local content
    /// ingestion. `Change` no longer carries bytes; peers pull them separately.
    enum WireKind {
        Added {
            file_id: FileId,
            logical_path: LogicalPath,
            content_hash: String,
            tags: Vec<TagId>,
        },
        Changed {
            file_id: FileId,
            content_hash: String,
        },
    }

    impl WireKind {
        fn into_change(self) -> Change {
            match self {
                WireKind::Added {
                    file_id,
                    logical_path,
                    content_hash,
                    tags,
                } => Change::FileMetadataAdded {
                    file_id,
                    logical_path,
                    content_hash,
                    tags,
                },
                WireKind::Changed {
                    file_id,
                    content_hash,
                } => Change::FileMetadataChanged {
                    file_id,
                    content_hash,
                },
            }
        }
    }

    /// Dispatch a local content ingestion to matching sync directories
    /// (streaming the bytes to disk) and announce a metadata-only wire `Change`
    /// to peers.
    ///
    /// The bytes are never buffered here for peers: `Change` is metadata-only,
    /// so a peer that wants the content pulls it over a separate transfer. This
    /// keeps large local ingests entirely off the heap regardless of how many
    /// peers are connected.
    async fn dispatch_and_forward(
        configuration: &Configuration,
        runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
        command_sender: &UnboundedSender<SyncDirectoryCommand>,
        targets: Vec<ContentTarget>,
        content: FileBytes,
        change_origin: &ChangeOrigin,
        wire: WireKind,
    ) {
        dispatch_content_to_sync_directories(command_sender, targets, content).await;
        let change = wire.into_change();
        forward_to_peers(configuration, runtime_configuration, &change, change_origin).await;
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
        let ingest = match message {
            DaemonMessage::Change(ingest, change_origin) => (ingest, change_origin),
            DaemonMessage::Fetch {
                file_id,
                expected_hash,
                respond_to,
            } => {
                if let Some(file_bytes) =
                    read_local_if_hash_matches(&command_sender, file_id, &expected_hash).await
                {
                    let _ = respond_to.send(Ok(file_bytes));
                    return;
                }

                pending_fetches
                    .start_local_fetch(file_id, expected_hash, respond_to)
                    .await;

                continue;
            }
            DaemonMessage::Materialize {
                file_id,
                content,
                content_hash,
                origin,
                placement,
            } => {
                // Bytes arrived over a peer transfer. Record the version *now*
                // (so `file_versions` reflects only what we actually hold), then
                // dispatch to the matching sync directories per `placement`.
                log::debug!(
                    "Materializing received content for {} ({})",
                    file_id.to_string(),
                    content_hash
                );
                if let Err(error) =
                    database.record_version(file_id, &content_hash, version_origin(&origin))
                {
                    log::error!(
                        "Materialize: failed to record version for {}: {:?}",
                        file_id.to_string(),
                        error
                    );
                }

                // Build the local placement targets *and* the metadata-only wire
                // `Change` to relay onward. We announce to other peers only here,
                // once the bytes are actually in hand — not when the originating
                // announcement first arrived. This is what makes a relay node
                // (e.g. a central hub) propagate a file it acquired by pull to
                // its *other* peers: `forward_to_peers` skips `origin`, so the
                // announcement flows down the tree away from where we pulled it,
                // and every downstream peer that pulls from us finds we can
                // actually serve the bytes (announce-after-you-have-it).
                let (targets, wire_change) = match placement {
                    bus::MaterializePlacement::Create { logical_path, tags } => {
                        // New file: create it in every matching sync directory,
                        // deriving each directory's physical path from the
                        // logical path.
                        let mut targets = Vec::new();
                        for sync_directory in &configuration.sync_directories {
                            if let SyncType::TagBased {
                                tags: sync_directory_tags,
                            } = &sync_directory.sync_type
                                && !contains_all_tags(sync_directory_tags, &tags)
                            {
                                continue;
                            }
                            let physical_path = sync_directory
                                .sync_type
                                .physical_for(&logical_path, file_id);
                            targets.push(ContentTarget::Create {
                                file_id,
                                physical_path,
                                sync_directory_path: sync_directory.path.clone(),
                            });
                        }
                        let wire_change = Change::FileMetadataAdded {
                            file_id,
                            logical_path,
                            content_hash: content_hash.clone(),
                            tags,
                        };
                        (targets, wire_change)
                    }
                    bus::MaterializePlacement::Change => {
                        // Existing file: overwrite it in the sync directories
                        // that already hold it (tag-filtered by current tags).
                        let file_tags = database
                            .tag_ids_for_file(file_id, database::SubtagRule::Exclude)
                            .map(|iter| iter.into_iter().collect::<Vec<TagId>>())
                            .unwrap_or_else(|error| {
                                log::error!(
                                    "Materialize: failed to read tags for {}: {:?}",
                                    file_id.to_string(),
                                    error
                                );
                                Vec::new()
                            });
                        // Peer-origin: no origin directory to skip. Sentinel
                        // empty path never matches a real sync directory.
                        let sentinel = ChangeOrigin::Local {
                            directory_path: std::path::PathBuf::new(),
                        };
                        let targets =
                            change_targets(&configuration, &sentinel, file_id, &file_tags);
                        let wire_change = Change::FileMetadataChanged {
                            file_id,
                            content_hash: content_hash.clone(),
                        };
                        (targets, wire_change)
                    }
                };
                dispatch_content_to_sync_directories(&command_sender, targets, content).await;
                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &wire_change,
                    &origin,
                )
                .await;
                continue;
            }
            DaemonMessage::AnnounceProvided {
                file_id,
                logical_path,
                content_hash,
                tags,
            } => {
                // A local client (CLI) uploaded/edited a file it serves on
                // demand. Record it locally and announce metadata-only to peers;
                // peers pull the bytes from the registered provider. No local
                // sync-directory placement: a CLI upload targets peers (files
                // already in a sync directory are synced without the CLI).
                let change = match logical_path {
                    Some(logical_path) => {
                        if let Err(error) = database.add_file(file_id, &logical_path) {
                            log::error!(
                                "AnnounceProvided: failed to add file {} ({}): {:?}",
                                file_id.to_string(),
                                logical_path,
                                error
                            );
                            continue;
                        }
                        Change::FileMetadataAdded {
                            file_id,
                            logical_path,
                            content_hash: content_hash.clone(),
                            tags,
                        }
                    }
                    None => Change::FileMetadataChanged {
                        file_id,
                        content_hash: content_hash.clone(),
                    },
                };
                let origin = ChangeOrigin::Local {
                    directory_path: std::path::PathBuf::new(),
                };
                if let Err(error) =
                    database.record_version(file_id, &content_hash, version_origin(&origin))
                {
                    log::error!(
                        "AnnounceProvided: failed to record version for {}: {:?}",
                        file_id.to_string(),
                        error
                    );
                }
                forward_to_peers(&configuration, &runtime_configuration, &change, &origin).await;
                continue;
            }
        };

        // Content-bearing ingestions (`ContentChange::FileAdded`/`FileChanged`)
        // carry a `FileBytes` that may still live on disk; they are handled
        // separately so the bytes are streamed into sync directories and only
        // buffered into a wire `Change` at the peer-forward boundary. Every
        // other change is pure metadata and flows through the wire-`Change`
        // match below.
        let (change, change_origin) = match ingest {
            (Ingest::Content(content_change), change_origin) => {
                handle_content_change(
                    &configuration,
                    &runtime_configuration,
                    &mut database,
                    &command_sender,
                    content_change,
                    change_origin,
                )
                .await;
                continue;
            }
            (Ingest::Meta(change), change_origin) => (change, change_origin),
        };

        match &change {
            // A metadata-only `FileMetadataAdded` announcement — always from a
            // peer (local ingestion carries bytes and arrives as
            // `Ingest::Content`). Record the file + version; the bytes are
            // pulled separately.
            Change::FileMetadataAdded {
                file_id,
                logical_path,
                content_hash,
                tags,
            } => {
                // Metadata-only announcement from a peer. Insert the `files` row
                // (so we know the file's logical identity) but DO NOT record the
                // version yet — the version is recorded only once we have the
                // bytes (in `Materialize`), so `file_versions` reflects what we
                // actually hold and reconciliation can re-request otherwise.
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
                        log::error!(
                            "Failed to add file {} ({}): {:?}; skipping change",
                            file_id.to_string(),
                            logical_path,
                            error
                        );
                        continue;
                    }
                } else {
                    // As in `FileMetadataChanged`: skip only if this is the
                    // version we *currently* hold (latest), not merely present
                    // somewhere in history. A revert to an older hash must still
                    // be pulled and materialized as a new version.
                    let current_hash = database
                        .latest_version(*file_id)
                        .ok()
                        .flatten()
                        .map(|version| version.content_hash);
                    if current_hash.as_deref() == Some(content_hash.as_str()) {
                        log::debug!(
                            "Ignoring no-op FileMetadataAdded for {} (already the current version)",
                            file_id.to_string()
                        );
                        // Still forward so the announcement propagates the tree.
                        forward_to_peers(
                            &configuration,
                            &runtime_configuration,
                            &change,
                            &change_origin,
                        )
                        .await;
                        continue;
                    }
                }

                // Trigger a byte pull from the announcing peer. The version is
                // recorded when the transfer completes (`Materialize`), which is
                // also where we relay this announcement onward to our *other*
                // peers — announce-after-you-have-the-bytes, so a downstream peer
                // that pulls from us finds we can actually serve it. We do NOT
                // forward here: forwarding before the pull completes would let a
                // downstream peer `TransferStart` against us before we hold the
                // bytes, forcing an abort with no retry until reconnect.
                request_pull_from_origin(
                    &runtime_configuration,
                    &change_origin,
                    *file_id,
                    content_hash.clone(),
                    bus::MaterializePlacement::Create {
                        logical_path: logical_path.clone(),
                        tags: tags.clone(),
                    },
                )
                .await;
            }
            // A metadata-only `FileMetadataChanged` announcement — always from a
            // peer. Pull the new bytes; version recorded on materialization.
            Change::FileMetadataChanged {
                file_id,
                content_hash,
            } => {
                // Skip the pull only if this hash is the version we *currently*
                // hold — i.e. the latest recorded version. It is NOT enough for
                // the hash to appear somewhere in history: we keep only the
                // newest version's bytes on disk, so a revert back to an older
                // hash (which is in history but is not current) is a genuine new
                // change we must pull and materialize as a new version. Checking
                // the whole history here silently kept the wrong bytes on disk
                // AND, because no pull fired, never released the CLI provider
                // (edit hung forever).
                let current_hash = database
                    .latest_version(*file_id)
                    .ok()
                    .flatten()
                    .map(|version| version.content_hash);
                if current_hash.as_deref() == Some(content_hash.as_str()) {
                    log::debug!(
                        "Ignoring no-op FileMetadataChanged for {} (already the current version)",
                        file_id.to_string()
                    );
                    // We already hold these exact bytes as the current version,
                    // so no pull will fire (and thus no `Materialize` to relay
                    // from). Announce onward here so the change still propagates
                    // the tree.
                    forward_to_peers(
                        &configuration,
                        &runtime_configuration,
                        &change,
                        &change_origin,
                    )
                    .await;
                } else {
                    // Pull the new bytes; the relay to our other peers happens in
                    // `Materialize` once we actually hold them (announce-after-
                    // you-have-it), not here.
                    request_pull_from_origin(
                        &runtime_configuration,
                        &change_origin,
                        *file_id,
                        content_hash.clone(),
                        bus::MaterializePlacement::Change,
                    )
                    .await;
                }
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

                // The file's tag set changed, so its tag-based placement may be
                // stale: a file that just gained a directory's tags should be
                // materialized there. This is also the recovery path for the
                // tag-vs-content reconciliation race (a peer transfer that
                // materialized before this `FileTagged` arrived placed the file
                // only where tags already matched). Re-run placement now.
                reconcile_tag_placement(&command_sender, &database, *file_id);

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

                // The file's tag set changed: a file that just lost a
                // directory's tags should be dropped from it. Re-run placement
                // (symmetric with `FileTagged`).
                reconcile_tag_placement(&command_sender, &database, *file_id);

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

#[cfg(test)]
mod reconcile_tests {
    use super::*;
    use tagnet_core::state::ManifestEntry;

    fn memory_db() -> FileDatabase {
        FileDatabase::initialize(":memory:").expect("open in-memory db")
    }

    /// A file the peer has but we've never seen IS reconciled with a `Create`
    /// placement carrying the manifest's `logical_path`: this is the
    /// offline-creation catch-up case (a file created on the peer while we
    /// were disconnected must sync on reconnect). Tags are left empty; they
    /// are reconciled independently via `Sync::TagManifest` and applied by
    /// `reconcile_tag_placement` when the corresponding `FileTagged` arrives.
    #[test]
    fn unknown_file_is_requested_as_create() {
        let database = memory_db();
        let file_id = FileId::new();
        let logical_path = LogicalPath::new("subdir/new.txt");
        let entry = ManifestEntry {
            file_id,
            history: vec![(1, "aaaa".to_owned()), (2, "bbbb".to_owned())],
            latest_observed_at: 100,
            logical_path: logical_path.clone(),
        };

        let wanted = reconcile_peer_manifest("peer", vec![entry], &database);
        assert_eq!(wanted.len(), 1);
        assert_eq!(wanted[0].file_id, file_id);
        assert_eq!(wanted[0].content_hash, "bbbb");
        match &wanted[0].placement {
            bus::MaterializePlacement::Create {
                logical_path: got_logical_path,
                tags,
            } => {
                assert_eq!(got_logical_path, &logical_path);
                assert!(tags.is_empty(), "tags reconcile via Sync::TagManifest");
            }
            other => panic!("expected Create placement, got {other:?}"),
        }
    }

    /// A file whose latest hash we already hold is not wanted.
    #[test]
    fn equal_latest_is_not_wanted() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("f.txt"))
            .unwrap();
        database.record_version(file_id, "bbbb", "local").unwrap();

        let entry = ManifestEntry {
            file_id,
            history: vec![(1, "bbbb".to_owned())],
            latest_observed_at: 100,
            logical_path: LogicalPath::new("f.txt"),
        };

        let wanted = reconcile_peer_manifest("peer", vec![entry], &database);
        assert!(wanted.is_empty());
    }

    /// When we are strictly behind (our latest is in the peer's history but not
    /// vice versa), the file is wanted at the peer's newer hash. Since the
    /// file is already known locally, placement is `Change`.
    #[test]
    fn behind_is_wanted_as_change() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("f.txt"))
            .unwrap();
        database.record_version(file_id, "v1", "local").unwrap();

        let entry = ManifestEntry {
            file_id,
            history: vec![(1, "v1".to_owned()), (2, "v2".to_owned())],
            latest_observed_at: 100,
            logical_path: LogicalPath::new("f.txt"),
        };

        let wanted = reconcile_peer_manifest("peer", vec![entry], &database);
        assert_eq!(wanted.len(), 1);
        assert_eq!(wanted[0].file_id, file_id);
        assert_eq!(wanted[0].content_hash, "v2");
        assert!(
            matches!(wanted[0].placement, bus::MaterializePlacement::Change),
            "known file placement must be Change, got {:?}",
            wanted[0].placement
        );
    }
}

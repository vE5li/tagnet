// Android backend: start an in-process sync engine and accept files shared to
// the app (portability plan section 8).
//
// Unlike Linux (which attaches to a daemon), this process OWNS the engine, DB,
// and identity. It also wires the Android share sheet so "Share to tagnet"
// uploads files, and surfaces this device's public key for pairing.
//
// Selected at build time via --dart-define=TAGNET_BACKEND=android (see main).

import 'dart:async';

import 'package:flutter/widgets.dart';
import 'package:path_provider/path_provider.dart';
import 'package:receive_sharing_intent/receive_sharing_intent.dart';

import '../rust/frb_generated.dart';
import '../rust/api.dart' as tagnet;
import 'bootstrap.dart';

class AndroidBootstrap extends TagnetBootstrap {
  StreamSubscription<List<SharedMediaFile>>? _shareSub;

  @override
  Future<TagnetSession> connect() async {
    // Loads libtagnet_bridge.so and wires up the generated bindings.
    await RustLib.init();

    // App-private storage: inotify works here with no storage permission
    // (plan section 8). getFilesDir() maps to getApplicationSupportDirectory.
    final dir = await getApplicationSupportDirectory();
    final dataDir = dir.path;
    final identityFile = '$dataDir/identity.key';

    // ---- HARDCODED POC CONFIG (edit this to connect to a peer) --------------
    //
    // This JSON is parsed by the Rust `Configuration` type
    // (tagnet/src/configuration.rs). Shapes:
    //
    //   sync_directories[].sync_type:
    //     "Universal"                       -> sync every file, or
    //     { "TagBased": { "tags": [...] } } -> only files with those tag ids
    //   listen_port: a port number to accept inbound peers, or null for
    //     outbound-only (realistic on mobile behind carrier NAT).
    //   peers[]:
    //     address:    [ "<ip>", <port> ] to dial the peer, or null to let
    //                 the peer dial us (only useful if listen_port is set).
    //     name:       label for logs only.
    //     public_key: the peer's base64 ed25519 public key. On the desktop
    //                 peer this is printed by `tagnet keygen` / found in its
    //                 config; identity is verified by this key, not the name.
    //
    // The Rust side generates THIS device's identity on first launch; its
    // public key is logged to logcat (`adb logcat -s tagnet`) — hand that to
    // the peer so it can add this phone to its own config.
    final configJson = '''
{
  "sync_directories": [],
  "listen_port": null,
  "peers": [
    {
      "address": ["192.168.188.10", 3468],
      "name": "central",
      "public_key": "AYWQNMCy5y20bjB1oxU79x5fUYQI4CGPbsqk7N+Qrgs="
    }
  ]
}
''';

    final app = tagnet.TagnetApp.start(
      configurationJson: configJson,
      dataDir: dataDir,
      identityFile: identityFile,
    );

    return TagnetSession(app: app, publicKey: app.publicKey());
  }

  /// Wire up the Android share sheet ("Share to tagnet"). Two cases:
  ///   1. app already running  -> getMediaStream() pushes new shares,
  ///   2. app launched by share -> getInitialMedia() returns the first batch.
  /// Both funnel into [_uploadSharedFiles].
  @override
  void attachInputs(
    TagnetSession session, {
    required void Function(String message) showMessage,
    required VoidCallback onChanged,
  }) {
    _shareSub = ReceiveSharingIntent.instance.getMediaStream().listen(
      (files) => _uploadSharedFiles(session.app, files, showMessage, onChanged),
      onError: (Object error) => showMessage('Share error: $error'),
    );

    ReceiveSharingIntent.instance.getInitialMedia().then((files) {
      if (files.isEmpty) return;
      _uploadSharedFiles(session.app, files, showMessage, onChanged);
      // Tell the plugin we consumed the initial intent so it is not redelivered.
      ReceiveSharingIntent.instance.reset();
    });
  }

  /// Hand each shared file's *path* to the sync engine via
  /// [tagnet.TagnetApp.uploadFile]. The engine streams the bytes from disk
  /// (hash then serve-on-demand); they are never buffered whole, matching the
  /// CLI's upload path.
  Future<void> _uploadSharedFiles(
    tagnet.TagnetApp app,
    List<SharedMediaFile> files,
    void Function(String) showMessage,
    VoidCallback onChanged,
  ) async {
    if (files.isEmpty) return;

    var uploaded = 0;
    for (final shared in files) {
      try {
        // Derive a display/logical name from the source path. The engine treats
        // this as the file's logical identity at the ingestion boundary.
        final name = shared.path.split('/').last;
        await app.uploadFile(path: shared.path, pathName: name, tags: const []);
        uploaded++;
      } catch (error) {
        showMessage('Failed to upload ${shared.path}: $error');
      }
    }

    if (uploaded > 0) {
      showMessage('Uploaded $uploaded file${uploaded == 1 ? '' : 's'} to tagnet');
      onChanged();
    }
  }

  @override
  void dispose() {
    _shareSub?.cancel();
    // Deliberately do NOT stop the runtime: it is owned by the foreground
    // service (crate::service) so sync keeps running after the UI is closed.
  }
}

/// Factory referenced by the backend selector in main.dart.
TagnetBootstrap createBootstrap() => AndroidBootstrap();

// Linux desktop backend: attach to the running tagnet daemon over IPC
// (portability plan sections 6-7, two-process topology).
//
// Unlike Android, this process does NOT start its own sync engine or open the
// database. The systemd daemon owns the DB and serves a Unix control socket
// (/run/tagnet/tagnet.sock); this app merely ATTACHES to it. So there is no
// config JSON, no data directory, no identity, and no public key to show here —
// they all belong to the daemon. There is likewise no share-intent input, so
// attachInputs/dispose fall back to the no-op defaults in TagnetBootstrap.
//
// Selected at build time via --dart-define=TAGNET_BACKEND=linux (see main).

import 'dart:io';

import 'package:flutter_rust_bridge/flutter_rust_bridge_for_generated.dart'
    show ExternalLibrary;

import '../rust/frb_generated.dart';
import '../rust/api.dart' as tagnet;
import 'bootstrap.dart';

class LinuxBootstrap extends TagnetBootstrap {
  @override
  Future<TagnetSession> connect() async {
    // On Linux the .so is built + bundled by the runner's CMake hook
    // (app/linux/CMakeLists.txt); load it explicitly (see _loadBridge).
    await RustLib.init(externalLibrary: _loadBridge());

    // Connect to the daemon's control socket (/run/tagnet/tagnet.sock). This
    // fails if the daemon is not running. No config/paths: the daemon owns the
    // engine, DB, and identity.
    final app = await tagnet.TagnetApp.attach();
    return TagnetSession(app: app, publicKey: null);
  }

  /// Resolve libtagnet_bridge.so for both run modes.
  ///
  /// frb's default loader derives a dev-only relative path from `rust_root`
  /// (../tagnet-bridge/target/release/) that does not exist for a Cargo
  /// *workspace* (which builds to the repo-root target/) nor for a bundled app.
  /// So load it explicitly:
  ///
  /// - Bundled release: the runner's CMake hook installs the .so into `lib/`
  ///   next to the executable (see app/linux/CMakeLists.txt).
  /// - `flutter run -d linux` (dev): the CWD is the Flutter project (app/) and
  ///   the workspace cdylib is at ../target/release/.
  static ExternalLibrary _loadBridge() {
    const soName = 'libtagnet_bridge.so';
    final candidates = <String>[
      // Bundled: <bundle>/lib/libtagnet_bridge.so
      '${File(Platform.resolvedExecutable).parent.path}/lib/$soName',
      // Dev (flutter run, CWD = app/): repo-root workspace target.
      '../target/release/$soName',
      // Dev fallback if run from repo root.
      'target/release/$soName',
    ];
    final found = candidates.firstWhere(
      (path) => File(path).existsSync(),
      orElse: () => soName, // last resort: let the dynamic loader search.
    );
    return ExternalLibrary.open(found);
  }
}

/// Factory referenced by the backend selector in main.dart.
TagnetBootstrap createBootstrap() => LinuxBootstrap();

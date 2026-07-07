// Shared entrypoint for BOTH the tagnet Android and Linux desktop apps.
//
// The two apps differ only in how they connect to the backend (Android starts
// an in-process engine; Linux attaches to a daemon over IPC). That difference
// is isolated in bootstrap/*_bootstrap.dart behind the TagnetBootstrap
// contract, and everything else — the whole UI — is shared (app.dart,
// screens/).
//
// The backend is chosen at build time with a compile-time define:
//
//   flutter run --dart-define=TAGNET_BACKEND=android   # (default)
//   flutter run --dart-define=TAGNET_BACKEND=linux -d linux
//
// The flake's run-android / run-linux apps pass the right value, so there is no
// longer a per-platform main.dart to swap in.

import 'package:flutter/material.dart';

import 'app.dart';
import 'bootstrap/bootstrap.dart';
import 'bootstrap/android_bootstrap.dart' as android;
import 'bootstrap/linux_bootstrap.dart' as linux;

/// Backend id baked in at build time; defaults to Android.
const String _backend = String.fromEnvironment(
  'TAGNET_BACKEND',
  defaultValue: 'android',
);

TagnetBootstrap _selectBootstrap() {
  switch (_backend) {
    case 'linux':
      return linux.createBootstrap();
    case 'android':
      return android.createBootstrap();
    default:
      throw StateError(
        'Unknown TAGNET_BACKEND "$_backend" '
        '(expected "android" or "linux").',
      );
  }
}

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  // RustLib.init() is done inside each bootstrap's connect(), because Linux
  // needs a custom library loader and Android uses the default.
  runApp(TagnetApp(bootstrap: _selectBootstrap()));
}

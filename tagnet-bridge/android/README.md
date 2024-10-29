# Building the tagnet POC for a phone

The Rust side is complete: `tagnet-bridge` is a `cdylib` that boots the sync
engine on a dedicated thread, generates the device identity on first launch,
and exposes the API to Dart. What does not exist in-repo yet is the Flutter app
tree — it must be generated with `flutter create`, which needs the Flutter
toolchain and a downloaded Android SDK. Both are provided by the dev shell
(`flake.nix`), so everything below runs inside `nix develop`.

The POC uses a hardcoded config (outbound-only, no peers, one app-private synced
dir) and renders a single line of text. See `../app-template/lib/main.dart`.

## The short way: `nix run`

Each README step is wrapped as a flake app (see `flake.nix`). All of them assume
you run from the repo root (override with `TAGNET_ROOT`), and pull in the Flutter
toolchain + Android SDK/NDK automatically.

```sh
nix run .#poc            # create-app -> codegen -> build-native -> run-app
```

Or run the steps individually:

| App                  | Does                                                       |
| -------------------- | ---------------------------------------------------------- |
| `nix run .#create-app`   | `flutter create` + `pub add` + sync the POC `main.dart`. Skips `flutter create` if `app/` exists. |
| `nix run .#codegen`      | `flutter_rust_bridge_codegen generate`.                |
| `nix run .#build-native` | cross-compile the `.so`(s) into `app/.../jniLibs`.     |
| `nix run .#run-app`      | `flutter run --release` on a connected device.         |

Native ABIs default to `arm64-v8a`; override with e.g.
`TAGNET_ANDROID_ABIS="arm64-v8a x86_64" nix run .#build-native`.

### One-time prerequisites

- Accept the Android licenses: `nix develop -c flutter doctor --android-licenses`.
- **Merge the Android glue** the first time (the scripts print a reminder but do
  not edit the generated project for you):
  - merge `manifest/AndroidManifest.xml` (permissions + `<service>`) into
    `app/android/app/src/main/AndroidManifest.xml`,
  - copy `service/TagnetService.kt` into
    `app/android/app/src/main/kotlin/<package>/` and set its `package`,
  - set `minSdkVersion 26` in `app/android/app/build.gradle`.
- Plug in a USB-debugging phone (or start an emulator); confirm with
  `nix develop -c flutter devices`.

You should see `tagnet running — 0 tag(s)`. Logs go to logcat under the tag
`tagnet` (`adb logcat -s tagnet`).

## The manual way

If you'd rather run the commands yourself, do them from inside `nix develop`:
`flutter create --platforms=android --project-name tagnet_app app`,
`(cd app && flutter pub add path_provider flutter_rust_bridge)`, merge the glue,
`cp tagnet-bridge/app-template/lib/main.dart app/lib/main.dart`,
`flutter_rust_bridge_codegen generate --config-file flutter_rust_bridge.yaml`,
`cargo ndk -t arm64-v8a -o app/android/app/src/main/jniLibs build --release -p tagnet-bridge --features generated`,
then `(cd app && flutter run --release)`.

## Notes / limitations of the POC

- Config is hardcoded in `main.dart`; there is no settings UI or peer entry yet.
- The foreground service (`TagnetService.kt`) is a stub that only holds the
  ongoing notification. For a real background lifecycle it must be started from
  the app; for a foreground POC you can skip starting it.
- Rebuild `.so`s (step 5) and rerun codegen (step 4) whenever `tagnet-bridge`'s
  Rust API changes.

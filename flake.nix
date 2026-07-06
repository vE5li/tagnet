{
  inputs = {
    flake-utils = {
      url = "github:numtide/flake-utils";
    };

    nixpkgs = {
      # Pinned to the exact revision the NixOS host runs
      # (vE5li/infrastructure flake.lock). The Flutter Linux runner uses GTK +
      # EGL and, at runtime, epoxy dlopen()s the system GPU driver from
      # /run/opengl-driver. That driver is built with the host's glibc/mesa, so
      # if this flake's nixpkgs diverges the two glibcs mismatch and EGL init
      # fails ("No provider of eglGetPlatformDisplayEXT found"). Matching the
      # host revision keeps a single glibc/mesa and lets the app use the normal
      # NixOS graphics path with no EGL wrapping. Bump this together with the
      # host when it updates.
      url = "github:NixOS/nixpkgs/9ae611a455b90cf061d8f332b977e387bda8e1ca";
    };

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
    };
  };

  outputs = {
    self,
    flake-utils,
    nixpkgs,
    rust-overlay,
  }:
    {
      overlays.default = final: prev: {
        tagnetd = final.callPackage ./tagnetd.nix {};
        tagnet = final.callPackage ./tagnet.nix {};
      };

      nixosModules.default = import ./module.nix self;
    }
    // flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = (import nixpkgs) {
          inherit system;
          overlays = [self.overlays.default (import rust-overlay)];
          config.android_sdk.accept_license = true;
          config.allowUnfree = true;
        };

        # Android SDK + NDK. The NDK cross-compiles the Rust core to Android
        # ABIs (rusqlite's bundled SQLite is built with the NDK C toolchain,
        # and cargo-ndk locates it via ANDROID_NDK_HOME / ANDROID_NDK_ROOT).
        # The SDK (platform-tools, build-tools, a platform, cmdline-tools) is
        # what the Flutter tool drives to build/install the app; Flutter finds
        # it via ANDROID_HOME / ANDROID_SDK_ROOT.
        # Pinned so the dev shell is reproducible. This MUST match the
        # `flutter.ndkVersion` baked into the Flutter release in this nixpkgs
        # (currently 28.2.13676358): plugin modules like `:jni` request that
        # exact version, and if it is not present at `ndk/<version>/` Gradle
        # tries to download one into the read-only Nix store and fails. Pinning
        # it here makes nixpkgs create `ndk/28.2.13676358/`, which Gradle finds.
        # Check `flutter.ndkVersion` if you bump Flutter.
        ndkVersion = "28.2.13676358";
        # Platform/build-tools match the Flutter release's defaults
        # (compileSdk/targetSdk 36, build-tools 35.0.0 for the R8 minify step).
        # As with the NDK, an unavailable version makes Gradle try to download
        # into the read-only store. Platform 35 is included as well because
        # Flutter plugin modules (e.g. jni_flutter, pulled in by
        # flutter_rust_bridge) pin their own compileSdk at 35. Bump these
        # together with Flutter.
        buildToolsVersion = "35.0.0";
        androidComposition = pkgs.androidenv.composeAndroidPackages {
          includeNDK = true;
          ndkVersions = [ndkVersion];
          platformVersions = ["36" "35"];
          buildToolsVersions = [buildToolsVersion];
          # A plugin's native build (via flutter_rust_bridge) requests this
          # exact CMake; provide it so Gradle doesn't try to download it.
          cmakeVersions = ["3.22.1"];
          cmdLineToolsVersion = "13.0";
        };
        androidSdkRoot = "${androidComposition.androidsdk}/libexec/android-sdk";
        # Canonical versioned NDK path (nixpkgs also exposes `ndk-bundle`, but
        # Gradle expects `ndk/<version>/`).
        androidNdkRoot = "${androidSdkRoot}/ndk/${ndkVersion}";

        # JDK for the Flutter Android (Gradle) build. Matches the Java 17
        # source/target compatibility in app/android/app/build.gradle.kts.
        jdk = pkgs.jdk17;

        # Tools every Android/Flutter step needs on PATH.
        androidTools = with pkgs; [
          (rust-bin.fromRustupToolchainFile ./rust-toolchain.toml)
          cargo-ndk
          flutter
          flutter_rust_bridge_codegen
          # flutter_rust_bridge_codegen shells out to `cargo expand`; provide it
          # so codegen is reproducible instead of auto-installing it at runtime.
          cargo-expand
          # Gradle (Flutter's Android build) needs a JDK.
          jdk
        ];

        # Tools the Flutter **Linux desktop** build needs (plan sections 6-7,
        # two-process topology). Unlike Android, `flutter build linux` drives a
        # CMake + Ninja + clang toolchain and links against GTK3; none of these
        # are pulled in by the Android tooling, so they must be listed
        # explicitly. `cargo build -p tagnet-bridge` (invoked from the Linux
        # runner's CMake hook) reuses the Rust toolchain already on PATH.
        linuxDesktopTools = with pkgs; [
          cmake
          ninja
          clang
          gtk3
          glib
          pcre2
          # `flutter build linux` links the runner against GTK3/GLib via
          # pkg-config; pkg-config itself is already in the dev shell's
          # nativeBuildInputs.
        ];

        # Environment shared by the dev shell and the `nix run` app scripts, so
        # a script produces the same build whether invoked directly or from a
        # `nix develop` prompt.
        androidEnv = {
          # cargo-ndk reads these to find the NDK clang/CC/AR toolchain.
          ANDROID_NDK_HOME = androidNdkRoot;
          ANDROID_NDK_ROOT = androidNdkRoot;
          # Flutter/Gradle locate the Android SDK through these.
          ANDROID_HOME = androidSdkRoot;
          ANDROID_SDK_ROOT = androidSdkRoot;
          # Gradle finds the JDK here (the `jdk` on PATH is not enough for
          # every Gradle invocation).
          JAVA_HOME = "${jdk}";
          # Flutter bundles its own Gradle-driven build; let it use the
          # Nix-provided AAPT2 instead of downloading one that won't run on
          # NixOS.
          GRADLE_OPTS = "-Dorg.gradle.project.android.aapt2FromMavenOverride=${androidSdkRoot}/build-tools/${buildToolsVersion}/aapt2";
        };

        # Turn a bash body into a `nix run`-able app, with the Android tools on
        # PATH and the shared env exported. `set -euo pipefail` and a `cd` to
        # the invoking directory's flake root are prepended.
        androidEnvExports =
          pkgs.lib.concatStringsSep "\n"
          (pkgs.lib.mapAttrsToList (name: value: "export ${name}=${pkgs.lib.escapeShellArg value}") androidEnv);

        mkApp = name: body: let
          script = pkgs.writeShellApplication {
            inherit name;
            # Both tool sets are on PATH: the Android steps ignore the desktop
            # tools and vice versa, but the Linux desktop apps (`run-linux`)
            # need CMake/Ninja/clang/GTK from `linuxDesktopTools`.
            runtimeInputs = androidTools ++ linuxDesktopTools;
            text = ''
              ${androidEnvExports}
              # Run from the repo root regardless of where `nix run` was invoked.
              cd "''${TAGNET_ROOT:-$PWD}"
              ${body}
            '';
          };
        in {
          type = "app";
          program = "${script}/bin/${name}";
        };

        # Steps 1-3 of tagnet-bridge/android/README.md: create the Flutter app
        # tree, add the Dart deps, and merge the Android glue. Skips if `app/`
        # already exists (idempotent).
        createAppBody = ''
          if [ -d app ]; then
            echo "app/ already exists; skipping 'flutter create'."
          else
            flutter create --platforms=android --project-name tagnet_app app
            ( cd app && flutter pub add path_provider flutter_rust_bridge )

            manifest=app/android/app/src/main/AndroidManifest.xml
            echo "NOTE: merge tagnet-bridge/android/manifest/AndroidManifest.xml"
            echo "      (permissions + <service>) into $manifest,"
            echo "      copy tagnet-bridge/android/service/TagnetService.kt into"
            echo "      app/android/app/src/main/kotlin/<package>/, and set"
            echo "      minSdkVersion 26 in app/android/app/build.gradle."
          fi

          # The POC entrypoint is always (re)synced from the template.
          cp tagnet-bridge/app-template/lib/main.dart app/lib/main.dart
        '';

        # Step 4: generate the Dart <-> Rust bindings.
        codegenBody = ''
          flutter_rust_bridge_codegen generate \
            --config-file flutter_rust_bridge.yaml
        '';

        # Step 5: cross-compile the native .so(s) into the app's jniLibs.
        # Override the ABIs with TAGNET_ANDROID_ABIS (space-separated).
        buildNativeBody = ''
          abis="''${TAGNET_ANDROID_ABIS:-arm64-v8a}"
          targets=()
          for abi in $abis; do targets+=("-t" "$abi"); done
          cargo ndk "''${targets[@]}" \
            -o app/android/app/src/main/jniLibs \
            build --release -p tagnet-bridge --features generated
        '';

        # Step 6: build/run on a connected device.
        runAppBody = ''
          ( cd app && flutter run --release )
        '';

        # Like run-app, but wipes the app's local data first by uninstalling the
        # existing package. `flutter run`/`flutter install -r` only *replace* the
        # APK and keep app-private storage (the DB *and* identity.key under
        # filesDir), so an explicit uninstall is the only way to start from a
        # clean slate. This regenerates the device identity (new public key) and
        # an empty database on next launch. The package id matches
        # app/android/app/build.gradle.kts (applicationId).
        runAppCleanBody = ''
          echo "Uninstalling com.example.tagnet_app to wipe local data..."
          # adb ships with the composed Android SDK's platform-tools; reference
          # it by absolute path rather than assuming it is on PATH. Don't fail if
          # the package isn't installed yet.
          "${androidSdkRoot}/platform-tools/adb" uninstall com.example.tagnet_app || true
          # Rebuild the native .so into jniLibs before running: a fresh install
          # (or a cleaned tree) has no bundled library, and `flutter run` alone
          # does not build it, so the app would crash with
          # "libtagnet_bridge.so not found".
          ${buildNativeBody}
          ( cd app && flutter run --release )
        '';

        # --- Linux desktop (two-process topology, plan sections 6-7) ---------

        # Add the Linux platform to the Flutter app tree and sync the Linux POC
        # entrypoint. Assumes `app/` already exists (created by create-app for
        # Android, or by `flutter create` directly). `flutter create` is
        # idempotent and only fills in the missing `app/linux/` runner.
        createLinuxAppBody = ''
          if [ ! -d app ]; then
            echo "app/ does not exist; run 'nix run .#create-app' first." >&2
            exit 1
          fi
          # Adds app/linux/ (the CMake + GTK runner) without touching existing
          # platforms. path_provider/flutter_rust_bridge already support Linux.
          ( cd app && flutter create --platforms=linux --project-name tagnet_app . )

          # The Linux POC entrypoint attaches to the running daemon over IPC
          # (it does NOT start its own engine, unlike the Android template).
          cp tagnet-bridge/app-template/lib/main_linux.dart app/lib/main.dart
        '';

        # Build the desktop daemon binary (the process that owns the DB and
        # serves the control socket). The user runs it separately; this just
        # produces the artifact for convenience.
        buildDaemonBody = ''
          cargo build --release -p tagnetd
          echo "Daemon built at target/release/tagnetd."
          echo "It serves the control socket at /run/tagnet/tagnet.sock."
        '';

        # Build/run the Flutter Linux desktop app. The native .so is built and
        # bundled by the runner's CMake hook (app/linux/CMakeLists.txt), so
        # there is no separate 'build-native' step here as there is for Android.
        # The daemon must already be running (it owns the control socket).
        runLinuxBody = ''
          ( cd app && flutter run -d linux )
        '';
      in {
        formatter = pkgs.alejandra;

        packages = rec {
          tagnetd = pkgs.tagnetd;
          tagnet = pkgs.tagnet;
          default = tagnet;
        };

        apps = {
          create-app = mkApp "tagnet-create-app" createAppBody;
          codegen = mkApp "tagnet-codegen" codegenBody;
          build-native = mkApp "tagnet-build-native" buildNativeBody;
          run-app = mkApp "tagnet-run-app" runAppBody;
          # Like run-app but uninstalls first to wipe local data (new identity +
          # empty DB). Use after a schema change or to reset a device.
          run-app-clean = mkApp "tagnet-run-app-clean" runAppCleanBody;
          # End-to-end: everything in sequence. Safe to re-run.
          poc = mkApp "tagnet-poc" ''
            ${createAppBody}
            ${codegenBody}
            ${buildNativeBody}
            ${runAppBody}
          '';

          # --- Linux desktop apps (two-process topology) ---------------------
          create-linux-app = mkApp "tagnet-create-linux-app" createLinuxAppBody;
          build-daemon = mkApp "tagnet-build-daemon" buildDaemonBody;
          run-linux = mkApp "tagnet-run-linux" runLinuxBody;
          # End-to-end desktop: add the linux platform, regenerate bindings,
          # then run against the already-running daemon. Safe to re-run.
          poc-linux = mkApp "tagnet-poc-linux" ''
            ${createLinuxAppBody}
            ${codegenBody}
            ${runLinuxBody}
          '';
        };

        devShell =
          pkgs.mkShell
          ({
              nativeBuildInputs =
                androidTools
                ++ linuxDesktopTools
                ++ (with pkgs; [pkg-config]);
              buildInputs = with pkgs; [
                openssl
              ];

              RUST_SRC_PATH = pkgs.rustPlatform.rustLibSrc;
            }
            // androidEnv);
      }
    );
}

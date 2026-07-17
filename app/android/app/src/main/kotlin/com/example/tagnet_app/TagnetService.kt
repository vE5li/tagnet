/*
 * Foreground service hosting the tagnet native runtime (portability plan
 * section 8).
 *
 * This is what keeps sync running after the Flutter UI is closed. The activity
 * and this service share one process; when the app is swiped away the Flutter
 * engine is destroyed but this foreground service (and its ongoing
 * notification) keeps the PROCESS alive, so the Rust runtime thread it started
 * over JNI keeps running.
 *
 * Ownership: this service owns the process-global runtime (crate::service via
 * the nativeStart/nativeStop JNI entry points). The Dart UI, when open, merely
 * attaches to the same global for reads/events.
 */

package com.example.tagnet_app

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Intent
import android.os.Build
import android.os.Environment
import android.os.IBinder
import android.util.Log

class TagnetService : Service() {

    companion object {
        private const val CHANNEL_ID = "tagnet_sync"
        private const val NOTIFICATION_ID = 1
        private const val TAG = "tagnet"

        init {
            // Loads libtagnet_bridge.so (bundled in jniLibs/<abi>/), which
            // exposes the nativeStart/nativeStop symbols below.
            System.loadLibrary("tagnet_bridge")
        }
    }

    /** Start the process-global runtime; returns this device's public key, or null on failure. */
    private external fun nativeStart(
        dataDir: String,
        identityFile: String,
        configJson: String,
    ): String?

    /** Stop the process-global runtime. */
    private external fun nativeStop()

    override fun onCreate() {
        super.onCreate()
        createNotificationChannel()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        // Promote to foreground immediately so the process (and the native
        // tokio runtime + inotify) survives the UI closing and Doze.
        startForeground(NOTIFICATION_ID, buildNotification())

        // App-private storage: inotify works here with no storage permission.
        // Identity + per-directory DBs live here (not user-browsable).
        val dataDir = filesDir.absolutePath
        val identityFile = "$dataDir/identity.key"

        // Shared external storage: Documents/tagnet is fully browsable in the
        // Files app / Gallery and survives uninstall. Writing here needs "All
        // files access" (MANAGE_EXTERNAL_STORAGE), which MainActivity gates on
        // before starting this service, so create_dir_all succeeds. Watcher
        // (inotify) reliability on shared storage varies by device (POC caveat).
        // Kept in sync with lib/bootstrap/android_bootstrap.dart.
        val documents =
            Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_DOCUMENTS)

        // Hardcoded POC config, kept in sync with lib/main.dart. The service is
        // the source of truth for the runtime; the Dart side attaches to it.
        val configJson = """
            {
              "sync_directories": [
                {
                  "path": "${documents.absolutePath}/phone",
                  "sync_type": { "TagBased": { "tags": ["467f35d7ae6f4dffb72905ff2bc743c5", "6450a8fe6eb945cc8b40adf4b97408bd"] } }
                },
                {
                  "path": "${documents.absolutePath}/audiobooks",
                  "sync_type": { "TagBased": { "tags": ["b053c022c8a6432eb88acb0452abceb2"] } }
                }
              ],
              "listen_port": null,
              "peers": [
                {
                  "address": ["192.168.188.10", 3468],
                  "name": "central",
                  "public_key": "AYWQNMCy5y20bjB1oxU79x5fUYQI4CGPbsqk7N+Qrgs="
                }
              ]
            }
        """.trimIndent()

        val publicKey = nativeStart(dataDir, identityFile, configJson)
        if (publicKey != null) {
            Log.i(TAG, "TagnetService started runtime; device public key $publicKey")
        } else {
            Log.e(TAG, "TagnetService: nativeStart failed")
        }

        // START_STICKY: if the OS kills us under memory pressure, restart so
        // sync resumes (the runtime re-scans + reconciles on reconnect).
        return START_STICKY
    }

    override fun onDestroy() {
        nativeStop()
        super.onDestroy()
    }

    // Not a bound service.
    override fun onBind(intent: Intent?): IBinder? = null

    private fun buildNotification(): Notification {
        val builder = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            Notification.Builder(this, CHANNEL_ID)
        } else {
            @Suppress("DEPRECATION")
            Notification.Builder(this)
        }
        return builder
            .setContentTitle("tagnet")
            .setContentText("Syncing in the background")
            .setOngoing(true)
            .setSmallIcon(android.R.drawable.stat_notify_sync)
            .build()
    }

    private fun createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel(
                CHANNEL_ID,
                "tagnet sync",
                NotificationManager.IMPORTANCE_LOW,
            )
            val manager = getSystemService(NotificationManager::class.java)
            manager.createNotificationChannel(channel)
        }
    }
}

package com.example.tagnet_app

import android.content.Intent
import android.os.Build
import android.os.Bundle
import io.flutter.embedding.android.FlutterActivity

class MainActivity : FlutterActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        // Start the foreground service that owns the native runtime, so sync
        // keeps running after this activity (and the Flutter engine) is closed.
        // Starting it is idempotent: the process-global runtime is only created
        // once (crate::service::start).
        val intent = Intent(this, TagnetService::class.java)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            startForegroundService(intent)
        } else {
            startService(intent)
        }
    }
}

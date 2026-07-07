// Shared application shell for both the Android and Linux apps.
//
// This is entirely platform-agnostic: it takes a [TagnetBootstrap] (chosen in
// main.dart via --dart-define) and drives the lifecycle that is identical on
// every platform — connect, subscribe to the change stream, and re-fetch the
// file/tag lists on each event. The actual pixels live in screens/.

import 'package:flutter/material.dart';

import 'bootstrap/bootstrap.dart';
import 'rust/api.dart' as tagnet;
import 'screens/home_screen.dart';

class TagnetApp extends StatefulWidget {
  const TagnetApp({super.key, required this.bootstrap});

  /// The platform backend (in-process engine on Android, daemon IPC on Linux).
  final TagnetBootstrap bootstrap;

  @override
  State<TagnetApp> createState() => _TagnetAppState();
}

// Shows feedback (SnackBars) from callbacks that can fire outside a build
// context (share-intent handlers, stream callbacks, cold-start).
final GlobalKey<ScaffoldMessengerState> _messengerKey =
    GlobalKey<ScaffoldMessengerState>();

class _TagnetAppState extends State<TagnetApp> {
  String _status = 'starting...';
  TagnetSession? _session;
  List<tagnet.FileEntry> _files = [];
  int _tagCount = 0;

  @override
  void initState() {
    super.initState();
    _boot();
  }

  Future<void> _boot() async {
    try {
      final session = await widget.bootstrap.connect();
      setState(() {
        _session = session;
        _status = 'connected';
      });

      // Wire any platform-only inputs (Android share sheet); no-op on Linux.
      widget.bootstrap.attachInputs(
        session,
        showMessage: _showMessage,
        onChanged: _refresh,
      );

      // Subscribe BEFORE the initial fetch. Peer connection and reconciliation
      // run asynchronously, so a change can land between fetch and subscribe
      // and be missed (the broadcast stream only delivers to subscribers
      // present when an event is sent). Subscribing first closes that race; the
      // initial _refresh() then captures whatever is already there.
      final events = await session.app.subscribe();
      await _refresh();

      while (mounted) {
        final event = await events.next();
        if (event == null) break; // stream closed (engine/daemon gone)
        await _refresh();
      }
    } catch (error) {
      setState(() => _status = 'failed: $error');
    }
  }

  Future<void> _refresh() async {
    final session = _session;
    if (session == null) return;
    final files = await session.app.listFileEntries();
    final tags = await session.app.listTagEntries();
    if (!mounted) return;
    setState(() {
      _files = files;
      _tagCount = tags.length;
    });
  }

  void _showMessage(String message) {
    _messengerKey.currentState
      ?..hideCurrentSnackBar()
      ..showSnackBar(
        SnackBar(content: Text(message), duration: const Duration(seconds: 2)),
      );
  }

  @override
  void dispose() {
    widget.bootstrap.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'tagnet',
      scaffoldMessengerKey: _messengerKey,
      home: HomeScreen(
        status: _status,
        session: _session,
        files: _files,
        tagCount: _tagCount,
        onRefresh: _session == null ? null : _refresh,
      ),
    );
  }
}

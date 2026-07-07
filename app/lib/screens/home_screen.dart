// Shared home screen: the status app bar, the (Android-only) public-key row,
// and the file list. Identical on every platform — the public-key row simply
// renders only when the session exposes a key (Android), and is absent on Linux
// where the daemon owns the identity. No platform imports here.
//
// It also hosts navigation to the Tags and Search screens (drawer) and the
// per-file detail screen (tap a row). Those are gated on a live session.

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

import '../bootstrap/bootstrap.dart';
import '../rust/api.dart' as tagnet;
import 'file_detail_screen.dart';
import 'search_screen.dart';
import 'tags_screen.dart';

class HomeScreen extends StatelessWidget {
  const HomeScreen({
    super.key,
    required this.status,
    required this.session,
    required this.files,
    required this.tagCount,
    required this.onRefresh,
  });

  final String status;
  final TagnetSession? session;
  final List<tagnet.FileEntry> files;
  final int tagCount;
  final VoidCallback? onRefresh;

  @override
  Widget build(BuildContext context) {
    final publicKey = session?.publicKey;
    final session_ = session;
    return Scaffold(
      drawer: session_ == null ? null : _NavDrawer(session: session_),
      appBar: AppBar(
        title: Text('$status — ${files.length} file(s), $tagCount tag(s)'),
        actions: [
          IconButton(
            icon: const Icon(Icons.refresh),
            onPressed: onRefresh,
          ),
        ],
      ),
      body: Column(
        children: [
          if (publicKey != null) _PublicKeyRow(publicKey: publicKey),
          if (publicKey != null) const Divider(height: 1),
          Expanded(child: _FileList(files: files, session: session_)),
        ],
      ),
    );
  }
}

class _NavDrawer extends StatelessWidget {
  const _NavDrawer({required this.session});

  final TagnetSession session;

  @override
  Widget build(BuildContext context) {
    return Drawer(
      child: ListView(
        children: [
          const DrawerHeader(
            child: Center(
              child: Text('tagnet', style: TextStyle(fontSize: 24)),
            ),
          ),
          ListTile(
            leading: const Icon(Icons.label_outline),
            title: const Text('Tags'),
            onTap: () {
              Navigator.pop(context);
              Navigator.push(
                context,
                MaterialPageRoute(
                  builder: (_) => TagsScreen(session: session),
                ),
              );
            },
          ),
          ListTile(
            leading: const Icon(Icons.search),
            title: const Text('Search by tag'),
            onTap: () {
              Navigator.pop(context);
              Navigator.push(
                context,
                MaterialPageRoute(
                  builder: (_) => SearchScreen(session: session),
                ),
              );
            },
          ),
        ],
      ),
    );
  }
}

/// Android-only: shows this device's public key with a copy button. Rendered by
/// [HomeScreen] only when the session carries a key.
class _PublicKeyRow extends StatelessWidget {
  const _PublicKeyRow({required this.publicKey});

  final String publicKey;

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.all(12),
      child: Row(
        children: [
          const Text('Key: '),
          Expanded(
            child: SelectableText(
              publicKey,
              maxLines: 1,
              style: const TextStyle(fontFamily: 'monospace'),
            ),
          ),
          IconButton(
            icon: const Icon(Icons.copy),
            tooltip: 'Copy public key',
            onPressed: () async {
              await Clipboard.setData(ClipboardData(text: publicKey));
              if (context.mounted) {
                ScaffoldMessenger.of(context).showSnackBar(
                  const SnackBar(
                    content: Text('Public key copied'),
                    duration: Duration(seconds: 1),
                  ),
                );
              }
            },
          ),
        ],
      ),
    );
  }
}

class _FileList extends StatelessWidget {
  const _FileList({required this.files, required this.session});

  final List<tagnet.FileEntry> files;
  final TagnetSession? session;

  @override
  Widget build(BuildContext context) {
    if (files.isEmpty) {
      return const Center(child: Text('No files yet'));
    }
    return ListView.builder(
      itemCount: files.length,
      itemBuilder: (context, index) {
        final file = files[index];
        final shortHash = file.contentHash.length > 12
            ? file.contentHash.substring(0, 12)
            : file.contentHash;
        final session_ = session;
        return ListTile(
          dense: true,
          title: Text(file.path),
          subtitle: Text(
            'v${file.versionNumber} · $shortHash',
            style: const TextStyle(fontFamily: 'monospace'),
          ),
          trailing: session_ == null ? null : const Icon(Icons.chevron_right),
          onTap: session_ == null
              ? null
              : () => Navigator.push(
                  context,
                  MaterialPageRoute(
                    builder: (_) => FileDetailScreen(
                      session: session_,
                      file: file,
                    ),
                  ),
                ),
        );
      },
    );
  }
}

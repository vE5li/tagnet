// File detail: show a file's fields, the tags applied to it, and let the user
// add/remove tags or delete the file. Drives tagIdsForFileString /
// tagFileByString / untagFileByString / deleteFileByString.

import 'package:flutter/material.dart';

import '../bootstrap/bootstrap.dart';
import '../rust/api.dart' as tagnet;
import '../tagnet_service.dart';
import '../widgets/tag_chip.dart';

class FileDetailScreen extends StatefulWidget {
  const FileDetailScreen({
    super.key,
    required this.session,
    required this.file,
  });

  final TagnetSession session;
  final tagnet.FileEntry file;

  @override
  State<FileDetailScreen> createState() => _FileDetailScreenState();
}

class _FileDetailScreenState extends State<FileDetailScreen> {
  /// All tags known to the engine, keyed by string id (for name/color lookup).
  Map<String, tagnet.TagEntry> _allTags = {};

  /// The string ids of tags currently applied to this file (direct only).
  List<String> _appliedTagIds = [];

  bool _loading = true;
  String? _error;

  tagnet.TagnetApp get _app => widget.session.app;
  tagnet.FileEntry get _file => widget.file;

  @override
  void initState() {
    super.initState();
    _load();
  }

  Future<void> _load() async {
    setState(() {
      _loading = true;
      _error = null;
    });
    try {
      final tags = await _app.listTagEntries();
      // Direct tags only (Exclude = no subtag recursion) — these are the ones
      // the user can meaningfully add/remove on this file.
      final applied = await _app.tagIdsForFileString(
        fileId: _file.fileId,
        subtagRule: tagnet.SubtagRule.exclude,
      );
      if (!mounted) return;
      setState(() {
        _allTags = {for (final t in tags) t.tagId: t};
        _appliedTagIds = applied;
        _loading = false;
      });
    } catch (error) {
      if (!mounted) return;
      setState(() {
        _error = '$error';
        _loading = false;
      });
    }
  }

  Future<void> _removeTag(String tagId) async {
    try {
      await _app.untagFileByString(tagId: tagId, fileId: _file.fileId);
      await _load();
    } catch (error) {
      _snack('Failed to remove tag: $error');
    }
  }

  Future<void> _addTag() async {
    final applied = _appliedTagIds.toSet();
    final available = _allTags.values
        .where((t) => !applied.contains(t.tagId))
        .toList();
    if (available.isEmpty) {
      _snack('No more tags to add. Create one on the Tags screen.');
      return;
    }
    final chosen = await showModalBottomSheet<tagnet.TagEntry>(
      context: context,
      builder: (_) => SafeArea(
        child: ListView(
          shrinkWrap: true,
          children: [
            const ListTile(title: Text('Add tag', style: TextStyle(fontWeight: FontWeight.bold))),
            for (final tag in available)
              ListTile(
                leading: TagColorSwatch(color: tag.color),
                title: Text(tag.name),
                onTap: () => Navigator.pop(context, tag),
              ),
          ],
        ),
      ),
    );
    if (chosen == null) return;
    try {
      await _app.tagFileByString(tagId: chosen.tagId, fileId: _file.fileId);
      await _load();
    } catch (error) {
      _snack('Failed to add tag: $error');
    }
  }

  Future<void> _deleteFile() async {
    final confirmed = await showDialog<bool>(
      context: context,
      builder: (_) => AlertDialog(
        title: const Text('Delete file?'),
        content: Text('Delete "${_file.path}"? This cannot be undone.'),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context, false),
            child: const Text('Cancel'),
          ),
          TextButton(
            onPressed: () => Navigator.pop(context, true),
            child: const Text('Delete'),
          ),
        ],
      ),
    );
    if (confirmed != true) return;
    try {
      await _app.deleteFileByString(_file.fileId);
      if (!mounted) return;
      Navigator.pop(context); // back to the list; the change stream refreshes it
    } catch (error) {
      _snack('Failed to delete file: $error');
    }
  }

  void _snack(String message) {
    if (!mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text(message)));
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text(_file.path, overflow: TextOverflow.ellipsis),
        actions: [
          IconButton(
            icon: const Icon(Icons.delete_outline),
            tooltip: 'Delete file',
            onPressed: _deleteFile,
          ),
        ],
      ),
      body: _buildBody(),
    );
  }

  Widget _buildBody() {
    if (_loading) return const Center(child: CircularProgressIndicator());
    if (_error != null) return Center(child: Text('Error: $_error'));

    return ListView(
      padding: const EdgeInsets.all(16),
      children: [
        _field('Path', _file.path),
        _field('File id', _file.fileId, mono: true),
        _field('Content hash', _file.contentHash, mono: true),
        _field('Version', 'v${_file.versionNumber}'),
        const SizedBox(height: 24),
        Row(
          children: [
            const Text('Tags', style: TextStyle(fontWeight: FontWeight.bold)),
            const Spacer(),
            TextButton.icon(
              icon: const Icon(Icons.add, size: 18),
              label: const Text('Add'),
              onPressed: _addTag,
            ),
          ],
        ),
        const SizedBox(height: 8),
        if (_appliedTagIds.isEmpty)
          const Text('No tags applied.')
        else
          Wrap(
            spacing: 8,
            runSpacing: 8,
            children: [
              for (final tagId in _appliedTagIds)
                _tagChipFor(tagId),
            ],
          ),
      ],
    );
  }

  Widget _tagChipFor(String tagId) {
    final tag = _allTags[tagId];
    if (tag == null) {
      // Applied tag not in the listing (e.g. race). Show the raw id.
      return Chip(label: Text(tagId, style: const TextStyle(fontFamily: 'monospace')));
    }
    return TagChip(tag: tag, onDeleted: () => _removeTag(tagId));
  }

  Widget _field(String label, String value, {bool mono = false}) {
    return Padding(
      padding: const EdgeInsets.only(bottom: 12),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(label, style: const TextStyle(fontSize: 12, color: Colors.grey)),
          const SizedBox(height: 2),
          SelectableText(
            value,
            style: mono ? const TextStyle(fontFamily: 'monospace') : null,
          ),
        ],
      ),
    );
  }
}

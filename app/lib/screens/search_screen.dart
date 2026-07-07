// Search / filter: pick a tag and list the files carrying it. The subtag rule
// toggle chooses between direct members (Exclude) and transitive membership
// through subtags (Include). Drives fileIdsForTagString + listFileEntries.

import 'package:flutter/material.dart';

import '../bootstrap/bootstrap.dart';
import '../rust/api.dart' as tagnet;
import '../widgets/tag_chip.dart';
import 'file_detail_screen.dart';

class SearchScreen extends StatefulWidget {
  const SearchScreen({super.key, required this.session});

  final TagnetSession session;

  @override
  State<SearchScreen> createState() => _SearchScreenState();
}

class _SearchScreenState extends State<SearchScreen> {
  List<tagnet.TagEntry> _tags = [];
  tagnet.TagEntry? _selected;
  bool _includeSubtags = true;

  /// Files matching the current filter (mapped from the returned id strings).
  List<tagnet.FileEntry> _results = [];
  bool _loadingTags = true;
  bool _searching = false;
  String? _error;

  tagnet.TagnetApp get _app => widget.session.app;

  @override
  void initState() {
    super.initState();
    _loadTags();
  }

  Future<void> _loadTags() async {
    try {
      final tags = await _app.listTagEntries();
      if (!mounted) return;
      setState(() {
        _tags = tags;
        _loadingTags = false;
      });
    } catch (error) {
      if (!mounted) return;
      setState(() {
        _error = '$error';
        _loadingTags = false;
      });
    }
  }

  Future<void> _runSearch() async {
    final tag = _selected;
    if (tag == null) return;
    setState(() {
      _searching = true;
      _error = null;
    });
    try {
      final matchingIds = await _app.fileIdsForTagString(
        tagId: tag.tagId,
        subtagRule: _includeSubtags
            ? tagnet.SubtagRule.include
            : tagnet.SubtagRule.exclude,
      );
      // Map the id strings back to FileEntry rows for display.
      final idSet = matchingIds.toSet();
      final allFiles = await _app.listFileEntries();
      final results = allFiles.where((f) => idSet.contains(f.fileId)).toList();
      if (!mounted) return;
      setState(() {
        _results = results;
        _searching = false;
      });
    } catch (error) {
      if (!mounted) return;
      setState(() {
        _error = '$error';
        _searching = false;
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('Search by tag')),
      body: Column(
        children: [
          _buildControls(),
          const Divider(height: 1),
          Expanded(child: _buildResults()),
        ],
      ),
    );
  }

  Widget _buildControls() {
    if (_loadingTags) {
      return const Padding(
        padding: EdgeInsets.all(16),
        child: Center(child: CircularProgressIndicator()),
      );
    }
    if (_tags.isEmpty) {
      return const Padding(
        padding: EdgeInsets.all(16),
        child: Text('No tags yet. Create one on the Tags screen first.'),
      );
    }
    return Padding(
      padding: const EdgeInsets.all(16),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          DropdownButtonFormField<tagnet.TagEntry>(
            initialValue: _selected,
            decoration: const InputDecoration(labelText: 'Tag'),
            items: [
              for (final tag in _tags)
                DropdownMenuItem(
                  value: tag,
                  child: Row(
                    children: [
                      TagColorSwatch(color: tag.color),
                      const SizedBox(width: 8),
                      Text(tag.name),
                    ],
                  ),
                ),
            ],
            onChanged: (tag) => setState(() => _selected = tag),
          ),
          const SizedBox(height: 8),
          SwitchListTile(
            contentPadding: EdgeInsets.zero,
            title: const Text('Include subtags'),
            subtitle: const Text('Also match files tagged via nested subtags'),
            value: _includeSubtags,
            onChanged: (v) => setState(() => _includeSubtags = v),
          ),
          const SizedBox(height: 8),
          SizedBox(
            width: double.infinity,
            child: FilledButton.icon(
              icon: const Icon(Icons.search),
              label: const Text('Search'),
              onPressed: _selected == null || _searching ? null : _runSearch,
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildResults() {
    if (_searching) return const Center(child: CircularProgressIndicator());
    if (_error != null) return Center(child: Text('Error: $_error'));
    if (_selected == null) {
      return const Center(child: Text('Pick a tag and search.'));
    }
    if (_results.isEmpty) {
      return const Center(child: Text('No files match this tag.'));
    }
    return ListView.builder(
      itemCount: _results.length,
      itemBuilder: (context, index) {
        final file = _results[index];
        final shortHash = file.contentHash.length > 12
            ? file.contentHash.substring(0, 12)
            : file.contentHash;
        return ListTile(
          dense: true,
          title: Text(file.path),
          subtitle: Text(
            'v${file.versionNumber} · $shortHash',
            style: const TextStyle(fontFamily: 'monospace'),
          ),
          onTap: () => Navigator.push(
            context,
            MaterialPageRoute(
              builder: (_) => FileDetailScreen(
                session: widget.session,
                file: file,
              ),
            ),
          ),
        );
      },
    );
  }
}

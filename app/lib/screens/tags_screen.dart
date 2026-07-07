// Tag management: list existing tags, create new ones (name + color), and
// delete them. Drives createTag / deleteTagByString / listTagEntries.

import 'package:flutter/material.dart';

import '../bootstrap/bootstrap.dart';
import '../rust/api.dart' as tagnet;
import '../tagnet_service.dart';
import '../widgets/tag_chip.dart';

class TagsScreen extends StatefulWidget {
  const TagsScreen({super.key, required this.session});

  final TagnetSession session;

  @override
  State<TagsScreen> createState() => _TagsScreenState();
}

class _TagsScreenState extends State<TagsScreen> {
  List<tagnet.TagEntry> _tags = [];
  bool _loading = true;
  String? _error;

  tagnet.TagnetApp get _app => widget.session.app;

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
      if (!mounted) return;
      setState(() {
        _tags = tags;
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

  Future<void> _createTag() async {
    final result = await showDialog<_NewTag>(
      context: context,
      builder: (_) => const _CreateTagDialog(),
    );
    if (result == null) return;
    try {
      await _app.createTag(name: result.name, color: result.color);
      await _load();
    } catch (error) {
      _snack('Failed to create tag: $error');
    }
  }

  Future<void> _deleteTag(tagnet.TagEntry tag) async {
    final confirmed = await showDialog<bool>(
      context: context,
      builder: (_) => AlertDialog(
        title: const Text('Delete tag?'),
        content: Text('Delete "${tag.name}"? This cannot be undone.'),
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
      await _app.deleteTagByString(tag.tagId);
      await _load();
    } catch (error) {
      _snack('Failed to delete tag: $error');
    }
  }

  void _snack(String message) {
    if (!mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text(message)));
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('Tags')),
      floatingActionButton: FloatingActionButton(
        onPressed: _createTag,
        tooltip: 'New tag',
        child: const Icon(Icons.add),
      ),
      body: _buildBody(),
    );
  }

  Widget _buildBody() {
    if (_loading) return const Center(child: CircularProgressIndicator());
    if (_error != null) {
      return Center(child: Text('Error: $_error'));
    }
    if (_tags.isEmpty) {
      return const Center(child: Text('No tags yet. Tap + to create one.'));
    }
    return ListView.separated(
      itemCount: _tags.length,
      separatorBuilder: (_, _) => const Divider(height: 1),
      itemBuilder: (context, index) {
        final tag = _tags[index];
        return ListTile(
          leading: TagColorSwatch(color: tag.color),
          title: Text(tag.name),
          subtitle: Text(
            tag.tagId,
            maxLines: 1,
            overflow: TextOverflow.ellipsis,
            style: const TextStyle(fontFamily: 'monospace', fontSize: 11),
          ),
          trailing: IconButton(
            icon: const Icon(Icons.delete_outline),
            tooltip: 'Delete',
            onPressed: () => _deleteTag(tag),
          ),
        );
      },
    );
  }
}

class _NewTag {
  final String name;
  final String color;
  const _NewTag(this.name, this.color);
}

class _CreateTagDialog extends StatefulWidget {
  const _CreateTagDialog();

  @override
  State<_CreateTagDialog> createState() => _CreateTagDialogState();
}

class _CreateTagDialogState extends State<_CreateTagDialog> {
  final _nameController = TextEditingController();
  String _color = kTagColorPalette.first;

  @override
  void dispose() {
    _nameController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      title: const Text('New tag'),
      content: Column(
        mainAxisSize: MainAxisSize.min,
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          TextField(
            controller: _nameController,
            autofocus: true,
            decoration: const InputDecoration(labelText: 'Name'),
            onSubmitted: (_) => _submit(),
          ),
          const SizedBox(height: 16),
          const Text('Color'),
          const SizedBox(height: 8),
          Wrap(
            spacing: 8,
            runSpacing: 8,
            children: [
              for (final color in kTagColorPalette)
                GestureDetector(
                  onTap: () => setState(() => _color = color),
                  child: TagColorSwatch(
                    color: color,
                    selected: color == _color,
                  ),
                ),
            ],
          ),
        ],
      ),
      actions: [
        TextButton(
          onPressed: () => Navigator.pop(context),
          child: const Text('Cancel'),
        ),
        TextButton(onPressed: _submit, child: const Text('Create')),
      ],
    );
  }

  void _submit() {
    final name = _nameController.text.trim();
    if (name.isEmpty) return;
    Navigator.pop(context, _NewTag(name, _color));
  }
}

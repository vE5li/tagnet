// Row for a labelled scalar property (name/color/id/path/...): a small label
// above the value, an optional trailing widget, and single-tap / long-press
// handlers so the whole row is one hit target for both edit (tap) and copy
// (long-press). Used by the tag detail and file detail screens to keep those
// two surfaces visually and behaviourally consistent.

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

class PropertyTile extends StatelessWidget {
  const PropertyTile({
    super.key,
    required this.label,
    required this.value,
    this.trailing,
    this.monospace = false,
    this.onTap,
  });

  final String label;
  final String value;
  final Widget? trailing;
  final bool monospace;
  final VoidCallback? onTap;

  Future<void> _copy() => Clipboard.setData(ClipboardData(text: value));

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final valueStyle = monospace
        ? const TextStyle(fontFamily: 'monospace')
        : theme.textTheme.bodyLarge;
    return InkWell(
      onTap: onTap,
      onLongPress: _copy,
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
        child: Row(
          crossAxisAlignment: CrossAxisAlignment.center,
          children: [
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    label,
                    style: theme.textTheme.labelMedium?.copyWith(
                      color: theme.colorScheme.onSurfaceVariant,
                      fontWeight: FontWeight.bold,
                    ),
                  ),
                  const SizedBox(height: 4),
                  Text(value, style: valueStyle),
                ],
              ),
            ),
            if (trailing != null) ...[
              const SizedBox(width: 12),
              trailing!,
            ],
          ],
        ),
      ),
    );
  }
}

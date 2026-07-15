// Minimal smoke test for the tagnet app shell.
//
// The real app boots a native backend (in-process engine on Android, daemon IPC
// on Linux) via flutter_rust_bridge in `initState`, which is not available in a
// plain widget test. So this uses a fake [TagnetBootstrap] whose `connect()`
// never completes, leaving the UI on its initial status; the test only checks
// that the widget tree constructs and shows that status.

import 'dart:async';

import 'package:flutter_test/flutter_test.dart';

import 'package:tagnet_app/app.dart';
import 'package:tagnet_app/bootstrap/bootstrap.dart';

class _PendingBootstrap extends TagnetBootstrap {
  @override
  Future<TagnetSession> connect() => Completer<TagnetSession>().future;
}

void main() {
  testWidgets('app renders initial status', (WidgetTester tester) async {
    await tester.pumpWidget(TagnetApp(bootstrap: _PendingBootstrap()));
    expect(find.text('tagnet'), findsOneWidget);
  });
}

// Minimal smoke test for the tagnet POC.
//
// The app boots the native Rust runtime (via flutter_rust_bridge) in
// `initState`, which is not available in a plain widget test, so this only
// checks that the widget tree constructs and shows the initial status text.

import 'package:flutter_test/flutter_test.dart';

import 'package:tagnet_app/main.dart';

void main() {
  testWidgets('POC renders initial status', (WidgetTester tester) async {
    await tester.pumpWidget(const TagnetPoc());
    expect(find.text('starting...'), findsOneWidget);
  });
}

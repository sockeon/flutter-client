// A minimal command-line example that connects to a Sockeon server, listens
// for events, joins a room and emits a message.
//
// Run a Sockeon PHP server first, then:
//   dart run example/sockeon_example.dart
import 'dart:async';

import 'package:sockeon/sockeon.dart';

Future<void> main() async {
  final client = SockeonClient(
    host: '127.0.0.1',
    port: 6001,
    path: '/',
    // authKey: 'your-secret-key', // only if the server enforces auth
  );

  // React to connection state changes.
  client.states.listen((state) {
    // ignore: avoid_print
    print('[state] $state');
  });

  // Surface transport/protocol errors.
  client.errors.listen((error) {
    // ignore: avoid_print
    print('[error] $error');
  });

  // App-specific events (whatever your server emits).
  client.on('welcome', (data) {
    // ignore: avoid_print
    print('[welcome] ${data['message']} (clientId: ${data['clientId']})');
  });

  client.on('message.new', (data) {
    // ignore: avoid_print
    print('[message.new] $data');
  });

  // Built-in error frames from the server.
  client.on('error', (data) {
    // ignore: avoid_print
    print('[server error] ${data['message']}');
  });

  await client.connect();

  // Join a room and emit an event into it.
  await client.joinRoom('general');
  client.emit('message.send', <String, dynamic>{'message': 'Hello from Dart!'});

  // Keep the process alive briefly to receive responses.
  await Future<void>.delayed(const Duration(seconds: 5));

  await client.leaveRoom('general');
  await client.dispose();
}

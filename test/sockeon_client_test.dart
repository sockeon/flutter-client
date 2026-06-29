@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:sockeon/sockeon.dart';
import 'package:test/test.dart';

/// A tiny in-process WebSocket server that mimics the Sockeon protocol enough
/// to exercise the client: it echoes `{event,data}` frames and acknowledges
/// `join_room`/`leave_room` like the framework's SystemRoomController.
class _FakeSockeonServer {
  late HttpServer _server;
  final List<WebSocket> _sockets = <WebSocket>[];

  int get port => _server.port;

  Future<void> start() async {
    _server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    _server.listen((HttpRequest request) async {
      if (WebSocketTransformer.isUpgradeRequest(request)) {
        final socket = await WebSocketTransformer.upgrade(request);
        _sockets.add(socket);
        // Greet the client like an app's OnConnect would.
        socket.add(jsonEncode({
          'event': 'welcome',
          'data': {'message': 'hello', 'clientId': 'sockeon_test_1'},
        }));
        socket.listen((dynamic raw) {
          final Map<String, dynamic> msg =
              jsonDecode(raw as String) as Map<String, dynamic>;
          final event = msg['event'] as String;
          final data = (msg['data'] as Map).cast<String, dynamic>();
          if (event == 'join_room') {
            socket.add(jsonEncode({
              'event': 'room_joined',
              'data': {'room': data['room'], 'namespace': data['namespace']},
            }));
          } else if (event == 'leave_room') {
            socket.add(jsonEncode({
              'event': 'room_left',
              'data': {'room': data['room'], 'namespace': data['namespace']},
            }));
          } else {
            // Echo back as `<event>.ack`.
            socket.add(jsonEncode({
              'event': '$event.ack',
              'data': data,
            }));
          }
        });
      } else {
        request.response.statusCode = HttpStatus.ok;
        await request.response.close();
      }
    });
  }

  Future<void> stop() async {
    for (final s in _sockets) {
      await s.close();
    }
    await _server.close(force: true);
  }
}

void main() {
  late _FakeSockeonServer server;

  setUp(() async {
    server = _FakeSockeonServer();
    await server.start();
  });

  tearDown(() async {
    await server.stop();
  });

  SockeonClient newClient() => SockeonClient(
        host: '127.0.0.1',
        port: server.port,
        reconnectOptions: SockeonReconnectOptions.disabled,
      );

  test('connects and transitions to connected state', () async {
    final client = newClient();
    await client.connect();
    expect(client.isConnected, isTrue);
    expect(client.state, SockeonConnectionState.connected);
    await client.dispose();
  });

  test('receives the welcome event with data', () async {
    final client = newClient();
    final welcome = Completer<Map<String, dynamic>>();
    client.on('welcome', welcome.complete);
    await client.connect();
    final data = await welcome.future.timeout(const Duration(seconds: 2));
    expect(data['clientId'], 'sockeon_test_1');
    await client.dispose();
  });

  test('emit + on round-trips through the server echo', () async {
    final client = newClient();
    final ack = Completer<Map<String, dynamic>>();
    client.on('message.send.ack', ack.complete);
    await client.connect();
    client.emit('message.send', {'message': 'ping'});
    final data = await ack.future.timeout(const Duration(seconds: 2));
    expect(data['message'], 'ping');
    await client.dispose();
  });

  test('joinRoom resolves on room_joined ack', () async {
    final client = newClient();
    await client.connect();
    await client.joinRoom('general').timeout(const Duration(seconds: 2));
    expect(client.joinedRooms, contains('general'));
    await client.leaveRoom('general').timeout(const Duration(seconds: 2));
    expect(client.joinedRooms, isNot(contains('general')));
    await client.dispose();
  });

  test('emit throws when not connected', () {
    final client = newClient();
    expect(
      () => client.emit('x', const {}),
      throwsA(isA<SockeonConnectionException>()),
    );
  });

  test('emit rejects invalid event names', () async {
    final client = newClient();
    await client.connect();
    expect(
      () => client.emit('bad name', const {}),
      throwsA(isA<SockeonProtocolException>()),
    );
    await client.dispose();
  });

  test('wildcard handler receives every event', () async {
    final client = newClient();
    final events = <Map<String, dynamic>>[];
    client.on('*', events.add);
    await client.connect();
    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(events, isNotEmpty); // at least the welcome frame
    await client.dispose();
  });

  test('subscription cancel removes only its handler', () async {
    final client = newClient();
    var aCount = 0;
    var bCount = 0;
    final subA = client.on('message.send.ack', (_) => aCount++);
    client.on('message.send.ack', (_) => bCount++);
    await client.connect();
    client.emit('message.send', const {});
    await Future<void>.delayed(const Duration(milliseconds: 100));
    subA.cancel();
    client.emit('message.send', const {});
    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(aCount, 1);
    expect(bCount, 2);
    await client.dispose();
  });

  test('failed initial connection throws SockeonConnectionException', () async {
    final client = SockeonClient(
      host: '127.0.0.1',
      port: 1, // nothing listening
      reconnectOptions: SockeonReconnectOptions.disabled,
    );
    await expectLater(
      client.connect(),
      throwsA(isA<SockeonConnectionException>()),
    );
    await client.dispose();
  });
}

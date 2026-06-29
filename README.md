# Sockeon — Dart & Flutter Client

A lightweight, dependency-light client for the [Sockeon](https://sockeon.com)
real-time framework. Sockeon speaks a tiny JSON-over-WebSocket protocol — every
frame is a JSON object shaped `{"event": "<name>", "data": { ... }}` — and this
package gives you an idiomatic, event-driven Dart API on top of it with
streams, futures and automatic reconnection.

Works in both pure Dart and Flutter (mobile, desktop and web) via
[`web_socket_channel`](https://pub.dev/packages/web_socket_channel).

## Features

- Connect over `ws://` or `wss://`, with optional auth key and query parameters.
- Event listeners: `on`, `once`, `off`, and a `*` wildcard.
- `emit(event, data)` with client-side event-name validation.
- Room helpers: `joinRoom` / `leaveRoom` that resolve on the server ack.
- Streams for connection state, all incoming messages and errors.
- Automatic reconnection with exponential backoff and optional room re-join.

## Installation

Add the dependency to your `pubspec.yaml`:

```yaml
dependencies:
  sockeon: ^1.0.0
```

Then run `dart pub get` (or `flutter pub get`).

## Quick start

```dart
import 'package:sockeon/sockeon.dart';

Future<void> main() async {
  final client = SockeonClient(
    host: '127.0.0.1',
    port: 6001,
    // authKey: 'secret', // only if the server enforces an auth key
  );

  // Listen for application events.
  client.on('welcome', (data) {
    print('Server says: ${data['message']}');
  });

  await client.connect();

  // Join a room and emit an event.
  await client.joinRoom('general');
  client.emit('message.send', {'message': 'Hello from Dart!'});
}
```

You can also build a client straight from a URL:

```dart
final client = SockeonClient.fromUrl('wss://example.com:6001/ws', authKey: 'secret');
```

## The protocol

Every message — in both directions — is a JSON text frame with exactly two
fields:

```json
{ "event": "message.send", "data": { "message": "hello" } }
```

Rules enforced by the server (and validated client-side before sending):

- `event` must be a non-empty string matching `^[a-zA-Z0-9._-]+$`.
- `data` is always an object. Pass `{}` (the default) when there is no payload.

There is no mandatory server greeting and the connection has no built-in
client ID — if your backend sends a `welcome` event with an ID, that is
application-specific. Keep-alive uses standard WebSocket ping/pong, which the
underlying socket handles for you.

## Receiving events

```dart
// Named listener; returns a cancellable subscription.
final sub = client.on('message.new', (data) {
  print('new message: ${data['text']}');
});
sub.cancel(); // remove just this listener

// One-shot listener.
client.once('ready', (data) => print('ready once'));

// Remove all listeners for an event.
client.off('message.new');

// Catch-all: receive the data of every incoming event.
client.on('*', (data) => print('any event: $data'));
```

Built-in server events you may want to handle:

| Event | `data` shape |
| --- | --- |
| `error` | `{ "message": String, "timestamp": int }` |
| `room_joined` | `{ "room": String, "namespace": String, "timestamp": int }` |
| `room_left` | `{ "room": String, "namespace": String, "timestamp": int }` |
| `rate_limit_exceeded` | `{ "error", "message", "original_event", "retry_after", "limit", "window", "type" }` |

## Rooms and namespaces

```dart
await client.joinRoom('general');                 // default namespace '/'
await client.joinRoom('chat', namespace: '/app');
await client.leaveRoom('general');

print(client.joinedRooms); // {'chat'}
```

`joinRoom` / `leaveRoom` emit `join_room` / `leave_room` and complete when the
server replies with `room_joined` / `room_left` (or after `ackTimeout`).

## Connection state, messages and errors

```dart
client.states.listen((state) => print('state: $state'));
client.messages.listen((msg) => print('${msg.event} -> ${msg.data}'));
client.errors.listen((err) => print('error: $err'));

print(client.isConnected);
print(client.state); // SockeonConnectionState.connected
```

## Reconnection

By default the client reconnects automatically with exponential backoff and
re-joins any rooms it had joined:

```dart
final client = SockeonClient(
  host: '127.0.0.1',
  port: 6001,
  reconnectOptions: const SockeonReconnectOptions(
    enabled: true,
    maxAttempts: 0,                 // 0 = unlimited
    initialDelay: Duration(seconds: 1),
    maxDelay: Duration(seconds: 30),
    backoffMultiplier: 2,
    rejoinRooms: true,
  ),
);

// Disable entirely:
// reconnectOptions: SockeonReconnectOptions.disabled,
```

Calling `disconnect()` stops reconnection; `connect()` re-enables it.

## Flutter usage

```dart
class ChatScreen extends StatefulWidget {
  const ChatScreen({super.key});
  @override
  State<ChatScreen> createState() => _ChatScreenState();
}

class _ChatScreenState extends State<ChatScreen> {
  late final SockeonClient _client;
  final _messages = <String>[];

  @override
  void initState() {
    super.initState();
    _client = SockeonClient(host: '10.0.2.2', port: 6001) // 10.0.2.2 = host from Android emulator
      ..on('message.new', (data) {
        setState(() => _messages.add(data['text'] as String));
      });
    _client.connect();
  }

  @override
  void dispose() {
    _client.dispose();
    super.dispose();
  }

  void _send(String text) => _client.emit('message.send', {'text': text});

  @override
  Widget build(BuildContext context) {
    return StreamBuilder<SockeonConnectionState>(
      stream: _client.states,
      initialData: _client.state,
      builder: (context, snapshot) {
        final connected = snapshot.data?.isConnected ?? false;
        return Scaffold(
          appBar: AppBar(title: Text(connected ? 'Online' : 'Connecting…')),
          body: ListView(children: [for (final m in _messages) ListTile(title: Text(m))]),
        );
      },
    );
  }
}
```

> On the Android emulator use `10.0.2.2` to reach a server running on your host
> machine. For `wss://` over the internet, set `secure: true` or use
> `SockeonClient.fromUrl('wss://...')`.

## API summary

| Member | Description |
| --- | --- |
| `SockeonClient({host, port, path, secure, authKey, queryParameters, reconnectOptions, protocols, ackTimeout})` | Create a client. |
| `SockeonClient.fromUrl(url, {...})` | Create a client from a `ws://`/`wss://` URL. |
| `connect()` | Open the connection (completes on handshake). |
| `disconnect({code, reason})` | Close gracefully and stop reconnecting. |
| `dispose()` | Release all resources permanently. |
| `emit(event, [data])` | Send an event. |
| `on(event, handler)` → `SockeonSubscription` | Add a listener. |
| `once(event, handler)` | Add a one-shot listener. |
| `off(event, [handler])` | Remove a listener (or all for an event). |
| `joinRoom(room, {namespace})` / `leaveRoom(...)` | Manage rooms. |
| `states` / `messages` / `errors` | Broadcast streams. |
| `state` / `isConnected` / `joinedRooms` | Current status getters. |

## License

MIT — see [LICENSE](LICENSE). Part of the [Sockeon](https://sockeon.com) project.

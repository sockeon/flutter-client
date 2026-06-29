import 'dart:async';

import 'package:web_socket_channel/web_socket_channel.dart';
import 'package:web_socket_channel/status.dart' as ws_status;

import 'sockeon_connection_state.dart';
import 'sockeon_exception.dart';
import 'sockeon_message.dart';
import 'sockeon_options.dart';

/// Signature for event handlers registered via [SockeonClient.on].
typedef SockeonEventHandler = void Function(Map<String, dynamic> data);

/// Handle returned by [SockeonClient.on] that can be used to cancel a single
/// event subscription without affecting other listeners for the same event.
class SockeonSubscription {
  SockeonSubscription._(this._cancel);

  final void Function() _cancel;
  bool _cancelled = false;

  /// Removes this listener. Safe to call multiple times.
  void cancel() {
    if (_cancelled) return;
    _cancelled = true;
    _cancel();
  }
}

/// A Dart/Flutter client for the [Sockeon](https://sockeon.github.io) framework.
///
/// Sockeon speaks a tiny JSON-over-WebSocket protocol: every frame is a JSON
/// object `{"event": "<name>", "data": { ... }}`. This client wraps a standard
/// WebSocket connection and exposes an event-based API that mirrors the PHP
/// client (`on`, `emit`, rooms, namespaces) while adding Dart niceties such as
/// streams, futures and automatic reconnection.
///
/// ```dart
/// final client = SockeonClient(host: '127.0.0.1', port: 6001);
/// client.on('welcome', (data) => print(data['message']));
/// await client.connect();
/// client.emit('message.send', {'message': 'hello'});
/// ```
class SockeonClient {
  /// Creates a client targeting `ws[s]://host:port/path`.
  ///
  /// Provide [authKey] if the server enforces an authentication key; it is sent
  /// as the `key` query parameter on the handshake URL. Extra
  /// [queryParameters] are appended to the handshake URL as-is.
  SockeonClient({
    required this.host,
    required this.port,
    this.path = '/',
    this.secure = false,
    this.authKey,
    Map<String, String>? queryParameters,
    this.reconnectOptions = const SockeonReconnectOptions(),
    this.protocols,
    this.ackTimeout = const Duration(seconds: 10),
  })  : queryParameters = Map<String, String>.unmodifiable(
            queryParameters ?? const <String, String>{}),
        _uri = _buildUri(
          host: host,
          port: port,
          path: path,
          secure: secure,
          authKey: authKey,
          queryParameters: queryParameters,
        );

  /// Creates a client from a fully-formed `ws://` or `wss://` URL.
  factory SockeonClient.fromUrl(
    String url, {
    String? authKey,
    Map<String, String>? queryParameters,
    SockeonReconnectOptions reconnectOptions = const SockeonReconnectOptions(),
    Iterable<String>? protocols,
    Duration ackTimeout = const Duration(seconds: 10),
  }) {
    final uri = Uri.parse(url);
    final scheme = uri.scheme.isEmpty ? 'ws' : uri.scheme;
    final secure = scheme == 'wss' || scheme == 'https';
    final defaultPort = secure ? 443 : 80;
    return SockeonClient(
      host: uri.host,
      port: uri.hasPort ? uri.port : defaultPort,
      path: uri.path.isEmpty ? '/' : uri.path,
      secure: secure,
      authKey: authKey,
      queryParameters: <String, String>{
        ...uri.queryParameters,
        if (queryParameters != null) ...queryParameters,
      },
      reconnectOptions: reconnectOptions,
      protocols: protocols,
      ackTimeout: ackTimeout,
    );
  }

  /// Server host name or IP address.
  final String host;

  /// Server TCP port.
  final int port;

  /// WebSocket endpoint path. Defaults to `/`.
  final String path;

  /// Whether to use a TLS (`wss://`) connection.
  final bool secure;

  /// Optional authentication key sent as the `key` query parameter.
  final String? authKey;

  /// Additional query parameters appended to the handshake URL.
  final Map<String, String> queryParameters;

  /// Reconnection behaviour.
  final SockeonReconnectOptions reconnectOptions;

  /// Optional WebSocket subprotocols.
  final Iterable<String>? protocols;

  /// How long [joinRoom]/[leaveRoom] wait for a server acknowledgement.
  final Duration ackTimeout;

  final Uri _uri;

  WebSocketChannel? _channel;
  StreamSubscription<dynamic>? _channelSub;

  final Map<String, List<SockeonEventHandler>> _handlers =
      <String, List<SockeonEventHandler>>{};
  final Set<_RoomKey> _joinedRooms = <_RoomKey>{};

  final StreamController<SockeonConnectionState> _stateController =
      StreamController<SockeonConnectionState>.broadcast();
  final StreamController<SockeonMessage> _messageController =
      StreamController<SockeonMessage>.broadcast();
  final StreamController<Object> _errorController =
      StreamController<Object>.broadcast();

  SockeonConnectionState _state = SockeonConnectionState.disconnected;
  Completer<void>? _connectCompleter;
  Timer? _reconnectTimer;
  int _reconnectAttempt = 0;
  bool _manualClose = false;
  bool _disposed = false;

  /// The resolved handshake URI (including auth key and query parameters).
  Uri get uri => _uri;

  /// The current connection state.
  SockeonConnectionState get state => _state;

  /// Whether the client currently has a live connection.
  bool get isConnected => _state == SockeonConnectionState.connected;

  /// Emits whenever the connection state changes.
  Stream<SockeonConnectionState> get states => _stateController.stream;

  /// Emits every decoded message received from the server, regardless of event.
  Stream<SockeonMessage> get messages => _messageController.stream;

  /// Emits transport and protocol errors as they occur.
  Stream<Object> get errors => _errorController.stream;

  /// Rooms currently considered joined (used for re-joining on reconnect).
  Set<String> get joinedRooms =>
      _joinedRooms.map((r) => r.room).toSet();

  // --- Connection lifecycle -------------------------------------------------

  /// Opens the WebSocket connection and completes once the handshake succeeds.
  ///
  /// Throws [SockeonConnectionException] if the connection cannot be
  /// established. Calling [connect] while already connected returns
  /// immediately.
  Future<void> connect() {
    if (_disposed) {
      throw const SockeonConnectionException('Client has been disposed');
    }
    if (_state == SockeonConnectionState.connected) {
      return Future<void>.value();
    }
    if (_connectCompleter != null) {
      return _connectCompleter!.future;
    }

    _manualClose = false;
    _reconnectAttempt = 0;
    return _open();
  }

  Future<void> _open() {
    _cancelReconnectTimer();
    final completer = Completer<void>();
    _connectCompleter = completer;
    _setState(_reconnectAttempt > 0
        ? SockeonConnectionState.reconnecting
        : SockeonConnectionState.connecting);

    try {
      final channel = WebSocketChannel.connect(_uri, protocols: protocols);
      _channel = channel;

      _channelSub = channel.stream.listen(
        _onData,
        onError: _onSocketError,
        onDone: _onSocketDone,
        cancelOnError: false,
      );

      // `ready` resolves once the underlying socket is connected. Some
      // platforms (e.g. browsers) report failures only through the stream's
      // onError, which is handled separately.
      channel.ready.then((_) {
        _reconnectAttempt = 0;
        _setState(SockeonConnectionState.connected);
        _rejoinRoomsIfNeeded();
        if (!completer.isCompleted) completer.complete();
        _connectCompleter = null;
      }).catchError((Object error, StackTrace stack) {
        _handleConnectFailure(error);
      });
    } catch (error) {
      _handleConnectFailure(error);
    }

    return completer.future;
  }

  void _handleConnectFailure(Object error) {
    final completer = _connectCompleter;
    _connectCompleter = null;
    _disposeChannel();

    if (completer != null && !completer.isCompleted) {
      // Initial connect: surface the failure to the caller.
      _setState(SockeonConnectionState.disconnected);
      completer.completeError(
        SockeonConnectionException('Failed to connect to $_uri', error),
      );
      return;
    }
    _errorController.add(error);
    _scheduleReconnect();
  }

  /// Closes the connection gracefully and disables reconnection.
  ///
  /// Pass a WebSocket close [code] and [reason] if desired.
  Future<void> disconnect({int code = ws_status.normalClosure, String? reason}) async {
    _manualClose = true;
    _cancelReconnectTimer();
    _setState(SockeonConnectionState.closing);
    final channel = _channel;
    try {
      await channel?.sink.close(code, reason);
    } catch (_) {
      // Ignore errors raised while closing an already-broken socket.
    }
    await _channelSub?.cancel();
    _channelSub = null;
    _channel = null;
    _setState(SockeonConnectionState.disconnected);
  }

  /// Permanently releases all resources. The client cannot be reused.
  Future<void> dispose() async {
    if (_disposed) return;
    _disposed = true;
    await disconnect();
    _handlers.clear();
    _joinedRooms.clear();
    await _stateController.close();
    await _messageController.close();
    await _errorController.close();
  }

  // --- Sending --------------------------------------------------------------

  /// Sends [event] with an optional [data] payload to the server.
  ///
  /// Throws [SockeonProtocolException] if [event] is not a valid event name and
  /// [SockeonConnectionException] if the client is not connected.
  void emit(String event, [Map<String, dynamic> data = const <String, dynamic>{}]) {
    if (!SockeonMessage.isValidEventName(event)) {
      throw SockeonProtocolException(
        'Invalid event name "$event": only letters, digits, dots, underscores '
        'and hyphens are allowed',
      );
    }
    _send(SockeonMessage(event, data));
  }

  void _send(SockeonMessage message) {
    final channel = _channel;
    if (channel == null || _state != SockeonConnectionState.connected) {
      throw const SockeonConnectionException(
        'Cannot send: client is not connected',
      );
    }
    channel.sink.add(message.encode());
  }

  // --- Receiving ------------------------------------------------------------

  /// Registers a [handler] for the given [event].
  ///
  /// Use the event name `*` to receive every message; the handler still
  /// receives only the `data` payload. Returns a [SockeonSubscription] that can
  /// be cancelled to remove just this handler.
  SockeonSubscription on(String event, SockeonEventHandler handler) {
    (_handlers[event] ??= <SockeonEventHandler>[]).add(handler);
    return SockeonSubscription._(() => off(event, handler));
  }

  /// Registers a one-shot [handler] for [event] that is removed after it fires
  /// once. Returns a subscription that can cancel it before it fires.
  SockeonSubscription once(String event, SockeonEventHandler handler) {
    late SockeonSubscription sub;
    void wrapper(Map<String, dynamic> data) {
      sub.cancel();
      handler(data);
    }

    sub = on(event, wrapper);
    return sub;
  }

  /// Removes a previously registered handler. If [handler] is `null`, all
  /// handlers for [event] are removed.
  void off(String event, [SockeonEventHandler? handler]) {
    if (handler == null) {
      _handlers.remove(event);
      return;
    }
    final list = _handlers[event];
    if (list == null) return;
    list.remove(handler);
    if (list.isEmpty) _handlers.remove(event);
  }

  void _onData(dynamic raw) {
    final String text;
    if (raw is String) {
      text = raw;
    } else if (raw is List<int>) {
      // Binary frames are also valid; the payload is still JSON.
      text = String.fromCharCodes(raw);
    } else {
      return;
    }

    SockeonMessage message;
    try {
      message = SockeonMessage.decode(text);
    } catch (error) {
      _errorController.add(SockeonProtocolException(
        'Failed to decode incoming frame',
        error,
      ));
      return;
    }

    if (!_messageController.isClosed) {
      _messageController.add(message);
    }
    _dispatch(message);
  }

  void _dispatch(SockeonMessage message) {
    _invokeHandlers(message.event, message.data);
    if (message.event != '*') {
      _invokeHandlers('*', message.data);
    }
  }

  void _invokeHandlers(String event, Map<String, dynamic> data) {
    final list = _handlers[event];
    if (list == null || list.isEmpty) return;
    // Copy so handlers can safely unsubscribe during iteration.
    for (final handler in List<SockeonEventHandler>.of(list)) {
      try {
        handler(data);
      } catch (error) {
        _errorController.add(error);
      }
    }
  }

  // --- Rooms ----------------------------------------------------------------

  /// Joins [room] within [namespace] and waits for the server's `room_joined`
  /// acknowledgement (or until [ackTimeout] elapses).
  Future<void> joinRoom(String room, {String namespace = '/'}) async {
    _joinedRooms.add(_RoomKey(room, namespace));
    await _roomRequest(
      emitEvent: 'join_room',
      ackEvent: 'room_joined',
      room: room,
      namespace: namespace,
    );
  }

  /// Leaves [room] within [namespace] and waits for the server's `room_left`
  /// acknowledgement (or until [ackTimeout] elapses).
  Future<void> leaveRoom(String room, {String namespace = '/'}) async {
    _joinedRooms.remove(_RoomKey(room, namespace));
    await _roomRequest(
      emitEvent: 'leave_room',
      ackEvent: 'room_left',
      room: room,
      namespace: namespace,
    );
  }

  Future<void> _roomRequest({
    required String emitEvent,
    required String ackEvent,
    required String room,
    required String namespace,
  }) {
    final completer = Completer<void>();
    late SockeonSubscription ackSub;
    Timer? timer;

    void finish([Object? error]) {
      if (completer.isCompleted) return;
      timer?.cancel();
      ackSub.cancel();
      if (error != null) {
        completer.completeError(error);
      } else {
        completer.complete();
      }
    }

    ackSub = on(ackEvent, (data) {
      if (data['room'] == room) finish();
    });

    timer = Timer(ackTimeout, () {
      finish(SockeonConnectionException(
        'Timed out waiting for "$ackEvent" acknowledgement for room "$room"',
      ));
    });

    try {
      emit(emitEvent, <String, dynamic>{'room': room, 'namespace': namespace});
    } catch (error) {
      finish(error);
    }

    return completer.future;
  }

  void _rejoinRoomsIfNeeded() {
    if (!reconnectOptions.rejoinRooms || _joinedRooms.isEmpty) return;
    for (final key in _joinedRooms) {
      try {
        emit('join_room',
            <String, dynamic>{'room': key.room, 'namespace': key.namespace});
      } catch (error) {
        _errorController.add(error);
      }
    }
  }

  // --- Internals ------------------------------------------------------------

  void _onSocketError(Object error, [StackTrace? stack]) {
    _errorController.add(error);
    final completer = _connectCompleter;
    if (completer != null && !completer.isCompleted) {
      _connectCompleter = null;
      _disposeChannel();
      _setState(SockeonConnectionState.disconnected);
      completer.completeError(
        SockeonConnectionException('Connection error', error),
      );
      return;
    }
    // Otherwise let onDone handle reconnection.
  }

  void _onSocketDone() {
    _disposeChannel();
    if (_manualClose || _disposed) {
      _setState(SockeonConnectionState.disconnected);
      return;
    }
    _scheduleReconnect();
  }

  void _scheduleReconnect() {
    if (_disposed || _manualClose || !reconnectOptions.enabled) {
      _setState(SockeonConnectionState.disconnected);
      return;
    }
    if (reconnectOptions.maxAttempts != 0 &&
        _reconnectAttempt >= reconnectOptions.maxAttempts) {
      _errorController.add(const SockeonConnectionException(
        'Maximum reconnection attempts reached',
      ));
      _setState(SockeonConnectionState.disconnected);
      return;
    }

    final delay = reconnectOptions.delayForAttempt(_reconnectAttempt);
    _reconnectAttempt++;
    _setState(SockeonConnectionState.reconnecting);
    _reconnectTimer = Timer(delay, () {
      if (_disposed || _manualClose) return;
      _open().catchError((Object error) {
        // _open already routes failures through _handleConnectFailure, which
        // re-schedules; swallow here to avoid unhandled errors.
      });
    });
  }

  void _cancelReconnectTimer() {
    _reconnectTimer?.cancel();
    _reconnectTimer = null;
  }

  void _disposeChannel() {
    _channelSub?.cancel();
    _channelSub = null;
    _channel = null;
  }

  void _setState(SockeonConnectionState next) {
    if (_state == next) return;
    _state = next;
    if (!_stateController.isClosed) {
      _stateController.add(next);
    }
  }

  static Uri _buildUri({
    required String host,
    required int port,
    required String path,
    required bool secure,
    required String? authKey,
    required Map<String, String>? queryParameters,
  }) {
    final query = <String, String>{
      if (queryParameters != null) ...queryParameters,
      if (authKey != null) 'key': authKey,
    };
    return Uri(
      scheme: secure ? 'wss' : 'ws',
      host: host,
      port: port,
      path: path.startsWith('/') ? path : '/$path',
      queryParameters: query.isEmpty ? null : query,
    );
  }
}

class _RoomKey {
  const _RoomKey(this.room, this.namespace);

  final String room;
  final String namespace;

  @override
  bool operator ==(Object other) =>
      other is _RoomKey && other.room == room && other.namespace == namespace;

  @override
  int get hashCode => Object.hash(room, namespace);
}

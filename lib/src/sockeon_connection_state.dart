/// Represents the lifecycle state of a [SockeonClient] connection.
enum SockeonConnectionState {
  /// Not connected and not attempting to connect.
  disconnected,

  /// Performing the initial WebSocket handshake.
  connecting,

  /// Connected and ready to send/receive events.
  connected,

  /// Connection was lost and the client is attempting to reconnect.
  reconnecting,

  /// A graceful shutdown was requested and is in progress.
  closing,
}

/// Convenience helpers for [SockeonConnectionState].
extension SockeonConnectionStateX on SockeonConnectionState {
  /// Whether the client currently has a live connection.
  bool get isConnected => this == SockeonConnectionState.connected;

  /// Whether the client is establishing or re-establishing a connection.
  bool get isConnecting =>
      this == SockeonConnectionState.connecting ||
      this == SockeonConnectionState.reconnecting;
}

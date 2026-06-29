/// Base class for all errors thrown by the Sockeon client.
class SockeonException implements Exception {
  /// Creates a Sockeon exception with a human readable [message].
  const SockeonException(this.message, [this.cause]);

  /// Description of what went wrong.
  final String message;

  /// The underlying error that triggered this exception, if any.
  final Object? cause;

  @override
  String toString() =>
      cause == null ? 'SockeonException: $message' : 'SockeonException: $message ($cause)';
}

/// Thrown when an operation requires an active connection but none exists.
class SockeonConnectionException extends SockeonException {
  /// Creates a connection exception.
  const SockeonConnectionException(super.message, [super.cause]);

  @override
  String toString() => 'SockeonConnectionException: $message'
      '${cause == null ? '' : ' ($cause)'}';
}

/// Thrown when a message fails client-side protocol validation before sending.
class SockeonProtocolException extends SockeonException {
  /// Creates a protocol exception.
  const SockeonProtocolException(super.message, [super.cause]);

  @override
  String toString() => 'SockeonProtocolException: $message'
      '${cause == null ? '' : ' ($cause)'}';
}

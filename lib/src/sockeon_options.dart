/// Controls automatic reconnection behaviour for a [SockeonClient].
class SockeonReconnectOptions {
  /// Creates reconnect options.
  const SockeonReconnectOptions({
    this.enabled = true,
    this.maxAttempts = 0,
    this.initialDelay = const Duration(seconds: 1),
    this.maxDelay = const Duration(seconds: 30),
    this.backoffMultiplier = 2.0,
    this.rejoinRooms = true,
  })  : assert(backoffMultiplier >= 1.0, 'backoffMultiplier must be >= 1.0'),
        assert(maxAttempts >= 0, 'maxAttempts must be >= 0');

  /// Whether the client should automatically reconnect when the connection
  /// drops unexpectedly.
  final bool enabled;

  /// Maximum number of consecutive reconnection attempts. `0` means unlimited.
  final int maxAttempts;

  /// Delay before the first reconnection attempt.
  final Duration initialDelay;

  /// Upper bound for the exponential backoff delay.
  final Duration maxDelay;

  /// Factor applied to the delay after each failed attempt.
  final double backoffMultiplier;

  /// Whether rooms joined via [SockeonClient.joinRoom] should be re-joined
  /// automatically after a successful reconnection.
  final bool rejoinRooms;

  /// Disabled reconnect preset.
  static const SockeonReconnectOptions disabled =
      SockeonReconnectOptions(enabled: false);

  /// Computes the backoff delay for a given zero-based [attempt] index.
  Duration delayForAttempt(int attempt) {
    final double factor = _pow(backoffMultiplier, attempt);
    final int millis = (initialDelay.inMilliseconds * factor).round();
    final int capped = millis.clamp(0, maxDelay.inMilliseconds);
    return Duration(milliseconds: capped);
  }

  static double _pow(double base, int exp) {
    var result = 1.0;
    for (var i = 0; i < exp; i++) {
      result *= base;
    }
    return result;
  }
}

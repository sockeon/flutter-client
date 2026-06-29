import 'dart:convert';

/// A single Sockeon protocol message.
///
/// Every frame exchanged with a Sockeon server is a JSON text frame shaped
/// exactly as `{"event": "<name>", "data": { ... }}`. This class models that
/// envelope and handles (de)serialization and validation.
class SockeonMessage {
  /// Creates a message with the given [event] name and [data] payload.
  const SockeonMessage(this.event, [this.data = const <String, dynamic>{}]);

  /// The event name. Must match [eventNamePattern].
  final String event;

  /// The event payload. Always a JSON object on the wire.
  final Map<String, dynamic> data;

  /// Allowed characters for an event name as enforced by the server:
  /// alphanumeric plus dot, underscore and hyphen.
  static final RegExp eventNamePattern = RegExp(r'^[a-zA-Z0-9._-]+$');

  /// Returns `true` if [event] is a valid Sockeon event name.
  static bool isValidEventName(String event) =>
      event.isNotEmpty && eventNamePattern.hasMatch(event);

  /// Parses a raw text frame into a [SockeonMessage].
  ///
  /// Throws [FormatException] if the frame is not a JSON object containing a
  /// string `event` field.
  factory SockeonMessage.decode(String raw) {
    final dynamic decoded = jsonDecode(raw);
    if (decoded is! Map<String, dynamic>) {
      throw const FormatException('Sockeon frame is not a JSON object');
    }
    final dynamic event = decoded['event'];
    if (event is! String) {
      throw const FormatException('Sockeon frame is missing a string "event"');
    }
    final dynamic data = decoded['data'];
    return SockeonMessage(
      event,
      data is Map<String, dynamic> ? data : const <String, dynamic>{},
    );
  }

  /// Serializes this message to a JSON string suitable for sending.
  String encode() => jsonEncode(<String, dynamic>{
        'event': event,
        'data': data,
      });

  @override
  String toString() => 'SockeonMessage(event: $event, data: $data)';
}

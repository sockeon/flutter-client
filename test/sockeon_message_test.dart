import 'package:sockeon/sockeon.dart';
import 'package:test/test.dart';

void main() {
  group('SockeonMessage', () {
    test('encodes to the {event, data} envelope', () {
      const msg = SockeonMessage('message.send', {'text': 'hi'});
      expect(msg.encode(), '{"event":"message.send","data":{"text":"hi"}}');
    });

    test('encodes empty data as {}', () {
      const msg = SockeonMessage('ping');
      expect(msg.encode(), '{"event":"ping","data":{}}');
    });

    test('decodes a valid frame', () {
      final msg = SockeonMessage.decode('{"event":"welcome","data":{"id":1}}');
      expect(msg.event, 'welcome');
      expect(msg.data['id'], 1);
    });

    test('decodes missing/invalid data to an empty map', () {
      final msg = SockeonMessage.decode('{"event":"x","data":"nope"}');
      expect(msg.data, isEmpty);
    });

    test('throws on non-object frames', () {
      expect(() => SockeonMessage.decode('[]'), throwsFormatException);
    });

    test('throws when event is missing', () {
      expect(() => SockeonMessage.decode('{"data":{}}'), throwsFormatException);
    });

    test('validates event names', () {
      expect(SockeonMessage.isValidEventName('room.join_1-a'), isTrue);
      expect(SockeonMessage.isValidEventName(''), isFalse);
      expect(SockeonMessage.isValidEventName('bad name'), isFalse);
      expect(SockeonMessage.isValidEventName('bad/name'), isFalse);
    });
  });

  group('SockeonReconnectOptions', () {
    test('applies exponential backoff capped at maxDelay', () {
      const opts = SockeonReconnectOptions(
        initialDelay: Duration(seconds: 1),
        maxDelay: Duration(seconds: 10),
        backoffMultiplier: 2,
      );
      expect(opts.delayForAttempt(0), const Duration(seconds: 1));
      expect(opts.delayForAttempt(1), const Duration(seconds: 2));
      expect(opts.delayForAttempt(2), const Duration(seconds: 4));
      expect(opts.delayForAttempt(3), const Duration(seconds: 8));
      // 16s would exceed the 10s cap.
      expect(opts.delayForAttempt(4), const Duration(seconds: 10));
    });
  });
}

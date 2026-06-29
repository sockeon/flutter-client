/// A Dart and Flutter client for the Sockeon real-time framework.
///
/// Connect to a Sockeon WebSocket server, listen for events, emit events and
/// manage rooms/namespaces with automatic reconnection.
library sockeon;

export 'src/sockeon_client.dart'
    show SockeonClient, SockeonEventHandler, SockeonSubscription;
export 'src/sockeon_connection_state.dart'
    show SockeonConnectionState, SockeonConnectionStateX;
export 'src/sockeon_exception.dart'
    show
        SockeonException,
        SockeonConnectionException,
        SockeonProtocolException;
export 'src/sockeon_message.dart' show SockeonMessage;
export 'src/sockeon_options.dart' show SockeonReconnectOptions;

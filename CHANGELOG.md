# Changelog

## 1.0.0

- Initial release of the Sockeon Dart/Flutter client.
- WebSocket connection with `ws`/`wss`, auth key and custom query parameters.
- Event API: `on`, `once`, `off`, `emit`, plus a `*` wildcard listener.
- Room helpers `joinRoom` / `leaveRoom` with server acknowledgement futures.
- Streams for connection state, all incoming messages and errors.
- Automatic reconnection with exponential backoff and optional room re-join.
